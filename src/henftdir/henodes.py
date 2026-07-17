"""Hive Engine RPC client: throttled, failover, JSON-RPC over the
`blockchain` and `contracts` endpoints.

Bounded concurrency, minimum spacing between calls, per-node cooldown after
a hard failure, full-list retry rounds with backoff -- the same posture
toward public HE nodes regardless of caller. There is no HE range API on
public nodes (getBlockRangeInfo is disabled), so block reads are one call
per block number; `getBlockInfo` is an O(1) point lookup regardless of
depth (verified live across genesis-era through current blocks), which is
what makes a direct block-walker (blockwatch.py) viable without any
intermediary service. There is no cross-node outcome confirmation
here: this client only ever mirrors HE's own current, authoritative state
(find/getBlockInfo), never derives or trusts a specific transaction's
logged outcome, so there is nothing to confirm against disagreement.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from typing import Any

import httpx

from . import config

logger = logging.getLogger(__name__)

_request_ids = itertools.count(1)


class HeRpcError(Exception):
    """The node answered with a JSON-RPC error object."""


class HeNodeError(Exception):
    """Transport-level failure across all nodes."""


def _parse_retry_after(response: httpx.Response) -> float | None:
    """Seconds from a Retry-After header, or None. Only the delta-seconds
    form is handled; the HTTP-date form (rare on these nodes) is ignored
    rather than parsed against a possibly-skewed local clock."""
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


class HENodes:
    def __init__(self, nodes: list[str] | None = None):
        self.nodes = list(nodes or config.HE_NODES)
        self.client: httpx.AsyncClient | None = None
        self._sem = asyncio.Semaphore(config.HE_MAX_CONCURRENCY)
        # HE_MIN_CALL_SPACING_SECONDS is enforced per node, not globally --
        # each public node is an independently operated service, so a
        # single shared cap would bottleneck the whole pool at one node's
        # budget even when the other two have room (defeating the point of
        # rotating across all three). Serialized via a per-node lock:
        # found live that an unguarded check-and-update let concurrent
        # callers race past the spacing check with the same stale
        # timestamp and all fire together -- a rate limit that isn't
        # concurrency-safe doesn't hold under the one condition
        # (concurrency > 1) it exists for.
        self._last_call: dict[str, float] = {}
        self._rate_locks: dict[str, asyncio.Lock] = {
            node: asyncio.Lock() for node in self.nodes
        }
        # Rotates which node each call() tries first. Without this, failover
        # (try nodes[0], only fall through on failure) means every
        # concurrent call prefers the same node while it's healthy --
        # measured live: a single public HE node starts returning 503s
        # (not 429; no Retry-After header at all) between 10 and 12
        # concurrent requests. Rotating spreads load across all configured
        # nodes instead of concentrating it on one while the others idle.
        self._rotation = itertools.count()
        # AIMD backoff per node: a
        # fixed cooldown re-hammers a persistently-degraded node every
        # window forever. _backoff is the current learned duration (doubles
        # per failure, capped, decays 0.95x per success); _cooldown_until
        # is the computed expiry derived from it.
        self._backoff: dict[str, float] = {}
        self._cooldown_until: dict[str, float] = {}

    async def __aenter__(self) -> "HENodes":
        self.client = httpx.AsyncClient(
            headers={"Content-Type": "application/json"},
            limits=httpx.Limits(max_connections=config.HE_MAX_CONCURRENCY * 2),
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self.client:
            await self.client.aclose()

    def _rotated_nodes(self) -> list[str]:
        start = next(self._rotation) % len(self.nodes)
        return self.nodes[start:] + self.nodes[:start]

    async def _throttle(self, node: str) -> None:
        """Block until this specific node has gone HE_MIN_CALL_SPACING_
        SECONDS since its last request. The lock makes the check-and-
        update atomic across concurrent callers targeting the same node --
        without it, several coroutines can read the same stale
        _last_call, compute the same wait, sleep the same amount, and
        fire together."""
        async with self._rate_locks[node]:
            now = time.monotonic()
            wait = self._last_call.get(node, 0.0) + config.HE_MIN_CALL_SPACING_SECONDS - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call[node] = time.monotonic()

    async def call(self, endpoint: str, method: str, params: dict) -> Any:
        """JSON-RPC call with failover over the node list, starting from a
        rotating offset each call (see __init__) so concurrent calls spread
        across all configured nodes instead of concentrating on one.

        endpoint: 'blockchain' | 'contracts'. RpcErrors raise immediately
        (bad request; retrying elsewhere wastes capacity); transport errors
        fail over, then back off and retry the whole list. The semaphore is
        only held while a node is actually being tried, not across the
        inter-round backoff sleep -- found live: a cold-fetch's ~150-way
        concurrent burst holds every semaphore slot through a multi-second
        sleep while struggling, starving fresh calls that would otherwise
        proceed immediately on a healthy node.
        """
        assert self.client is not None
        payload = {"jsonrpc": "2.0", "method": method, "params": params,
                   "id": next(_request_ids)}
        last_cause = "no nodes"
        for round_num in range(config.HE_RETRY_ROUNDS):
            if round_num > 0:
                delay = config.HE_RETRY_BACKOFF * 2 ** (round_num - 1)
                logger.warning(
                    "all HE nodes failed for %s (round %d); retrying in %.0fs",
                    method, round_num, delay,
                )
                await asyncio.sleep(delay)
            async with self._sem:
                for node in self._rotated_nodes():
                    if self._in_cooldown(node):
                        continue
                    await self._throttle(node)
                    try:
                        resp = await self.client.post(
                            f"{node}/{endpoint}", json=payload,
                            timeout=config.HE_REQUEST_TIMEOUT_SECONDS,
                        )
                        resp.raise_for_status()
                        body = resp.json()
                    except httpx.HTTPStatusError as exc:
                        # The server answered (unlike a transport failure
                        # below) but with an error status -- verified live:
                        # a 503 here means "briefly busy under a concurrent
                        # burst," not "node is down" (the same query against
                        # the same table succeeds instantly in isolation).
                        # A short backoff lets rotation route around it
                        # without a big cold-fetch cascading every node into
                        # the same cooldown reserved for real outages.
                        # A 429 is the opposite case: the node is telling
                        # this client it's over budget, so the 3s floor
                        # just re-earns the same 429 -- park the node on
                        # the outage-grade floor instead, or at least as
                        # long as its Retry-After asks (capped; AIMD
                        # doubling in _record_failure still applies).
                        if exc.response.status_code == 429:
                            floor = config.HE_RATE_LIMIT_BACKOFF_FLOOR_SECONDS
                            retry_after = _parse_retry_after(exc.response)
                            if retry_after is not None:
                                floor = min(
                                    max(floor, retry_after),
                                    config.HE_RETRY_AFTER_CAP_SECONDS,
                                )
                        else:
                            floor = config.HE_STATUS_FAILURE_BACKOFF_FLOOR_SECONDS
                        self._record_failure(node, floor=floor)
                        last_cause = f"{node}: {exc!r}"
                        logger.debug("HE failover: %s", last_cause)
                        continue
                    except (httpx.HTTPError, ValueError) as exc:
                        self._record_failure(node)
                        last_cause = f"{node}: {exc!r}"
                        logger.debug("HE failover: %s", last_cause)
                        continue
                    if "error" in body:
                        raise HeRpcError(f"{node} {method}: {body['error']}")
                    self._record_success(node)
                    return body.get("result")
        raise HeNodeError(f"{method}: all HE nodes failed ({last_cause})")

    def _record_failure(self, node: str, floor: float | None = None) -> None:
        floor = config.HE_FAILURE_BACKOFF_FLOOR_SECONDS if floor is None else floor
        backoff = min(self._backoff.get(node, floor) * 2, config.HE_FAILURE_BACKOFF_CAP_SECONDS)
        self._backoff[node] = backoff
        self._cooldown_until[node] = time.monotonic() + backoff
        logger.debug("HE node %s backoff now %.0fs", node, backoff)

    def _record_success(self, node: str) -> None:
        if node in self._backoff:
            self._backoff[node] = max(
                config.HE_FAILURE_BACKOFF_FLOOR_SECONDS,
                self._backoff[node] * config.HE_FAILURE_BACKOFF_DECAY,
            )

    def _in_cooldown(self, node: str) -> bool:
        return time.monotonic() < self._cooldown_until.get(node, -1.0)

    # -- blockchain endpoint ---------------------------------------------------

    async def get_latest_block(self) -> dict:
        return await self.call("blockchain", "getLatestBlockInfo", {})

    async def get_block(self, he_block: int) -> dict | None:
        return await self.call(
            "blockchain", "getBlockInfo", {"blockNumber": he_block}
        )

    # -- contracts endpoint ------------------------------------------------

    async def find(
        self, contract: str, table: str, query: dict,
        limit: int = 1000, offset: int = 0, indexes: list | None = None,
    ) -> list[dict]:
        params: dict = {"contract": contract, "table": table, "query": query,
                        "limit": limit, "offset": offset}
        if indexes:
            params["indexes"] = indexes
        return await self.call("contracts", "find", params) or []

    async def find_one(self, contract: str, table: str, query: dict) -> dict | None:
        return await self.call(
            "contracts", "findOne",
            {"contract": contract, "table": table, "query": query},
        )
