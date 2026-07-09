"""HENodes: node rotation (spreads load instead of concentrating on one
node -- see config.py for the live-measured reasoning), failover, and
per-node backoff.

Node names are lowercase throughout: httpx normalizes URL hosts to
lowercase, so a mixed-case node name here would never match itself in a
request's observed URL."""
import asyncio
import time

import httpx

from henftdir import config
from henftdir.henodes import HENodes


def make_nodes(handler, nodes=None):
    n = HENodes(nodes or ["https://node-a", "https://node-b", "https://node-c"])
    n.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return n


def rpc_response(result):
    return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": result})


def test_rotated_nodes_advances_the_starting_point():
    nodes = HENodes(["a", "b", "c"])
    assert nodes._rotated_nodes() == ["a", "b", "c"]
    assert nodes._rotated_nodes() == ["b", "c", "a"]
    assert nodes._rotated_nodes() == ["c", "a", "b"]
    assert nodes._rotated_nodes() == ["a", "b", "c"]


async def test_call_spreads_across_nodes_when_all_healthy():
    hits = []

    def handler(request):
        hits.append(request.url.host)
        return rpc_response({"ok": True})

    nodes = make_nodes(handler)
    for _ in range(3):
        await nodes.call("blockchain", "getLatestBlockInfo", {})
    assert set(hits) == {"node-a", "node-b", "node-c"}  # all 3 used, not just one


async def test_call_fails_over_to_next_node_on_error():
    calls = []

    def handler(request):
        calls.append(request.url.host)
        if request.url.host == "node-a":
            return httpx.Response(503)
        return rpc_response({"ok": True})

    nodes = make_nodes(handler)
    result = await nodes.call("blockchain", "getLatestBlockInfo", {})
    assert result == {"ok": True}
    assert calls == ["node-a", "node-b"]  # node-a failed, node-b answered


async def test_failed_node_backs_off_and_is_skipped_next_call():
    calls = []

    def handler(request):
        calls.append(request.url.host)
        if request.url.host == "node-a":
            return httpx.Response(503)
        return rpc_response({"ok": True})

    nodes = make_nodes(handler)
    await nodes.call("blockchain", "getLatestBlockInfo", {})
    assert nodes._in_cooldown("https://node-a")

    calls.clear()
    await nodes.call("blockchain", "getLatestBlockInfo", {})
    assert "node-a" not in calls  # skipped -- still cooling down


# -- 503 (busy) vs transport failure (down) get different backoff floors -----

def test_status_error_gets_the_short_backoff_floor():
    """A 503 means the server answered but was briefly busy -- verified
    live during a real cold-fetch (a ~150-way concurrent burst pushed one
    node into 503s; the SAME query against the SAME table succeeded
    instantly once isolated from the burst). That must not cost the same
    30s(->60s) floor reserved for a genuine transport failure, or one
    momentary burst cascades into knocking out every configured node."""
    nodes = HENodes(["a", "b", "c"])
    nodes._record_failure("a", floor=config.HE_STATUS_FAILURE_BACKOFF_FLOOR_SECONDS)
    assert nodes._backoff["a"] == config.HE_STATUS_FAILURE_BACKOFF_FLOOR_SECONDS * 2


def test_transport_error_keeps_the_long_backoff_floor():
    nodes = HENodes(["a", "b", "c"])
    nodes._record_failure("a")  # no floor override -- transport-failure path
    assert nodes._backoff["a"] == config.HE_FAILURE_BACKOFF_FLOOR_SECONDS * 2


async def test_call_uses_short_backoff_for_a_503_status(monkeypatch):
    # single round: with one node and >1 round, the same node fails
    # repeatedly across rounds and compounds the backoff further --
    # irrelevant to what this test checks (which floor a 503 selects).
    monkeypatch.setattr(config, "HE_RETRY_ROUNDS", 1)

    def handler(request):
        return httpx.Response(503)

    nodes = make_nodes(handler, nodes=["https://node-a"])
    try:
        await nodes.call("blockchain", "getLatestBlockInfo", {})
    except Exception:
        pass
    assert nodes._backoff["https://node-a"] == config.HE_STATUS_FAILURE_BACKOFF_FLOOR_SECONDS * 2


async def test_call_uses_long_backoff_for_a_connection_error(monkeypatch):
    monkeypatch.setattr(config, "HE_RETRY_ROUNDS", 1)

    def handler(request):
        raise httpx.ConnectError("boom", request=request)

    nodes = make_nodes(handler, nodes=["https://node-a"])
    try:
        await nodes.call("blockchain", "getLatestBlockInfo", {})
    except Exception:
        pass
    assert nodes._backoff["https://node-a"] == config.HE_FAILURE_BACKOFF_FLOOR_SECONDS * 2


# -- the retry-round backoff sleep must not hold the concurrency slot --------

async def test_backoff_sleep_does_not_hold_the_semaphore():
    """A call() stuck retrying (all nodes failing, sleeping between rounds)
    must not block an unrelated concurrent call from using the freed-up
    semaphore slot -- found live: holding it through the sleep meant a
    struggling cold-fetch call starved fresh, would-succeed calls for the
    whole backoff duration instead of just the request time."""
    def slow_handler(request):
        return httpx.Response(503)  # always fails -- forces retry rounds

    fast_started = asyncio.Event()

    def fast_handler(request):
        fast_started.set()
        return rpc_response({"ok": True})

    nodes = HENodes(["https://only-node"])
    nodes.client = httpx.AsyncClient(transport=httpx.MockTransport(slow_handler))
    nodes._sem = asyncio.Semaphore(1)  # force real contention with 1 slot

    slow_task = asyncio.create_task(nodes.call("blockchain", "getLatestBlockInfo", {}))
    await asyncio.sleep(0.01)  # let the slow call fail its first round and start sleeping

    nodes2 = HENodes(["https://only-node"])
    nodes2.client = httpx.AsyncClient(transport=httpx.MockTransport(fast_handler))
    nodes2._sem = nodes._sem  # share the same (now-contended) semaphore

    fast_task = asyncio.create_task(nodes2.call("blockchain", "getLatestBlockInfo", {}))
    await asyncio.wait_for(fast_started.wait(), timeout=config.HE_RETRY_BACKOFF)
    # reaching here (before the slow call's own retry-backoff sleep even
    # elapses) proves the semaphore was released during that sleep
    await fast_task
    slow_task.cancel()


# -- per-node rate limiting must hold under real concurrency -----------------

async def test_throttle_serializes_concurrent_calls_to_the_same_node(monkeypatch):
    """Found live: an unguarded check-and-update let concurrent callers
    race past the spacing check with the same stale timestamp and all
    fire together, defeating the rate limit exactly when concurrency > 1
    -- the one condition it exists for. With the fix, concurrent
    _throttle() calls for the same node must come out properly spaced."""
    monkeypatch.setattr(config, "HE_MIN_CALL_SPACING_SECONDS", 0.05)
    nodes = HENodes(["a", "b", "c"])
    passed_at = []

    async def throttle_and_record():
        await nodes._throttle("a")
        passed_at.append(time.monotonic())

    await asyncio.gather(*(throttle_and_record() for _ in range(5)))
    passed_at.sort()
    gaps = [b - a for a, b in zip(passed_at, passed_at[1:])]
    # allow a little slack for scheduling jitter -- the point is they are
    # spaced out, not that they hit the exact interval to the millisecond
    assert all(gap >= config.HE_MIN_CALL_SPACING_SECONDS * 0.8 for gap in gaps)


async def test_throttle_does_not_serialize_across_different_nodes():
    """Per-node locks -- one node's spacing must not delay calls to a
    different, independent node (the whole point of rotating load across
    all configured nodes instead of a single shared budget)."""
    nodes = HENodes(["a", "b"])
    t0 = time.monotonic()
    await asyncio.gather(nodes._throttle("a"), nodes._throttle("b"))
    elapsed = time.monotonic() - t0
    assert elapsed < config.HE_MIN_CALL_SPACING_SECONDS  # both passed through immediately
