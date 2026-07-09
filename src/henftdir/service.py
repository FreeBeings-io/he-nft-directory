"""The he-nft-directory sync service.

Three independent loops, none of them a ledger:

- block_watcher (blockwatch.py): walks HE blocks directly (no Hive L1
  node needed), queueing accounts touched by nft/nftmarket txs for refresh.
- refresh_worker (sync.py): drains that queue, re-fetching each account's
  full current holdings straight from HE.
- periodic sweeps: the collection catalog and market books (cheap, full
  mirrors), and a slow safety-net re-touch of every known account (catches
  anything the block-watcher ever missed -- insurance, not the primary
  freshness path).

HE node pools are split three ways, each an isolated HENodes instance with
its own concurrency budget, rate limiter, and backoff state:

- block_watcher: its own pool. Found live: a single shared instance meant a
  bursty account refresh (up to ~150 concurrent per-symbol lookups) could
  knock every configured HE node into cooldown at once, taking the
  block-watcher's own single, otherwise-trivial getBlockInfo call down with
  it. Missing a block is the one failure this design can't route around
  later, so it must never share.
- market loop: its own pool. Found live: sharing with the account-refresh
  loops meant the market sweep was starved for minutes during the heavy
  account-refresh bursts (each account is ~150 per-symbol lookups; a
  safety-net batch or a queue drain saturates the pool), so market coverage
  crawled in. Its own budget keeps floors/last-sale fresh independent of
  refresh load.
- refresh_worker + catalog + safety-net: share one pool. These are the
  "bulk, some delay is fine" account-refresh loops; the catalog sweep is
  light and periodic, so it rides along without issue.

The read-through-cache shape is deliberate: work scales with actual usage
(accounts queried), never with collection size.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal

from . import blockwatch, config, db, sync
from .henodes import HENodes

logger = logging.getLogger(__name__)

# Always refreshed like any other account: a listed instance's current HE
# owner is 'nftmarket' (escrow), so its properties/display data only stay
# available if 'nftmarket' itself is a known, refreshed account.
_ALWAYS_KNOWN_ACCOUNTS = ("nftmarket",)


class Service:
    def __init__(
        self,
        app_dsn: str,
        *,
        catalog_interval: float = config.CATALOG_INTERVAL_SECONDS,
        market_interval: float = config.MARKET_INTERVAL_SECONDS,
        safety_net_interval: float = config.SAFETY_NET_INTERVAL_SECONDS,
    ):
        self.app_dsn = app_dsn
        self.catalog_interval = catalog_interval
        self.market_interval = market_interval
        self.safety_net_interval = safety_net_interval
        self.stop = asyncio.Event()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, lambda s=sig: (logger.info("received %s", s.name),
                                    self.stop.set()),
            )

    async def run(self) -> None:
        setup = await db.connect(self.app_dsn)
        await db.apply_schema(setup)
        for account in _ALWAYS_KNOWN_ACCOUNTS:
            await setup.execute(
                "INSERT INTO known_accounts (account) VALUES (%s) "
                "ON CONFLICT (account) DO NOTHING", (account,),
            )
        await setup.commit()
        await setup.close()

        async with HENodes() as watcher_nodes, HENodes() as market_nodes, \
                HENodes() as bulk_nodes:
            tasks = [
                asyncio.create_task(blockwatch.run(self.app_dsn, watcher_nodes, self.stop)),
                asyncio.create_task(self._refresh_worker(bulk_nodes)),
                asyncio.create_task(self._catalog_loop(bulk_nodes)),
                asyncio.create_task(self._market_loop(market_nodes)),
                asyncio.create_task(self._safety_net_loop(bulk_nodes)),
            ]
            stop_task = asyncio.create_task(self.stop.wait())
            try:
                done, _ = await asyncio.wait(
                    {*tasks, stop_task}, return_when=asyncio.FIRST_COMPLETED,
                )
                for task in tasks:
                    if task in done:
                        task.result()  # surface a crashed loop
            finally:
                self.stop.set()
                for task in (*tasks, stop_task):
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task
        logger.info("shutdown complete")

    async def _refresh_worker(self, nodes: HENodes) -> None:
        conn = await db.connect(self.app_dsn)
        try:
            await sync.refresh_worker(conn, nodes, self.stop)
        finally:
            await conn.close()

    async def _catalog_loop(self, nodes: HENodes) -> None:
        conn = await db.connect(self.app_dsn)
        try:
            while not self.stop.is_set():
                try:
                    n = await sync.refresh_catalog(conn, nodes)
                    logger.info("catalog refresh: %d collection(s)", n)
                except Exception as exc:
                    logger.error("catalog refresh failed: %r", exc)
                    # a failed statement poisons the connection for every
                    # later iteration until rolled back -- see sync.py's
                    # refresh_worker for the live crash this pattern caused
                    await conn.rollback()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), timeout=self.catalog_interval)
        finally:
            await conn.close()

    async def _market_loop(self, nodes: HENodes) -> None:
        conn = await db.connect(self.app_dsn)
        try:
            while not self.stop.is_set():
                try:
                    await sync.refresh_all_markets(conn, nodes)
                except Exception as exc:
                    logger.error("market refresh failed: %r", exc)
                    await conn.rollback()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), timeout=self.market_interval)
        finally:
            await conn.close()

    async def _safety_net_loop(self, nodes: HENodes) -> None:
        conn = await db.connect(self.app_dsn)
        try:
            while not self.stop.is_set():
                try:
                    n = await sync.safety_net_sweep(conn, nodes)
                    if n:
                        logger.info("safety-net sweep: re-touched %d account(s)", n)
                except Exception as exc:
                    logger.error("safety-net sweep failed: %r", exc)
                    await conn.rollback()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), timeout=self.safety_net_interval)
        finally:
            await conn.close()
