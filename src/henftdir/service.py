"""The he-nft-directory sync service.

Three independent loops, none of them a ledger:

- block_watcher (blockwatch.py): walks HE blocks directly (no Hive L1
  node needed), queueing accounts touched by nft/nftmarket txs for refresh.
- refresh_worker (sync.py): drains that queue, re-fetching each account's
  full current holdings straight from HE.
- periodic sweeps: the collection catalog and market books (cheap, full
  mirrors). There is deliberately NO full-fleet account sweep: failed
  lookups retry durably from the queue with backoff, and a stale account
  is re-fetched when someone reads it (api._ensure_known) -- correction
  scales with usage and failures, not fleet size.

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
  queue drain saturates the pool), so market coverage
  crawled in. Its own budget keeps floors/last-sale fresh independent of
  refresh load.
- refresh_worker + catalog: share one pool. These are the "bulk, some
  delay is fine" loops; the catalog sweep is light and periodic, so it
  rides along without issue. (The hourly safety-net sweep that used to
  share this pool was removed 2026-07-17 -- durable queue retries +
  read-staleness refreshes replaced it; see sync.py.)
- activity backfill: its own pool. It's the lowest-priority loop in the
  service (an advisory feed filling backward through old blocks), so it
  must never compete with anything above -- and nothing above should ever
  be slowed because the backfill is running.

Two more loops complete the activity feed: the live capture rides inside
the block-watcher (parse_block), and a daily prune keeps nft_events inside
the retention window.

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
    ):
        self.app_dsn = app_dsn
        self.catalog_interval = catalog_interval
        self.market_interval = market_interval
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
                HENodes() as bulk_nodes, HENodes() as activity_nodes:
            tasks = [
                asyncio.create_task(blockwatch.run(self.app_dsn, watcher_nodes, self.stop)),
                asyncio.create_task(self._refresh_worker(bulk_nodes)),
                asyncio.create_task(self._catalog_loop(bulk_nodes)),
                asyncio.create_task(self._market_loop(market_nodes)),
                asyncio.create_task(self._activity_backfill_loop(activity_nodes)),
                asyncio.create_task(self._activity_prune_loop()),
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

    async def _activity_backfill_loop(self, nodes: HENodes) -> None:
        """Fill the activity window BACKWARD from the deploy-time head
        toward (head - window). Low priority by design: small bursts with
        pauses, its own node pool, checkpointed in sync_state so restarts
        resume, and per-block failures are retried then skipped (missing
        one historical block from an advisory feed beats wedging). On
        completion it parks on stop.wait() -- returning would end the
        service's task group and shut everything down."""
        conn = await db.connect(self.app_dsn)
        try:
            state = {r["name"]: r["last_he_block"] for r in await (await conn.execute(
                "SELECT name, last_he_block FROM sync_state WHERE name IN "
                "('activity_backfill', 'activity_backfill_target')"
            )).fetchall()}
            cursor = state.get("activity_backfill")
            target = state.get("activity_backfill_target")
            if cursor is None or target is None:
                latest = await nodes.get_latest_block()
                head = latest["blockNumber"]
                target = max(1, head - config.ACTIVITY_WINDOW_DAYS * config.ACTIVITY_BLOCKS_PER_DAY)
                cursor = head - 1  # live watcher owns head onward
                await conn.execute(
                    "INSERT INTO sync_state (name, last_he_block) VALUES "
                    "('activity_backfill', %s), ('activity_backfill_target', %s) "
                    "ON CONFLICT (name) DO UPDATE SET "
                    "last_he_block = EXCLUDED.last_he_block, updated_at = now()",
                    (cursor, target),
                )
                await conn.commit()
                logger.info("activity backfill: %d -> %d (~%d blocks)",
                            cursor, target, cursor - target)
            while not self.stop.is_set() and cursor > target:
                try:
                    for _ in range(config.ACTIVITY_BACKFILL_BATCH):
                        if self.stop.is_set() or cursor <= target:
                            break
                        block = await nodes.get_block(cursor)
                        if block is not None:
                            await blockwatch.process_block_capture_only(conn, block)
                        cursor -= 1
                    await conn.execute(
                        "UPDATE sync_state SET last_he_block = %s, updated_at = now() "
                        "WHERE name = 'activity_backfill'", (cursor,),
                    )
                    await conn.commit()
                except Exception as exc:
                    logger.error("activity backfill: %r (at block %d)", exc, cursor)
                    await conn.rollback()
                    cursor -= 1  # skip a persistently-failing block; feed is advisory
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self.stop.wait(), timeout=config.ACTIVITY_BACKFILL_PAUSE_SECONDS)
            if cursor <= target:
                logger.info("activity backfill complete (window start block %d)", target)
            await self.stop.wait()
        finally:
            await conn.close()

    async def _activity_prune_loop(self) -> None:
        """Drop activity events older than the retention window. Runs once
        at startup then daily (founder call: relaxed cadence over constant
        delete churn -- a 30-day window overshoots by at most ~3%)."""
        conn = await db.connect(self.app_dsn)
        try:
            while not self.stop.is_set():
                try:
                    cur = await conn.execute(
                        "DELETE FROM nft_events WHERE ts < now() - interval '1 day' * %s",
                        (config.ACTIVITY_WINDOW_DAYS,),
                    )
                    await conn.commit()
                    if cur.rowcount:
                        logger.info("activity prune: dropped %d event(s)", cur.rowcount)
                except Exception as exc:
                    logger.error("activity prune failed: %r", exc)
                    await conn.rollback()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self.stop.wait(), timeout=config.ACTIVITY_PRUNE_INTERVAL_SECONDS)
        finally:
            await conn.close()

