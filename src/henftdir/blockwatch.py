"""The block-watcher: a doorbell, not a ledger.

Walks HE blocks directly by number (no Hive L1 node needed -- HE's own
`getBlockInfo` is a self-contained, O(1) point lookup carrying every
transaction's contract/action/payload/logs inline; verified live across
genesis-era through current blocks). For each
transaction that touches NFTs (targets nft/nftmarket directly, OR emits
nft/nftmarket events from inside another contract -- pack openings etc.;
see catalog.has_nft_activity), it queues every account
plausibly touched (catalog.touched_accounts) for a refresh -- it never
interprets *what* happened, only *who* to re-fetch. sync.py's refresh
worker is what turns a queued account into current, correct state, by
reading directly from HE, not from anything this module inferred.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timezone

import psycopg

from . import catalog, config, db
from .henodes import HENodes

logger = logging.getLogger(__name__)


def _block_ts(value) -> datetime:
    """HE block `timestamp` is an ISO string, usually UTC without an offset
    (e.g. '2020-03-25T14:42:30'). Treat a naive value as UTC; fall back to
    now() if it's missing or unparseable (a sale with a slightly-off ts is
    far better than dropping the sale)."""
    if isinstance(value, str) and value:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


async def queue_refresh(
    conn: psycopg.AsyncConnection, pairs: set[tuple[str, str]]
) -> None:
    """Queue (account, symbol) refresh work. symbol '' = full-account
    refresh; a non-empty symbol is a targeted single-collection re-check
    (see the refresh_queue schema comment for why that distinction is the
    difference between draining and stalling at scale)."""
    if not pairs:
        return
    accounts, symbols = zip(*pairs)
    # Only queue touches for accounts we already TRACK (someone queried them
    # at least once). The cache is a read-through over accounts people ask
    # about -- it does not discover new accounts from chain activity, so its
    # size scales with usage, not with how busy Hive Engine is. A brand-new
    # account is populated by the API cold-fetch, which marks it tracked
    # BEFORE its scan, so any touch landing during that scan still matches
    # this filter and is not lost.
    await conn.execute(
        "INSERT INTO refresh_queue (account, symbol) "
        "SELECT a, s FROM unnest(%s::text[], %s::text[]) AS t(a, s) "
        "WHERE EXISTS (SELECT 1 FROM known_accounts k WHERE k.account = t.a) "
        "ON CONFLICT (account, symbol) DO UPDATE SET queued_at = now()",
        (list(accounts), list(symbols)),
    )


async def queue_market_refresh(
    conn: psycopg.AsyncConnection, symbols: set[str]
) -> None:
    """Mark symbols dirty for the market loop after a LIVE market event
    (list/cancel/price-change/buy). The loop refreshes only these instead
    of blindly re-polling every cached symbol's sellBook -- market work
    then scales with trading activity, not symbol count."""
    if not symbols:
        return
    await conn.execute(
        "INSERT INTO market_refresh_queue (symbol) "
        "SELECT unnest(%s::text[]) ON CONFLICT (symbol) DO UPDATE SET queued_at = now()",
        (list(symbols),),
    )


async def record_sales(conn: psycopg.AsyncConnection, sales: list[dict]) -> None:
    """Append completed trades to market_sales (forward-only valuation log).
    Idempotent on (symbol, nft_id, he_block) so reprocessing a block on
    restart never double-counts."""
    if not sales:
        return
    async with conn.cursor() as cur:
        await cur.executemany(
            "INSERT INTO market_sales (symbol, nft_id, price, price_symbol, "
            "seller, buyer, he_block, ts) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (symbol, nft_id, he_block) DO NOTHING",
            [
                (s["symbol"], s["nft_id"], s["price"], s["price_symbol"],
                 s["seller"], s["buyer"], s["he_block"], s["ts"])
                for s in sales
            ],
        )


async def record_events(conn: psycopg.AsyncConnection, events: list[dict]) -> None:
    """Append activity-feed rows (rolling window; see schema nft_events).
    tx_seq is assigned by the caller per block; the parse is deterministic,
    so (he_block, tx_seq) makes block reprocessing idempotent."""
    if not events:
        return
    async with conn.cursor() as cur:
        await cur.executemany(
            "INSERT INTO nft_events (he_block, tx_seq, symbol, nft_id, op, "
            "account, counterparty, price, price_symbol, tx_id, ts) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (he_block, tx_seq) DO NOTHING",
            [
                (e["he_block"], e["tx_seq"], e["symbol"], e["nft_id"], e["op"],
                 e["account"], e["counterparty"], e["price"],
                 e["price_symbol"], e["tx_id"], e["ts"])
                for e in events
            ],
        )


def parse_block(block: dict) -> tuple[set[tuple[str, str]], list[dict], list[dict]]:
    """One deterministic pass over a block's nft/nftmarket txs:
    ((account, symbol) refresh pairs, completed sales, activity events
    with tx_seq assigned in encounter order).

    Refresh pairs are TARGETED wherever an emitted event names the exact
    collection an account was touched in (transfer/issue/market ops all
    do); only accounts that appear in the tx without any event naming
    their symbol (payload-only parses, e.g. setProperties-shaped actions)
    fall back to a full-account refresh (symbol '')."""
    pairs: set[tuple[str, str]] = set()
    sales: list[dict] = []
    events: list[dict] = []
    he_block = block.get("blockNumber")
    ts = _block_ts(block.get("timestamp"))
    for tx in block.get("transactions", []):
        # per-EVENT nft detection, not per-tx: other contracts (pack
        # openings etc.) issue/move NFTs from inside their own txs, which a
        # tx-contract gate silently drops -- both for activity capture and,
        # worse, for refresh queueing (found in the activity-feed review;
        # previously only the hourly safety-net caught those holders).
        if not catalog.has_nft_activity(tx):
            continue
        tx_events = catalog.nft_events(tx, he_block, ts)
        sales.extend(catalog.market_sales(tx, he_block, ts))
        events.extend(tx_events)
        targeted: set[tuple[str, str]] = {
            (acct, e["symbol"])
            for e in tx_events
            for acct in (e.get("account"), e.get("counterparty"))
            if acct and e.get("symbol")
        }
        pairs |= targeted
        covered = {acct for acct, _ in targeted}
        pairs |= {(acct, "") for acct in catalog.touched_accounts(tx)
                  if acct not in covered}
    for seq, event in enumerate(events):
        event["tx_seq"] = seq
    return pairs, sales, events


async def process_block(conn: psycopg.AsyncConnection, block: dict) -> int:
    """Queue refresh work for every account touched by this block's
    nft/nftmarket txs (targeted per symbol where the events name one), and
    record any completed market trades + activity events. Returns the
    number of distinct accounts queued."""
    pairs, sales, events = parse_block(block)
    await queue_refresh(conn, pairs)
    await record_sales(conn, sales)
    await record_events(conn, events)
    # Any symbol with a live market event this block is now stale on the
    # order-book side -- flag it for the (event-driven) market loop.
    await queue_market_refresh(conn, {
        e["symbol"] for e in events
        if e.get("symbol") and str(e.get("op", "")).startswith("market_")
    })
    return len({acct for acct, _ in pairs})


async def process_block_capture_only(conn: psycopg.AsyncConnection, block: dict) -> int:
    """Backfill variant: record sales + events but queue NO refreshes --
    old blocks say nothing about *current* holdings (the live path and
    read-staleness refreshes own freshness), and queueing weeks of historical accounts
    would swamp the refresh worker for zero cache benefit. Returns the
    number of events recorded."""
    _, sales, events = parse_block(block)
    await record_sales(conn, sales)
    await record_events(conn, events)
    return len(events)


async def run(app_dsn: str, nodes: HENodes, stop: asyncio.Event) -> None:
    """Continuously walk HE blocks from the last-seen checkpoint to head,
    then idle briefly and repeat. A gap on restart (last-seen far behind
    head) is caught up sequentially -- each getBlockInfo call is a cheap
    point lookup regardless of block age, so catch-up cost is proportional
    to the gap, not to total chain depth.

    The whole iteration body is guarded: found live that this loop had NO
    exception handling at all, so a single transient HE failure (a node
    hiccup, or -- before the isolated-HENodes-instance fix -- a burst
    elsewhere exhausting every node) crashed the entire sync service, not
    just this loop. Missing a block is the one failure this design can't
    route around later, so this is the one loop that must never let an
    unhandled exception through.

    The checkpoint advances ONLY after a block is actually processed: a
    null result (a lagging node) or a processing error leaves last_he_block
    where it is and retries, so the walk can never skip a block. Skipping is
    the root failure -- every downstream cache gap traces back to an event
    in an unprocessed block."""
    conn = await db.connect(app_dsn)
    try:
        row = await (await conn.execute(
            "SELECT last_he_block FROM sync_state WHERE name = 'block_watcher'"
        )).fetchone()
        last = row["last_he_block"] if row else None
        while not stop.is_set():
            try:
                latest = await nodes.get_latest_block()
                head = latest["blockNumber"]
                if last is None:
                    last = head - 1  # start from the current tip, not genesis
                if last >= head:
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=config.BLOCKWATCH_IDLE_SECONDS)
                    continue
                next_block = last + 1
                block = await nodes.get_block(next_block)
                if block is None:
                    # next_block <= head (guarded above), so this block DOES
                    # exist -- a node returned null because it's lagging
                    # behind the head another node reported, NOT because the
                    # block is absent. Advancing past it would SILENTLY skip
                    # the block and every NFT event in it, forever: the one
                    # failure this design cannot recover from, and the root
                    # of any downstream cache gap. So do NOT advance -- retry
                    # next cycle (node rotation tries a different node). A
                    # genuinely stuck null surfaces as a stalled
                    # last_he_block in /status: a visible halt, never a
                    # silent skip. The brief idle avoids hot-looping a
                    # lagging node during catch-up.
                    logger.warning(
                        "block_watcher: null for block %d (<= head %d) -- not "
                        "advancing, retrying (lagging node?)", next_block, head)
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(
                            stop.wait(), timeout=config.BLOCKWATCH_IDLE_SECONDS)
                    continue
                touched = await process_block(conn, block)
                if touched:
                    logger.debug("HE block %d: queued %d account(s)",
                                 next_block, touched)
                await conn.execute(
                    "INSERT INTO sync_state (name, last_he_block) VALUES "
                    "('block_watcher', %s) ON CONFLICT (name) DO UPDATE SET "
                    "last_he_block = EXCLUDED.last_he_block, updated_at = now()",
                    (next_block,),
                )
                await conn.commit()
                last = next_block  # only after the block is actually processed
            except Exception as exc:
                logger.error("block_watcher: %r", exc)
                await conn.rollback()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=config.BLOCKWATCH_IDLE_SECONDS)
    finally:
        await conn.close()
