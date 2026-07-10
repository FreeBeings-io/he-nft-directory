"""The block-watcher: a doorbell, not a ledger.

Walks HE blocks directly by number (no Hive L1 node needed -- HE's own
`getBlockInfo` is a self-contained, O(1) point lookup carrying every
transaction's contract/action/payload/logs inline; verified live across
genesis-era through current blocks). For each
transaction where `contract in {nft, nftmarket}`, it queues every account
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


async def queue_refresh(conn: psycopg.AsyncConnection, accounts: set[str]) -> None:
    if not accounts:
        return
    await conn.execute(
        "INSERT INTO refresh_queue (account) SELECT unnest(%s::text[]) "
        "ON CONFLICT (account) DO UPDATE SET queued_at = now()",
        (list(accounts),),
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


def parse_block(block: dict) -> tuple[set[str], list[dict], list[dict]]:
    """One deterministic pass over a block's nft/nftmarket txs:
    (touched accounts, completed sales, activity events with tx_seq
    assigned in encounter order)."""
    accounts: set[str] = set()
    sales: list[dict] = []
    events: list[dict] = []
    he_block = block.get("blockNumber")
    ts = _block_ts(block.get("timestamp"))
    for tx in block.get("transactions", []):
        if catalog.is_nft_tx(tx.get("contract")):
            accounts |= catalog.touched_accounts(tx)
            sales.extend(catalog.market_sales(tx, he_block, ts))
            events.extend(catalog.nft_events(tx, he_block, ts))
    for seq, event in enumerate(events):
        event["tx_seq"] = seq
    return accounts, sales, events


async def process_block(conn: psycopg.AsyncConnection, block: dict) -> int:
    """Queue every account touched by this block's nft/nftmarket txs, and
    record any completed market trades + activity events. Returns the
    number of distinct accounts queued."""
    accounts, sales, events = parse_block(block)
    await queue_refresh(conn, accounts)
    await record_sales(conn, sales)
    await record_events(conn, events)
    return len(accounts)


async def process_block_capture_only(conn: psycopg.AsyncConnection, block: dict) -> int:
    """Backfill variant: record sales + events but queue NO refreshes --
    old blocks say nothing about *current* holdings (the live path and
    safety-net own freshness), and queueing weeks of historical accounts
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
    unhandled exception through."""
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
                if block is not None:
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
                last = next_block  # avoid re-reading what we just wrote
            except Exception as exc:
                logger.error("block_watcher: %r", exc)
                await conn.rollback()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=config.BLOCKWATCH_IDLE_SECONDS)
    finally:
        await conn.close()
