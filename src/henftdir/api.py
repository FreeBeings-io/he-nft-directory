"""Public HTTP API — raw WSGI, served by gunicorn.

    gunicorn -w 4 'henftdir.api:application'

A read-through cache over Hive Engine's own current state: there is no
transaction ledger, so there are no /history endpoints. An account not yet
in `known_accounts` is populated synchronously on first query
(_ensure_known) via the same refresh path the background service uses
(sync.refresh_account) -- a one-time ~150-table lookup, a few seconds, and
only ever paid once per account.

DSN comes from HENFT_DSN (default `dbname=henftdir`). Sync reads use one
connection per worker thread; the rare cold-fetch opens its own short-lived
async connection (see _ensure_known).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable
from urllib.parse import parse_qs

from . import config, db, sync
from .display import apply_mapping, load_mappings
from .henodes import HENodes

DSN = os.environ.get("HENFT_DSN", "dbname=henftdir")
MAX_LIMIT = 1000
DEFAULT_LIMIT = 100
MAPPINGS_TTL_SECONDS = 60

# Arbitrary, stable namespace tag ('HENF') for pg_advisory_xact_lock's first
# key -- keeps this app's cold-fetch locks from colliding with any other
# advisory-lock use on the same Postgres instance.
_COLD_FETCH_LOCK_NAMESPACE = 0x48454e46

_local = threading.local()
_mappings_lock = threading.Lock()
_mappings: dict | None = None
_mappings_loaded_at = 0.0


def _conn():
    conn = getattr(_local, "conn", None)
    if conn is None or conn.closed:
        conn = db.connect_sync(DSN)
        conn.read_only = True
        _local.conn = conn
    return conn


def _q(sql: str, params: tuple = ()) -> list[dict]:
    conn = _conn()
    try:
        rows = conn.execute(sql, params).fetchall()
        conn.commit()
        return rows
    except Exception:
        conn.rollback()
        raise


async def _refresh_account_locked(
    conn, nodes: HENodes, account: str, symbols: list[str],
) -> None:
    """Acquire a per-account advisory lock before refreshing -- without
    this, two concurrent requests for the same never-before-seen account
    (two gunicorn workers, two users checking the same account at once)
    would each pay for their own full ~150-table cold-fetch burst against
    HE at the same time, doubling exactly the kind of load this session
    has spent most of today trying to keep polite. Transaction-scoped:
    released automatically when refresh_account's own commit happens on
    this connection. Re-checks known_accounts after acquiring the lock,
    since the other request may have already finished the work."""
    await conn.execute(
        "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
        (_COLD_FETCH_LOCK_NAMESPACE, account),
    )
    row = await (await conn.execute(
        "SELECT 1 FROM known_accounts WHERE account = %s", (account,)
    )).fetchone()
    if row is not None:
        return  # someone else already finished this while we waited
    await sync.refresh_account(conn, nodes, account, symbols)


async def _cold_fetch_account(account: str) -> None:
    conn = await db.connect(DSN)
    try:
        async with HENodes() as nodes:
            symbols = await sync.known_symbols(conn)
            await _refresh_account_locked(conn, nodes, account, symbols)
    finally:
        await conn.close()


def _ensure_known(account: str) -> None:
    """First query for this account ever -> populate it now, synchronously.
    Every later query is served straight from the cache -- see sync.py's
    module docstring for why a per-account refresh is fast regardless of
    how large any single collection is."""
    if _q("SELECT 1 FROM known_accounts WHERE account = %s", (account,)):
        return
    asyncio.run(_cold_fetch_account(account))


async def _cold_fetch_instance(symbol: str, nft_id: int) -> bool:
    async with HENodes() as nodes:
        rec = await nodes.find_one("nft", f"{symbol}instances", {"_id": nft_id})
        if rec is None:
            return False
        conn = await db.connect(DSN)
        try:
            symbols = await sync.known_symbols(conn)
            await _refresh_account_locked(conn, nodes, rec["account"], symbols)
        finally:
            await conn.close()
        return True


def mappings() -> dict:
    """Latest display mapping per symbol, cached per process with a short
    TTL -- without one, an operator adding a mapping (the DEPLOY.md-
    documented workflow) would need to restart the API process before it
    ever took effect."""
    global _mappings, _mappings_loaded_at
    with _mappings_lock:
        now = time.monotonic()
        if _mappings is None or (now - _mappings_loaded_at) > MAPPINGS_TTL_SECONDS:
            _mappings = load_mappings(_conn())
            _mappings_loaded_at = now
        return _mappings


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _params(environ) -> dict[str, str]:
    qs = parse_qs(environ.get("QUERY_STRING", ""))
    return {k: v[0] for k, v in qs.items()}

def _page(params: dict) -> tuple[int, int]:
    try:
        cursor = int(params.get("cursor", 0))
    except ValueError:
        cursor = 0
    try:
        limit = min(int(params.get("limit", DEFAULT_LIMIT)), MAX_LIMIT)
    except ValueError:
        limit = DEFAULT_LIMIT
    return cursor, max(1, limit)


def _display_for(row: dict, mapping_by_symbol: dict) -> dict | None:
    return apply_mapping(
        mapping_by_symbol.get(row["symbol"]), row["symbol"], row["nft_id"],
        row.get("properties") or {}, row.get("collection_name"),
    )


# -- endpoint handlers ----------------------------------------------------------

def account_nfts(params: dict, account: str) -> dict:
    """Wallet display: owned + delegated-in/out, grouped by symbol."""
    _ensure_known(account)
    # Resolved once per request, not once per row -- found live: an
    # account with ~1400 items took ~37ms vs ~5ms for other endpoints,
    # almost entirely the per-row mappings() call (a threading.Lock
    # acquisition + TTL check on every single item).
    mapping_by_symbol = mappings()
    where, args = "i.account = %s", [account]
    if params.get("symbol"):
        where += " AND i.symbol = %s"
        args.append(params["symbol"])
    owned = _q(
        f"SELECT i.symbol, i.nft_id, i.account, i.owned_by, i.delegated_to, "
        f"i.properties, c.name AS collection_name "
        f"FROM instances i LEFT JOIN collections c ON c.symbol = i.symbol "
        f"WHERE {where} ORDER BY i.symbol, i.nft_id",
        tuple(args),
    )
    delegated_in = _q(
        "SELECT i.symbol, i.nft_id, i.account, i.owned_by, i.delegated_to, "
        "i.properties, c.name AS collection_name "
        "FROM instances i LEFT JOIN collections c ON c.symbol = i.symbol "
        "WHERE i.delegated_to = %s ORDER BY i.symbol, i.nft_id",
        (account,),
    )

    # NFTs the account has listed on the market are held in escrow
    # (instances.account = 'nftmarket', owned_by 'c'), so they drop out of
    # `owned` above. 'nftmarket' is always a known/refreshed account (see
    # service.py) specifically so this join has properties to show.
    listed = _q(
        "SELECT i.symbol, i.nft_id, i.owned_by, i.delegated_to, i.properties, "
        "c.name AS collection_name, o.price, o.price_symbol "
        "FROM market_orders o "
        "JOIN instances i ON i.symbol = o.symbol AND i.nft_id = o.nft_id "
        "LEFT JOIN collections c ON c.symbol = i.symbol "
        "WHERE o.account = %s ORDER BY i.symbol, i.nft_id",
        (account,),
    )

    def grouped(rows: list[dict], extra: tuple = ()) -> dict:
        out: dict[str, list] = {}
        for r in rows:
            item = {
                "nft_id": r["nft_id"],
                "account": r.get("account", account),
                "owned_by": r["owned_by"],
                "delegated_to": r["delegated_to"],
                "properties": r["properties"],
                "display": _display_for(r, mapping_by_symbol),
            }
            for k in extra:
                item[k] = r.get(k)
            out.setdefault(r["symbol"], []).append(item)
        return out

    return {"account": account,
            "owned": grouped(owned),
            "delegated_in": grouped(delegated_in),
            "listed": grouped(listed, extra=("price", "price_symbol"))}


def collections_list(params: dict) -> dict:
    cursor, limit = _page(params)
    rows = _q(
        "SELECT c.*, "
        "(SELECT count(*) FROM display_mappings m WHERE m.symbol = c.symbol) > 0 "
        "  AS has_display_mapping "
        "FROM collections c WHERE c.symbol > %s ORDER BY c.symbol LIMIT %s",
        (params.get("cursor_symbol", ""), limit),
    )
    for row in rows:
        if row["supply"] is not None and row["circulating_supply"] is not None:
            row["burned"] = row["supply"] - row["circulating_supply"]
    return {"collections": rows,
            "next_cursor_symbol":
                rows[-1]["symbol"] if len(rows) == limit else None}


def collection_detail(params: dict, symbol: str) -> dict | None:
    rows = _q("SELECT * FROM collections WHERE symbol = %s", (symbol,))
    if not rows:
        return None
    col = rows[0]
    if col["supply"] is not None and col["circulating_supply"] is not None:
        col["burned"] = col["supply"] - col["circulating_supply"]
    col["has_display_mapping"] = symbol in mappings()
    return col


def nft_detail(params: dict, symbol: str, nft_id: str) -> dict | None:
    rows = _q(
        "SELECT i.*, c.name AS collection_name FROM instances i "
        "LEFT JOIN collections c ON c.symbol = i.symbol "
        "WHERE i.symbol = %s AND i.nft_id = %s", (symbol, int(nft_id)),
    )
    if not rows:
        # The full HE catalog is already mirrored locally (refreshed every
        # few minutes) -- if the symbol isn't a real collection at all, we
        # already know that for free and can 404 without a live HE round
        # trip. Found live: repeatedly querying a bogus symbol otherwise
        # re-pays a live lookup every single time, since a not-found
        # result is never cached anywhere. Narrows the live cold-fetch to
        # the genuinely ambiguous case: a real collection, but this
        # specific id may not exist (or was burned).
        if not _q("SELECT 1 FROM collections WHERE symbol = %s", (symbol,)):
            return None
        if not asyncio.run(_cold_fetch_instance(symbol, int(nft_id))):
            return None
        rows = _q(
            "SELECT i.*, c.name AS collection_name FROM instances i "
            "LEFT JOIN collections c ON c.symbol = i.symbol "
            "WHERE i.symbol = %s AND i.nft_id = %s", (symbol, int(nft_id)),
        )
        if not rows:
            return None
    inst = rows[0]
    inst["display"] = _display_for(inst, mappings())
    order = _q(
        "SELECT price, price_symbol, account, ts FROM market_orders "
        "WHERE symbol = %s AND nft_id = %s", (symbol, int(nft_id)),
    )
    inst["open_order"] = order[0] if order else None
    return inst


def market(params: dict, symbol: str) -> dict:
    where, args = "symbol = %s", [symbol]
    if params.get("account"):
        where += " AND account = %s"
        args.append(params["account"])
    cursor, limit = _page(params)
    orders = _q(
        f"SELECT nft_id, account, owned_by, price, price_symbol, fee, ts "
        f"FROM market_orders WHERE {where} AND nft_id > %s "
        f"ORDER BY nft_id LIMIT %s",
        tuple(args) + (cursor, limit),
    )
    rollups = _q(
        "SELECT price_symbol, grouping, floor_price, open_orders "
        "FROM market_rollups WHERE symbol = %s "
        "ORDER BY price_symbol, grouping_key",
        (symbol,),
    )
    # Last-sale + trailing-30d volume per payment token, from the forward-only
    # market_sales log (empty until trades happen after the block-watcher's
    # start point -- there is no pre-launch trade history to backfill).
    last_sales = _q(
        "SELECT price_symbol, "
        "  (array_agg(price ORDER BY ts DESC, he_block DESC))[1] AS last_price, "
        "  (array_agg(ts ORDER BY ts DESC, he_block DESC))[1] AS last_ts, "
        "  count(*) FILTER (WHERE ts > now() - interval '30 days') AS sales_30d, "
        "  sum(price) FILTER (WHERE ts > now() - interval '30 days') AS volume_30d "
        "FROM market_sales WHERE symbol = %s GROUP BY price_symbol "
        "ORDER BY price_symbol",
        (symbol,),
    )
    return {"symbol": symbol, "rollups": rollups, "open_orders": orders,
            "last_sales": last_sales,
            "next_cursor":
                orders[-1]["nft_id"] if len(orders) == limit else None}


def _activity_coverage() -> dict:
    """How much of the advisory window the feed actually holds right now:
    live capture runs from deploy; the backfill fills backward toward the
    window start and reports its own progress."""
    state = {r["name"]: r["last_he_block"] for r in _q(
        "SELECT name, last_he_block FROM sync_state "
        "WHERE name IN ('activity_backfill', 'activity_backfill_target')")}
    cursor = state.get("activity_backfill")
    target = state.get("activity_backfill_target")
    oldest = _q("SELECT min(ts) AS t FROM nft_events")[0]["t"]
    return {
        "window_days": config.ACTIVITY_WINDOW_DAYS,
        "oldest_event_ts": oldest,
        "backfill_complete": (cursor is not None and target is not None
                              and cursor <= target),
        "backfill_cursor_block": cursor,
        "backfill_target_block": target,
    }


def account_activity(params: dict, account: str) -> dict:
    """Rolling recent-activity feed for one account (as actor OR
    counterparty) -- capture-only rows from nft_events, newest first,
    keyset-paginated on (he_block, tx_seq) packed into one integer cursor.
    Deliberately does NOT trigger a cold-fetch: activity is what we
    captured while watching, independent of holdings-cache population."""
    cursor, limit = _page(params)
    where = "(account = %s OR counterparty = %s)"
    args: list = [account, account]
    if params.get("symbol"):
        where += " AND symbol = %s"
        args.append(params["symbol"])
    if params.get("op"):
        where += " AND op = %s"
        args.append(params["op"])
    # pack (he_block, tx_seq) into one orderable int; parse is bounded well
    # under 10k events per block, so the packing can't collide
    if cursor:
        where += " AND (he_block * 10000 + tx_seq) < %s"
        args.append(cursor)
    rows = _q(
        f"SELECT he_block, tx_seq, symbol, nft_id, op, account, counterparty, "
        f"price, price_symbol, tx_id, ts FROM nft_events WHERE {where} "
        f"ORDER BY he_block DESC, tx_seq DESC LIMIT %s",
        (*args, limit),
    )
    next_cursor = (rows[-1]["he_block"] * 10000 + rows[-1]["tx_seq"]
                   if len(rows) == limit else None)
    for row in rows:
        row.pop("tx_seq", None)  # internal ordering detail, not API surface
    return {"account": account, "events": rows,
            "coverage": _activity_coverage(), "next_cursor": next_cursor}


def status(params: dict) -> dict:
    # sync = loop FRESHNESS only. The activity backfill's cursor/target
    # checkpoints also live in sync_state but are internal bookkeeping (the
    # target row never updates after init, so surfacing it here made the
    # service look stale); they're served through `activity` instead.
    sync_state = {r["name"]: r for r in _q(
        "SELECT * FROM sync_state "
        "WHERE name NOT IN ('activity_backfill', 'activity_backfill_target') "
        "ORDER BY name")}
    known = _q("SELECT count(*) AS n FROM known_accounts")[0]["n"]
    queue_depth = _q("SELECT count(*) AS n FROM refresh_queue")[0]["n"]
    coverage = _q(
        "SELECT count(*) FILTER (WHERE m.symbol IS NOT NULL) AS mapped, "
        "count(*) AS total FROM ("
        "  SELECT DISTINCT symbol FROM instances) s "
        "LEFT JOIN (SELECT DISTINCT symbol FROM display_mappings) m "
        "  USING (symbol)",
    )[0]
    return {
        "sync": sync_state,
        "known_accounts": known,
        "refresh_queue_depth": queue_depth,
        "display_mapping_coverage": coverage,
        "activity": _activity_coverage(),
        "disclosure": "This service is a read-through cache over Hive "
                      "Engine's own current state, populated only for "
                      "accounts that have been queried at least once. It "
                      "is not a historical ledger -- holdings reflect the "
                      "last refresh, and the only event data served is a "
                      "rolling recent-activity feed (see activity.window_"
                      "days); older events are pruned, not archived.",
    }


# -- routing -------------------------------------------------------------------

ROUTES: list[tuple[re.Pattern, Callable]] = [
    (re.compile(r"^/accounts/([a-z0-9.-]{3,16})/nfts$"), account_nfts),
    (re.compile(r"^/accounts/([a-z0-9.-]{3,16})/activity$"), account_activity),
    (re.compile(r"^/collections$"), collections_list),
    (re.compile(r"^/collections/([A-Z0-9]+)$"), collection_detail),
    (re.compile(r"^/nfts/([A-Z0-9]+)/(\d+)$"), nft_detail),
    (re.compile(r"^/market/([A-Z0-9]+)$"), market),
    (re.compile(r"^/status$"), status),
]


def application(environ, start_response):
    def reply(code: str, body: dict | None):
        data = json.dumps(
            body if body is not None else {"error": code},
            default=_json_default,
        ).encode()
        start_response(code, [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(data))),
            ("Access-Control-Allow-Origin", "*"),
        ])
        return [data]

    if environ.get("REQUEST_METHOD") != "GET":
        return reply("405 Method Not Allowed", None)
    path = environ.get("PATH_INFO", "") or "/"
    for pattern, handler in ROUTES:
        match = pattern.match(path)
        if match:
            try:
                result = handler(_params(environ), *match.groups())
            except ValueError:
                return reply("400 Bad Request", None)
            except Exception:  # surface as 500, never a stack trace
                import logging
                logging.getLogger(__name__).exception("handler failed: %s", path)
                return reply("500 Internal Server Error", None)
            if result is None:
                return reply("404 Not Found", None)
            return reply("200 OK", result)
    return reply("404 Not Found", None)
