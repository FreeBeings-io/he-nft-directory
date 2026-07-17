"""Cache population: the only place that reads Hive Engine's own current
state and writes it into our tables. Nothing here derives anything from a
transaction log -- every write is "this is what HE just told us," full
stop. The design choices below are all backed by live measurement against
mainnet HE nodes.

refresh_account() is the one real hot path: there is no single HE query
for "everything account X holds" (verified live -- the nft contract has
one `{symbol}instances` table per collection, ~150 of them, no shared
index like tokens' `balances`), so a refresh checks every known symbol.
Each check is an indexed, per-account `find()` -- fast regardless of
collection size (confirmed live: ~0.2-0.6s even against STAR's 27M+ rows)
-- unlike bare pagination through a whole collection, which scales with
collection size and is why this design never does that.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from . import config
from .henodes import HENodes

logger = logging.getLogger(__name__)


def _epoch_ms_to_ts(value) -> datetime | None:
    """HE's sellBook `timestamp` is milliseconds since epoch (verified
    live), not a timestamptz-compatible value on its own."""
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _parse(value, default):
    if isinstance(value, str):
        try:
            return json.loads(value) if value else default
        except ValueError:
            return default
    return value if value is not None else default


# -- collection catalog -------------------------------------------------------

_CATALOG_UPSERT = (
    "INSERT INTO collections (symbol, name, org_name, product_name, "
    "issuer, url, metadata, group_by, properties, delegation_enabled, "
    "market_enabled, supply, circulating_supply, max_supply, "
    "undelegation_cooldown_days, refreshed_at) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()) "
    "ON CONFLICT (symbol) DO UPDATE SET "
    "name = EXCLUDED.name, org_name = EXCLUDED.org_name, "
    "product_name = EXCLUDED.product_name, issuer = EXCLUDED.issuer, "
    "url = EXCLUDED.url, metadata = EXCLUDED.metadata, "
    "group_by = EXCLUDED.group_by, properties = EXCLUDED.properties, "
    "delegation_enabled = EXCLUDED.delegation_enabled, "
    "market_enabled = EXCLUDED.market_enabled, "
    "supply = EXCLUDED.supply, "
    "circulating_supply = EXCLUDED.circulating_supply, "
    "max_supply = EXCLUDED.max_supply, "
    "undelegation_cooldown_days = EXCLUDED.undelegation_cooldown_days, "
    "refreshed_at = now()"
)


def _catalog_row(row: dict) -> tuple:
    metadata = _parse(row.get("metadata"), {})
    return (
        row["symbol"], row.get("name"), row.get("orgName"),
        row.get("productName"), row.get("issuer"), metadata.get("url"),
        Jsonb(metadata), Jsonb(_parse(row.get("groupBy"), [])),
        Jsonb(_parse(row.get("properties"), {})),
        bool(row.get("delegationEnabled")), bool(row.get("marketEnabled")),
        row.get("supply"), row.get("circulatingSupply"),
        row.get("maxSupply") or None, row.get("undelegationCooldown"),
    )


async def refresh_catalog(conn: psycopg.AsyncConnection, nodes: HENodes) -> int:
    """Full mirror of HE's `nft`/`nfts` table -- ~150 rows platform-wide,
    cheap to refresh in full every time (no per-symbol targeting needed;
    this is the one table small enough that eager beats lazy)."""
    offset, total = 0, 0
    while True:
        page = await nodes.find("nft", "nfts", {}, limit=1000, offset=offset)
        if not page:
            break
        async with conn.cursor() as cur:
            await cur.executemany(_CATALOG_UPSERT, [_catalog_row(row) for row in page])
        total += len(page)
        if len(page) < 1000:
            break
        offset += 1000
    await conn.commit()
    return total


async def known_symbols(conn: psycopg.AsyncConnection) -> list[str]:
    """Symbols worth querying for an account refresh: collections with at
    least one issued instance. A 2026-07-10 full-catalog audit found 37 of
    152 collections have zero instances platform-wide -- querying those on
    every refresh is ~25% of the cold-fetch burst for guaranteed-empty
    answers. NULL circulating_supply is kept (unknown != empty); a
    collection's first-ever issue is picked up at the next catalog sweep.

    The EXISTS arm closes a burn-race staleness hole: if a collection's
    last instances are burned while we still hold cached rows, filtering
    on circulating_supply alone would skip the symbol forever and the
    stale "owned" rows could never be refreshed away. Any symbol we still
    have cached instances for stays queryable until a confirmed-empty
    refresh deletes those rows -- after which this arm stops matching, so
    the extra queries self-extinguish. (instances' PK is (symbol, nft_id),
    so the EXISTS probe is an index prefix hit.)"""
    rows = await (await conn.execute(
        "SELECT symbol FROM collections c "
        "WHERE c.circulating_supply IS NULL OR c.circulating_supply > 0 "
        "   OR EXISTS (SELECT 1 FROM instances i WHERE i.symbol = c.symbol)"
    )).fetchall()
    return [r["symbol"] for r in rows]


# -- account refresh (the cache's read-through path) -------------------------

def _instance_row(symbol: str, rec: dict) -> tuple:
    # HE returns `delegatedTo` as an OBJECT ({"account": ..., "ownedBy": ...}),
    # not a string -- found live: passing that dict straight into the text
    # `delegated_to` column raised "cannot adapt type 'dict'" and crashed the
    # entire refresh for any account holding a delegated NFT (one bad row
    # fails the whole executemany batch). Pull the account/type out of the
    # object; tolerate a plain-string shape too, just in case a node differs.
    deleg = rec.get("delegatedTo")
    if isinstance(deleg, dict):
        delegated_to, delegated_to_type = deleg.get("account"), deleg.get("ownedBy")
    else:
        delegated_to, delegated_to_type = deleg, rec.get("delegatedToType")
    return (
        symbol, rec["_id"], rec["account"], rec.get("ownedBy", "u"),
        delegated_to, delegated_to_type,
        bool(rec.get("soulbound") or rec.get("soulBound")),
        Jsonb(_parse(rec.get("properties"), {})),
    )


async def _fetch_symbol_for_account(
    nodes: HENodes, symbol: str, account: str
) -> list[dict] | None:
    """None means "couldn't check" (transient HE failure), NOT "confirmed
    empty" -- found live: treating them the same silently erased a real
    account's real holdings (448 real STAR instances) from the cache the
    moment that one symbol's lookup failed under cold-fetch burst load.

    No extra retry here -- HENodes.call() already retries internally
    (config.HE_RETRY_ROUNDS rounds with exponential backoff). Found live:
    wrapping that in a second retry loop doesn't help when the underlying
    condition is sustained node contention, not a one-off blip -- it just
    doubles how long the failure (and the cooldown it triggers on other
    concurrent lookups) lasts. Being retried at all is what None protects
    against here: this symbol just gets picked up by a later refresh."""
    try:
        return await nodes.find(
            "nft", f"{symbol}instances", {"account": account}, limit=1000,
        )
    except Exception as exc:
        logger.warning("refresh: %s/%s failed: %r", account, symbol, exc)
        return None


async def _requeue_failed(
    conn: psycopg.AsyncConnection, account: str, failed_symbols: list[str],
) -> None:
    """Re-enqueue symbols whose lookup failed this pass, with exponential
    per-row backoff (attempts doubles the delay, capped). This is what
    makes every refresh path self-healing: a transient HE failure becomes
    a visible pending retry instead of silent staleness waiting for a
    sweep that no longer exists. not_before keeps a struggling symbol from
    hot-looping at the head of the queue while nodes are down."""
    if not failed_symbols:
        return
    await conn.execute(
        "INSERT INTO refresh_queue (account, symbol, attempts, not_before) "
        "SELECT %s, s, 1, now() + make_interval(secs => %s) "
        "FROM unnest(%s::text[]) AS s "
        "ON CONFLICT (account, symbol) DO UPDATE SET "
        "attempts = refresh_queue.attempts + 1, "
        "not_before = now() + least("
        "  make_interval(secs => %s * power(2, least(refresh_queue.attempts, 10))), "
        "  make_interval(secs => %s))",
        (account, config.REFRESH_RETRY_BASE_SECONDS, failed_symbols,
         config.REFRESH_RETRY_BASE_SECONDS, config.REFRESH_RETRY_CAP_SECONDS),
    )


async def refresh_account(
    conn: psycopg.AsyncConnection, nodes: HENodes, account: str,
    symbols: list[str], pace: float = 0.0,
    dequeue_symbols: list[str] | None = None,
) -> int:
    """Authoritative re-fetch of one account's current holdings across every
    known symbol. Used both for the first-ever (cold) lookup and for
    routine re-fetches -- there is no separate "cold-fetch" code path, only
    this, called synchronously on a cache miss or asynchronously from the
    refresh queue.

    Only replaces rows for symbols that were actually confirmed this pass
    (fetch succeeded, even if the confirmed result was empty) -- a symbol
    whose lookup failed keeps whatever was cached before (or stays absent,
    on a first-ever fetch) rather than being wiped to "owns nothing" on a
    transient HE hiccup.

    pace > 0 inserts that many seconds between concurrency-sized chunks of
    symbol lookups -- for BACKGROUND callers (the refresh worker),
    where latency is free and the un-paced burst was found live to push
    every node into cooldown at once (a "no nodes" retry storm on anything
    else in flight). The synchronous API cold-fetch keeps pace=0: a user is
    waiting on that path, and one burst per never-seen account is the
    documented cost."""
    if not symbols:
        # The catalog hasn't been populated yet (a narrow window right
        # after a fresh deploy, before the first catalog refresh
        # completes) -- do NOT mark this account as known/refreshed with
        # zero symbols actually checked, or it would look permanently
        # "confirmed empty" to callers even though nothing was verified.
        # Leave it exactly as it was; a later refresh (queued or cold-
        # fetched again) picks it up once the catalog exists.
        logger.warning(
            "refresh_account(%s): no known symbols yet -- catalog not "
            "populated, skipping", account,
        )
        return 0
    if pace > 0:
        results = []
        step = config.HE_MAX_CONCURRENCY
        for i in range(0, len(symbols), step):
            results.extend(await asyncio.gather(
                *(_fetch_symbol_for_account(nodes, s, account)
                  for s in symbols[i:i + step])
            ))
            if i + step < len(symbols):
                await asyncio.sleep(pace)
    else:
        results = await asyncio.gather(
            *(_fetch_symbol_for_account(nodes, symbol, account) for symbol in symbols)
        )
    confirmed_symbols = [s for s, r in zip(symbols, results) if r is not None]
    rows = [
        _instance_row(symbol, rec)
        for symbol, page in zip(symbols, results)
        if page is not None
        for rec in page
    ]
    failed = len(symbols) - len(confirmed_symbols)
    if failed:
        logger.warning(
            "refresh: %s: %d/%d symbol(s) could not be confirmed this pass",
            account, failed, len(symbols),
        )
    if confirmed_symbols:
        await conn.execute(
            "DELETE FROM instances WHERE account = %s AND symbol = ANY(%s)",
            (account, confirmed_symbols),
        )
    if rows:
        # ON CONFLICT, not a plain INSERT -- found live (crashed the whole
        # sync service): the primary key is (symbol, nft_id), not including
        # account, because an instance has exactly one owner. The DELETE
        # above only clears *this* account's stale rows; if the instance
        # was transferred here from an account we haven't refreshed yet,
        # its stale row (still crediting the old owner) is still present
        # under the same (symbol, nft_id) key and collides on a plain
        # INSERT. The old owner's own next refresh naturally stops
        # re-inserting it once HE confirms they no longer hold it.
        async with conn.cursor() as cur:
            await cur.executemany(
                "INSERT INTO instances (symbol, nft_id, account, owned_by, "
                "delegated_to, delegated_to_type, soul_bound, properties) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (symbol, nft_id) DO UPDATE SET "
                "account = EXCLUDED.account, owned_by = EXCLUDED.owned_by, "
                "delegated_to = EXCLUDED.delegated_to, "
                "delegated_to_type = EXCLUDED.delegated_to_type, "
                "soul_bound = EXCLUDED.soul_bound, "
                "properties = EXCLUDED.properties, refreshed_at = now()",
                rows,
            )
    # refreshed_at means "last FULL pass over every known symbol" -- it is
    # what the read-staleness bound (api._ensure_known) measures against.
    # A targeted single-symbol re-check must NOT reset it: with the
    # safety-net sweep gone, letting frequent targeted touches keep
    # bumping the clock would mean an active account never trips its
    # periodic full re-check while its untouched symbols drift.
    if dequeue_symbols is None:
        # Full pass: mark populated_at too -- this is the scan that makes an
        # account safe to serve and eligible for the cheap targeted paths.
        await conn.execute(
            "INSERT INTO known_accounts (account, populated_at) VALUES (%s, now()) "
            "ON CONFLICT (account) DO UPDATE SET refreshed_at = now(), "
            "populated_at = now()",
            (account,),
        )
    else:
        # Targeted pass: never creates or populates an account (a partial
        # check must not make an account look complete). It only runs for
        # accounts already tracked+populated, so DO NOTHING is right.
        await conn.execute(
            "INSERT INTO known_accounts (account) VALUES (%s) "
            "ON CONFLICT (account) DO NOTHING", (account,),
        )
    # Dequeue only what this pass actually covered: a targeted refresh
    # (dequeue_symbols set) must not clear other symbols' queue rows --
    # including ones that arrived while it ran. A full refresh clears the
    # account's whole queue, '' entry included. Failed symbols are then
    # re-enqueued with backoff (after the dequeue, so the retry rows
    # survive it) -- no lookup failure is ever silently dropped.
    if dequeue_symbols is None:
        await conn.execute(
            "DELETE FROM refresh_queue WHERE account = %s", (account,))
    else:
        await conn.execute(
            "DELETE FROM refresh_queue WHERE account = %s AND symbol = ANY(%s)",
            (account, dequeue_symbols),
        )
    await _requeue_failed(
        conn, account, [s for s, r in zip(symbols, results) if r is None])
    await conn.commit()
    return len(rows)


async def _reschedule_account(conn: psycopg.AsyncConnection, account: str) -> None:
    """Push an account's queue rows out by an exponential backoff after a
    HARD refresh failure (whole pass raised, not just some symbols). This
    un-sticks the queue without dropping work -- the old code DELETEd the
    row, which only the (now-removed) safety-net sweep ever came back for.
    not_before hides the account for the backoff window."""
    await conn.execute(
        "UPDATE refresh_queue SET attempts = attempts + 1, "
        "not_before = now() + least("
        "  make_interval(secs => %s * power(2, least(attempts, 10))), "
        "  make_interval(secs => %s)) "
        "WHERE account = %s",
        (config.REFRESH_RETRY_BASE_SECONDS,
         config.REFRESH_RETRY_CAP_SECONDS, account),
    )
    await conn.commit()


async def _cached_account_symbols(
    conn: psycopg.AsyncConnection, account: str
) -> list[str]:
    """Symbols the account currently has cached holdings in -- owned or
    delegated-in. Uses instances_account_idx / instances_delegated_idx."""
    rows = await (await conn.execute(
        "SELECT symbol FROM instances WHERE account = %s "
        "UNION SELECT symbol FROM instances WHERE delegated_to = %s",
        (account, account),
    )).fetchall()
    return [r["symbol"] for r in rows]


async def _refresh_one(
    conn: psycopg.AsyncConnection, nodes: HENodes, account: str,
    syms: list[str], all_symbols: list[str],
) -> None:
    """Refresh one claimed account on its OWN connection, so a batch of
    accounts refreshes concurrently and one account's failure (or its
    connection's rollback) can never touch another's transaction.

    A non-empty symbol list is a TARGETED re-check of just the touched
    collections (~40x cheaper), dequeuing exactly those rows so touches
    arriving mid-refresh aren't lost.

    A '' entry is a touch that named no symbol -- a payload-only action
    (setProperties etc.) or a staleness re-verify. Those never transfer
    ownership: a genuine acquisition always emits a symbol-naming event and
    so refreshes targeted, never via ''. So a '' refresh re-checks only the
    collections the account ALREADY holds, not all ~115 known symbols --
    falling back to a full re-check when nothing is cached (unknown /
    holds-nothing account, effectively a cold fetch). NOTE: this narrows
    missed-block recovery -- a new-collection acquisition during a
    block-watcher gap is caught only by a later targeted event, not by a ''
    refresh; consistent with the watcher-completeness bet that also retired
    the safety-net sweep."""
    try:
        row = await (await conn.execute(
            "SELECT populated_at FROM known_accounts WHERE account = %s", (account,)
        )).fetchone()
        if row is None or row["populated_at"] is None:
            # Not yet fully populated -- either a touch arrived for an
            # account still mid-initial-scan (tracked, populated_at NULL), or
            # a row we somehow haven't scanned. Do a FULL scan so it's
            # completely populated before it is ever served, and so the ''
            # narrowing below only ever applies to already-fully-scanned
            # accounts. (The API cold-fetch normally does this first; this is
            # the belt-and-suspenders path for the touch-driven case.)
            await refresh_account(conn, nodes, account, all_symbols,
                                  pace=config.REFRESH_PACE_SECONDS)
        elif "" in syms:
            cached = await _cached_account_symbols(conn, account)
            await refresh_account(conn, nodes, account, cached or all_symbols,
                                  pace=config.REFRESH_PACE_SECONDS)
        else:
            await refresh_account(conn, nodes, account, syms,
                                  pace=config.REFRESH_PACE_SECONDS,
                                  dequeue_symbols=syms)
    except Exception as exc:
        logger.error("refresh_worker: %s failed: %r", account, exc)
        # Fully isolated: roll back and reschedule on THIS account's own
        # connection; guard the cleanup too so a broken connection can't
        # escape into the gather and take the batch down.
        try:
            await conn.rollback()
            await _reschedule_account(conn, account)
        except Exception as exc2:
            logger.error("refresh_worker: reschedule for %s failed: %r", account, exc2)
            with contextlib.suppress(Exception):
                await conn.rollback()


async def refresh_worker(
    conn: psycopg.AsyncConnection, connect, nodes: HENodes,
    stop: asyncio.Event, batch: int | None = None,
) -> None:
    """Drains refresh_queue -- accounts the block-watcher flagged as
    touched -- up to `batch` accounts CONCURRENTLY, each on its own
    connection. Serial draining was the throughput ceiling (~1.1s/account
    measured live); parallel draining fills the shared HE node budget
    (config.HE_MAX_CONCURRENCY, which still caps total HE-call concurrency)
    instead of leaving it idle between one account's lookups.

    `conn` is the CONTROL connection (claim + known_symbols + idle wait,
    caller-owned); `connect` is an async factory returning a fresh
    connection per concurrent worker slot (owned + closed here).

    The claim/idle body is guarded so a failure in the claim SELECT or
    known_symbols can't crash the service; per-account failures are handled
    inside _refresh_one on their own connections."""
    batch = batch or config.REFRESH_WORKER_BATCH
    workers = [await connect() for _ in range(batch)]
    try:
        symbols = await known_symbols(conn)
        await conn.rollback()  # release the read's snapshot (see idle note below)
        while not stop.is_set():
            try:
                # not_before gates retry rows: a symbol/account in backoff
                # is invisible until its window opens, so a struggling
                # lookup can't hot-loop the queue head while fresh work
                # waits. GROUP BY account so all of one account's queued
                # symbols collapse into a single pass.
                rows = await (await conn.execute(
                    "SELECT account, array_agg(DISTINCT symbol) AS syms "
                    "FROM refresh_queue WHERE not_before <= now() "
                    "GROUP BY account ORDER BY min(queued_at) LIMIT %s",
                    (batch,),
                )).fetchall()
                # Release the claim read's transaction before slow work or
                # idling. This conn is autocommit=False, so an uncommitted
                # read leaves it "idle in transaction" pinning its MVCC
                # snapshot; the next poll would reuse that stale snapshot
                # and never see newly-queued rows -- the wedge found live
                # 2026-07-17. (refresh_account commits on the worker conns.)
                await conn.rollback()
                if not rows:
                    # Catalog rarely changes -- refresh the symbol list only
                    # when idle, not once per queued account.
                    symbols = await known_symbols(conn)
                    await conn.rollback()
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=config.REFRESH_IDLE_SECONDS)
                    continue
                # Distinct accounts (GROUP BY), so each worker slot owns a
                # different account -- no cross-slot queue contention.
                await asyncio.gather(*(
                    _refresh_one(workers[i], nodes, r["account"],
                                 r["syms"] or [], symbols)
                    for i, r in enumerate(rows)
                ))
            except Exception as exc:
                # Claim-side failure only (per-account failures never reach
                # here -- _refresh_one swallows them). Roll back and idle.
                logger.error("refresh_worker: claim failed: %r", exc)
                with contextlib.suppress(Exception):
                    await conn.rollback()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=config.REFRESH_IDLE_SECONDS)
    finally:
        for w in workers:
            with contextlib.suppress(Exception):
                await w.close()


# The hourly safety-net sweep was REMOVED (2026-07-17, founder decision):
# with failed lookups durably re-enqueued (see _requeue_failed), staleness-
# bounded read refreshes (api._ensure_known), name-agnostic trigger
# scanning (catalog.touched_accounts), and the unrecognized-event alarm
# (catalog._check_unknown_event), the sweep's only remaining job was
# refreshing accounts nobody queries -- work with no beneficiary. All
# correction now scales with actual usage and actual failures, never with
# fleet size.

# -- market -------------------------------------------------------------------

async def refresh_market(
    conn: psycopg.AsyncConnection, nodes: HENodes, symbol: str,
) -> int:
    """Full mirror of one symbol's open sell orders -- small relative to
    total supply even for huge collections (most instances aren't listed
    for sale at any given time), so unlike instances this is refreshed in
    full per symbol, not lazily per account."""
    orders = []
    offset = 0
    for _ in range(config.MARKET_MAX_PAGES):
        try:
            page = await nodes.find(
                "nftmarket", f"{symbol}sellBook", {}, limit=1000, offset=offset,
            )
        except Exception as exc:
            # HE caps `offset` (~10k, verified live: it 400s beyond that), so a
            # sellBook larger than the cap simply cannot be fully paginated.
            # Stop and use the orders gathered so far -- a best-effort floor
            # from a partial book beats abandoning the symbol entirely (which
            # is what letting this propagate did: the whole symbol got rolled
            # back and showed no market data at all). Floor is exact for any
            # book within the cap -- the large majority -- and approximate only
            # for the handful of mega-collections above it.
            logger.warning(
                "refresh_market(%s): pagination stopped at offset %d: %r",
                symbol, offset, exc,
            )
            break
        if not page:
            break
        orders.extend(page)
        if len(page) < 1000:
            break
        offset += 1000
    await conn.execute("DELETE FROM market_orders WHERE symbol = %s", (symbol,))
    if orders:
        async with conn.cursor() as cur:
            await cur.executemany(
                "INSERT INTO market_orders (symbol, nft_id, account, owned_by, "
                "price, price_symbol, fee, ts, grouping) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (symbol, nft_id) DO NOTHING",
                [
                    (symbol, o["nftId"], o["account"], o.get("ownedBy", "u"),
                     o["price"], o["priceSymbol"], o.get("fee"),
                     _epoch_ms_to_ts(o.get("timestamp")),
                     Jsonb(o.get("grouping") or {}))
                    for o in orders
                ],
            )
    await conn.execute("DELETE FROM market_rollups WHERE symbol = %s", (symbol,))
    # Floor per (payment token, group). For an ungrouped collection every
    # order's grouping is {}, so this collapses to one row per token -- the
    # plain symbol-wide floor -- with no special-casing.
    await conn.execute(
        "INSERT INTO market_rollups "
        "(symbol, price_symbol, grouping, grouping_key, floor_price, open_orders) "
        "SELECT symbol, price_symbol, grouping, grouping::text, min(price), count(*) "
        "FROM market_orders WHERE symbol = %s "
        "GROUP BY symbol, price_symbol, grouping",
        (symbol,),
    )
    # HE's `nft` catalog has no market flag (verified: the collection row
    # simply doesn't carry one), so market-enablement is derived here from
    # whether the symbol actually has an open sellBook.
    await conn.execute(
        "UPDATE collections SET market_enabled = %s WHERE symbol = %s",
        (bool(orders), symbol),
    )
    await conn.commit()
    return len(orders)


async def refresh_dirty_markets(conn: psycopg.AsyncConnection, nodes: HENodes) -> int:
    """Refresh only the symbols the block-watcher flagged dirty (a live
    market event landed), draining market_refresh_queue. Event-driven, so
    steady-state market work scales with trading activity instead of
    re-polling every cached symbol's full sellBook on a blind timer. A
    slow full sweep (refresh_all_markets) still runs as a backstop for
    anything a missed event would leave stale. One symbol's failure leaves
    it queued (rolled back, not deleted) for the next cycle."""
    rows = await (await conn.execute(
        "SELECT symbol, queued_at FROM market_refresh_queue ORDER BY queued_at"
    )).fetchall()
    await conn.rollback()  # release the read snapshot (autocommit=False)
    for row in rows:
        try:
            await refresh_market(conn, nodes, row["symbol"])  # commits on success
            # Delete only if NO newer market event re-flagged this symbol
            # while we were refreshing -- an unconditional delete would drop
            # that event's book change until the hourly full sweep. A newer
            # event's ON CONFLICT bumps queued_at, so this leaves the row for
            # the next cycle.
            await conn.execute(
                "DELETE FROM market_refresh_queue "
                "WHERE symbol = %s AND queued_at <= %s",
                (row["symbol"], row["queued_at"]))
            await conn.commit()
        except Exception as exc:
            logger.warning("refresh_dirty_markets(%s) failed: %r", row["symbol"], exc)
            await conn.rollback()  # symbol stays queued -> retried next cycle
    return len(rows)


async def refresh_all_markets(conn: psycopg.AsyncConnection, nodes: HENodes) -> None:
    # Scope to symbols someone has actually queried (same lazy-cache
    # principle as instances): a symbol shows up here once it's in the cache,
    # not eagerly for all ~150 catalog entries. NOT gated on
    # collections.market_enabled -- that flag is derived *by* this loop, so
    # gating on it would be circular and (on a fresh DB) always empty, which
    # is exactly the bug that left every market table empty.
    rows = await (await conn.execute(
        "SELECT DISTINCT symbol FROM instances ORDER BY symbol"
    )).fetchall()
    for row in rows:
        try:
            await refresh_market(conn, nodes, row["symbol"])
        except Exception as exc:
            logger.warning("refresh_market(%s) failed: %r", row["symbol"], exc)
            # see refresh_worker's own rollback -- a failed statement
            # poisons this connection for every symbol still left in this
            # loop (and the next scheduled run) until rolled back
            await conn.rollback()
