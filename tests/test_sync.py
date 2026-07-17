"""sync.py: cache population against Postgres, HE calls stubbed with a fake
node (duck-typed -- sync.py takes `nodes` as a plain parameter, no HENodes
subclassing needed)."""

import asyncio
import os

import pytest

from henftdir import config, db, sync

TEST_DSN = os.environ.get("HENFT_TEST_DSN")
pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="HENFT_TEST_DSN not set (needs PostgreSQL)"
)


async def fresh_conn(schema: str):
    admin = await db.connect(TEST_DSN)
    await admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    await admin.execute(f"CREATE SCHEMA {schema}")
    await admin.commit()
    await admin.close()
    conn = await db.connect(f"{TEST_DSN} options='-c search_path={schema}'")
    await db.apply_schema(conn)
    return conn


def _connect(schema: str):
    """A connection factory bound to a test schema -- what refresh_worker
    uses to open its per-slot worker connections."""
    return lambda: db.connect(f"{TEST_DSN} options='-c search_path={schema}'")


async def _run_worker(schema, nodes, stop, batch=None):
    """Run refresh_worker with its own control connection (never the test's
    assertion/poller connection) + the per-slot factory. Closes the control
    conn when the worker returns."""
    ctrl = await db.connect(f"{TEST_DSN} options='-c search_path={schema}'")
    try:
        await sync.refresh_worker(ctrl, _connect(schema), nodes, stop, batch=batch)
    finally:
        await ctrl.close()


class FakeNodes:
    """find() keyed by (contract, table) -> list of pages (each call pops
    the next page; a missing key means "no more results"). A table listed
    in `fail` raises instead of returning, simulating a transient HE
    failure for that specific lookup."""

    def __init__(self, pages: dict[tuple[str, str], list[list[dict]]], fail: set = frozenset()):
        self._pages = {k: list(v) for k, v in pages.items()}
        self._fail = set(fail)
        self.calls = []

    async def find(self, contract, table, query, limit=1000, offset=0, indexes=None):
        self.calls.append((contract, table, query, offset))
        if (contract, table) in self._fail:
            raise RuntimeError(f"simulated failure for {contract}/{table}")
        pages = self._pages.get((contract, table))
        if not pages:
            return []
        return pages.pop(0)

    async def find_one(self, contract, table, query):
        rows = await self.find(contract, table, query)
        return rows[0] if rows else None


# -- refresh_catalog -----------------------------------------------------------

async def test_refresh_catalog_upserts_and_paginates():
    conn = await fresh_conn("sync_catalog")
    try:
        page1 = [{"symbol": f"S{i}", "name": f"Coll {i}",
                  "metadata": '{"url":"https://x"}', "supply": i,
                  "circulatingSupply": i} for i in range(1000)]
        page2 = [{"symbol": "LAST", "name": "Last One",
                  "metadata": "{}", "supply": 5, "circulatingSupply": 3,
                  "marketEnabled": True}]
        nodes = FakeNodes({("nft", "nfts"): [page1, page2]})
        n = await sync.refresh_catalog(conn, nodes)
        assert n == 1001
        row = await (await conn.execute(
            "SELECT * FROM collections WHERE symbol = 'LAST'"
        )).fetchone()
        assert row["supply"] == 5 and row["circulating_supply"] == 3
        assert row["market_enabled"] is True
        row0 = await (await conn.execute(
            "SELECT url FROM collections WHERE symbol = 'S0'"
        )).fetchone()
        assert row0["url"] == "https://x"
    finally:
        await conn.close()


# -- refresh_account -------------------------------------------------------------

async def test_refresh_account_populates_instances_across_symbols():
    conn = await fresh_conn("sync_refresh")
    try:
        nodes = FakeNodes({
            ("nft", "CARDinstances"): [[{"_id": 1, "account": "alice", "ownedBy": "u",
                                         "properties": {"Name": "Dragon"}}]],
            ("nft", "OTHERinstances"): [[]],
        })
        n = await sync.refresh_account(conn, nodes, "alice", ["CARD", "OTHER"])
        assert n == 1
        rows = await (await conn.execute(
            "SELECT * FROM instances WHERE account = 'alice'"
        )).fetchall()
        assert len(rows) == 1 and rows[0]["symbol"] == "CARD"
        known = await (await conn.execute(
            "SELECT 1 FROM known_accounts WHERE account = 'alice'"
        )).fetchone()
        assert known is not None
    finally:
        await conn.close()


async def test_refresh_account_handles_object_shaped_delegation():
    """HE returns `delegatedTo` as an object ({account, ownedBy}), not a
    string -- found live crashing the whole refresh for delegated holders.
    The account/type must be pulled out into the text columns."""
    conn = await fresh_conn("sync_delegation")
    try:
        nodes = FakeNodes({
            ("nft", "LANDinstances"): [[
                {"_id": 5, "account": "alice", "ownedBy": "u",
                 "delegatedTo": {"account": "gameco", "ownedBy": "u"},
                 "properties": {"Name": "Plot"}},
            ]],
        })
        n = await sync.refresh_account(conn, nodes, "alice", ["LAND"])
        assert n == 1
        row = await (await conn.execute(
            "SELECT delegated_to, delegated_to_type FROM instances "
            "WHERE symbol = 'LAND' AND nft_id = 5"
        )).fetchone()
        assert row["delegated_to"] == "gameco"
        assert row["delegated_to_type"] == "u"
    finally:
        await conn.close()


async def test_refresh_account_skips_marking_known_when_catalog_is_empty():
    """A narrow but real race: right after a fresh deploy, the catalog
    sweep may not have populated `collections` yet. An account queried in
    that window must not get marked known/refreshed with zero symbols
    actually checked -- that would look like a permanently confirmed-empty
    account to every later caller."""
    conn = await fresh_conn("sync_emptycatalog")
    try:
        nodes = FakeNodes({})
        n = await sync.refresh_account(conn, nodes, "alice", [])
        assert n == 0
        assert nodes.calls == []
        known = await (await conn.execute(
            "SELECT 1 FROM known_accounts WHERE account = 'alice'"
        )).fetchone()
        assert known is None
    finally:
        await conn.close()


async def test_refresh_account_replaces_stale_holdings():
    """A previously-cached instance the account no longer holds (transferred
    away or burned -- HE's own find() simply stops returning it) must
    disappear from our mirror, not linger."""
    conn = await fresh_conn("sync_stale")
    try:
        await conn.execute(
            "INSERT INTO instances (symbol, nft_id, account, owned_by) "
            "VALUES ('CARD', 1, 'alice', 'u')"
        )
        await conn.commit()
        nodes = FakeNodes({("nft", "CARDinstances"): [[]]})
        await sync.refresh_account(conn, nodes, "alice", ["CARD"])
        rows = await (await conn.execute(
            "SELECT * FROM instances WHERE account = 'alice'"
        )).fetchall()
        assert rows == []
    finally:
        await conn.close()


async def test_refresh_account_upserts_across_a_transfer():
    """Found live: a plain INSERT crashed the whole sync service with
    UniqueViolation. The primary key is (symbol, nft_id), not including
    account, since an instance has exactly one owner -- but the pre-refresh
    DELETE only clears *this* account's stale rows. If the instance was
    transferred in from an account we haven't refreshed yet, its stale row
    (still crediting the old owner) occupies the same key and collides on
    a plain INSERT. Must upsert instead."""
    conn = await fresh_conn("sync_transfer")
    try:
        await conn.execute(
            "INSERT INTO instances (symbol, nft_id, account, owned_by) "
            "VALUES ('CARD', 1, 'oldowner', 'u')"
        )
        await conn.commit()
        nodes = FakeNodes({
            ("nft", "CARDinstances"): [[{"_id": 1, "account": "newowner",
                                          "ownedBy": "u", "properties": {}}]],
        })
        n = await sync.refresh_account(conn, nodes, "newowner", ["CARD"])
        assert n == 1
        rows = await (await conn.execute(
            "SELECT account FROM instances WHERE symbol = 'CARD' AND nft_id = 1"
        )).fetchall()
        assert len(rows) == 1 and rows[0]["account"] == "newowner"
    finally:
        await conn.close()


async def test_refresh_worker_continues_after_a_failure(monkeypatch):
    """A failed refresh must not poison the connection for the next queued
    account -- found live: without a rollback, the except block's own
    cleanup query failed with InFailedSqlTransaction, an unhandled
    exception distinct from the one being handled, crashing the whole
    service (including the unrelated, already-isolated block-watcher)."""
    conn = await fresh_conn("sync_workerfail")
    try:
        await conn.execute("INSERT INTO collections (symbol) VALUES ('CARD')")
        await conn.execute(
            "INSERT INTO refresh_queue (account) VALUES ('alice'), ('bob')"
        )
        await conn.commit()

        calls = []
        real_refresh_account = sync.refresh_account

        async def flaky_refresh(conn, nodes, account, symbols, **kw):
            calls.append(account)
            if account == "alice":
                raise RuntimeError("simulated failure")
            return await real_refresh_account(conn, nodes, account, symbols, **kw)

        monkeypatch.setattr(sync, "refresh_account", flaky_refresh)
        nodes = FakeNodes({("nft", "CARDinstances"): [[]]})
        stop = asyncio.Event()

        async def stop_once_drained():
            # done when bob's row is gone and alice's has been pushed into
            # backoff (rescheduled, NOT deleted -- deleting lost the work)
            for _ in range(100):
                pending = await (await conn.execute(
                    "SELECT count(*) AS n FROM refresh_queue "
                    "WHERE not_before <= now()")).fetchone()
                if pending["n"] == 0:
                    break
                await asyncio.sleep(0.01)
            stop.set()

        await asyncio.gather(
            _run_worker("sync_workerfail", nodes, stop, batch=2), stop_once_drained(),
        )
        assert set(calls) == {"alice", "bob"}  # bob still processed after alice's failure
        row = await (await conn.execute(
            "SELECT account, attempts, not_before > now() AS deferred "
            "FROM refresh_queue")).fetchone()
        # alice's failed row survives as a backoff-deferred retry
        assert row["account"] == "alice"
        assert row["attempts"] == 1 and row["deferred"] is True
    finally:
        await conn.close()




async def test_refresh_account_does_not_wipe_holdings_for_a_failed_symbol():
    """Found live: a transient HE failure on one symbol (a real 503/429
    under cold-fetch burst load, not a real outage -- the exact same query
    succeeded instantly once isolated) must not be treated as "confirmed
    this account owns nothing here" -- a real account's real 448 STAR
    instances vanished from the response the moment STAR's lookup failed.
    A failed symbol must leave existing holdings untouched."""
    conn = await fresh_conn("sync_failkeep")
    try:
        await conn.execute(
            "INSERT INTO instances (symbol, nft_id, account, owned_by) "
            "VALUES ('STAR', 1, 'alice', 'u')"
        )
        await conn.commit()
        nodes = FakeNodes(
            {("nft", "CARDinstances"): [[{"_id": 2, "account": "alice",
                                           "ownedBy": "u", "properties": {}}]]},
            fail={("nft", "STARinstances")},
        )
        n = await sync.refresh_account(conn, nodes, "alice", ["CARD", "STAR"])
        assert n == 1  # only CARD's confirmed row counted
        rows = await (await conn.execute(
            "SELECT symbol, nft_id FROM instances WHERE account = 'alice' ORDER BY symbol"
        )).fetchall()
        # STAR's pre-existing row survives (failed, not confirmed empty);
        # CARD's freshly-confirmed row is there too.
        assert [(r["symbol"], r["nft_id"]) for r in rows] == [("CARD", 2), ("STAR", 1)]
    finally:
        await conn.close()


async def test_fetch_symbol_returns_none_on_failure_without_retrying_itself():
    """No extra retry here -- HENodes.call() already retries internally.
    Found live: a second retry layer on top of that just doubles how long
    a struggling symbol (and the cooldown it triggers for others) lasts
    under sustained contention, without improving the odds of success."""
    calls = {"n": 0}

    class AlwaysFails:
        async def find(self, contract, table, query, limit=1000, offset=0, indexes=None):
            calls["n"] += 1
            raise RuntimeError("transient")

    result = await sync._fetch_symbol_for_account(AlwaysFails(), "CARD", "alice")
    assert calls["n"] == 1
    assert result is None


async def test_refresh_account_dequeues() -> None:
    conn = await fresh_conn("sync_dequeue")
    try:
        await conn.execute("INSERT INTO refresh_queue (account) VALUES ('alice')")
        await conn.commit()
        nodes = FakeNodes({("nft", "CARDinstances"): [[]]})
        await sync.refresh_account(conn, nodes, "alice", ["CARD"])
        row = await (await conn.execute(
            "SELECT 1 FROM refresh_queue WHERE account = 'alice'"
        )).fetchone()
        assert row is None
    finally:
        await conn.close()


async def test_refresh_account_requeues_failed_symbols_with_backoff():
    """A failed lookup must never be silently dropped: with the safety-net
    sweep removed, the durable retry row IS the guarantee that the symbol
    gets re-checked. The retry row carries attempts > 0 and a not_before
    in the future, so the worker leaves it alone until the window opens."""
    conn = await fresh_conn("sync_requeue")
    try:
        nodes = FakeNodes(
            {("nft", "CARDinstances"): [[]]},
            fail={("nft", "STARinstances")},
        )
        await sync.refresh_account(conn, nodes, "alice", ["CARD", "STAR"])
        row = await (await conn.execute(
            "SELECT symbol, attempts, not_before > now() AS deferred "
            "FROM refresh_queue WHERE account = 'alice'"
        )).fetchone()
        assert row["symbol"] == "STAR"
        assert row["attempts"] == 1
        assert row["deferred"] is True
    finally:
        await conn.close()


async def test_requeue_backoff_grows_with_attempts():
    conn = await fresh_conn("sync_backoff")
    try:
        for _ in range(3):
            await sync._requeue_failed(conn, "alice", ["STAR"])
        await conn.commit()
        row = await (await conn.execute(
            "SELECT attempts, not_before > now() + interval '3 minutes' AS grew "
            "FROM refresh_queue WHERE account = 'alice' AND symbol = 'STAR'"
        )).fetchone()
        # attempts 1 -> 2 -> 3; third upsert schedules base * 2^2 = 4 min out
        assert row["attempts"] == 3
        assert row["grew"] is True
    finally:
        await conn.close()


# -- refresh_worker ---------------------------------------------------------------

async def test_refresh_worker_skips_rows_still_in_backoff():
    """A queue row whose not_before window hasn't opened is invisible to
    the worker -- it must idle rather than hot-loop on a struggling
    symbol while nodes are down."""
    conn = await fresh_conn("sync_backoffgate")
    try:
        await conn.execute("INSERT INTO collections (symbol) VALUES ('CARD')")
        await conn.execute(
            "INSERT INTO refresh_queue (account, symbol, attempts, not_before) "
            "VALUES ('alice', 'CARD', 1, now() + interval '1 hour')"
        )
        await conn.commit()
        nodes = FakeNodes({("nft", "CARDinstances"): [[]]})
        stop = asyncio.Event()

        async def stop_soon():
            await asyncio.sleep(0.3)
            stop.set()

        await asyncio.gather(_run_worker("sync_backoffgate", nodes, stop), stop_soon())
        assert nodes.calls == []  # never touched: row is in backoff
        row = await (await conn.execute(
            "SELECT 1 FROM refresh_queue WHERE account = 'alice'"
        )).fetchone()
        assert row is not None  # and still queued, not lost
    finally:
        await conn.close()


async def test_refresh_worker_drains_queue_then_idles():
    conn = await fresh_conn("sync_worker")
    try:
        await conn.execute("INSERT INTO collections (symbol) VALUES ('CARD')")
        await conn.execute("INSERT INTO refresh_queue (account) VALUES ('alice')")
        await conn.commit()
        nodes = FakeNodes({("nft", "CARDinstances"): [[{"_id": 9, "account": "alice",
                                                          "ownedBy": "u", "properties": {}}]]})
        stop = asyncio.Event()

        async def stop_once_drained():
            for _ in range(50):
                row = await (await conn.execute(
                    "SELECT 1 FROM refresh_queue"
                )).fetchone()
                if row is None:
                    break
                await asyncio.sleep(0.01)
            stop.set()

        await asyncio.gather(
            _run_worker("sync_worker", nodes, stop),
            stop_once_drained(),
        )
        rows = await (await conn.execute("SELECT * FROM instances")).fetchall()
        assert len(rows) == 1 and rows[0]["nft_id"] == 9
    finally:
        await conn.close()


async def test_refresh_worker_caches_known_symbols_across_queued_accounts(monkeypatch):
    """known_symbols() must not be re-queried once per queued account --
    the catalog rarely changes, so paying for that query on every single
    item in a busy queue is pure overhead. Refreshed only when the worker
    goes idle (queue empty), not mid-burst."""
    conn = await fresh_conn("sync_workercache")
    try:
        await conn.execute("INSERT INTO collections (symbol) VALUES ('CARD')")
        await conn.execute(
            "INSERT INTO refresh_queue (account) VALUES ('alice'), ('bob'), ('carol')"
        )
        await conn.commit()

        calls = {"n": 0}
        real_known_symbols = sync.known_symbols

        async def counting_known_symbols(c):
            calls["n"] += 1
            return await real_known_symbols(c)

        monkeypatch.setattr(sync, "known_symbols", counting_known_symbols)
        nodes = FakeNodes({("nft", "CARDinstances"): [[]]})
        stop = asyncio.Event()

        async def stop_once_drained():
            for _ in range(100):
                row = await (await conn.execute("SELECT 1 FROM refresh_queue")).fetchone()
                if row is None:
                    break
                await asyncio.sleep(0.01)
            stop.set()

        await asyncio.gather(
            _run_worker("sync_workercache", nodes, stop),
            stop_once_drained(),
        )
        # 1 initial fetch, at most 1 more on first empty-queue detection --
        # never one per queued account (which would be 3+ here).
        assert calls["n"] <= 2
    finally:
        await conn.close()




# -- market ---------------------------------------------------------------------

async def test_refresh_market_mirrors_sellbook_and_rollups():
    conn = await fresh_conn("sync_market")
    try:
        await conn.execute("INSERT INTO collections (symbol) VALUES ('CARD')")
        await conn.commit()
        nodes = FakeNodes({
            ("nftmarket", "CARDsellBook"): [[
                {"_id": 1, "nftId": "1", "account": "bob", "ownedBy": "u",
                 "price": "10.5", "priceSymbol": "SWAP.HIVE", "fee": 500,
                 "timestamp": 123, "grouping": {"rarity": "common"}},
                {"_id": 2, "nftId": "2", "account": "bob", "ownedBy": "u",
                 "price": "8", "priceSymbol": "SWAP.HIVE", "fee": 500,
                 "timestamp": 124, "grouping": {"rarity": "common"}},
                {"_id": 3, "nftId": "3", "account": "bob", "ownedBy": "u",
                 "price": "50", "priceSymbol": "SWAP.HIVE", "fee": 500,
                 "timestamp": 125, "grouping": {"rarity": "legendary"}},
            ]],
        })
        n = await sync.refresh_market(conn, nodes, "CARD")
        assert n == 3
        # floor is per group, not symbol-wide: common floor is 8, legendary 50
        rollups = await (await conn.execute(
            "SELECT grouping, floor_price, open_orders FROM market_rollups "
            "WHERE symbol = 'CARD' ORDER BY floor_price"
        )).fetchall()
        assert len(rollups) == 2
        assert rollups[0]["grouping"] == {"rarity": "common"}
        assert rollups[0]["floor_price"] == 8 and rollups[0]["open_orders"] == 2
        assert rollups[1]["grouping"] == {"rarity": "legendary"}
        assert rollups[1]["floor_price"] == 50 and rollups[1]["open_orders"] == 1
        # market_enabled is derived from the presence of an open sellBook
        me = await (await conn.execute(
            "SELECT market_enabled FROM collections WHERE symbol = 'CARD'"
        )).fetchone()
        assert me["market_enabled"] is True
    finally:
        await conn.close()


async def test_refresh_market_ungrouped_collapses_to_one_row():
    """Orders with no grouping all fall under {} -- one rollup row per token,
    i.e. the plain symbol-wide floor, no special-casing."""
    conn = await fresh_conn("sync_market_ungrouped")
    try:
        nodes = FakeNodes({
            ("nftmarket", "PLAINsellBook"): [[
                {"_id": 1, "nftId": "1", "account": "bob", "ownedBy": "u",
                 "price": "3", "priceSymbol": "SWAP.HIVE", "fee": 0, "timestamp": 1},
                {"_id": 2, "nftId": "2", "account": "bob", "ownedBy": "u",
                 "price": "9", "priceSymbol": "SWAP.HIVE", "fee": 0, "timestamp": 2},
            ]],
        })
        await sync.refresh_market(conn, nodes, "PLAIN")
        rollups = await (await conn.execute(
            "SELECT grouping, floor_price FROM market_rollups WHERE symbol = 'PLAIN'"
        )).fetchall()
        assert len(rollups) == 1
        assert rollups[0]["grouping"] == {} and rollups[0]["floor_price"] == 3
    finally:
        await conn.close()


async def test_refresh_all_markets_selects_symbols_with_instances():
    """Market refresh is scoped to symbols that have cached instances -- NOT
    gated on collections.market_enabled (which this loop derives, so gating on
    it would be circular and, on a fresh DB, always empty)."""
    conn = await fresh_conn("sync_allmarkets")
    try:
        # HELD has a cached instance; UNHELD is only in the catalog.
        await conn.execute(
            "INSERT INTO collections (symbol) VALUES ('HELD'), ('UNHELD')")
        await conn.execute(
            "INSERT INTO instances (symbol, nft_id, account, owned_by) "
            "VALUES ('HELD', 1, 'carol', 'u')")
        await conn.commit()
        nodes = FakeNodes({
            ("nftmarket", "HELDsellBook"): [[
                {"_id": 1, "nftId": "1", "account": "bob", "ownedBy": "u",
                 "price": "2", "priceSymbol": "SWAP.HIVE", "fee": 0, "timestamp": 1},
            ]],
        })
        await sync.refresh_all_markets(conn, nodes)
        # only HELD's sellBook was fetched
        tables = {c[1] for c in nodes.calls}
        assert tables == {"HELDsellBook"}
        rows = await (await conn.execute(
            "SELECT symbol FROM market_rollups")).fetchall()
        assert [r["symbol"] for r in rows] == ["HELD"]
    finally:
        await conn.close()


async def test_refresh_market_degrades_on_offset_error():
    """HE 400s past its ~10k offset cap. A large sellBook must degrade to a
    best-effort floor from what was fetched, not abandon the whole symbol."""
    conn = await fresh_conn("sync_market_offseterr")
    try:
        class OffsetCapNodes:
            def __init__(self):
                self.calls = 0
            async def find(self, contract, table, query, limit=1000, offset=0, indexes=None):
                self.calls += 1
                if offset == 0:
                    return [{"_id": i, "nftId": str(i), "account": "bob",
                             "ownedBy": "u", "price": str(i + 1),
                             "priceSymbol": "SWAP.HIVE", "fee": 0, "timestamp": 0}
                            for i in range(1000)]
                raise RuntimeError("400 Invalid request")  # offset cap hit
        nodes = OffsetCapNodes()
        n = await sync.refresh_market(conn, nodes, "BIG")
        assert n == 1000   # kept the first page instead of crashing
        rollup = await (await conn.execute(
            "SELECT floor_price, open_orders FROM market_rollups WHERE symbol = 'BIG'"
        )).fetchone()
        assert rollup["floor_price"] == 1 and rollup["open_orders"] == 1000
    finally:
        await conn.close()


async def test_refresh_market_caps_pagination(monkeypatch):
    """A defensive cap, mirroring the retired HISTORY_MAX_PAGES lesson: an
    unbounded paginator against a pathological sell book is a real risk."""
    conn = await fresh_conn("sync_marketcap")
    try:
        monkeypatch.setattr(config, "MARKET_MAX_PAGES", 2)
        full_page = [{"_id": i, "nftId": str(i), "account": "bob", "ownedBy": "u",
                      "price": "1", "priceSymbol": "SWAP.HIVE", "fee": 0,
                      "timestamp": 0} for i in range(1000)]
        nodes = FakeNodes({("nftmarket", "CARDsellBook"): [full_page, full_page, full_page]})
        await sync.refresh_market(conn, nodes, "CARD")
        assert len(nodes.calls) == 2
    finally:
        await conn.close()


async def test_targeted_refresh_does_not_reset_staleness_clock():
    """refreshed_at is what the read-staleness bound measures against, so
    it must mean "last FULL pass". If targeted touches bumped it, an
    account with frequent activity in one collection would never trip its
    periodic full re-check while every other symbol drifted."""
    conn = await fresh_conn("sync_staleclock")
    try:
        await conn.execute("INSERT INTO collections (symbol) VALUES ('CARD')")
        await conn.execute(
            "INSERT INTO known_accounts (account, refreshed_at) "
            "VALUES ('alice', now() - interval '2 days')"
        )
        await conn.commit()
        nodes = FakeNodes({("nft", "CARDinstances"): [[], []]})
        await sync.refresh_account(conn, nodes, "alice", ["CARD"],
                                   dequeue_symbols=["CARD"])  # targeted
        row = await (await conn.execute(
            "SELECT refreshed_at < now() - interval '1 day' AS still_old "
            "FROM known_accounts WHERE account = 'alice'")).fetchone()
        assert row["still_old"] is True  # targeted pass left the clock alone
        await sync.refresh_account(conn, nodes, "alice", ["CARD"])  # full
        row = await (await conn.execute(
            "SELECT refreshed_at > now() - interval '1 minute' AS fresh "
            "FROM known_accounts WHERE account = 'alice'")).fetchone()
        assert row["fresh"] is True  # full pass resets it
    finally:
        await conn.close()


async def test_refresh_worker_does_not_linger_idle_in_transaction():
    """Regression (live 2026-07-17): the worker connect is autocommit=False,
    so the read-only idle branch (empty queue -> known_symbols -> wait) left
    a transaction open for the whole idle period, pinning its MVCC snapshot.
    The next poll reused that stale snapshot and never saw rows inserted by
    other connections (block-watcher/API) -- the queue wedged and only grew.
    After idling on an empty queue the worker must hold NO open transaction,
    and must then see a row a SECOND connection inserts afterward."""
    from psycopg.pq import TransactionStatus

    conn = await fresh_conn("sync_idletxn")
    try:
        await conn.execute("INSERT INTO collections (symbol) VALUES ('CARD')")
        await conn.commit()
        nodes = FakeNodes({("nft", "CARDinstances"): [[]]})
        stop = asyncio.Event()
        seen_idle = asyncio.Event()
        # `conn` is the worker's CONTROL connection here (so we can inspect
        # its transaction state); the observer touches the DB only through a
        # SEPARATE connection, never `conn`, while the worker is running.
        obs = await db.connect(f"{TEST_DSN} options='-c search_path=sync_idletxn'")

        async def observe_then_feed():
            # let the worker poll the empty queue and enter its idle wait
            await asyncio.sleep(0.3)
            # the fix: control conn is not left mid-transaction while idling
            assert conn.info.transaction_status == TransactionStatus.IDLE
            seen_idle.set()
            # work inserted by a DIFFERENT connection AFTER the worker idled
            await obs.execute(
                "INSERT INTO refresh_queue (account, symbol) VALUES ('late', 'CARD')")
            await obs.commit()
            # worker must pick it up (a stale pinned snapshot never would)
            for _ in range(100):
                left = await (await obs.execute(
                    "SELECT count(*) AS n FROM refresh_queue")).fetchone()
                await obs.rollback()
                if left["n"] == 0:
                    break
                await asyncio.sleep(0.05)
            stop.set()

        try:
            await asyncio.gather(
                sync.refresh_worker(conn, _connect("sync_idletxn"), nodes, stop),
                observe_then_feed())
            assert seen_idle.is_set()
            left = await (await obs.execute(
                "SELECT count(*) AS n FROM refresh_queue")).fetchone()
            assert left["n"] == 0  # the late-inserted row was drained
        finally:
            await obs.close()
    finally:
        await conn.close()


async def test_refresh_worker_drains_accounts_concurrently():
    """Account-level parallelism: a batch of queued accounts must refresh
    concurrently (each on its own connection), not serially. We gate each
    account's HE lookup on a shared barrier that only releases once all
    `batch` accounts are in-flight at once -- if the worker were serial it
    would deadlock on the barrier and the test would time out."""
    conn = await fresh_conn("sync_parallel")
    try:
        await conn.execute("INSERT INTO collections (symbol) VALUES ('CARD')")
        accounts = [f"acct{i}" for i in range(4)]
        for a in accounts:
            await conn.execute(
                "INSERT INTO refresh_queue (account, symbol) VALUES (%s, 'CARD')", (a,))
        await conn.commit()

        in_flight = 0
        all_in_flight = asyncio.Event()

        class BarrierNodes:
            def __init__(self): self.calls = []
            async def find(self, contract, table, query, limit=1000, offset=0, indexes=None):
                nonlocal in_flight
                self.calls.append(account := query.get("account"))
                in_flight += 1
                if in_flight >= len(accounts):
                    all_in_flight.set()
                # block until every account's lookup is concurrently in-flight
                await asyncio.wait_for(all_in_flight.wait(), timeout=5)
                return []
            async def find_one(self, contract, table, query):
                return None

        nodes = BarrierNodes()
        stop = asyncio.Event()

        async def stop_once_drained():
            for _ in range(200):
                row = await (await conn.execute("SELECT 1 FROM refresh_queue")).fetchone()
                if row is None:
                    break
                await asyncio.sleep(0.02)
            stop.set()

        # batch >= number of accounts so they can all be claimed together
        await asyncio.gather(
            _run_worker("sync_parallel", nodes, stop, batch=4),
            stop_once_drained(),
        )
        # barrier only releases if all 4 were concurrently in-flight
        assert all_in_flight.is_set()
        assert sorted(nodes.calls) == accounts
        left = await (await conn.execute("SELECT count(*) AS n FROM refresh_queue")).fetchone()
        assert left["n"] == 0
    finally:
        await conn.close()
