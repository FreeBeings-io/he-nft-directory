"""blockwatch.py: process_block/queue_refresh against Postgres; run()'s
catch-up loop against a fake node stub (no real network -- the block
shapes used here mirror real HE blocks observed live)."""

import asyncio
import json
import os

import pytest

from henftdir import blockwatch, config, db

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


def tx(contract, sender, action="x", payload=None, events=None):
    logs = {"events": events} if events else {}
    return {"contract": contract, "action": action, "sender": sender,
            "payload": json.dumps(payload or {}), "logs": json.dumps(logs)}


async def test_process_block_queues_only_nft_contract_accounts():
    conn = await fresh_conn("bw_filter")
    try:
        block = {"transactions": [
            tx("nft", "alice"),
            tx("tokens", "eve"),   # not nft/nftmarket -- ignored
            tx("nftmarket", "bob"),
        ]}
        n = await blockwatch.process_block(conn, block)
        await conn.commit()
        assert n == 2
        rows = await (await conn.execute(
            "SELECT account FROM refresh_queue ORDER BY account"
        )).fetchall()
        assert [r["account"] for r in rows] == ["alice", "bob"]
    finally:
        await conn.close()


async def test_process_block_records_market_sales():
    conn = await fresh_conn("bw_sales")
    try:
        buy = tx("nftmarket", "carol", action="buy", events=[{
            "contract": "nftmarket", "event": "hitSellOrder",
            "data": {"symbol": "CARD", "priceSymbol": "SWAP.HIVE",
                     "account": "carol", "sellers": [
                         {"account": "bob", "nftSales": [
                             {"id": "7", "price": "9", "fee": 0, "symbol": "SWAP.HIVE"}]}]},
        }])
        block = {"blockNumber": 555,
                 "timestamp": "2026-07-08T12:00:00",
                 "transactions": [buy]}
        await blockwatch.process_block(conn, block)
        await conn.commit()
        rows = await (await conn.execute(
            "SELECT symbol, nft_id, price, seller, buyer, he_block FROM market_sales"
        )).fetchall()
        assert len(rows) == 1
        r = rows[0]
        assert (r["symbol"], r["nft_id"], r["seller"], r["buyer"], r["he_block"]) \
            == ("CARD", 7, "bob", "carol", 555)
        assert r["price"] == 9
    finally:
        await conn.close()


async def test_record_sales_is_idempotent_on_reprocess():
    """Replaying a block on restart must not double-count a sale."""
    conn = await fresh_conn("bw_sales_idem")
    try:
        sale = {"symbol": "CARD", "nft_id": 7, "price": "9",
                "price_symbol": "SWAP.HIVE", "seller": "bob", "buyer": "carol",
                "he_block": 555, "ts": "2026-07-08T12:00:00+00:00"}
        await blockwatch.record_sales(conn, [sale])
        await blockwatch.record_sales(conn, [sale])   # same block again
        await conn.commit()
        n = await (await conn.execute("SELECT count(*) AS n FROM market_sales")).fetchone()
        assert n["n"] == 1
    finally:
        await conn.close()


async def test_queue_refresh_dedupes():
    conn = await fresh_conn("bw_dedupe")
    try:
        await blockwatch.queue_refresh(conn, {"alice"})
        await blockwatch.queue_refresh(conn, {"alice", "bob"})
        await conn.commit()
        rows = await (await conn.execute("SELECT account FROM refresh_queue")).fetchall()
        assert sorted(r["account"] for r in rows) == ["alice", "bob"]
    finally:
        await conn.close()


class FakeNodes:
    def __init__(self, head: int, blocks: dict[int, dict]):
        self.head = head
        self.blocks = blocks
        self.requested: list[int] = []

    async def get_latest_block(self):
        return {"blockNumber": self.head}

    async def get_block(self, n: int):
        self.requested.append(n)
        return self.blocks.get(n)


async def test_run_catches_up_sequentially_from_checkpoint():
    schema = "bw_catchup"
    conn = await fresh_conn(schema)
    try:
        await conn.execute(
            "INSERT INTO sync_state (name, last_he_block) VALUES ('block_watcher', 100)"
        )
        await conn.commit()
        blocks = {
            101: {"transactions": [tx("nft", "alice")]},
            102: {"transactions": [tx("nftmarket", "bob")]},
            103: {"transactions": []},
        }
        nodes = FakeNodes(head=103, blocks=blocks)
        stop = asyncio.Event()

        async def stop_soon():
            await asyncio.sleep(0.05)
            stop.set()

        await asyncio.gather(
            blockwatch.run(f"{TEST_DSN} options='-c search_path={schema}'", nodes, stop),
            stop_soon(),
        )
        assert nodes.requested == [101, 102, 103]
        row = await (await conn.execute(
            "SELECT last_he_block FROM sync_state WHERE name='block_watcher'"
        )).fetchone()
        assert row["last_he_block"] == 103
        queued = await (await conn.execute(
            "SELECT account FROM refresh_queue ORDER BY account"
        )).fetchall()
        assert [r["account"] for r in queued] == ["alice", "bob"]
    finally:
        await conn.close()


class FlakyFakeNodes:
    """get_block raises once for a chosen block, then succeeds on retry --
    simulates a transient HE failure."""

    def __init__(self, head: int, blocks: dict[int, dict], fail_block: int):
        self.head = head
        self.blocks = blocks
        self.fail_block = fail_block
        self.failed_once = False
        self.requested: list[int] = []

    async def get_latest_block(self):
        return {"blockNumber": self.head}

    async def get_block(self, n: int):
        self.requested.append(n)
        if n == self.fail_block and not self.failed_once:
            self.failed_once = True
            raise RuntimeError("simulated transient HE failure")
        return self.blocks.get(n)


async def test_run_recovers_from_a_transient_failure_without_crashing(monkeypatch):
    """Found live: this loop had zero exception handling at all, so a
    single transient HE failure crashed the entire sync service, not just
    this loop -- the one loop that must never let that happen, since
    missing a block is the one failure this design can't route around
    later. Must retry the SAME block (not skip it) and keep going."""
    monkeypatch.setattr(config, "BLOCKWATCH_IDLE_SECONDS", 0.01)
    schema = "bw_flaky"
    conn = await fresh_conn(schema)
    try:
        await conn.execute(
            "INSERT INTO sync_state (name, last_he_block) VALUES ('block_watcher', 100)"
        )
        await conn.commit()
        blocks = {
            101: {"transactions": [tx("nft", "alice")]},
            102: {"transactions": []},
        }
        nodes = FlakyFakeNodes(head=102, blocks=blocks, fail_block=101)
        stop = asyncio.Event()

        async def stop_once_caught_up():
            for _ in range(300):
                row = await (await conn.execute(
                    "SELECT last_he_block FROM sync_state WHERE name='block_watcher'"
                )).fetchone()
                if row and row["last_he_block"] == 102:
                    break
                await asyncio.sleep(0.01)
            stop.set()

        await asyncio.gather(
            blockwatch.run(f"{TEST_DSN} options='-c search_path={schema}'", nodes, stop),
            stop_once_caught_up(),
        )
        # 101 requested at least twice: the failed attempt, then a
        # successful retry -- proving the loop survived and didn't skip it.
        assert nodes.requested.count(101) >= 2
        row = await (await conn.execute(
            "SELECT last_he_block FROM sync_state WHERE name='block_watcher'"
        )).fetchone()
        assert row["last_he_block"] == 102
        queued = await (await conn.execute("SELECT account FROM refresh_queue")).fetchall()
        assert [r["account"] for r in queued] == ["alice"]
    finally:
        await conn.close()


async def test_run_starts_from_current_tip_when_no_checkpoint():
    """A fresh install has no sync_state row -- must start at the current
    HE head, not genesis (this design never eagerly backfills history)."""
    schema = "bw_notip"
    conn = await fresh_conn(schema)
    try:
        nodes = FakeNodes(head=500, blocks={500: {"transactions": []}})
        stop = asyncio.Event()

        async def stop_soon():
            await asyncio.sleep(0.05)
            stop.set()

        await asyncio.gather(
            blockwatch.run(f"{TEST_DSN} options='-c search_path={schema}'", nodes, stop),
            stop_soon(),
        )
        assert nodes.requested == [500]
    finally:
        await conn.close()
