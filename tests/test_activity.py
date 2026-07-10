"""Activity feed: catalog.nft_events parsing (pure), blockwatch capture into
nft_events (Postgres), and the /accounts/{account}/activity keyset cursor."""

import json
import os
from datetime import datetime, timezone

import pytest

from henftdir import blockwatch, catalog, db

TEST_DSN = os.environ.get("HENFT_TEST_DSN")

TS = datetime(2026, 7, 10, tzinfo=timezone.utc)


def tx(contract, sender, action="x", events=None, txid="tx-1"):
    logs = {"events": events} if events else {}
    return {"contract": contract, "action": action, "sender": sender,
            "transactionId": txid, "payload": "{}", "logs": json.dumps(logs)}


# -- pure parser ---------------------------------------------------------------

def test_nft_events_transfer():
    t = tx("nft", "alice", action="transfer", events=[{
        "contract": "nft", "event": "transfer",
        "data": {"from": "alice", "to": "bob", "symbol": "STAR", "id": "77"},
    }])
    [e] = catalog.nft_events(t, 100, TS)
    assert (e["op"], e["symbol"], e["nft_id"]) == ("transfer", "STAR", 77)
    assert (e["account"], e["counterparty"]) == ("alice", "bob")


def test_nft_events_expands_hit_sell_order_per_instance():
    t = tx("nftmarket", "carol", action="buy", events=[{
        "contract": "nftmarket", "event": "hitSellOrder",
        "data": {"symbol": "CARD", "account": "carol", "priceSymbol": "BEE",
                 "sellers": [{"account": "dave",
                              "nftSales": [{"id": "5", "price": "9.9", "symbol": "BEE"},
                                           {"id": "6", "price": "1.0", "symbol": "BEE"}]}]},
    }])
    events = catalog.nft_events(t, 100, TS)
    assert [(e["op"], e["nft_id"], e["account"], e["counterparty"]) for e in events] == [
        ("market_buy", 5, "carol", "dave"),
        ("market_buy", 6, "carol", "dave"),
    ]


def test_nft_events_ignores_admin_and_non_nft():
    admin = tx("nft", "issuer", action="setProperties", events=[{
        "contract": "nft", "event": "setProperties",
        "data": {"symbol": "STAR", "id": "1"},
    }])
    other = tx("tokens", "eve", events=[{
        "contract": "tokens", "event": "transfer",
        "data": {"from": "eve", "to": "bob", "symbol": "BEE", "id": "1"},
    }])
    assert catalog.nft_events(admin, 100, TS) == []
    assert catalog.nft_events(other, 100, TS) == []


def test_parse_block_assigns_sequential_tx_seq():
    block = {"blockNumber": 100, "timestamp": "2026-07-10T00:00:00",
             "transactions": [
                 tx("nft", "alice", action="transfer", events=[{
                     "contract": "nft", "event": "transfer",
                     "data": {"from": "alice", "to": "bob", "symbol": "A", "id": 1}}]),
                 tx("nft", "bob", action="burn", events=[{
                     "contract": "nft", "event": "burn",
                     "data": {"account": "bob", "symbol": "A", "id": 2}}]),
             ]}
    _, _, events = blockwatch.parse_block(block)
    assert [e["tx_seq"] for e in events] == [0, 1]


# -- Postgres capture + endpoint -------------------------------------------------

pytestmark_db = pytest.mark.skipif(
    not TEST_DSN, reason="HENFT_TEST_DSN not set (needs PostgreSQL)")


async def fresh_conn(schema: str):
    admin = await db.connect(TEST_DSN)
    await admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    await admin.execute(f"CREATE SCHEMA {schema}")
    await admin.commit()
    await admin.close()
    conn = await db.connect(f"{TEST_DSN} options='-c search_path={schema}'")
    await db.apply_schema(conn)
    return conn


@pytestmark_db
async def test_capture_is_idempotent_per_block():
    conn = await fresh_conn("act_idem")
    try:
        block = {"blockNumber": 100, "timestamp": "2026-07-10T00:00:00",
                 "transactions": [tx("nft", "alice", action="transfer", events=[{
                     "contract": "nft", "event": "transfer",
                     "data": {"from": "alice", "to": "bob", "symbol": "A", "id": 1}}])]}
        await blockwatch.process_block(conn, block)
        await blockwatch.process_block(conn, block)  # reprocess: no dupes
        await conn.commit()
        rows = await (await conn.execute(
            "SELECT count(*) AS n FROM nft_events")).fetchone()
        assert rows["n"] == 1
    finally:
        await conn.close()


@pytestmark_db
async def test_capture_only_variant_skips_refresh_queue():
    conn = await fresh_conn("act_cap")
    try:
        block = {"blockNumber": 100, "timestamp": "2026-07-10T00:00:00",
                 "transactions": [tx("nft", "alice", action="transfer", events=[{
                     "contract": "nft", "event": "transfer",
                     "data": {"from": "alice", "to": "bob", "symbol": "A", "id": 1}}])]}
        n = await blockwatch.process_block_capture_only(conn, block)
        await conn.commit()
        assert n == 1
        queued = await (await conn.execute(
            "SELECT count(*) AS n FROM refresh_queue")).fetchone()
        assert queued["n"] == 0
        captured = await (await conn.execute(
            "SELECT account, counterparty, op FROM nft_events")).fetchone()
        assert (captured["account"], captured["counterparty"], captured["op"]) == \
            ("alice", "bob", "transfer")
    finally:
        await conn.close()
