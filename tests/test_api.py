"""API endpoint tests: seed the cache tables directly, call the WSGI app.

No derivation engine anymore -- state is seeded the
same way the real service populates it: direct rows, as if a refresh had
just happened. Cold-fetch paths (_ensure_known / _cold_fetch_instance) are
monkeypatched where exercised, so tests never hit the live network."""

import io
import json
import os

import pytest
from psycopg.types.json import Jsonb

import henftdir.api as api_mod
from henftdir import db

TEST_DSN = os.environ.get("HENFT_TEST_DSN")
pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="HENFT_TEST_DSN not set (needs PostgreSQL)"
)

SCHEMA = "api_test"


@pytest.fixture(scope="module", autouse=True)
async def seeded():
    admin = await db.connect(TEST_DSN)
    await admin.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    await admin.execute(f"CREATE SCHEMA {SCHEMA}")
    await admin.commit()
    await admin.close()
    conn = await db.connect(f"{TEST_DSN} options='-c search_path={SCHEMA}'")
    await db.apply_schema(conn)

    await conn.execute(
        "INSERT INTO collections (symbol, name, supply, circulating_supply, "
        "market_enabled) VALUES ('CARD', 'Card Game', 2, 1, true)"
    )
    await conn.execute(
        "INSERT INTO known_accounts (account) VALUES ('carol'), ('bob'), ('nftmarket')"
    )
    await conn.execute(
        "INSERT INTO instances (symbol, nft_id, account, owned_by, properties) "
        "VALUES ('CARD', %s, %s, %s, %s)",
        ("1", "carol", "u", Jsonb({"Name": "Dragon", "Rarity": "rare"})),
    )
    await conn.execute(
        "INSERT INTO instances (symbol, nft_id, account, owned_by, properties) "
        "VALUES ('CARD', %s, %s, %s, %s)",
        ("2", "nftmarket", "c", Jsonb({"Name": "Goblin"})),
    )
    await conn.execute(
        "INSERT INTO market_orders (symbol, nft_id, account, owned_by, price, "
        "price_symbol, grouping) VALUES ('CARD', 2, 'bob', 'u', 7, 'SWAP.HIVE', %s)",
        (Jsonb({"rarity": "rare"}),),
    )
    await conn.execute(
        "INSERT INTO market_rollups (symbol, price_symbol, grouping, grouping_key, "
        "floor_price, open_orders) VALUES ('CARD', 'SWAP.HIVE', %s, %s, 7, 1)",
        (Jsonb({"rarity": "rare"}), '{"rarity": "rare"}'),
    )
    await conn.execute(
        "INSERT INTO market_sales (symbol, nft_id, price, price_symbol, seller, "
        "buyer, he_block) VALUES ('CARD', 1, 5, 'SWAP.HIVE', 'bob', 'carol', 100)"
    )
    await conn.execute(
        "INSERT INTO display_mappings (symbol, version, mapping) VALUES "
        "('CARD', 1, %s)",
        (Jsonb({"name": {"from": "properties.Name"},
                "attributes": ["Rarity"]}),),
    )
    await conn.commit()
    await conn.close()

    api_mod.DSN = f"{TEST_DSN} options='-c search_path={SCHEMA}'"
    api_mod._mappings = None            # reset per-process caches
    api_mod._local.__dict__.clear()
    yield


def get(path: str, query: str = "") -> tuple[str, dict]:
    environ = {"REQUEST_METHOD": "GET", "PATH_INFO": path,
               "QUERY_STRING": query, "wsgi.input": io.BytesIO()}
    captured = {}

    def start_response(code, headers):
        captured["code"] = code

    body = b"".join(api_mod.application(environ, start_response))
    return captured["code"], json.loads(body)


def test_account_nfts_with_display():
    code, body = get("/accounts/carol/nfts")
    assert code == "200 OK"
    cards = body["owned"]["CARD"]
    assert len(cards) == 1 and cards[0]["nft_id"] == 1
    assert cards[0]["display"] == {
        "name": "Dragon", "image": None, "collection": "Card Game",
        "attributes": {"Rarity": "rare"}}
    assert "listed" in body and "delegated_in" in body


def test_account_nfts_listed_via_market_escrow():
    code, body = get("/accounts/bob/nfts")
    assert code == "200 OK"
    listed = body["listed"]["CARD"]
    assert listed[0]["nft_id"] == 2
    assert listed[0]["price"] == "7" and listed[0]["price_symbol"] == "SWAP.HIVE"


async def _fake_cold_fetch_account(account: str) -> None:
    conn = await db.connect(api_mod.DSN)
    try:
        await conn.execute(
            "INSERT INTO known_accounts (account) VALUES (%s)", (account,)
        )
        await conn.execute(
            "INSERT INTO instances (symbol, nft_id, account, owned_by, properties) "
            "VALUES ('CARD', 3, %s, 'u', '{}')", (account,),
        )
        await conn.commit()
    finally:
        await conn.close()


def test_account_nfts_cold_fetches_an_unseen_account(monkeypatch):
    monkeypatch.setattr(api_mod, "_cold_fetch_account", _fake_cold_fetch_account)
    code, body = get("/accounts/newperson/nfts")
    assert code == "200 OK"
    assert body["owned"]["CARD"][0]["nft_id"] == 3


def test_collections_and_detail():
    code, body = get("/collections")
    assert code == "200 OK"
    assert [c["symbol"] for c in body["collections"]] == ["CARD"]
    assert body["collections"][0]["has_display_mapping"] is True
    assert body["collections"][0]["burned"] == 1  # supply=2, circulating=1
    code, col = get("/collections/CARD")
    assert col["burned"] == 1


def test_nft_detail():
    code, nft = get("/nfts/CARD/1")
    assert code == "200 OK"
    assert nft["account"] == "carol" and nft["display"]["name"] == "Dragon"
    assert nft["open_order"] is None


def test_market_rollups():
    code, body = get("/market/CARD")
    assert code == "200 OK"
    assert len(body["open_orders"]) == 1 and body["open_orders"][0]["nft_id"] == 2
    assert body["rollups"][0]["floor_price"] == "7"
    # per-group floor: rollup carries the order's grouping
    assert body["rollups"][0]["grouping"] == {"rarity": "rare"}


def test_market_last_sales():
    code, body = get("/market/CARD")
    assert code == "200 OK"
    ls = body["last_sales"]
    assert len(ls) == 1
    assert ls[0]["price_symbol"] == "SWAP.HIVE"
    assert ls[0]["last_price"] == "5"
    assert ls[0]["sales_30d"] == 1 and ls[0]["volume_30d"] == "5"


async def _no_cold_fetch_instance(symbol: str, nft_id: int) -> bool:
    return False


def test_status_and_404(monkeypatch):
    monkeypatch.setattr(api_mod, "_cold_fetch_instance", _no_cold_fetch_instance)
    code, body = get("/status")
    assert code == "200 OK"
    assert body["display_mapping_coverage"] == {"mapped": 1, "total": 1}
    assert "read-through cache" in body["disclosure"]
    code, _ = get("/nfts/CARD/999")
    assert code == "404 Not Found"
    code, _ = get("/nope")
    assert code == "404 Not Found"


def test_nft_detail_404_for_unknown_symbol_never_calls_he(monkeypatch):
    """The full HE catalog is already mirrored locally -- a symbol that
    isn't even a real collection must 404 without ever attempting a live
    HE lookup. Found live: without this, repeatedly querying a bogus
    symbol re-paid a live round trip every single time, since a not-found
    result is never cached anywhere."""
    calls = []

    async def tracking_cold_fetch_instance(symbol, nft_id):
        calls.append((symbol, nft_id))
        return False

    monkeypatch.setattr(api_mod, "_cold_fetch_instance", tracking_cold_fetch_instance)
    code, _ = get("/nfts/NOTAREALSYMBOL/1")
    assert code == "404 Not Found"
    assert calls == []


async def test_refresh_account_locked_skips_when_already_known(monkeypatch):
    """Found live: two concurrent requests for the same never-before-seen
    account would each pay for their own full ~150-table cold-fetch burst
    against HE at once. If another request already finished the work while
    we waited for the advisory lock, don't redundantly fetch again."""
    conn = await db.connect(api_mod.DSN)
    try:
        await conn.execute(
            "INSERT INTO known_accounts (account) VALUES ('alreadyknown') "
            "ON CONFLICT (account) DO NOTHING"
        )
        await conn.commit()

        called = {"n": 0}

        async def fake_refresh_account(conn, nodes, account, symbols):
            called["n"] += 1

        monkeypatch.setattr(api_mod.sync, "refresh_account", fake_refresh_account)
        await api_mod._refresh_account_locked(conn, None, "alreadyknown", ["CARD"])
        assert called["n"] == 0
    finally:
        await conn.execute("DELETE FROM known_accounts WHERE account = 'alreadyknown'")
        await conn.commit()
        await conn.close()


def test_mappings_cache_has_a_ttl():
    """Without a TTL, an operator adding a mapping (the documented
    DEPLOY.md workflow) would need to restart the API process before it
    ever took effect. api_mod._conn() is deliberately read-only, so
    setup/teardown here use their own writable connection."""
    write_conn = db.connect_sync(api_mod.DSN)
    api_mod._mappings = None
    api_mod._mappings_loaded_at = 0.0
    try:
        assert "NEWSYM" not in api_mod.mappings()  # fresh load, baseline

        write_conn.execute(
            "INSERT INTO display_mappings (symbol, version, mapping) "
            "VALUES ('NEWSYM', 1, %s)",
            (Jsonb({"name": {"template": "{symbol}"}}),),
        )
        write_conn.commit()

        assert "NEWSYM" not in api_mod.mappings()  # still within TTL

        api_mod._mappings_loaded_at -= (api_mod.MAPPINGS_TTL_SECONDS + 1)
        assert "NEWSYM" in api_mod.mappings()
    finally:
        write_conn.execute("DELETE FROM display_mappings WHERE symbol = 'NEWSYM'")
        write_conn.commit()
        write_conn.close()
        api_mod._mappings = None
        api_mod._mappings_loaded_at = 0.0
