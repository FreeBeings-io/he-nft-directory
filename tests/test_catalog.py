import json

from henftdir.catalog import is_nft_tx, market_sales, touched_accounts


def test_is_nft_tx():
    assert is_nft_tx("nft") and is_nft_tx("nftmarket")
    assert not is_nft_tx("tokens") and not is_nft_tx("market")


def tx(sender, payload, events=None):
    return {
        "sender": sender,
        "payload": json.dumps(payload),
        "logs": json.dumps({"events": events} if events else {}),
    }


def test_touched_accounts_always_includes_sender():
    assert touched_accounts(tx("alice", {})) == {"alice"}


def test_touched_accounts_from_event_data():
    t = tx("alice", {"to": "bob", "nfts": [{"symbol": "CARD", "ids": ["1"]}]},
           events=[{"contract": "nft", "event": "transfer",
                     "data": {"from": "alice", "to": "bob", "symbol": "CARD", "id": "1"}}])
    assert touched_accounts(t) == {"alice", "bob"}


def test_touched_accounts_recurses_into_nested_lists():
    """hitSellOrder's `sellers` is a list of dicts, each with its own
    `account` -- a buy can clear several sellers' orders at once."""
    t = tx("carol", {"symbol": "CARD", "nfts": ["1"]},
           events=[{"contract": "nftmarket", "event": "hitSellOrder",
                     "data": {"account": "carol",
                              "sellers": [{"account": "bob", "nftIds": ["1"]},
                                          {"account": "dave", "nftIds": ["2"]}]}}])
    assert touched_accounts(t) == {"carol", "bob", "dave"}


def test_touched_accounts_falls_back_to_payload_when_no_events():
    """setProperties (a PAYLOAD_APPLIED_ACTION) emits no event on success --
    only the sender is reliably known."""
    t = tx("cardco", {"symbol": "CARD", "nfts": [{"id": "1", "properties": {}}]})
    assert touched_accounts(t) == {"cardco"}


def test_touched_accounts_tolerates_malformed_logs():
    t = {"sender": "alice", "payload": "{}", "logs": "not json"}
    assert touched_accounts(t) == {"alice"}


# -- market_sales -------------------------------------------------------------

def _buy_tx(sellers, symbol="CARD", buyer="carol", price_symbol="SWAP.HIVE"):
    return {
        "sender": buyer, "contract": "nftmarket", "action": "buy",
        "logs": json.dumps({"events": [{
            "contract": "nftmarket", "event": "hitSellOrder",
            "data": {"symbol": symbol, "priceSymbol": price_symbol,
                     "account": buyer, "sellers": sellers},
        }]}),
    }


def test_market_sales_extracts_one_row_per_nft_sold():
    """A single buy can clear several sellers, each with several nftSales."""
    t = _buy_tx([
        {"account": "bob", "nftSales": [
            {"id": "1", "price": "5", "fee": 0, "symbol": "SWAP.HIVE"},
            {"id": "2", "price": "8", "fee": 0, "symbol": "SWAP.HIVE"}]},
        {"account": "dave", "nftSales": [
            {"id": "3", "price": "12", "fee": 0, "symbol": "SWAP.HIVE"}]},
    ])
    sales = market_sales(t, he_block=100, ts="2026-07-08T00:00:00Z")
    assert len(sales) == 3
    s = {row["nft_id"]: row for row in sales}
    assert s[1]["price"] == "5" and s[1]["seller"] == "bob" and s[1]["buyer"] == "carol"
    assert s[2]["price"] == "8"
    assert s[3]["seller"] == "dave" and s[3]["price"] == "12"
    assert all(r["symbol"] == "CARD" and r["he_block"] == 100 for r in sales)


def test_market_sales_price_symbol_falls_back_to_event_level():
    t = _buy_tx([{"account": "bob", "nftSales": [
        {"id": "1", "price": "5", "fee": 0}]}])  # no per-sale symbol
    sales = market_sales(t, 1, None)
    assert sales[0]["price_symbol"] == "SWAP.HIVE"


def test_market_sales_ignores_non_buy_and_non_nftmarket():
    assert market_sales({"contract": "nft", "action": "transfer"}, 1, None) == []
    assert market_sales({"contract": "nftmarket", "action": "sell"}, 1, None) == []


def test_market_sales_tolerates_malformed_logs():
    t = {"contract": "nftmarket", "action": "buy", "logs": "not json"}
    assert market_sales(t, 1, None) == []
