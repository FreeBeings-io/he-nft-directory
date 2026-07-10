"""Hive Engine NFT/nftmarket transaction parsing for blockwatch.py:
touched_accounts() (who to re-fetch) and market_sales() (trades to record).
Neither derives holdings state -- they only read what HE's own transaction
already reported inline (emitted events / payload).
"""

import json


def is_nft_tx(contract: str) -> bool:
    return contract in ("nft", "nftmarket")


def tx_events(tx: dict) -> list[dict]:
    """A transaction's emitted events, parsed once (logs arrive as a dict or
    a JSON string depending on the node)."""
    logs = _load_json(tx.get("logs"), {})
    return (logs or {}).get("events", []) or []


def has_nft_activity(tx: dict) -> bool:
    """True if this transaction touches NFTs at all — either it targets the
    nft/nftmarket contract directly, or ANY OTHER contract's execution
    emitted nft/nftmarket events (packmanager pack openings, and any future
    contract that issues/moves NFTs internally, surface this way: the tx's
    top-level contract is theirs, but the logs carry nft-contract events).
    Gating on the tx-level contract alone silently drops that activity."""
    if is_nft_tx(tx.get("contract")):
        return True
    return any(is_nft_tx(e.get("contract")) for e in tx_events(tx))


def _load_json(value, default):
    """HE inlines logs/payload as either a dict or a JSON string."""
    if isinstance(value, str):
        try:
            return json.loads(value) if value else default
        except ValueError:
            return default
    return value if value is not None else default


# Keys that hold an account name in nft/nftmarket event data (contracts/
# nft.js, contracts/nftmarket.js `api.emit` call sites). Deliberately over-
# inclusive: this only decides who gets queued for a refresh (blockwatch.py),
# never derives state, so a false positive just costs one wasted cache
# refresh -- a false negative (a real change to an account we never refresh)
# is the only mistake that matters.
_ACCOUNT_FIELDS = frozenset({"account", "to", "from", "counterparty", "issuer"})


def touched_accounts(tx: dict) -> set[str]:
    """Every account name plausibly affected by one HE transaction (a block
    entry from getBlockInfo, or getTransactionInfo) -- for queueing a
    refresh, not for interpreting what happened. Always includes the
    sender; best-effort beyond that from the tx's own emitted events."""
    accounts = set()
    sender = tx.get("sender")
    if sender:
        accounts.add(sender)

    def scan(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key in _ACCOUNT_FIELDS and isinstance(value, str):
                    accounts.add(value)
                elif isinstance(value, (dict, list)):
                    scan(value)
        elif isinstance(node, list):
            for item in node:
                scan(item)

    logs = _load_json(tx.get("logs"), {})
    for event in (logs or {}).get("events", []):
        scan(event.get("data"))

    # PAYLOAD_APPLIED_ACTIONS (setProperties etc.) emit no event on success,
    # so fall back to whatever the payload itself names directly -- best
    # effort only; a payload like setProperties has no owner field at all,
    # so an affected holder's cache can go briefly stale until their own
    # next refresh (acceptable: the periodic safety-net sweep catches it).
    scan(_load_json(tx.get("payload"), {}))

    return accounts


# nft/nftmarket emitted-event names -> activity-feed op labels. Only events
# that describe something a wallet user would recognize as "activity" on an
# instance; contract-admin events (create, setProperties, addProperty, ...)
# are deliberately out. Like touched_accounts, this reads only what HE's own
# transaction already reported inline -- no derivation.
_EVENT_OPS = {
    "issue": "issue",
    "transfer": "transfer",
    "burn": "burn",
    "delegate": "delegate",
    "undelegate": "undelegate",
    "undelegateStart": "undelegate_start",
    "undelegateDone": "undelegate_done",
    "sellOrder": "market_list",
    "cancelOrder": "market_cancel",
    "changePrice": "market_price_change",
}


def nft_events(tx: dict, he_block: int, ts) -> list[dict]:
    """Activity-feed rows from one transaction's emitted nft/nftmarket
    events (see _EVENT_OPS; `hitSellOrder` expands into one `market_buy`
    per instance sold, mirroring market_sales()). Filters per EVENT
    contract, not per tx contract -- pack openings and other contracts
    issue NFTs from inside their own transactions (see has_nft_activity).
    Rows carry no tx_seq -- the caller assigns it per block, since
    idempotency is per (block, seq)."""
    tx_id = tx.get("transactionId")
    events: list[dict] = []

    def row(op, symbol, nft_id, account, counterparty, price, price_symbol):
        events.append({
            "symbol": symbol, "nft_id": nft_id, "op": op,
            "account": account, "counterparty": counterparty,
            "price": price, "price_symbol": price_symbol,
            "tx_id": tx_id, "he_block": he_block, "ts": ts,
        })

    for event in tx_events(tx):
        if not is_nft_tx(event.get("contract")):
            continue
        name = event.get("event")
        data = event.get("data") or {}
        if name == "hitSellOrder":
            symbol = data.get("symbol")
            buyer = data.get("account")
            for seller in data.get("sellers", []):
                for sale in seller.get("nftSales", []):
                    if sale.get("id") is None or symbol is None:
                        continue
                    try:
                        nft_id = int(sale["id"])
                    except (TypeError, ValueError):
                        continue
                    row("market_buy", symbol, nft_id, buyer,
                        seller.get("account"), sale.get("price"),
                        sale.get("symbol") or data.get("priceSymbol"))
            continue
        op = _EVENT_OPS.get(name)
        if op is None:
            continue
        symbol = data.get("symbol")
        raw_id = data.get("id", data.get("nftId"))
        if symbol is None or raw_id is None:
            continue
        try:
            nft_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        row(op, symbol, nft_id,
            data.get("from") or data.get("account"), data.get("to"),
            data.get("price", data.get("newPrice")), data.get("priceSymbol"))
    return events


def market_sales(tx: dict, he_block: int, ts) -> list[dict]:
    """Completed trades from one nftmarket `buy` transaction's own
    `hitSellOrder` events (the ack packet HE emits per fill). Each event
    carries the NFT symbol, the buyer (`account`), and a `sellers` list
    whose `nftSales` entries are the per-instance sales
    (`{id, price, fee, symbol}`) -- exactly and only what HE itself reported,
    no derivation. Returns one dict per NFT sold."""
    if tx.get("contract") != "nftmarket" or tx.get("action") != "buy":
        return []
    logs = _load_json(tx.get("logs"), {})
    sales: list[dict] = []
    for event in (logs or {}).get("events", []):
        if event.get("event") != "hitSellOrder":
            continue
        data = event.get("data") or {}
        symbol = data.get("symbol")
        buyer = data.get("account")
        for seller in data.get("sellers", []):
            seller_acct = seller.get("account")
            for sale in seller.get("nftSales", []):
                if sale.get("id") is None or sale.get("price") is None:
                    continue
                sales.append({
                    "symbol": symbol,
                    "nft_id": int(sale["id"]),
                    "price": sale["price"],
                    "price_symbol": sale.get("symbol") or data.get("priceSymbol"),
                    "seller": seller_acct,
                    "buyer": buyer,
                    "he_block": he_block,
                    "ts": ts,
                })
    return sales
