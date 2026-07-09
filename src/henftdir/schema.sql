-- he-nft-directory schema: a read-through cache over Hive Engine's own
-- current state. There is
-- no local ledger and no op-derivation: every row
-- here is a mirror of what HE's own `find()` reported at last refresh,
-- never something we computed from a transaction log. Correctness means
-- "matches HE right now," not "correctly replayed everything that ever
-- happened."

-- One row per NFT symbol -- a full mirror of HE's own `nft`/`nfts` catalog
-- (~150 rows platform-wide; cheap to refresh in full on every catalog
-- sweep, no per-symbol targeting needed).
CREATE TABLE IF NOT EXISTS collections (
    symbol           text PRIMARY KEY,
    name             text,
    org_name         text,
    product_name     text,
    issuer           text,
    url              text,
    metadata         jsonb NOT NULL DEFAULT '{}',
    group_by         jsonb NOT NULL DEFAULT '[]',
    properties       jsonb NOT NULL DEFAULT '{}',
    delegation_enabled boolean NOT NULL DEFAULT false,
    market_enabled   boolean NOT NULL DEFAULT false,
    -- HE's own self-reported totals (free on every catalog sweep) -- the
    -- fallback for collections we haven't (or can't cheaply) fully mirror
    -- via instances, e.g. STAR at 27M+ instances. See api.py.
    supply           bigint,
    circulating_supply bigint,
    max_supply       bigint,
    undelegation_cooldown_days integer,
    refreshed_at     timestamptz NOT NULL DEFAULT now()
);

-- Accounts we've actually been asked about (the wallet query shape),
-- populated on first query, never eagerly. This IS the cache boundary: an
-- account not in this table has never been looked up, and its holdings
-- (if any) are not mirrored anywhere below.
CREATE TABLE IF NOT EXISTS known_accounts (
    account       text PRIMARY KEY,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    refreshed_at  timestamptz NOT NULL DEFAULT now()
);
-- safety_net_sweep orders by this on every sweep; without an index this
-- is a full-table sort that gets slower as more accounts get cached.
CREATE INDEX IF NOT EXISTS known_accounts_refreshed_at_idx
    ON known_accounts (refreshed_at);

-- Accounts flagged by the block-watcher as touched by a recent nft/
-- nftmarket transaction, awaiting the refresh worker. Not a queue of
-- *events* -- just "go re-fetch this account's current state," deduped by
-- account (a burst of activity for the same account collapses to one
-- re-fetch, not one per touch).
CREATE TABLE IF NOT EXISTS refresh_queue (
    account     text PRIMARY KEY,
    queued_at   timestamptz NOT NULL DEFAULT now()
);
-- refresh_worker orders by this on every poll.
CREATE INDEX IF NOT EXISTS refresh_queue_queued_at_idx
    ON refresh_queue (queued_at);

-- Current holdings, mirrored from HE's own `{symbol}instances` tables --
-- but ONLY for accounts in known_accounts. Never eagerly populated for an
-- entire collection (HE's own `find()` pagination cost scales with
-- collection size, not offset -- some collections are 10s of millions of
-- rows; per-account queries are indexed and cheap regardless of
-- collection size).
CREATE TABLE IF NOT EXISTS instances (
    symbol         text NOT NULL,
    nft_id         bigint NOT NULL,
    account        text NOT NULL,
    owned_by       text NOT NULL,          -- 'u' user | 'c' contract
    delegated_to   text,
    delegated_to_type text,
    soul_bound     boolean NOT NULL DEFAULT false,
    properties     jsonb NOT NULL DEFAULT '{}',
    refreshed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, nft_id)
);
CREATE INDEX IF NOT EXISTS instances_account_idx ON instances (account, symbol);
CREATE INDEX IF NOT EXISTS instances_delegated_idx
    ON instances (delegated_to) WHERE delegated_to IS NOT NULL;

-- Open sell orders, mirrored per-symbol from HE's own `{symbol}sellBook`
-- (small relative to total supply even for huge collections -- most
-- instances aren't listed for sale at any given time -- so a full
-- per-symbol mirror is cheap regardless of collection size).
CREATE TABLE IF NOT EXISTS market_orders (
    symbol        text NOT NULL,
    nft_id        bigint NOT NULL,
    account       text NOT NULL,          -- seller
    owned_by      text NOT NULL,
    price         numeric NOT NULL,
    price_symbol  text NOT NULL,
    fee           integer,
    ts            timestamptz,
    -- HE's own per-order grouping (e.g. {"class":"fanboost","type":"..."}),
    -- straight off the sellBook. One symbol commonly spans hundreds of
    -- groups (STAR: 227), so a symbol-wide floor is meaningless -- floor is
    -- rolled up per group below. {} for ungrouped collections.
    grouping      jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (symbol, nft_id)
);
CREATE INDEX IF NOT EXISTS market_orders_account_idx ON market_orders (account);

-- Market rollups computed from market_orders on each market refresh: floor
-- price per (payment token, group) and open-order count. grouping_key is
-- grouping::text (jsonb's canonical form), used only as a stable PK
-- component; grouping carries the structured value for the API.
CREATE TABLE IF NOT EXISTS market_rollups (
    symbol          text NOT NULL,
    price_symbol    text NOT NULL,
    grouping        jsonb NOT NULL DEFAULT '{}',
    grouping_key    text NOT NULL DEFAULT '',
    floor_price     numeric,
    open_orders     integer NOT NULL DEFAULT 0,
    PRIMARY KEY (symbol, price_symbol, grouping_key)
);

-- Forward-only record of completed market trades, captured by the
-- block-watcher from nftmarket `hitSellOrder` events. This is the one
-- deliberate exception to this design's "no event log" rule: HE has no
-- trade-history endpoint, so last-sale price and volume can't be obtained
-- any other way, and valuation needs them. It is capture-only and
-- forward-only (from the block-watcher's start point) -- it never feeds
-- holdings/ownership state, which stays a pure mirror of HE's current
-- tables. A given nft_id can trade many times over its life but not twice
-- in one block, so (symbol, nft_id, he_block) is a safe idempotent key.
CREATE TABLE IF NOT EXISTS market_sales (
    symbol       text NOT NULL,
    nft_id       bigint NOT NULL,
    price        numeric NOT NULL,
    price_symbol text NOT NULL,
    seller       text NOT NULL,
    buyer        text NOT NULL,
    he_block     bigint NOT NULL,
    ts           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, nft_id, he_block)
);
CREATE INDEX IF NOT EXISTS market_sales_symbol_ts_idx ON market_sales (symbol, ts DESC);

-- Versioned display adapters: raw properties -> normalized
-- {name, image, collection, attributes}. Append-only per symbol.
CREATE TABLE IF NOT EXISTS display_mappings (
    symbol     text NOT NULL,
    version    integer NOT NULL,
    mapping    jsonb NOT NULL,
    created_ts timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, version)
);

-- Freshness/liveness markers surfaced on /status: one row per named
-- background loop (block_watcher, refresh_worker, catalog_sweep, ...).
CREATE TABLE IF NOT EXISTS sync_state (
    name         text PRIMARY KEY,
    last_he_block bigint,
    updated_at   timestamptz NOT NULL DEFAULT now()
);
