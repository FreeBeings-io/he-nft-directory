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
-- An account has two states. A row here means TRACKED: the block-watcher
-- queues its touches to keep it fresh (the cache follows only accounts
-- someone has actually queried -- see the disclosure in api.status). Rows
-- are created by the API cold-fetch; the block-watcher never creates them,
-- so activity for never-queried accounts is ignored, and cache size scales
-- with usage, not chain activity.
--   populated_at IS NULL  -> tracked, but the initial full scan hasn't
--     finished yet (set at cold-fetch start so touches landing DURING the
--     scan are queued and not lost). NOT served by the API.
--   populated_at set      -> a full scan has completed at least once; safe
--     to serve, and eligible for the cheap targeted/narrowed refresh paths.
CREATE TABLE IF NOT EXISTS known_accounts (
    account       text PRIMARY KEY,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    refreshed_at  timestamptz NOT NULL DEFAULT now(),
    populated_at  timestamptz
);
-- migrate a pre-two-state deployment: existing rows were populated under the
-- old model, so treat them as populated.
ALTER TABLE known_accounts ADD COLUMN IF NOT EXISTS populated_at timestamptz;
UPDATE known_accounts SET populated_at = refreshed_at WHERE populated_at IS NULL;
-- refreshed_at = last FULL refresh pass (targeted single-symbol
-- re-checks deliberately don't bump it -- it's what the read-staleness
-- bound measures against). The index's original consumer (the safety-net
-- sweep's ORDER BY) is gone; kept because it's tiny and useful for ops
-- queries over staleness distribution.
CREATE INDEX IF NOT EXISTS known_accounts_refreshed_at_idx
    ON known_accounts (refreshed_at);

-- Accounts flagged by the block-watcher as touched by a recent nft/
-- nftmarket transaction, awaiting the refresh worker. Not a queue of
-- *events* -- just "go re-fetch this account's current state," deduped by
-- account (a burst of activity for the same account collapses to one
-- re-fetch, not one per touch).
-- symbol = '' means "full-account refresh" (all known symbols): used for
-- first-ever fetches and for touches where the affected symbol can't be
-- determined (payload-only parses). A non-empty symbol is a TARGETED
-- refresh: the block-watcher's event parse knows exactly which collection
-- a tx touched, and re-checking only that one is ~40x cheaper than the
-- full ~115-symbol sweep -- the difference between draining a busy queue
-- and stalling behind it as known accounts grow.
-- attempts/not_before make this queue the DURABLE RETRY LEDGER too: a
-- symbol lookup that fails during any refresh (queued, cold-fetch, or
-- read-staleness) is re-inserted here with attempts+1 and an exponential
-- not_before backoff, so no failure can silently become permanent
-- staleness -- every touch either completes or sits here, visibly
-- pending (surfaced via /status refresh_retries). This is what replaced
-- the hourly safety-net sweep: correction work now scales with actual
-- failures and actual usage, never with fleet size.
CREATE TABLE IF NOT EXISTS refresh_queue (
    account     text NOT NULL,
    symbol      text NOT NULL DEFAULT '',
    queued_at   timestamptz NOT NULL DEFAULT now(),
    attempts    int NOT NULL DEFAULT 0,
    not_before  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account, symbol)
);
-- refresh_worker orders by this on every poll.
CREATE INDEX IF NOT EXISTS refresh_queue_queued_at_idx
    ON refresh_queue (queued_at);
-- The worker's claim filters `not_before <= now()` every poll; since the
-- queue doubles as the retry ledger, backoff rows accumulate here, and
-- without this index that filter scans them all during a large retry
-- backlog (exactly when the service is already under upstream stress).
CREATE INDEX IF NOT EXISTS refresh_queue_not_before_idx
    ON refresh_queue (not_before, queued_at);
-- migrate a pre-retry-ledger deployment in place
ALTER TABLE refresh_queue ADD COLUMN IF NOT EXISTS attempts int NOT NULL DEFAULT 0;
ALTER TABLE refresh_queue ADD COLUMN IF NOT EXISTS not_before timestamptz NOT NULL DEFAULT now();
-- migrate a pre-(account,symbol) deployment in place (idempotent: the DO
-- block only fires while the old single-column PK is still present)
ALTER TABLE refresh_queue ADD COLUMN IF NOT EXISTS symbol text NOT NULL DEFAULT '';
DO $$
BEGIN
    IF (SELECT count(*) FROM information_schema.key_column_usage
        WHERE table_name = 'refresh_queue'
          AND constraint_name = 'refresh_queue_pkey') < 2 THEN
        ALTER TABLE refresh_queue DROP CONSTRAINT refresh_queue_pkey;
        ALTER TABLE refresh_queue ADD PRIMARY KEY (account, symbol);
    END IF;
END $$;

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

-- Event-driven market refresh: the block-watcher inserts a symbol here when
-- it sees a market event for it (list/cancel/price-change/buy), and the
-- market loop refreshes only these dirty symbols each cycle instead of
-- re-polling every cached symbol's full sellBook on a blind timer. Market
-- work then scales with actual trading activity, not with symbol count. A
-- slow full sweep still runs as a backstop (see service._market_loop).
CREATE TABLE IF NOT EXISTS market_refresh_queue (
    symbol      text PRIMARY KEY,
    queued_at   timestamptz NOT NULL DEFAULT now()
);

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

-- Rolling activity feed: nft/nftmarket events captured by the block-watcher
-- (live) and the activity backfill loop (backward from deploy toward the
-- retention window's start). Like market_sales this is capture-only -- it
-- never feeds holdings state -- but unlike market_sales it is PRUNED to the
-- retention window (ACTIVITY_WINDOW_DAYS): a bounded recent-activity feed,
-- deliberately not an archive. (he_block, tx_seq) is idempotent because the
-- parse is deterministic: reprocessing a block yields the same sequence.
CREATE TABLE IF NOT EXISTS nft_events (
    he_block     bigint NOT NULL,
    tx_seq       integer NOT NULL,
    symbol       text NOT NULL,
    nft_id       bigint,
    op           text NOT NULL,
    account      text,
    counterparty text,
    price        numeric,
    price_symbol text,
    tx_id        text,
    ts           timestamptz NOT NULL,
    PRIMARY KEY (he_block, tx_seq)
);
CREATE INDEX IF NOT EXISTS nft_events_account_ts_idx ON nft_events (account, ts DESC);
CREATE INDEX IF NOT EXISTS nft_events_counterparty_ts_idx ON nft_events (counterparty, ts DESC);
CREATE INDEX IF NOT EXISTS nft_events_symbol_ts_idx ON nft_events (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS nft_events_ts_idx ON nft_events (ts);

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
