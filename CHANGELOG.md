# Changelog

All notable changes to **he-nft-directory** are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com); this project uses
[SemVer](https://semver.org). Response shapes are pre-release drafts until 1.0
(display-object shape, valuation semantics, freshness, and rate limits may still
change); a history endpoint is deliberately not part of this design.

## [Unreleased]

## [0.8.2] - 2026-07-17

### Changed
- Block-watcher now stays `HENFT_BLOCKWATCH_SETTLE_BLOCKS` (default 1)
  blocks behind whatever `getLatestBlockInfo` reports as head, instead of
  chasing it immediately. The block HE just reported as head is often
  still propagating across the node pool, so a rotation pick that differs
  from the node that answered `getLatestBlockInfo` would null on it (found
  live: several-per-minute "lagging node" retries at the bleeding edge,
  after 0.8.0's never-skip fix started surfacing them as warnings). The
  null-block retry guard still applies underneath -- this reduces how
  often it fires, it doesn't replace it. Costs one HE block time (~3s) of
  extra tip latency.

## [0.8.1] - 2026-07-17

### Fixed
- Refresh worker could spin (not a wedge, a hot-loop) on a fresh DB: the
  worker caches the known-symbols list and only re-read it when the queue
  went idle, but the startup-seeded `nftmarket` full-refresh keeps the
  queue non-idle, so if the catalog loop hadn't populated `collections`
  yet the empty symbol list was cached forever and every claim was skipped
  in a tight loop. The worker now re-reads the symbol list while it's
  empty and idles (rather than spinning) until the catalog is ready.

## [0.8.0] - 2026-07-17

### Fixed
- The block-watcher could **silently skip a block**: it advanced its
  checkpoint even when a node returned null for a block that exists (a node
  lagging behind the head another node reported), dropping every NFT event
  in that block forever. The checkpoint now advances only after a block is
  actually processed; a null (or a processing error) retries the same block
  instead of skipping it. This is the root correctness guarantee — every
  downstream cache gap traces back to an unprocessed block.
- An account is now fully populated before it is ever served or refreshed
  cheaply. Previously an account first seen via a block-watcher touch was
  marked known after a single targeted collection check, so a read returned
  incomplete holdings (anything acquired before we started watching was
  invisible). Accounts now have two states — *tracked* (a row exists) and
  *populated* (a full scan has completed, `populated_at` set) — and only a
  full scan sets populated; the worker escalates a not-yet-populated account
  to a full scan, and the API serves only populated accounts.

### Changed
- The cache is now strictly **read-through over queried accounts**. The
  block-watcher queues refreshes only for tracked accounts (someone queried
  them at least once), so cache size scales with usage, not chain activity —
  matching the `/status` disclosure, which was previously aspirational. An
  account is marked *tracked* at the start of its cold-fetch (before the
  scan), so a block touching it DURING its initial scan is still queued and
  refreshed rather than lost.
- **Event-driven market refresh.** The market loop now refreshes only
  symbols the block-watcher flags on a live market event
  (list/cancel/price-change/buy), draining a `market_refresh_queue`, plus
  a slow full-sweep backstop (default hourly) and one sweep at startup.
  Steady-state market work scales with trading activity instead of
  re-polling every cached symbol's full sellBook every 5 min.
- **Payload-only refreshes are narrowed.** A refresh that names no symbol
  (`setProperties`-shaped touches, staleness re-verifies) now re-checks
  only the collections the account already holds instead of all ~115 known
  symbols — ownership changes always arrive as symbol-naming (targeted)
  events. Falls back to a full re-check for accounts with nothing cached.
- **Short-TTL response cache** (30s) on `/collections` and `/market/*` —
  their data only changes on the background refresh cycle, so this turns
  the repeated full-payload rebuild into a cache hit.

### Performance
- Refresh-queue claim gained an index on `(not_before, queued_at)` so the
  worker's every-poll filter doesn't scan accumulated retry-backoff rows.
- `/collections` uses `EXISTS` instead of a per-row `count(*)` subquery.

## [0.7.0] - 2026-07-17

### Changed
- The refresh worker now drains queued accounts **concurrently** (up to
  `HENFT_REFRESH_WORKER_BATCH`, default = the HE node budget), each on its
  own connection, instead of one account at a time. Serial draining was
  the throughput ceiling — measured ~1.1s/account, so a block touching
  1000 accounts took ~19 min to reflect. Total HE-call concurrency stays
  bounded by the shared node semaphore, so this fills the already-node-safe
  budget rather than adding upstream load. Per-account failures are now
  isolated on their own connections (one account's error can't disturb the
  others in the batch).

## [0.6.1] - 2026-07-17

### Fixed
- Refresh worker could wedge and stop draining the queue. Its connection
  is non-autocommit, and the idle-poll branch (empty queue) read
  `known_symbols` without committing, leaving the connection *idle in
  transaction* for the whole idle wait — pinning its MVCC snapshot so
  later polls never saw rows other connections had since queued. The
  queue only grew (observed stuck at 8). The idle branch now releases its
  transaction before waiting. Surfaced once the 0.6.0 safety-net removal
  created the first sustained quiet period for the worker to idle in.

## [0.6.0] - 2026-07-17

### Changed
- Freshness now scales with usage instead of fleet size — the hourly
  safety-net sweep is removed, replaced by three guarantees:
  - **Durable retries**: a symbol lookup that fails during any refresh is
    re-queued with exponential backoff (`refresh_queue` gained
    `attempts`/`not_before`, migrated in place on startup) instead of
    silently dropped; a failed refresh-worker pass reschedules rather than
    deleting the queue entry. Pending retries are visible at `/status`
    (`refresh_retries`).
  - **Staleness-bounded reads**: reading an account whose cache is older
    than `HENFT_ACCOUNT_STALE_AFTER` (default 6h) serves the cache and
    enqueues a background re-fetch.
  - **Unknown-event alarm**: an nft-contract event name the parser has
    never seen logs a warning (once per name) — refresh triggering is
    name-agnostic and unaffected, but this makes an HE contract change
    visible instead of silent.

## [0.5.0] - 2026-07-17

### Changed
- Widened the default Hive Engine node pool from 4 to 9 (added
  herpc.kanibot.com, api.primersion.com, herpc.tribaldex.com,
  herpc.liotes.com, he.atexoras.com:2083 — each verified on every RPC
  call shape the service uses and against the official node's
  block/database hashes at a settled block) after production showed
  sustained all-nodes-failed windows under upstream 429/503 pressure.
- A 429 response now parks the node on the outage-grade backoff floor
  (30s, vs the 3s floor kept for 503s) and honors a delta-seconds
  Retry-After header (capped at 120s): retrying a rate-limited node on
  the short floor just re-earned the same 429 and kept the client
  flagged.
- Slowed the activity backfill from 20 blocks/5s to 10 blocks/10s
  (~1 block/s) — the old burst rate was drawing 429s/503s and stalling
  the cursor on retries. Now overridable via
  `HENFT_ACTIVITY_BACKFILL_BATCH` / `HENFT_ACTIVITY_BACKFILL_PAUSE` for
  self-hosted nodes.

## [0.4.0] - 2026-07-11

### Changed
- Refresh throughput now scales with activity instead of with catalog
  size: the block-watcher's event parse knows exactly which collection a
  transaction touched, so the refresh queue carries (account, symbol)
  pairs and the worker re-checks only the touched collections (~40x
  cheaper than the full sweep it previously ran for every queued touch).
  Full-account refreshes remain for first-ever fetches, payload-only
  touches where no event names a symbol, and the safety-net's periodic
  insurance pass. Existing deployments migrate the queue schema in place
  on startup.

## [0.3.1] - 2026-07-11

### Fixed
- Background account refreshes no longer hammer the node pool: a heavy
  account's ~150-symbol refresh at full pool speed put every HE node into
  cooldown at once, causing a "no nodes" retry storm for everything else
  in flight. Queue-drain and safety-net refreshes are now paced between
  chunks (the synchronous first-query fetch stays full-speed — a user is
  waiting on it), and collections with zero issued instances (37 of 152)
  are skipped entirely.

## [0.3.0] - 2026-07-10

### Added
- Deployment-shape knobs are env-overridable, so Docker operators can
  point at a self-hosted Hive Engine node without rebuilding the image:
  `HENFT_HE_NODES` (comma-separated), `HENFT_HE_MAX_CONCURRENCY`,
  `HENFT_HE_CALL_SPACING`. See docs/DEPLOY.md §3.
- Display-mapping coverage extended to the full Hive Engine catalog: every
  collection was audited against live on-chain property samples, adding 23
  collections for 91 mapped in total. The rest are deliberately unmapped —
  no issued instances, empty/test properties, or properties the mapping
  engine cannot reach (JSON-encoded strings / CSV blobs).

## [0.2.1] - 2026-07-10

### Fixed
- `/status` could look stale: the activity backfill's checkpoint rows live
  in the same table as the loop-freshness markers, and one is written once
  at init and never updated — surfaced through `/status.sync` it made the
  service read as hours behind while the block-watcher was in fact current.
  `sync` now carries only real loop-freshness rows; backfill progress is
  reported under `activity`.

## [0.2.0] - 2026-07-10

### Added
- Recent-activity feed: `GET /accounts/{account}/activity` — NFT events
  (issue, transfer, burn, delegation, market list/cancel/price-change/buy)
  where the account was actor or counterparty, newest first. A **rolling
  window** (default 30 days), not a history archive: the block-watcher
  captures events live, a low-priority background loop backfills backward
  toward the window start (its own node pool; the service is fully usable
  while it runs, and `/status` + the response's `coverage` object report
  progress honestly), and a daily prune drops events past the window.
  Hive Engine itself offers no working NFT history source — its RPC nodes
  have no history methods and the official sidecar's `nftHistory` endpoint
  serves no data (verified live) — so this feed is capture-only from what
  the block-watcher itself witnessed, and deliberately bounded.

### Fixed
- NFT activity happening inside OTHER contracts' transactions (pack
  openings and any contract that issues/moves NFTs internally) was
  invisible to the block-watcher: it gated on the transaction's own
  contract, but those txs carry their nft-contract events in the logs
  under a different top-level contract. Detection is now per emitted
  event, which fixes both refresh queueing (previously only the hourly
  safety-net caught those holders) and activity capture.

### Removed
- One rotation node added in 0.1.1 was removed: it serves the `blockchain`
  endpoint fine but rate-limits `contracts` `find` queries by policy
  (hundreds of 429s in hours, including aborted market-book pagination).
  Rotation membership now requires both endpoints unrestricted.

## [0.1.1] - 2026-07-10

### Changed
- Added two consensus-verified community HE nodes to the failover rotation
  (identical block/database hashes at a settled block), for deeper
  burst/outage headroom. (One is removed again in 0.2.0 — see above.)

## [0.1.0] - 2026-07-10

First production release.

### Added
- Hive Engine NFT Directory: a read-through cache over Hive Engine's own
  current state — no Hive L1 dependency, no transaction ledger. A
  block-watcher reads HE blocks directly to queue touched accounts, a
  refresh worker re-fetches them straight from HE, and an account is
  populated on first query if never seen before. Versioned display-mapping
  layer; read-only WSGI API (accounts, collections, nfts, market, status).
  This design was chosen because it stays fast and cheap at any collection
  size: HE's largest collections hold tens of millions of instances, so
  anything that replays or bootstraps full history scales with collection
  size instead of with actual usage.
- Market valuation. Floor price is rolled up per payment token **and per
  group** — HE sell orders carry a grouping (e.g. rarity/type), and one
  collection often spans hundreds of groups, so a single symbol-wide floor
  is misleading. Order books commonly span several payment tokens; prices
  are served in their own token, with no USD conversion. A forward-only
  trade log, captured by the block-watcher from HE's own sale events, adds
  last-sale price and trailing-30-day volume per token — HE exposes no
  trade-history endpoint, so this is the only source. It is capture-only and
  never feeds holdings state.

### Fixed
All found live during and after the first real (pre-1.0) deployment:

- A never-before-seen account's first query does a synchronous ~150-table
  cold-fetch before responding; gunicorn's 30s default worker timeout
  killed that request mid-flight. Raised to 90s (`WEB_TIMEOUT`).
- A cold-fetch's own concurrent burst could push an HE node into brief
  503s, which then cascaded: the 30s(→60s) backoff floor for that (reserved
  for genuine outages) concentrated the burst onto the remaining nodes in
  turn. 503s now get a much shorter starting backoff than a transport
  failure, and the semaphore is no longer held through the inter-round
  retry sleep (was starving unrelated, would-succeed calls).
- A symbol lookup that failed was treated as "confirmed this account owns
  nothing here," silently erasing real cached holdings on a transient HE
  hiccup. A failed symbol now leaves existing cached data untouched.
- All five background loops shared one HTTP client/rate-limit state, so a
  bursty account refresh could starve the block-watcher's own trivial,
  otherwise-reliable block reads. The block-watcher now has its own,
  isolated client.
- The block-watcher's entire loop body, and part of the refresh worker's,
  had no exception handling at all — a single transient failure crashed
  the whole sync service. Both now catch, roll back, and retry.
- `instances`' primary key is `(symbol, nft_id)`, not including account
  (an instance has exactly one owner) — a plain INSERT crashed the sync
  service with a duplicate-key error on a genuine ownership transfer.
  Fixed to upsert. Every loop with this shape (refresh worker, safety-net
  sweep, market refresh, catalog/market loops) now rolls back on failure
  instead of leaving the connection poisoned for its next iteration.
  An account refresh during the narrow window right after a fresh deploy
  (before the catalog populates) no longer gets marked known/refreshed
  with zero symbols actually checked.
  Two concurrent requests for the same never-before-seen account no
  longer each pay for their own full cold-fetch burst — a per-account
  Postgres advisory lock serializes them.
- `/accounts/{account}/nfts` for a large account was ~10x slower than
  other endpoints; the display-mappings cache was being resolved (with a
  lock + TTL check) once per row instead of once per request.
  `/nfts/{symbol}/{id}` for a symbol that isn't a real HE collection
  re-paid a live HE round trip on every single query, since a not-found
  result was never cached; now checked against the locally-mirrored
  catalog first, for free.
- The per-node request-rate limiter's check-and-update was unguarded, so
  concurrent callers could race past it with the same stale timestamp and
  fire together — silently defeating the limit under the one condition
  (concurrency > 1) it exists for. Now serialized per node under a lock,
  and the limit itself is per-node rather than one shared budget across
  all configured nodes.
- Market data never populated at all: the catalog read a `marketEnabled`
  flag that HE's collection records don't carry, so every symbol looked
  market-disabled and the market refresh loop processed nothing. Market
  refresh is now scoped to symbols that have cached instances, and
  market-enablement is derived from whether an open sell book exists.
- Any account holding a delegated NFT failed to refresh entirely — HE
  returns an instance's delegation as an object, not a string, and it was
  written straight into a text column ("cannot adapt type 'dict'"), failing
  the whole insert batch. The account and delegation type are now pulled out
  of the object.
- Large sell books (>~10k open orders) couldn't be mirrored because HE
  rejects deep pagination offsets, which abandoned the whole symbol. The
  paginator now stops gracefully and computes a best-effort floor from what
  it fetched: exact for books within HE's offset cap (nearly all),
  approximate only for the few mega-collections above it.
- The market sweep shared an HE node pool with the account-refresh loops and
  was starved for minutes during refresh bursts (each account refresh is
  ~150 per-symbol lookups); it now has its own pool, like the block-watcher,
  so floor/last-sale coverage stays fresh independent of refresh load.

[Unreleased]: https://github.com/FreeBeings-io/he-nft-directory/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/FreeBeings-io/he-nft-directory/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/FreeBeings-io/he-nft-directory/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/FreeBeings-io/he-nft-directory/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/FreeBeings-io/he-nft-directory/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/FreeBeings-io/he-nft-directory/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/FreeBeings-io/he-nft-directory/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/FreeBeings-io/he-nft-directory/releases/tag/v0.1.0
