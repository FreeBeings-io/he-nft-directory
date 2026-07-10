# Changelog

All notable changes to **he-nft-directory** are recorded here. Format follows
[Keep a Changelog](https://keepachangelog.com); this project uses
[SemVer](https://semver.org).

## [Unreleased]

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
- API response shapes are pre-release drafts and may change (display-object
  shape, valuation semantics, freshness, and rate limits are still being
  finalized) — including whether a history endpoint is ever added; there is
  deliberately none in this design.
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
- Market valuation. Floor price is rolled up per payment token **and per
  group** — HE sell orders carry a grouping (e.g. rarity/type), and one
  collection often spans hundreds of groups, so a single symbol-wide floor
  is misleading. Order books commonly span several payment tokens; prices
  are served in their own token, with no USD conversion. A forward-only
  trade log, captured by the block-watcher from HE's own sale events, adds
  last-sale price and trailing-30-day volume per token — HE exposes no
  trade-history endpoint, so this is the only source. It is capture-only and
  never feeds holdings state.

- Deployment-shape knobs are env-overridable, so Docker operators can
  point at a self-hosted Hive Engine node without rebuilding the image:
  `HENFT_HE_NODES` (comma-separated), `HENFT_HE_MAX_CONCURRENCY`,
  `HENFT_HE_CALL_SPACING`. See docs/DEPLOY.md §3.

### Fixed

All found live during and after the first real deploy:

- NFT activity happening inside OTHER contracts' transactions (pack
  openings and any contract that issues/moves NFTs internally) was
  invisible to the block-watcher: it gated on the transaction's own
  contract, but those txs carry their nft-contract events in the logs
  under a different top-level contract. Detection is now per emitted
  event, which fixes both refresh queueing (previously only the hourly
  safety-net caught those holders) and activity capture.
- One newly-added rotation node was removed the same day: it serves the
  `blockchain` endpoint fine but rate-limits `contracts` `find` queries
  by policy (hundreds of 429s in hours, including aborted market-book
  pagination). Rotation membership now requires both endpoints
  unrestricted.

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
  all three configured nodes.
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
