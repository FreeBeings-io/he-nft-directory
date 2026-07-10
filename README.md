# he-nft-directory

**Hive Engine NFT Directory** — a standalone open-source service serving Hive
Engine NFT balances, collection metadata, and market state through a public
HTTP API.

Hive Engine NFT collections each encode name/image/attributes in their own
property conventions; wallets have no unified way to fetch and display them.
This service is a **read-through cache** over Hive Engine's own current
state — no Hive L1 dependency, no local transaction ledger — plus a
versioned **display-mapping layer** that normalizes per-collection
properties into a single display object: the "directory" itself.

## Design

An account's holdings are populated the first time anyone queries it (a
bounded, parallel fetch straight from Hive Engine, a few seconds, paid
once) and kept current afterward by a lightweight background service:

- **block-watcher**: reads HE's own blocks directly by number (`getBlockInfo`
  is an O(1) point lookup carrying every transaction's contract/action/
  payload/logs inline — no Hive L1 node needed). For each nft/
  nftmarket transaction, it queues the accounts involved for a refresh —
  it never derives state, only decides who to re-check.
- **refresh worker**: drains that queue, re-fetching each account's full
  current holdings straight from HE and overwriting the cache.
- periodic sweeps keep the collection catalog and market order books
  fresh, and a slow safety-net re-touches every known account as
  insurance against a missed block.

There is deliberately no transaction ledger and no `/history` endpoint:
Hive Engine's largest collections hold tens of millions of instances, so
any design that replays or bulk-loads history scales with collection size
rather than with actual usage. A read-through cache pays only for the
accounts people actually query.

## Status

Implemented and live against mainnet. The API surface is a pre-release
draft — display-object shape, valuation semantics, freshness, and rate
limits may still change before endpoints freeze at v1.

## Running

```bash
# sync service: block-watcher + refresh worker + periodic sweeps
python -m henftdir --dsn dbname=henftdir

# HTTP API (read-only; HENFT_DSN defaults to dbname=henftdir)
gunicorn -w 4 'henftdir.api:application'
```

Endpoints: `/accounts/{account}/nfts`, `/accounts/{account}/activity`,
`/collections[/{symbol}]`, `/nfts/{symbol}/{id}`, `/market/{symbol}`,
`/status`.

Runs against public Hive Engine RPC nodes out of the box. To run against
your own HE node instead, set `HENFT_HE_NODES` (comma-separated) on the
sync service — no rebuild needed; see
[docs/DEPLOY.md §3](docs/DEPLOY.md) for details and rate-tuning.

## Principles (binding on the build)

- **Cache, not ledger:** every row mirrors what Hive Engine's own `find()`
  reported at last refresh. Correctness means "matches HE right now," not
  "correctly replayed everything that ever happened."
- **On-demand, never eager:** an account or collection is only fetched in
  full once someone asks about it (or, for the small collection catalog
  and market books, on a cheap periodic sweep) — never a bulk scan of an
  entire large collection, which doesn't scale.
- **Politeness:** bounded concurrency, a real per-node rate limit (each
  node's request spacing is serialized under an async lock — found live
  that an unguarded check-and-update let concurrent callers race past it
  entirely, silently defeating the limit), and AIMD backoff on failure,
  spread across nodes by rotation rather than always preferring one.
  Self-hosting your own HE node is a first-class supported deployment
  option (see [docs/DEPLOY.md](docs/DEPLOY.md) §3).

## Data scope & disclosure

This service stores and serves **public blockchain data**, but unlike a
ledger it is not append-only: holdings reflect the last refresh, not a
permanent record, and can be overwritten or go stale. See `/status` for
per-account freshness signals.

## License

MIT — see [LICENSE](LICENSE).
