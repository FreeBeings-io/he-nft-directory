# Deploying he-nft-directory

Two processes against one database — no Hive L1 node needed:

| Process | Reads | Writes | Image role |
|---|---|---|---|
| henftdir sync | HE nodes | henftdir DB | `HENFT_ROLE=sync` |
| henftdir API | henftdir DB | — | `HENFT_ROLE=api` |

The sync process runs a block-watcher (walks HE blocks directly, queues
touched accounts) and a refresh worker (re-fetches queued accounts'
current holdings from HE), plus periodic catalog/market/safety-net sweeps
— see `src/henftdir/service.py`.

Disk: `instances` only ever holds rows for accounts someone has actually
queried, not the whole platform — this is a cache, not a full mirror. It
grows with real traffic, not with HE's total history. Expect low disk use
even at meaningful scale.

## 1. Database & role

```bash
sudo -u postgres createdb henftdir
sudo -u postgres psql <<'SQL'
  CREATE ROLE henft_rw LOGIN PASSWORD 'CHANGE_ME';
  ALTER DATABASE henftdir OWNER TO henft_rw;
SQL
```

The schema (`src/henftdir/schema.sql`) is applied automatically on sync
service startup — no separate migration step.

## 2. Compose example

```yaml
  henftdir-sync:
    image: he-nft-directory:latest
    environment:
      HENFT_ROLE: sync
      HENFT_DSN: host=<postgres-host> dbname=henftdir user=henft_rw password=${HENFT_RW_PW}
    restart: unless-stopped

  henftdir-api:
    image: he-nft-directory:latest
    environment:
      HENFT_ROLE: api
      HENFT_DSN: host=<postgres-host> dbname=henftdir user=henft_rw password=${HENFT_RW_PW}
      PORT: "8080"
    ports:
      - "127.0.0.1:8080:8080"   # bind privately; expose via your reverse proxy
    restart: unless-stopped
```

Notes:
- The API can use the `_rw` role or its own SELECT-only role; it opens
  read-only transactions either way.
- Both processes are light: the sync process makes small, indexed HE
  queries (per-account, per-symbol), never a bulk scan of an entire
  collection — see `sync.py`'s module docstring for why that matters
  (some HE collections have 10s of millions of instances).
- Optional env vars (both roles): `LOG_LEVEL` (default `INFO`);
  `HENFT_HE_NODES` (comma-separated Hive Engine RPC endpoints — overrides
  the shipped public-node list, see §3), `HENFT_HE_MAX_CONCURRENCY` and
  `HENFT_HE_CALL_SPACING` (politeness tuning, see §3). API-only:
  `WEB_CONCURRENCY` (gunicorn workers, default 2), `WEB_TIMEOUT` (gunicorn
  worker timeout in seconds, default 90 — a never-before-seen account's
  first query does a synchronous ~150-table cold-fetch before responding;
  gunicorn's own 30s default killed that request mid-flight, found live).

## 3. Community / self-hosted deployment

The service ships pointed at public Hive Engine RPC nodes and works
out of the box against them. For a production or community deployment,
point it at a self-hosted HE node instead (the witness node software is
open source): no shared public rate limits, lower latency, and you're
not dependent on a third party's node staying up. This is a first-class
supported deployment shape, not a hack — set one env var on the sync
service, no rebuild needed:

```yaml
  henftdir-sync:
    environment:
      HENFT_HE_NODES: http://my-he-node:5000        # comma-separate for several
      # Politeness defaults are tuned for SHARED PUBLIC nodes. A node you
      # own can likely take much more -- measure, then raise:
      HENFT_HE_MAX_CONCURRENCY: "16"                 # default 4
      HENFT_HE_CALL_SPACING: "0.01"                  # seconds/node, default 0.1
```

The same defaults live in `src/henftdir/config.py` (`HE_NODES`,
`HE_MAX_CONCURRENCY`, `HE_MIN_CALL_SPACING_SECONDS`) if you're running
from source and prefer editing them directly. One membership rule,
learned in production: a rotation node must serve BOTH the `blockchain`
and `contracts` endpoints unrestricted — some public nodes rate-limit
`contracts` `find` queries by policy while answering block lookups fine,
which quietly degrades market-book pagination and account refreshes.

## 4. Display mappings

`../seed_display_mappings.sql` seeds every collection that had cached
instances as of 2026-07-08 (68 of 77 — the rest are excluded: empty,
broken/burned, or clearly-test on-chain data). Built from real property
samples plus live research into each project's official site/source for
an image URL; most collections have none (dead domain, no public
API/CDN, or on-chain data that the mapping engine can't reach into) and
ship with name + attributes only — see the file's per-symbol comments
for what was checked and why. Apply with:

```bash
psql henftdir -f ../seed_display_mappings.sql
```

New collections (or ones that gain a real image source later) follow
the same shape:

```sql
INSERT INTO display_mappings (symbol, version, mapping) VALUES
('SYMBOL', 1, '{"name": {"from": "properties.name"},
                "image": {"from": "properties.thumb"},
                "attributes": ["type", "rarity"]}');
```

Unmapped symbols serve `display: null` — the API never blocks on
coverage; check `/status` → `display_mapping_coverage`.

## 5. Monitoring

- `GET /status`: per-loop last-seen HE block, known-account count,
  refresh-queue depth, mapping coverage. Alert if
  `sync.block_watcher.updated_at` goes stale, or if
  `refresh_queue_depth` grows without bound (the refresh worker can't
  keep up with the block-watcher's pace).
- The sync service logs a debug line per block with any queued accounts,
  and warnings on catalog/market/refresh failures.
- Watch the rate of `"all HE nodes failed"` warnings in the sync log.
  Found live: a sustained few-hundred-per-minute rate over hours was not
  the public nodes being down (isolated, low-volume probes against the
  exact same calls succeeded instantly) -- it was this app's own
  aggregate request rate exceeding what the nodes tolerate. If this
  climbs, check `HE_MAX_CONCURRENCY` / `HE_MIN_CALL_SPACING_SECONDS`
  (`src/henftdir/config.py`) before assuming the nodes themselves are
  degraded.
