"""Configuration: static defaults, with the deployment-shape knobs
overridable via environment variables (see docs/DEPLOY.md) -- a Docker
operator must be able to point at their own Hive Engine node and retune
politeness without rebuilding the image."""

import os


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default

# Public Hive Engine RPC nodes (health + cross-node consensus verified
# 2026-07-10: identical block/database hashes at a settled block; the client
# fails over down this list — membership is a starting list, not a guarantee).
#
# herpc.actifit.io was added and removed the same day: its `blockchain`
# endpoint is fine (it passed the consensus probe), but its `contracts`
# endpoint 429s `find` calls with "restricted by policy" — 422 failures in
# a few hours of production rotation, every one of them from that node,
# including aborted market-book pagination. A node must serve BOTH
# endpoints unrestricted to belong in this rotation.
HE_NODES = [
    "https://api.hive-engine.com/rpc",
    "https://api2.hive-engine.com/rpc",
    "https://herpc.dtools.dev",
    "https://enginerpc.com",
    # Added 2026-07-17 after production logs showed sustained "all HE nodes
    # failed" windows (429s/503s) with only four nodes. Each passed the same
    # admission bar as the originals: both endpoints unrestricted (blockchain
    # getLatestBlockInfo + contracts find) AND identical block/database
    # hashes vs api.hive-engine.com at settled block 61160000.
    "https://herpc.kanibot.com",
    "https://api.primersion.com",
    "https://herpc.tribaldex.com",
    "https://herpc.liotes.com",
    "https://he.atexoras.com:2083",
]
# 2026-07-17 admission sweep also REJECTED: engine.deathwing.me (525 at the
# edge), engine.beeswap.tools / herpc.hivelive.me / he.ausbit.dev /
# engine.rishipanthee.com (no response), he.sourov.dev (Cloudflare
# challenge page, not JSON), herpc.actifit.io (still barred -- see above).
# Deployment override: HENFT_HE_NODES as a comma-separated list, e.g.
#   HENFT_HE_NODES=http://my-he-node:5000   (self-hosted; see DEPLOY.md §3)
if os.environ.get("HENFT_HE_NODES"):
    HE_NODES = [n.strip() for n in os.environ["HENFT_HE_NODES"].split(",")
                if n.strip()]

# Politeness toward public HE nodes: bounded concurrency, spacing between
# calls, AIMD backoff on failure -- a fixed cooldown re-hammers a
# persistently-degraded node every window forever; learned backoff
# (doubling per failure, capped, decaying per success) stops wasting time
# on a node in extended distress while still recovering it gradually once
# it's healthy again.
#
# HE_MAX_CONCURRENCY is pool-wide, not per-node (one semaphore shared by
# HENodes.call()) -- but henodes.py rotates which node each call tries
# first specifically so this concurrency actually spreads across all
# configured nodes instead of concentrating on one. An isolated steady-
# load test against one node found headroom through ~10 concurrent
# requests -- but a *real* cold-fetch (one account, ~150 symbols, this
# constant's worth of concurrent lookups) cascaded into 503s AND a 429
# across all three nodes even at 8, well under that isolated ceiling: a
# failover pushes several concurrently-struggling calls onto the same
# "next" node at once, concentrating load beyond what steady, independent
# traffic would produce. 4 leaves real margin against the *bursty* access
# pattern actually seen live, not the isolated one -- re-measure with a
# real cold-fetch (not an isolated ramp) before raising it.
HE_MAX_CONCURRENCY = _env_int("HENFT_HE_MAX_CONCURRENCY", 4)
# Per node (see henodes.py's HENodes._throttle -- fixed 2026-07-08 to
# actually enforce this per node under concurrency; it was previously an
# unguarded global check-and-update that let concurrent callers race past
# it entirely). 0.1s = 10 req/s/node. Chosen as a conservative starting
# point, not a clean measurement -- a sustained-rate test right after the
# race-condition fix produced contradictory results (the two official
# hive-engine.com nodes failing at just 5 req/s, herpc.dtools.dev showing
# zero failures at any rate tried, the opposite of what production logs
# blamed) most likely because this session's own cumulative testing had
# gotten the testing IP flagged. Re-measure from a clean vantage point
# before trusting this number either way.
HE_MIN_CALL_SPACING_SECONDS = _env_float("HENFT_HE_CALL_SPACING", 0.1)
HE_REQUEST_TIMEOUT_SECONDS = 20
HE_FAILURE_BACKOFF_FLOOR_SECONDS = 30
# A 503 response (the server answered, just under momentary load) gets a
# much shorter starting backoff than a transport-level failure (connection
# refused/timeout/DNS) -- found live: a real account's cold-fetch (~150
# concurrent per-symbol lookups) pushed one node into brief 503s under its
# own burst; the 30s-floor treatment (which doubles to 60s on a first
# failure) then cascaded, since losing one of three nodes concentrates the
# same burst onto the remaining two, pushing them over their own limits in
# turn. A query against the exact same "failing" table succeeded instantly
# once isolated from the burst -- confirming it's momentary, not a real
# outage. See henodes.py's call().
HE_STATUS_FAILURE_BACKOFF_FLOOR_SECONDS = 3
# 429 is not a 503: the node is explicitly saying "you, specifically, are
# too fast," so re-trying it on the 503 floor (3s) just re-earns the same
# 429 and keeps the client flagged. Give it the full outage-grade floor,
# and honor an explicit Retry-After header (capped -- a misconfigured node
# must not park itself for an hour) when the node provides one.
HE_RATE_LIMIT_BACKOFF_FLOOR_SECONDS = 30
HE_RETRY_AFTER_CAP_SECONDS = 120
HE_FAILURE_BACKOFF_CAP_SECONDS = 600
HE_FAILURE_BACKOFF_DECAY = 0.95
# Found live: under *sustained* contention (not a one-off blip), a symbol
# lookup that fails round 1 usually keeps failing rounds 2-4 too, since
# 14s (2+4+8) of cumulative backoff isn't long enough for real relief when
# ~150 other concurrent lookups are contributing to the same contention.
# All that extra retrying does is keep every node in cooldown for longer,
# widening the window where *other* concurrent lookups also fail. 2 rounds
# (2s max backoff) still gives one genuine transient blip a chance to
# clear, without prolonging a real, sustained crunch. A symbol that still
# fails is safe to leave for a later pass now that a failed lookup no
# longer overwrites existing cached data with "confirmed empty" (see
# sync._fetch_symbol_for_account).
HE_RETRY_ROUNDS = 2
HE_RETRY_BACKOFF = 2.0

# block_watcher: idle wait between polls once caught up to HE's head (HE
# blocks land roughly every 3s, matching Hive's own block time).
BLOCKWATCH_IDLE_SECONDS = 3.0
# The watcher never fetches closer than this many blocks behind whatever
# getLatestBlockInfo reports as head -- i.e. it treats (head - MARGIN) as
# the real, fetchable tip. Without this, the block just-reported as head is
# often still propagating across the node pool: a DIFFERENT node than the
# one that answered getLatestBlockInfo gets asked for it via rotation and
# returns null because it hasn't caught up yet (found live 2026-07-17:
# several-per-minute "lagging node" retries at the bleeding edge). Those
# retries are harmless (the null-block guard never skips), just wasted
# calls and log noise. A 1-block margin costs ~3s of extra tip latency
# (one HE block time) and gives the pool that long to settle before this
# service ever asks for it.
BLOCKWATCH_SETTLE_BLOCKS = _env_int("HENFT_BLOCKWATCH_SETTLE_BLOCKS", 1)

# refresh_worker: idle wait when refresh_queue is empty.
REFRESH_IDLE_SECONDS = 5.0
# Account-level parallelism: how many queued accounts the worker drains
# concurrently, each on its own DB connection. Serial draining (batch=1)
# was the throughput ceiling -- measured live 2026-07-17 at ~1.1s/account,
# so a block touching 1000 accounts took ~19 min to reflect. Total HE-call
# concurrency stays bounded by the shared node semaphore
# (HE_MAX_CONCURRENCY) no matter how large this is, so batching just fills
# that already-node-safe budget instead of leaving it idle between one
# account's lookups. Defaults to the node budget (enough to keep it full
# when accounts are single-symbol); raise for multi-symbol-heavy workloads
# at the cost of more DB connections (host PG max_connections is 100).
REFRESH_WORKER_BATCH = _env_int("HENFT_REFRESH_WORKER_BATCH", HE_MAX_CONCURRENCY)
# Pause between concurrency-sized chunks of symbol lookups for BACKGROUND
# account refreshes (queue drain) -- found live 2026-07-10: an
# un-paced ~115-symbol refresh saturates the pool for several seconds,
# 503-cooldowns every node at once, and produces a "no nodes" retry storm
# for anything else in flight (~4k retry warnings in hours from one heavy
# account). Background latency is free; the API's synchronous cold-fetch
# stays un-paced (a user is waiting).
REFRESH_PACE_SECONDS = 0.5

# Retry backoff for failed symbol lookups re-enqueued into refresh_queue
# (sync._requeue_failed): base * 2^attempts, capped. The cap matters more
# than the base -- during a sustained multi-node outage every pending
# retry converges to one attempt per cap window, which must stay polite.
REFRESH_RETRY_BASE_SECONDS = 60
REFRESH_RETRY_CAP_SECONDS = 21600  # 6h

# A cached account older than this gets a background refresh enqueued when
# it's read (api._ensure_known) -- the freshness bound is tied to access,
# so correction work scales with real usage, not fleet size. This replaced
# the hourly full-fleet safety-net sweep (see sync.py).
ACCOUNT_STALE_AFTER_SECONDS = _env_float("HENFT_ACCOUNT_STALE_AFTER", 21600.0)

# Full collection-catalog mirror (~150 rows platform-wide) -- cheap, so
# refreshed often.
CATALOG_INTERVAL_SECONDS = 300

# Market loop cadence. Each cycle drains market_refresh_queue -- symbols the
# block-watcher flagged dirty on a live market event -- which is cheap and
# usually empty, so this polls more often than the old blind all-symbols
# sweep did. Event-driven: steady-state market work now scales with trading
# activity, not with how many symbols are cached.
MARKET_INTERVAL_SECONDS = 60
# Backstop full sweep of every cached symbol's sellBook, in case a market
# event was ever missed. Rare relative to the dirty drain -- the whole point
# of the event-driven path is to make this the exception, not the steady
# state. Runs once at startup too (fresh process needs floors immediately).
MARKET_FULL_SWEEP_SECONDS = 3600
# 10 pages * 1000 keeps `offset` <= 9000, under HE's ~10k offset cap (it
# returns 400 beyond that -- verified live). A sellBook larger than this
# can't be fully paginated on public HE at all; refresh_market degrades to a
# best-effort floor from what it fetched rather than failing the symbol.
MARKET_MAX_PAGES = 10

# Rolling activity feed (nft_events): captured live by the block-watcher,
# backfilled backward toward the window start by a low-priority background
# loop, pruned past the window. The window is a product promise ("recent
# activity"), not an archive -- see the nft_events schema comment.
ACTIVITY_WINDOW_DAYS = 30
# Backfill politeness: BATCH blocks per burst, then PAUSE. Was 20/5s
# (~4 blocks/s); production logs 2026-07-17 showed those bursts drawing
# 429s from herpc.dtools.dev and 503s from api2.hive-engine.com, stalling
# the cursor on "all HE nodes failed" for whole passes. 10 blocks / 10s
# (~1 block/s) walks a 30-day window (~850k blocks) in roughly 10 days --
# slower still, but a cursor that advances beats one that retries; the
# feed serves partial coverage honestly meanwhile (see /status
# activity.backfill). Uses its own small HENodes pool so it can never
# starve the block-watcher or the market sweep. Env-overridable so an
# operator on a self-hosted node can open the throttle.
ACTIVITY_BACKFILL_BATCH = _env_int("HENFT_ACTIVITY_BACKFILL_BATCH", 10)
ACTIVITY_BACKFILL_PAUSE_SECONDS = _env_float("HENFT_ACTIVITY_BACKFILL_PAUSE", 10.0)
# Prune once a day (founder call): with a 30-day window a daily prune
# overshoots retention by at most ~3%, and one small indexed DELETE per day
# beats constant delete churn.
ACTIVITY_PRUNE_INTERVAL_SECONDS = 86400
# HE blocks track Hive blocks ~1:1 (measured 0.98 recently), so the window
# start is approximated as head - days * 28800 Hive-blocks/day. The window
# is advisory ("about 30 days"), so drift of a few percent is fine.
ACTIVITY_BLOCKS_PER_DAY = 28800
