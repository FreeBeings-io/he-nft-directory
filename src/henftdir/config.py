"""Static configuration defaults."""

# Public Hive Engine RPC nodes (health observed 2026-07-03; the client fails
# over down this list — membership is a starting list, not a guarantee).
HE_NODES = [
    "https://api.hive-engine.com/rpc",
    "https://api2.hive-engine.com/rpc",
    "https://herpc.dtools.dev",
]

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
HE_MAX_CONCURRENCY = 4
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
HE_MIN_CALL_SPACING_SECONDS = 0.1
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

# refresh_worker: idle wait when refresh_queue is empty.
REFRESH_IDLE_SECONDS = 5.0

# Full collection-catalog mirror (~150 rows platform-wide) -- cheap, so
# refreshed often relative to the safety-net sweep below.
CATALOG_INTERVAL_SECONDS = 300

# Per-symbol sell-book mirror, for every symbol that has cached instances
# (i.e. symbols someone has actually queried) -- not gated on a market flag.
MARKET_INTERVAL_SECONDS = 300
# 10 pages * 1000 keeps `offset` <= 9000, under HE's ~10k offset cap (it
# returns 400 beyond that -- verified live). A sellBook larger than this
# can't be fully paginated on public HE at all; refresh_market degrades to a
# best-effort floor from what it fetched rather than failing the symbol.
MARKET_MAX_PAGES = 10

# Safety-net sweep: NOT the primary freshness mechanism (the block-watcher
# + refresh_queue is) -- just periodic insurance against a missed/
# misparsed block. Batched and slow on purpose.
SAFETY_NET_INTERVAL_SECONDS = 3600
SAFETY_NET_BATCH_SIZE = 200
