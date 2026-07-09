#!/bin/sh
# he-nft-directory maps its config from CLI flags/env; this wrapper builds the
# invocation from the container environment so DSNs (which carry Postgres
# passwords) come from the env/secret, never the image or the compose file.
#
# HENFT_ROLE=sync (default): block-watcher + refresh worker + periodic
#   sweeps, reading Hive Engine nodes directly (no Hive L1 node needed).
#   Needs HENFT_DSN (own db).
# HENFT_ROLE=api: gunicorn serving henftdir.api. Needs HENFT_DSN.
set -eu

: "${HENFT_DSN:?set HENFT_DSN, e.g. 'host=172.17.0.1 dbname=henftdir user=henft_rw password=...'}"

case "${HENFT_ROLE:-sync}" in
  sync)
    set -- --dsn "$HENFT_DSN"
    if [ -n "${LOG_LEVEL:-}" ]; then
      set -- "$@" --log-level "$LOG_LEVEL"
    fi
    exec python -m henftdir "$@"
    ;;
  api)
    # A cold account (never queried before) triggers a synchronous fetch
    # across every known symbol (sync.refresh_account) before responding --
    # found live: gunicorn's 30s default worker timeout kills that request
    # mid-flight and returns an empty 500. 90s leaves real margin over the
    # measured cold-fetch cost.
    exec gunicorn --bind "0.0.0.0:${PORT:-8080}" \
      --workers "${WEB_CONCURRENCY:-2}" \
      --timeout "${WEB_TIMEOUT:-90}" \
      --access-logfile - \
      'henftdir.api:application'
    ;;
  *)
    echo "unknown HENFT_ROLE '${HENFT_ROLE}' (expected sync|api)" >&2
    exit 64
    ;;
esac
