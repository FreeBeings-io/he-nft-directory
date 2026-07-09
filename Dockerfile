# he-nft-directory — Hive Engine NFT Directory.
# One image, two roles selected by the entrypoint: the sync service
# (HENFT_ROLE=sync, default) and the read-only HTTP API (HENFT_ROLE=api).
# Single-stage slim image: psycopg[binary] + httpx ship their own wheels.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
RUN pip install '.[server]'

COPY docker-entrypoint.sh /usr/local/bin/henftdir-entrypoint
RUN chmod +x /usr/local/bin/henftdir-entrypoint \
 && useradd --system --no-create-home app
USER app

ENTRYPOINT ["henftdir-entrypoint"]
