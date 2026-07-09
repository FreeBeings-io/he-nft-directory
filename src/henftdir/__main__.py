"""Entrypoint: python -m henftdir [options]

Runs the sync service: a block-watcher (queues accounts touched by HE
nft/nftmarket transactions) and a refresh worker (re-fetches those
accounts' current state directly from HE), plus periodic catalog/market/
safety-net sweeps. No Hive L1 dependency -- see
service.py's module docstring. The HTTP API is served separately:
gunicorn 'henftdir.api:application' (HENFT_DSN).
"""

import argparse
import asyncio
import logging

from .service import Service


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="henftdir",
        description="Hive Engine NFT Directory sync service",
    )
    parser.add_argument(
        "--dsn", default="dbname=henftdir",
        help="this app's database (default: %(default)s)",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    service = Service(args.dsn)

    async def run() -> None:
        service.install_signal_handlers()
        await service.run()

    asyncio.run(run())


if __name__ == "__main__":
    main()
