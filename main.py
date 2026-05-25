"""
main.py  –  ETL Pipeline Entry Point
-------------------------------------
Run the full pipeline:
    python main.py

Or run individual stages via flags:
    python main.py --stage extract
    python main.py --stage transform
    python main.py --stage load
    python main.py --dry-run        # Extract + Transform only, no DB writes
"""

import argparse
import sys
import os
from typing import Optional

# Make sure project root is on sys.path regardless of where script is called
sys.path.insert(0, os.path.dirname(__file__))

from config.logger import get_logger
from etl.extract.extract_weapons import extract_all as extract_weapons_all
from etl.extract.extract_prices import extract_prices
from etl.transform.transform import transform_all
from etl.load.load import load_all, load_prices_dimensions

logger = get_logger("pipeline")


def run_extract(max_price_files: Optional[int] = None):
    logger.info("=== STAGE: EXTRACT ===")
    raw = extract_weapons_all()
    price_frames = extract_prices(max_files=max_price_files)
    for key, df in raw.items():
        logger.info(f"  {key}: {len(df)} rows extracted.")
    logger.info(f"  prices: {len(price_frames)} price frames extracted.")
    return raw, price_frames


def run_transform(raw, price_frames):
    logger.info("=== STAGE: TRANSFORM ===")
    transformed = transform_all(raw, price_frames)
    for key, df in transformed.items():
        logger.info(f"  {key}: {len(df)} rows after transform.")
    return transformed


def run_load(transformed):
    logger.info("=== STAGE: LOAD ===")
    load_all(transformed)
    load_prices_dimensions(price_frames=transformed.get("prices"))


def main():
    parser = argparse.ArgumentParser(description="CS Skin DW – ETL pipeline")
    parser.add_argument(
        "--stage",
        choices=["extract", "transform", "load", "all"],
        default="all",
        help="Which stage to run (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run Extract + Transform only; skip loading into the DB.",
    )
    args = parser.parse_args()

    try:
        if args.stage in ("extract", "all") or args.dry_run:
            raw, price_frames = run_extract()
        else:
            logger.warning("Skipping extract – no data available for later stages.")
            return

        if args.stage in ("transform", "all") or args.dry_run:
            transformed = run_transform(raw, price_frames)
        else:
            return

        if args.dry_run:
            logger.info("Dry-run mode: skipping load stage.")
            return

        if args.stage in ("load", "all"):
            run_load(transformed)

        logger.info("Pipeline finished successfully.")

    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
