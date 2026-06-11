"""
load_containers.py
------------------
Loads container price data into dim_container and fact_marketprice.

Strategy
--------
- Container names already exist in dim_container (populated elsewhere).
- This module:
    1. Optionally updates container_price / release_date / container_type
       on existing rows (UPDATE … WHERE container_name = …).
    2. Streams price frames and inserts rows into fact_marketprice with
       container_id filled in (weapon_id / skin_id / wear_range_id left NULL
       because containers are not weapon skins).

Idempotency
-----------
The dim_container upsert uses ON CONFLICT (container_name) DO UPDATE so that
re-runs refresh prices and dates without duplicating rows.
The fact insert has no duplicate guard — call only once per ETL run, or add
a unique constraint / dedup logic as needed.

Usage
-----
    from etl.load.load_containers import load_container_dimensions

    load_container_dimensions(
        price_frames=price_frames,          # list of DataFrames with columns:
                                            #   container_name, price, date, quantity
        container_csv_path="data/processed/containers.csv",  # optional
    )
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional
import sys

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.config import DB_CONFIG
from config.logger import get_logger

logger = get_logger(__name__)

CSV_OUT_DIR = Path("data/processed/to_csv")


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to the database: {e}")
        raise


# ---------------------------------------------------------------------------
# CSV helper
# ---------------------------------------------------------------------------

def _save_csv(df: pd.DataFrame, name: str) -> None:
    CSV_OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = CSV_OUT_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    logger.info(f"[csv] Saved {len(df)} rows → {path}")


# ---------------------------------------------------------------------------
# Fetch the container name → id map from the DB
# (names are already loaded; we just need the IDs for FK resolution)
# ---------------------------------------------------------------------------

def _fetch_container_map(conn) -> Dict[str, int]:
    """Return {container_name: container_id} for all rows in dim_container."""
    with conn.cursor() as cur:
        cur.execute('SELECT container_id, container_name FROM "dim_container"')
        return {row[1]: row[0] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Optional: upsert container metadata (price, release_date, container_type)
# into existing dim_container rows identified by container_name.
# Skip this function if you only manage names outside this module.
# ---------------------------------------------------------------------------

def upsert_container_metadata(conn, df: pd.DataFrame) -> int:
    """Update container_price / release_date / container_type for existing rows.

    Expects df columns: container_name, container_price, release_date, container_type
    Uses INSERT … ON CONFLICT (container_name) DO UPDATE so new names are also
    inserted if they somehow appear here first.

    Returns the total row count in dim_container after the operation.
    """
    required = {"container_name"}
    if not required.issubset(df.columns):
        raise ValueError(f"df must contain at least: {required}")

    df = df.copy()
    for col in ("container_price", "release_date", "container_type"):
        if col not in df.columns:
            df[col] = None

    rows = [
        (
            row["container_name"],
            row["container_price"] if pd.notna(row["container_price"]) else None,
            row["release_date"]    if pd.notna(row["release_date"])    else None,
            row["container_type"]  if pd.notna(row["container_type"])  else None,
        )
        for _, row in df.iterrows()
    ]

    sql = """
        INSERT INTO "dim_container" (container_name, container_price, release_date, container_type)
        VALUES %s
        ON CONFLICT (container_name) DO UPDATE
            SET container_price  = EXCLUDED.container_price,
                release_date     = EXCLUDED.release_date,
                container_type   = EXCLUDED.container_type
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM "dim_container"')
        total = cur.fetchone()[0]

    logger.info(f"[dim_container] Upsert complete. Total rows: {total}")
    return total


# ---------------------------------------------------------------------------
# Price-range helpers  (reused from the main loader)
# ---------------------------------------------------------------------------

def _normalize_market_date(date_series: pd.Series) -> pd.Series:
    return pd.to_datetime(
        date_series.astype(str).str.replace(r"\s*:\s*\+0$", "", regex=True),
        errors="coerce",
    )


def _assign_price_range_vectorized(
    prices: pd.Series,
    price_range_db: pd.DataFrame,
) -> pd.Series:
    pr = price_range_db.sort_values("min_price").reset_index(drop=True)
    bins   = list(pr["min_price"]) + [float("inf")]
    labels = list(pr["price_range_id"].astype(int))
    return pd.cut(
        prices,
        bins=bins,
        labels=labels,
        right=False,
        include_lowest=True,
    ).astype(object)


# ---------------------------------------------------------------------------
# Chunk processor — one price_frame → fact rows
# ---------------------------------------------------------------------------

def _process_container_chunk(
    chunk: pd.DataFrame,
    container_map: Dict[str, int],
    price_range_db: pd.DataFrame,
    item_type_map: Dict[str, int],
) -> pd.DataFrame:
    """Transform one container price frame into fact_marketprice rows.

    Expected chunk columns (at minimum):
        container_name, price, date
    Optional:
        quantity, item_type  (defaults to "Standard" if absent)

    Returns an empty DataFrame if nothing matches.
    """
    chunk = chunk[chunk["container_name"].notna()].copy()
    if chunk.empty:
        return pd.DataFrame()

    chunk["date_parsed"]   = _normalize_market_date(chunk["date"])
    chunk["date_id"]       = chunk["date_parsed"].dt.date.where(
        chunk["date_parsed"].notna(), other=None
    )
    chunk["container_id"]  = chunk["container_name"].map(container_map)
    chunk["price_range_id"] = _assign_price_range_vectorized(chunk["price"], price_range_db)

    # item_type_id — use "Standard" unless the frame carries a type column
    standard_id = item_type_map.get("Standard")
    if "item_type" in chunk.columns:
        chunk["item_type_id"] = chunk["item_type"].map(item_type_map).fillna(standard_id)
    else:
        chunk["item_type_id"] = standard_id

    if "quantity" not in chunk.columns:
        chunk["quantity"] = None

    # Drop rows without a resolvable container_id or required fields
    before = len(chunk)
    chunk = chunk[chunk["container_id"].notna() & chunk["price"].notna() & chunk["date_id"].notna()]
    dropped = before - len(chunk)
    if dropped:
        logger.debug(f"[container chunk] Dropped {dropped} unresolvable rows.")

    if chunk.empty:
        return pd.DataFrame()

    out = chunk[[
        "price", "quantity", "date_id", "container_id", "price_range_id", "item_type_id",
    ]].copy()
    out = out.rename(columns={"quantity": "volume"})

    # Nullable FK columns that don't apply to containers
    for col in ("skin_id", "weapon_id", "wear_range_id", "sticker_id", "team_id", "match_id"):
        out[col] = None

    # Coerce types
    out["container_id"]   = out["container_id"].apply(lambda x: None if pd.isna(x) else int(x))
    out["price_range_id"] = out["price_range_id"].apply(lambda x: None if pd.isna(x) else int(x))
    out["item_type_id"]   = out["item_type_id"].apply(lambda x: None if pd.isna(x) else int(x))
    out["volume"]         = out["volume"].apply(lambda x: None if pd.isna(x) else int(x))
    out["price"]          = out["price"].apply(lambda x: None if pd.isna(x) else float(x))

    return out


# ---------------------------------------------------------------------------
# Streaming fact loader
# ---------------------------------------------------------------------------

FACT_COLUMNS = [
    "price", "volume", "date_id", "skin_id", "weapon_id",
    "wear_range_id", "price_range_id", "item_type_id",
    "container_id", "sticker_id", "team_id", "match_id",
]


def load_container_facts_streaming(
    conn,
    price_frames: List[pd.DataFrame],
    container_map: Dict[str, int],
    price_range_db: pd.DataFrame,
    item_type_map: Dict[str, int],
    batch_size: int = 50,
) -> int:
    """Insert container price rows into fact_marketprice in batches.

    Parameters
    ----------
    conn            : open psycopg2 connection
    price_frames    : list of DataFrames, each with at least
                      [container_name, price, date]
    container_map   : {container_name: container_id} from dim_container
    price_range_db  : DataFrame with [price_range_id, price_range, min_price, max_price]
    item_type_map   : {item_type: item_type_id} from dim_itemtype
    batch_size      : number of frames per INSERT round-trip

    Returns total rows inserted.
    """
    col_str    = ", ".join(f'"{c}"' for c in FACT_COLUMNS)
    insert_sql = f'INSERT INTO "fact_marketprice" ({col_str}) VALUES %s'

    total_inserted = 0
    num_batches    = (len(price_frames) + batch_size - 1) // batch_size

    for batch_idx, batch_start in enumerate(range(0, len(price_frames), batch_size), start=1):
        batch  = price_frames[batch_start : batch_start + batch_size]
        chunks = [
            _process_container_chunk(frame, container_map, price_range_db, item_type_map)
            for frame in batch
        ]
        valid_chunks = [c for c in chunks if c is not None and len(c) > 0]

        if not valid_chunks:
            logger.info(
                f"[fact_marketprice/containers] Batch {batch_idx}/{num_batches} "
                "— no rows matched, skipping."
            )
            continue

        facts_batch = pd.concat(valid_chunks, ignore_index=True)

        # Drop rows that would violate NOT NULL: date_id, price, item_type_id, container_id
        required = ["date_id", "price", "item_type_id", "container_id"]
        null_mask  = facts_batch[required].isnull().any(axis=1)
        null_rows  = facts_batch[null_mask]
        facts_batch = facts_batch[~null_mask]

        if not null_rows.empty:
            null_path = CSV_OUT_DIR / "null_values_containers.csv"
            CSV_OUT_DIR.mkdir(parents=True, exist_ok=True)
            null_rows.to_csv(null_path, mode="a", header=not null_path.exists(), index=False)
            logger.warning(
                f"[fact_marketprice/containers] Batch {batch_idx}/{num_batches} "
                f"— dropped {len(null_rows)} rows with nulls, appended to {null_path}"
            )

        if facts_batch.empty:
            logger.info(
                f"[fact_marketprice/containers] Batch {batch_idx}/{num_batches} "
                "— all rows dropped, skipping."
            )
            continue

        rows = [tuple(r) for r in facts_batch[FACT_COLUMNS].itertuples(index=False, name=None)]
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows, page_size=1000)
        conn.commit()

        total_inserted += len(rows)
        logger.info(
            f"[fact_marketprice/containers] Batch {batch_idx}/{num_batches} "
            f"— inserted {len(rows)} rows (total so far: {total_inserted})"
        )

    logger.info(
        f"[fact_marketprice/containers] Streaming load complete. "
        f"Total rows inserted: {total_inserted}"
    )
    return total_inserted


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_container_dimensions(
    price_frames: List[pd.DataFrame],
    container_metadata_df: Optional[pd.DataFrame] = None,
    save_csv: bool = True,
    load_to_db: bool = True,
    batch_size: int = 50,
) -> None:
    """Full container ETL: (optionally) update dim_container metadata, then
    stream price rows into fact_marketprice.

    Parameters
    ----------
    price_frames
        List of DataFrames. Each must have at minimum:
            container_name  – must match a name already in dim_container
            price           – float
            date            – string or datetime parseable by pandas
        Optional columns:
            quantity        – int
            item_type       – "Standard" | "StatTrak" | "Souvenir"

    container_metadata_df
        Optional DataFrame with [container_name, container_price,
        release_date, container_type] to upsert into dim_container.
        Pass None to skip the metadata update entirely.

    save_csv
        Write dim_container snapshot and rejected rows to
        data/processed/to_csv/ before loading.

    load_to_db
        Set False for a dry-run: builds everything and saves CSVs but
        makes no DB writes.

    batch_size
        Frames per INSERT round-trip for fact loading.
    """
    if not price_frames:
        logger.warning("[load_containers] price_frames is empty — nothing to do.")
        return

    conn = get_connection()
    try:
        # ------------------------------------------------------------------
        # 1. Optionally upsert container metadata
        # ------------------------------------------------------------------
        if container_metadata_df is not None and not container_metadata_df.empty:
            if save_csv:
                _save_csv(container_metadata_df, "dim_container_update")
            if load_to_db:
                upsert_container_metadata(conn, container_metadata_df)
            else:
                logger.info("[load_containers] load_to_db=False — skipping dim_container upsert.")

        # ------------------------------------------------------------------
        # 2. Resolve lookup tables from DB
        # ------------------------------------------------------------------
        container_map = _fetch_container_map(conn)
        logger.info(f"[load_containers] Resolved {len(container_map)} containers from DB.")

        with conn.cursor() as cur:
            cur.execute('SELECT price_range_id, price_range FROM "dim_price_range"')
            pr_rows = cur.fetchall()

        # We need min_price / max_price to bucket with pd.cut.
        # Build them from the canonical ranges if not stored in the DB.
        PRICE_RANGE_BINS = [
            (1, "0-10",    0,   10),
            (2, "10-50",   10,  50),
            (3, "50-100",  50,  100),
            (4, "100-500", 100, 500),
            (5, "500+",    500, float("inf")),
        ]
        pr_df = pd.DataFrame(pr_rows, columns=["price_range_id", "price_range"])
        bin_df = pd.DataFrame(PRICE_RANGE_BINS,
                              columns=["price_range_id", "price_range", "min_price", "max_price"])
        price_range_db = pr_df.merge(
            bin_df[["price_range", "min_price", "max_price"]],
            on="price_range",
            how="left",
        )

        with conn.cursor() as cur:
            cur.execute('SELECT item_type_id, item_type FROM "dim_itemtype"')
            item_type_map = {row[1]: row[0] for row in cur.fetchall()}

        # ------------------------------------------------------------------
        # 3. Stream facts
        # ------------------------------------------------------------------
        if not load_to_db:
            logger.info("[load_containers] load_to_db=False — skipping fact_marketprice insert.")
            return

        load_container_facts_streaming(
            conn,
            price_frames,
            container_map,
            price_range_db,
            item_type_map,
            batch_size=batch_size,
        )

    finally:
        conn.close()
        logger.info("[load_containers] Database connection closed.")


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    from etl.extract.extract_prices import extract_prices

    parser = argparse.ArgumentParser(
        description="Load container price data into fact_marketprice."
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        metavar="N",
        help="Limit how many price files extract_prices loads (useful for testing).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        metavar="N",
        help="Number of price frames per INSERT round-trip (default: 50).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and validate everything but make no DB writes.",
    )
    parser.add_argument(
        "--container-metadata-csv",
        type=str,
        default=None,
        metavar="PATH",
        help=(
            "Optional CSV with columns [container_name, container_price, "
            "release_date, container_type] to upsert into dim_container."
        ),
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # 1. Extract price frames and keep only container rows
    # ------------------------------------------------------------------
    logger.info("Extracting price frames …")
    all_frames = extract_prices(max_files=args.max_files)
    logger.info(f"Loaded {len(all_frames)} price frames total.")

    container_frames = [
        df for df in all_frames
        if "container_name" in df.columns and df["container_name"].notna().any()
    ]
    logger.info(f"Found {len(container_frames)} frames with container data.")

    if not container_frames:
        logger.warning(
            "No container frames found. "
            "Make sure your price frames have a 'container_name' column."
        )
        raise SystemExit(1)

    # ------------------------------------------------------------------
    # 2. Optionally load container metadata CSV
    # ------------------------------------------------------------------
    metadata_df: Optional[pd.DataFrame] = None
    if args.container_metadata_csv:
        metadata_df = pd.read_csv(args.container_metadata_csv)
        logger.info(
            f"Loaded container metadata from {args.container_metadata_csv} "
            f"({len(metadata_df)} rows)."
        )

    # ------------------------------------------------------------------
    # 3. Run
    # ------------------------------------------------------------------
    load_container_dimensions(
        price_frames=container_frames,
        container_metadata_df=metadata_df,
        save_csv=True,
        load_to_db=not args.dry_run,
        batch_size=args.batch_size,
    )

    logger.info("Done.")