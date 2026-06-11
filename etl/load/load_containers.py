"""
load_containers.py
------------------
Loads container / package / souvenir-package price data from
data/raw/market/ into dim_container and fact_marketprice.

Mirrors the weapons loader in load.py exactly:
  1. Reads a name-conversion table to identify container files.
  2. Upserts container names + metadata into dim_container
     (ON CONFLICT (container_name) DO UPDATE).
  3. Streams price rows into fact_marketprice in batches, with
     weapon_id / skin_id / wear_range_id left NULL because
     containers are not weapon skins.

Idempotency
-----------
- dim_container upsert is safe to re-run (ON CONFLICT … DO UPDATE).
- fact_marketprice has no duplicate guard — call once per ETL run, or
  add a unique constraint / dedup logic as needed.

Usage (programmatic)
--------------------
    from etl.load.load_containers import load_container_dimensions

    load_container_dimensions(
        market_dir="data/raw/market",
        conversion_table_path="data/raw/name_conversion_table.csv",
        batch_size=50,
        load_to_db=True,
        save_csv=True,
    )

Usage (CLI)
-----------
    python load_containers.py \\
        --market-dir data/raw/market \\
        --conversion-table data/raw/name_conversion_table.csv \\
        --batch-size 50
"""

from __future__ import annotations

import urllib.parse
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

# Columns written to fact_marketprice (must match table definition order)
FACT_COLUMNS = [
    "price", "volume", "date_id",
    "skin_id", "weapon_id", "wear_range_id",
    "price_range_id", "item_type_id",
    "container_id", "sticker_id", "team_id", "match_id",
]

# Canonical price buckets — kept in sync with load.py / _build_price_range_dim()
PRICE_RANGE_BINS: List[tuple] = [

    (1, "0-10",    0,   10),
    (2, "10-50",   10,  50),
    (3, "50-100",  50,  100),
    (4, "100-500", 100, 500),
    (5, "500+",    500, float("inf")),
]


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established.")
        return conn
    except psycopg2.OperationalError as exc:
        logger.error(f"Could not connect to the database: {exc}")
        raise


# ---------------------------------------------------------------------------
# CSV helper  (mirrors load.py / _save_csv)
# ---------------------------------------------------------------------------

def _save_csv(df: pd.DataFrame, name: str) -> None:
    CSV_OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = CSV_OUT_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    logger.info(f"[csv] Saved {len(df)} rows → {path}")


# ---------------------------------------------------------------------------
# Container-type classifier
# ---------------------------------------------------------------------------

def _classify_container_type(name: str) -> Optional[str]:
    """Return 'Case', 'Package', or 'Souvenir Package'; None if not a container."""
    if not name or pd.isna(name):
        return None
    name = str(name).strip()
    if name.endswith("Case Key"):          # keys are not containers
        return None
    if name.endswith("Souvenir Package"):
        return "Souvenir Package"
    if name.endswith("Package"):
        return "Package"
    if name.endswith("Case"):
        return "Case"
    return None




# ---------------------------------------------------------------------------
# Discover container price files from data/raw/market/
# ---------------------------------------------------------------------------

def _load_container_frames(
    market_dir: str,
    conversion_table_path: str,
) -> tuple[List[pd.DataFrame], pd.DataFrame]:
    """Read matching CSVs from market_dir and return (price_frames, metadata_df).

    price_frames : list of DataFrames, each with at minimum
                   [container_name, price, date]
    metadata_df  : [container_name, container_type, release_date] for
                   dim_container upsert.  release_date is the oldest date
                   found in the container's market CSV.
    """
    market_path = Path(market_dir)
    if not market_path.exists():
        raise FileNotFoundError(f"Market directory not found: {market_path}")

    conv_df = pd.read_csv(conversion_table_path, header=0)
    conv_df.columns = ["encoded", "decoded"]
    conv_df["encoded"] = conv_df["encoded"].astype(str).str.strip()
    conv_df["decoded"] = conv_df["decoded"].astype(str).str.strip()
    conv_df["container_type"] = conv_df["decoded"].apply(_classify_container_type)

    container_rows = conv_df[conv_df["container_type"].notna()].copy()
    if container_rows.empty:
        raise ValueError(
            "No Cases / Packages / Souvenir Packages found in conversion table."
        )
    logger.info(
        f"Found {container_rows['decoded'].nunique()} "
        "container definitions in conversion table."
    )

    # Build a stem → path index for fast lookup
    all_csvs: Dict[str, Path] = {
        p.stem: p for p in market_path.rglob("*.csv") if p.is_file()
    }

    encoded_to_name: Dict[str, str] = dict(
        zip(container_rows["encoded"], container_rows["decoded"])
    )

    price_frames: List[pd.DataFrame] = []
    missing: List[str] = []

    for encoded_stem, container_name in encoded_to_name.items():
        csv_path = all_csvs.get(encoded_stem) or all_csvs.get(
            urllib.parse.unquote(encoded_stem)
        )
        if csv_path is None:
            missing.append(container_name)
            continue

        try:
            df = pd.read_csv(csv_path)
        except Exception as exc:
            logger.warning(f"Failed reading {csv_path}: {exc}")
            continue

        # Normalise column names
        if "unix timestamp" in df.columns:
            df = df.rename(columns={"unix timestamp": "timestamp"})

        for col in ("price", "quantity", "date", "timestamp"):
            if col not in df.columns:
                df[col] = None

        df["container_name"] = container_name
        price_frames.append(df)

    if missing:
        logger.warning(f"{len(missing)} container(s) had no matching market CSV.")
    logger.info(f"Loaded {len(price_frames)} container price files.")

    # Derive release_date as the oldest date seen in each container's market data
    release_date_map: Dict[str, Optional[str]] = {}
    for frame in price_frames:
        name = frame["container_name"].iloc[0] if "container_name" in frame.columns and len(frame) > 0 else None
        if name is None:
            continue
        dates = _normalize_market_date(frame["date"]) if "date" in frame.columns else pd.Series(dtype="datetime64[ns]")
        oldest = dates.dropna().min()
        if pd.notna(oldest):
            existing = release_date_map.get(name)
            candidate = oldest.strftime("%Y-%m-%d")
            if existing is None or candidate < existing:
                release_date_map[name] = candidate

    metadata_df = (
        container_rows[["decoded", "container_type"]]
        .drop_duplicates()
        .rename(columns={"decoded": "container_name"})
        .assign(container_price=None)
    )
    metadata_df["release_date"] = metadata_df["container_name"].map(release_date_map)

    logger.info(
        f"[load_containers] Release dates derived from market data: "
        f"{metadata_df['release_date'].notna().sum()}/{len(metadata_df)} containers resolved."
    )

    return price_frames, metadata_df


# ---------------------------------------------------------------------------
# dim_container upsert  (mirrors load.py / load_weapons → _insert_dimension)
# ---------------------------------------------------------------------------

def _upsert_container_metadata(conn, df: pd.DataFrame) -> int:
    """Upsert container rows into dim_container.

    Uses ON CONFLICT (container_name) DO UPDATE so re-runs refresh
    container_price / release_date / container_type without duplicating rows.

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
        INSERT INTO "dim_container"
            (container_name, container_price, release_date, container_type)
        VALUES %s
        ON CONFLICT (container_name) DO UPDATE
            SET container_price = EXCLUDED.container_price,
                release_date    = COALESCE(EXCLUDED.release_date, "dim_container".release_date),
                container_type  = COALESCE(EXCLUDED.container_type, "dim_container".container_type)
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM "dim_container"')
        total: int = cur.fetchone()[0]

    logger.info(f"[dim_container] Upsert complete. Total rows: {total}")
    return total


# ---------------------------------------------------------------------------
# Lookup helpers  (same pattern as load.py)
# ---------------------------------------------------------------------------

def _fetch_container_map(conn) -> Dict[str, int]:
    """Return {container_name: container_id} for every row in dim_container."""
    with conn.cursor() as cur:
        cur.execute('SELECT container_id, container_name FROM "dim_container"')
        return {row[1]: row[0] for row in cur.fetchall()}


def _fetch_item_type_map(conn) -> Dict[str, int]:
    with conn.cursor() as cur:
        cur.execute('SELECT item_type_id, item_type FROM "dim_itemtype"')
        return {row[1]: row[0] for row in cur.fetchall()}


def _fetch_price_range_db(conn) -> pd.DataFrame:
    """Fetch price ranges from DB and join with canonical bin boundaries."""
    with conn.cursor() as cur:
        cur.execute('SELECT price_range_id, price_range FROM "dim_price_range"')
        pr_rows = cur.fetchall()

    pr_df  = pd.DataFrame(pr_rows, columns=["price_range_id", "price_range"])
    bin_df = pd.DataFrame(
        PRICE_RANGE_BINS,
        columns=["price_range_id", "price_range", "min_price", "max_price"],
    )
    return pr_df.merge(
        bin_df[["price_range", "min_price", "max_price"]],
        on="price_range",
        how="left",
    )


# ---------------------------------------------------------------------------
# Price-range bucketing  (identical to load.py)
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
    pr    = price_range_db.sort_values("min_price").reset_index(drop=True)
    bins  = list(pr["min_price"]) + [float("inf")]
    labels = list(pr["price_range_id"].astype(int))
    return pd.cut(
        prices,
        bins=bins,
        labels=labels,
        right=False,
        include_lowest=True,
    ).astype(object)


# ---------------------------------------------------------------------------
# Per-chunk transformer  (mirrors load.py / _process_chunk)
# ---------------------------------------------------------------------------

def _process_container_chunk(
    chunk: pd.DataFrame,
    container_map: Dict[str, int],
    price_range_db: pd.DataFrame,
    item_type_map: Dict[str, int],
    standard_id: int,
) -> pd.DataFrame:
    """Transform one container price frame into fact_marketprice rows.

    Expected columns (minimum): container_name, price, date
    Optional columns          : quantity, item_type

    Returns an empty DataFrame when nothing is usable.
    """
    chunk = chunk[chunk["container_name"].notna()].copy()
    if chunk.empty:
        return pd.DataFrame()

    chunk["date_parsed"]    = _normalize_market_date(chunk["date"])
    chunk["date_id"]        = chunk["date_parsed"].dt.date.where(
        chunk["date_parsed"].notna(), other=None
    )
    chunk["container_id"]   = chunk["container_name"].map(container_map)
    chunk["price_range_id"] = _assign_price_range_vectorized(chunk["price"], price_range_db)

    # item_type_id — honour a 'type' column when present, else default to Standard
    if "type" in chunk.columns:
        chunk["item_type_id"] = chunk["type"].map(item_type_map).fillna(standard_id)
    elif "item_type" in chunk.columns:
        chunk["item_type_id"] = chunk["item_type"].map(item_type_map).fillna(standard_id)
    else:
        chunk["item_type_id"] = standard_id

    if "quantity" not in chunk.columns:
        chunk["quantity"] = None

    # Drop rows without a resolvable container, price, or date
    before = len(chunk)
    chunk  = chunk[
        chunk["container_id"].notna()
        & chunk["price"].notna()
        & chunk["date_id"].notna()
    ]
    dropped = before - len(chunk)
    if dropped:
        logger.debug(f"[container chunk] Dropped {dropped} unresolvable rows.")

    if chunk.empty:
        return pd.DataFrame()

    out = chunk[["price", "quantity", "date_id", "container_id", "price_range_id", "item_type_id"]].copy()
    out = out.rename(columns={"quantity": "volume"})

    # Nullable FK columns that don't apply to containers
    for col in ("skin_id", "weapon_id", "wear_range_id", "sticker_id", "team_id", "match_id"):
        out[col] = None

    # Coerce types — mirrors load.py / _process_chunk
    out["container_id"]   = out["container_id"].apply(lambda x: None if pd.isna(x) else int(x))
    out["price_range_id"] = out["price_range_id"].apply(lambda x: None if pd.isna(x) else int(x))
    out["item_type_id"]   = out["item_type_id"].apply(lambda x: None if pd.isna(x) else int(x))
    out["volume"]         = out["volume"].apply(lambda x: None if pd.isna(x) else int(x))
    out["price"]          = out["price"].apply(lambda x: None if pd.isna(x) else float(x))

    return out


# ---------------------------------------------------------------------------
# Streaming fact loader  (mirrors load.py / load_facts_streaming)
# ---------------------------------------------------------------------------

def _load_container_facts_streaming(
    conn,
    price_frames: List[pd.DataFrame],
    container_map: Dict[str, int],
    price_range_db: pd.DataFrame,
    item_type_map: Dict[str, int],
    batch_size: int = 50,
) -> int:
    """Insert container price rows into fact_marketprice in batches.

    Never concatenates the full dataset into memory.
    Lower batch_size (10-20) if you hit memory limits; raise it (100-200)
    for fewer DB round-trips when you have headroom.

    Returns the total number of rows inserted.
    """
    standard_id = item_type_map.get("Standard")

    col_str    = ", ".join(f'"{c}"' for c in FACT_COLUMNS)
    insert_sql = f'INSERT INTO "fact_marketprice" ({col_str}) VALUES %s'

    total_inserted = 0
    num_batches    = (len(price_frames) + batch_size - 1) // batch_size

    for batch_idx, batch_start in enumerate(range(0, len(price_frames), batch_size), start=1):
        batch = price_frames[batch_start : batch_start + batch_size]

        chunks = [
            _process_container_chunk(
                frame, container_map, price_range_db, item_type_map, standard_id
            )
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
        required  = ["date_id", "price", "item_type_id", "container_id"]
        null_mask = facts_batch[required].isnull().any(axis=1)
        null_rows = facts_batch[null_mask]
        facts_batch = facts_batch[~null_mask]

        if not null_rows.empty:
            null_path = CSV_OUT_DIR / "null_values_containers.csv"
            CSV_OUT_DIR.mkdir(parents=True, exist_ok=True)
            null_rows.to_csv(
                null_path, mode="a", header=not null_path.exists(), index=False
            )
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

        rows = [
            tuple(r)
            for r in facts_batch[FACT_COLUMNS].itertuples(index=False, name=None)
        ]
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
# Public API  (drop-in equivalent of load.py / load_prices_dimensions)
# ---------------------------------------------------------------------------

def load_container_dimensions(
    market_dir: str = "data/raw/market",
    conversion_table_path: str = "data/raw/name_conversion_table.csv",
    price_frames: Optional[List[pd.DataFrame]] = None,
    save_csv: bool = True,
    load_to_db: bool = True,
    batch_size: int = 50,
) -> None:
    """Full container ETL: upsert dim_container, then stream facts into fact_marketprice.

    Parameters
    ----------
    market_dir
        Directory containing per-item market CSV files (data/raw/market/).
    conversion_table_path
        Two-column CSV mapping URL-encoded filenames to decoded item names.
    price_frames
        Optional pre-loaded list of DataFrames (skips the CSV discovery step
        when provided — useful for testing).  Each frame must have at minimum:
            container_name, price, date
        Optional: quantity, type / item_type
    save_csv
        Write dim_container snapshot to data/processed/to_csv/ before DB work.
    load_to_db
        Set False for a dry-run: builds everything and saves CSVs but makes
        no DB writes.
    batch_size
        Number of price frames per INSERT round-trip (default 50).
    """
    # ------------------------------------------------------------------
    # 1. Discover / accept price frames and build metadata
    # ------------------------------------------------------------------
    if price_frames is None:
        price_frames, metadata_df = _load_container_frames(
            market_dir, conversion_table_path
        )
    else:
        # Caller supplied frames directly; derive metadata from them
        names = (
            pd.concat(
                [f[["container_name"]] for f in price_frames if "container_name" in f.columns],
                ignore_index=True,
            )["container_name"]
            .dropna()
            .unique()
        )
        release_date_map: Dict[str, Optional[str]] = {}
        for frame in price_frames:
            name = frame["container_name"].iloc[0] if "container_name" in frame.columns and len(frame) > 0 else None
            if name is None:
                continue
            dates = _normalize_market_date(frame["date"]) if "date" in frame.columns else pd.Series(dtype="datetime64[ns]")
            oldest = dates.dropna().min()
            if pd.notna(oldest):
                existing = release_date_map.get(name)
                candidate = oldest.strftime("%Y-%m-%d")
                if existing is None or candidate < existing:
                    release_date_map[name] = candidate

        metadata_df = pd.DataFrame({
            "container_name":  names,
            "container_price": None,
            "release_date":    [release_date_map.get(n) for n in names],
            "container_type":  [_classify_container_type(n) for n in names],
        })

    if not price_frames:
        logger.warning("[load_containers] No price frames to process — nothing to do.")
        return

    if save_csv:
        _save_csv(metadata_df, "dim_container_update")

    # ------------------------------------------------------------------
    # 2. Upsert container metadata into dim_container
    # ------------------------------------------------------------------
    conn = _get_connection()
    try:
        if load_to_db:
            _upsert_container_metadata(conn, metadata_df)
        else:
            logger.info("[load_containers] load_to_db=False — skipping dim_container upsert.")

        # ------------------------------------------------------------------
        # 3. Resolve FK lookup tables from DB
        #    (same pattern as load.py / load_prices_dimensions)
        # ------------------------------------------------------------------
        container_map  = _fetch_container_map(conn)
        item_type_map  = _fetch_item_type_map(conn)
        price_range_db = _fetch_price_range_db(conn)

        logger.info(f"[load_containers] Resolved {len(container_map)} containers from DB.")

        # ------------------------------------------------------------------
        # 4. Stream facts into fact_marketprice
        # ------------------------------------------------------------------
        if not load_to_db:
            logger.info("[load_containers] load_to_db=False — skipping fact_marketprice insert.")
            return

        _load_container_facts_streaming(
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
# CLI entry point  (mirrors load.py's __main__ block)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Load container price data into fact_marketprice.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--market-dir",
        default="data/raw/market",
        metavar="PATH",
        help="Directory containing per-item market CSV files.",
    )
    parser.add_argument(
        "--conversion-table",
        default="data/raw/name_conversion_table.csv",
        metavar="PATH",
        help="CSV mapping URL-encoded filenames to decoded item names.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        metavar="N",
        help="Number of price frames per INSERT round-trip.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and validate everything but make no DB writes.",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Skip saving CSV snapshots to data/processed/to_csv/.",
    )
    args = parser.parse_args()

    load_container_dimensions(
        market_dir=args.market_dir,
        conversion_table_path=args.conversion_table,
        batch_size=args.batch_size,
        load_to_db=not args.dry_run,
        save_csv=not args.no_csv,
    )