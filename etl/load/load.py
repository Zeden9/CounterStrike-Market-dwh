"""
Load
----
Inserts cleaned DataFrames into the PostgreSQL data warehouse
dimension tables, and saves every dimension + fact table to
data/processed/to_csv/*.csv before any DB work.

Strategy: INSERT … ON CONFLICT DO NOTHING
  - Idempotent: safe to re-run without duplicating rows.
  - Relies on a UNIQUE constraint on the name column of each dim table
    (see add_unique_constraints.sql if not already present).

Performance notes:
  - Dimension builds run in parallel (ThreadPoolExecutor) where independent.
  - CSV saves run in parallel (ThreadPoolExecutor) — I/O bound, threads help.
  - Price-range bucketing uses pd.cut (vectorized) instead of row-by-row loops.
  - CSV saving is isolated in save_all_csvs() — comment out the call to skip it.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional
import os
import sys

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.config import DB_CONFIG
from config.logger import get_logger
from etl.extract.extract_prices import extract_prices

logger = get_logger(__name__)

CSV_OUT_DIR = Path("data/processed/to_csv")

# Canonical item types — NULL/standard item is represented as None in data
# but stored as "Standard" in the dimension so every fact row has a non-null FK.
ITEM_TYPES = ["Standard", "StatTrak", "Souvenir"]


# ---------------------------------------------------------------------------
# CSV save helpers  —  call save_all_csvs() or comment it out entirely
# ---------------------------------------------------------------------------

def _save_csv(df: pd.DataFrame, name: str) -> None:
    """Write df to data/processed/to_csv/<name>.csv (single file)."""
    CSV_OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = CSV_OUT_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    logger.info(f"[csv] Saved {len(df)} rows → {path}")


def save_all_csvs(named_frames: Dict[str, pd.DataFrame]) -> None:
    """Save all DataFrames to CSV in parallel.

    Pass a dict of {logical_name: df}.  Writes are parallelised with threads
    (I/O-bound) so large saves don't block each other.

    To skip CSV output entirely, just comment out the call to this function
    in load_prices_dimensions() / load_all().
    """
    CSV_OUT_DIR.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=min(8, len(named_frames))) as pool:
        futures = {
            pool.submit(_save_csv, df, name): name
            for name, df in named_frames.items()
            if df is not None and not df.empty
        }
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                fut.result()
            except Exception as exc:
                logger.error(f"[csv] Failed to save '{name}': {exc}")


# ---------------------------------------------------------------------------
# Connection helper
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
# Weapon taxonomy loader
# ---------------------------------------------------------------------------

def load_weapon_taxonomy(
    weapons_txt_path: str = "data/raw/weapons.txt",
) -> Dict[str, str]:
    """Parse data/raw/weapons.txt into {weapon_name: weapon_type}."""
    taxonomy: Dict[str, str] = {}
    with open(weapons_txt_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(",", 1)]
            if len(parts) == 2:
                taxonomy[parts[0]] = parts[1]
    return taxonomy


# ---------------------------------------------------------------------------
# Generic upsert helpers
# ---------------------------------------------------------------------------

def _has_unique_or_pk_on_column(conn, table: str, column: str) -> bool:
    """Return True if `table` has any unique or PK constraint covering exactly `column`."""
    sql = """
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ordinality) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = cols.attnum
        WHERE n.nspname = 'public'
          AND t.relname = %s
          AND c.contype IN ('u', 'p')
        GROUP BY c.oid
        HAVING array_agg(a.attname ORDER BY cols.ordinality) = array[%s]::name[]
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table, column))
        return cur.fetchone() is not None


def _insert_dimension(
    conn,
    table: str,
    df: pd.DataFrame,
    columns: List[str],
    conflict_column: str,
) -> int:
    """Bulk-insert rows from df into `table`, skipping duplicates.

    Uses ON CONFLICT (column) DO NOTHING when the column has a unique/PK
    constraint.  Falls back to row-by-row existence checks otherwise.

    Returns total row count in the table after insert.
    """
    if df.empty:
        logger.warning(f"[{table}] DataFrame is empty – nothing to load.")
        return 0

    col_str = ", ".join([f'"{c}"' for c in columns])
    rows = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]

    if _has_unique_or_pk_on_column(conn, table, conflict_column):
        sql = (
            f'INSERT INTO "{table}" ({col_str}) VALUES %s '
            f'ON CONFLICT ("{conflict_column}") DO NOTHING'
        )
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
            conn.commit()
    else:
        logger.warning(
            f"[{table}] No unique/PK constraint on '{conflict_column}'. "
            "Using row-by-row existence checks (consider adding a constraint)."
        )
        col_str_plain = ", ".join([f'"{c}"' for c in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        col_idx = columns.index(conflict_column)
        with conn.cursor() as cur:
            for row in rows:
                conflict_value = row[col_idx]
                if conflict_value is None:
                    cur.execute(
                        f'INSERT INTO "{table}" ({col_str_plain}) VALUES ({placeholders})',
                        row,
                    )
                    continue
                cur.execute(
                    f'SELECT 1 FROM "{table}" WHERE "{conflict_column}" = %s LIMIT 1',
                    (conflict_value,),
                )
                if not cur.fetchone():
                    cur.execute(
                        f'INSERT INTO "{table}" ({col_str_plain}) VALUES ({placeholders})',
                        row,
                    )
            conn.commit()

    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        total = cur.fetchone()[0]

    logger.info(f"[{table}] Load complete. Total rows in table: {total}")
    return total


# ---------------------------------------------------------------------------
# Item type dimension
# ---------------------------------------------------------------------------

def _build_item_type_dim() -> pd.DataFrame:
    return pd.DataFrame({"item_type": ITEM_TYPES})


def load_item_types(conn, df: pd.DataFrame) -> int:
    logger.info(f"[dim_itemtype] Loading {len(df)} rows …")
    return _insert_dimension(conn, "dim_itemtype", df, ["item_type"], "item_type")


def _fetch_item_type_map(conn) -> Dict[str, int]:
    with conn.cursor() as cur:
        cur.execute('SELECT item_type_id, item_type FROM "dim_itemtype"')
        return {row[1]: row[0] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------

def _normalize_market_date(date_series: pd.Series) -> pd.Series:
    return pd.to_datetime(
        date_series.astype(str).str.replace(r"\s*:\s*\+0$", "", regex=True),
        errors="coerce",
    )


def _build_skin_dim(
    skins_df: pd.DataFrame,
    price_frames: Optional[List[pd.DataFrame]] = None,
) -> pd.DataFrame:
    base = skins_df[["skin_name", "rarity"]].drop_duplicates()

    if price_frames:
        price_skin_names = (
            pd.concat(
                [df["skin_name"] for df in price_frames if "skin_name" in df.columns],
                ignore_index=True,
            )
            .dropna()
            .astype(str)
            .drop_duplicates()
        )
        extra = pd.DataFrame({"skin_name": price_skin_names})
        extra = extra[~extra["skin_name"].isin(base["skin_name"])].copy()
        extra["rarity"] = None
        base = pd.concat([base, extra], ignore_index=True)

    df = base.drop_duplicates(subset="skin_name").sort_values("skin_name").reset_index(drop=True)
    df["skin_id"] = df.index + 1
    return df[["skin_id", "skin_name", "rarity"]]


def _build_weapon_dim(
    price_frames: List[pd.DataFrame],
    taxonomy: Dict[str, str],
    knives_csv_path: str = "data/processed/from_market_data/knives.csv",
) -> pd.DataFrame:
    all_weapon_names: pd.Series = pd.concat(
        [df["weapon_name"] for df in price_frames if "weapon_name" in df.columns],
        ignore_index=True,
    ).dropna().astype(str).str.replace("★ ", "", regex=False)

    weapon_names = sorted(all_weapon_names.unique())
    weapons_df = pd.DataFrame({"weapon_name": weapon_names})

    knives_path = Path(knives_csv_path)
    if knives_path.exists():
        knives_df = pd.read_csv(knives_path)
        knife_names = knives_df["weapon"].str.strip().dropna().unique()
        weapons_df = (
            pd.concat([weapons_df, pd.DataFrame({"weapon_name": knife_names})], ignore_index=True)
            .drop_duplicates(subset="weapon_name")
        )
    else:
        logger.warning(f"Knives CSV not found at {knives_csv_path}; knives skipped from dim_weapon.")

    weapons_df = weapons_df.sort_values("weapon_name").reset_index(drop=True)
    weapons_df["weapon_type"] = weapons_df["weapon_name"].map(taxonomy).fillna("Unknown")
    return weapons_df[["weapon_name", "weapon_type"]]


def _build_time_dim(price_frames: List[pd.DataFrame]) -> pd.DataFrame:
    dates = [
        _normalize_market_date(df["date"])
        for df in price_frames
        if "date" in df.columns
    ]
    if not dates:
        return pd.DataFrame(columns=["date_id", "day", "month", "year"])

    all_dates = (
        pd.concat(dates, ignore_index=True)
        .dropna()
        .dt.date
        .drop_duplicates()
    )
    df = pd.DataFrame({"date_id": sorted(all_dates)})
    df["day"]   = df["date_id"].apply(lambda d: d.day)
    df["month"] = df["date_id"].apply(lambda d: d.month)
    df["year"]  = df["date_id"].apply(lambda d: d.year)
    return df[["date_id", "day", "month", "year"]]


def _build_wear_dim(price_frames: List[pd.DataFrame]) -> pd.DataFrame:
    wear_conditions: set = set()
    for df in price_frames:
        if "wear" in df.columns:
            wear_conditions.update(df["wear"].dropna().unique())

    if not wear_conditions:
        wear_conditions = {
            "Factory New", "Minimal Wear", "Field-Tested", "Well-Worn", "Battle-Scarred"
        }

    df = pd.DataFrame({"wear_range": sorted(wear_conditions)})
    df["wear_range_id"] = df.index + 1
    return df[["wear_range_id", "wear_range"]]


def _build_price_range_dim() -> pd.DataFrame:
    price_ranges = [
        (0,   10,           "0-10"),
        (10,  50,           "10-50"),
        (50,  100,          "50-100"),
        (100, 500,          "100-500"),
        (500, float("inf"), "500+"),
    ]
    df = pd.DataFrame(price_ranges, columns=["min_price", "max_price", "price_range"])
    df["price_range_id"] = df.index + 1
    return df[["price_range_id", "price_range", "min_price", "max_price"]]


def _build_containers(
    skins_df: pd.DataFrame,
    price_frames: List[pd.DataFrame],
) -> pd.DataFrame:
    skins_ref = skins_df[["weapon_name", "skin_name", "collection"]].drop_duplicates()

    price_rows = [
        df[["weapon_name", "skin_name", "date"]].copy()
        for df in price_frames
        if {"weapon_name", "skin_name", "date"}.issubset(df.columns)
    ]
    if not price_rows:
        return pd.DataFrame(
            columns=["container_id", "container_name", "release_date", "container_type"]
        )

    price_all = pd.concat(price_rows, ignore_index=True)
    price_all["date_parsed"] = _normalize_market_date(price_all["date"])
    joined = price_all.merge(skins_ref, on=["weapon_name", "skin_name"], how="inner")

    if joined.empty:
        logger.warning(
            "No price rows matched skins.csv for container release-date computation."
        )
        return pd.DataFrame(
            columns=["container_id", "container_name", "release_date", "container_type"]
        )

    release_dates = (
        joined.groupby("collection", dropna=False)["date_parsed"]
        .min()
        .reset_index(name="release_date")
    )
    container_types = (
        skins_ref.assign(
            container_type=skins_ref["skin_name"]
            .str.lower()
            .str.startswith("sticker")
            .map({True: "Sticker Capsule", False: "Case"})
        )
        .groupby("collection", dropna=False)["container_type"]
        .first()
        .reset_index()
    )

    containers = (
        release_dates.merge(container_types, on="collection", how="left")
        .rename(columns={"collection": "container_name"})
        .sort_values("container_name")
        .reset_index(drop=True)
    )
    containers["container_id"] = containers.index + 1
    return containers[["container_id", "container_name", "release_date", "container_type"]]


# ---------------------------------------------------------------------------
# Fact builder
# ---------------------------------------------------------------------------

def _assign_price_range_vectorized(
    prices: pd.Series,
    price_range_db: pd.DataFrame,
) -> pd.Series:
    """Vectorized price-range bucketing using pd.cut — much faster than row iteration."""
    pr = price_range_db.sort_values("min_price").reset_index(drop=True)
    bins   = list(pr["min_price"]) + [float("inf")]
    labels = list(pr["price_range_id"].astype(int))
    return pd.cut(
        prices,
        bins=bins,
        labels=labels,
        right=False,
        include_lowest=True,
    ).astype(object)  # keeps NaN as None-compatible object


def _build_type_lookup(
    weapons_csv_path: str,
    knives_csv_path: str,
    gloves_csv_path: str,
) -> pd.DataFrame:
    """Build the (weapon_name, skin_name) → type lookup once, before streaming."""
    type_frames = []
    for csv_path in (weapons_csv_path, knives_csv_path, gloves_csv_path):
        p = Path(csv_path)
        if p.exists():
            df_csv = pd.read_csv(p)
            if {"weapon", "skin_name", "type"}.issubset(df_csv.columns):
                type_frames.append(
                    df_csv[["weapon", "skin_name", "type"]].rename(columns={"weapon": "weapon_name"})
                )
        else:
            logger.warning(f"[facts] CSV not found for item-type lookup: {csv_path}")

    if not type_frames:
        return pd.DataFrame(columns=["weapon_name", "skin_name", "type"])

    lookup = (
        pd.concat(type_frames, ignore_index=True)
        .drop_duplicates(subset=["weapon_name", "skin_name"])
    )
    lookup["type"] = lookup["type"].fillna("Standard")
    return lookup


def _process_chunk(
    chunk: pd.DataFrame,
    price_range_db: pd.DataFrame,
    skins_db: pd.DataFrame,
    weapons_db: pd.DataFrame,
    wear_db: pd.DataFrame,
    item_type_map: Dict[str, int],
    type_lookup: pd.DataFrame,
    standard_id: int,
) -> pd.DataFrame:
    """Transform one price_frame into fact rows. Returns empty df if nothing matches."""
    chunk = chunk[chunk["weapon_name"].notna()].copy()
    if chunk.empty:
        return pd.DataFrame()

    chunk["date_parsed"] = _normalize_market_date(chunk["date"])
    chunk["weapon_name"] = chunk["weapon_name"].str.replace("★ ", "", regex=False)

    if not type_lookup.empty:
        chunk = chunk.merge(type_lookup, on=["weapon_name", "skin_name"], how="left")
        chunk["type"] = chunk["type"].fillna("Standard")
    else:
        chunk["type"] = "Standard"

    chunk["item_type_id"] = chunk["type"].map(item_type_map).fillna(standard_id).astype(int)
    chunk["price_range_id"] = _assign_price_range_vectorized(chunk["price"], price_range_db)
    chunk["date_id"] = chunk["date_parsed"].dt.date.where(chunk["date_parsed"].notna(), other=None)

    chunk = (
        chunk
        .merge(weapons_db, on="weapon_name", how="left")
        .merge(skins_db,   on="skin_name",   how="left")
        .merge(wear_db,    left_on="wear", right_on="wear_range", how="left")
    )
    chunk = chunk[chunk["skin_id"].notna() & chunk["weapon_id"].notna()]
    if chunk.empty:
        return pd.DataFrame()

    out = chunk[[
        "price", "quantity", "date_id", "skin_id", "weapon_id",
        "wear_range_id", "price_range_id", "item_type_id",
    ]].copy()
    out.columns = [
        "price", "volume", "date_id", "skin_id", "weapon_id",
        "wear_range_id", "price_range_id", "item_type_id",
    ]
    out["container_id"] = None
    out["sticker_id"]   = None
    out["team_id"]      = None
    out["match_id"]     = None

    int_fk_cols = ["skin_id", "weapon_id", "wear_range_id", "price_range_id", "item_type_id"]
    for col in int_fk_cols:
        out[col] = out[col].apply(lambda x: None if pd.isna(x) else int(x))
    out["volume"] = out["volume"].apply(lambda x: None if pd.isna(x) else int(x))
    out["price"]  = out["price"].apply(lambda x: None if pd.isna(x) else float(x))

    return out


def load_facts_streaming(
    conn,
    price_frames: List[pd.DataFrame],
    price_range_db: pd.DataFrame,
    skins_db: pd.DataFrame,
    weapons_db: pd.DataFrame,
    wear_db: pd.DataFrame,
    item_type_map: Dict[str, int],
    weapons_csv_path: str,
    knives_csv_path: str,
    gloves_csv_path: str,
    batch_size: int = 50,
) -> int:
    """Process price_frames in batches and insert directly — never concatenates
    the full dataset into memory.

    batch_size controls how many price_frames are merged into one INSERT.
    50 is a good default; lower it to 10-20 if you still hit memory limits,
    or raise it to 200 if you have headroom and want fewer DB round-trips.
    """
    type_lookup = _build_type_lookup(weapons_csv_path, knives_csv_path, gloves_csv_path)
    standard_id = item_type_map.get("Standard")
    total_inserted = 0

    columns = [
        "price", "volume", "date_id", "skin_id", "weapon_id",
        "wear_range_id", "price_range_id", "item_type_id",
        "container_id", "sticker_id", "team_id", "match_id",
    ]
    col_str    = ", ".join(f'"{c}"' for c in columns)
    insert_sql = f'INSERT INTO "fact_marketprice" ({col_str}) VALUES %s'

    num_batches = (len(price_frames) + batch_size - 1) // batch_size
    for batch_idx, batch_start in enumerate(range(0, len(price_frames), batch_size), start=1):
        batch  = price_frames[batch_start : batch_start + batch_size]
        chunks = [
            _process_chunk(
                frame, price_range_db, skins_db, weapons_db, wear_db,
                item_type_map, type_lookup, standard_id,
            )
            for frame in batch
        ]
        facts_batch = pd.concat([c for c in chunks if not c.empty], ignore_index=True)
        if facts_batch.empty:
            logger.info(f"[fact_marketprice] Batch {batch_idx}/{num_batches} — no rows matched, skipping.")
            continue

        rows = [tuple(r) for r in facts_batch[columns].itertuples(index=False, name=None)]

        # Drop rows that would violate NOT NULL constraints
        required = ["date_id", "skin_id", "weapon_id", "price", "item_type_id"]
        null_mask = facts_batch[required].isnull().any(axis=1)
        null_rows = facts_batch[null_mask]
        facts_batch = facts_batch[~null_mask]
        if not null_rows.empty:
            null_path = CSV_OUT_DIR / "null_values.csv"
            CSV_OUT_DIR.mkdir(parents=True, exist_ok=True)
            null_rows.to_csv(null_path, mode="a", header=not null_path.exists(), index=False)
            logger.warning(
                f"[fact_marketprice] Batch {batch_idx}/{num_batches} — "
                f"dropped {len(null_rows)} rows with nulls, appended to {null_path}"
            )

        if facts_batch.empty:
            logger.info(f"[fact_marketprice] Batch {batch_idx}/{num_batches} — all rows dropped, skipping.")
            continue

        rows = [tuple(r) for r in facts_batch[columns].itertuples(index=False, name=None)]
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows, page_size=1000)
        conn.commit()

        total_inserted += len(rows)
        logger.info(
            f"[fact_marketprice] Batch {batch_idx}/{num_batches} "
            f"— inserted {len(rows)} rows (total so far: {total_inserted})"
        )

    logger.info(f"[fact_marketprice] Streaming load complete. Total rows: {total_inserted}")
    return total_inserted


# ---------------------------------------------------------------------------
# Load helpers (DB)
# ---------------------------------------------------------------------------

def load_skins(conn, df: pd.DataFrame) -> int:
    logger.info(f"[dim_skin] Loading {len(df)} rows …")
    columns = ["skin_id", "skin_name", "rarity"] if "skin_id" in df.columns else ["skin_name", "rarity"]
    return _insert_dimension(conn, "dim_skin", df, columns, "skin_name")


def load_weapons(conn, df: pd.DataFrame) -> int:
    logger.info(f"[dim_weapon] Loading {len(df)} rows …")
    return _insert_dimension(conn, "dim_weapon", df, ["weapon_name", "weapon_type"], "weapon_name")


def load_containers(conn, df: pd.DataFrame) -> int:
    logger.info(f"[dim_container] Loading {len(df)} rows …")
    return _insert_dimension(
        conn,
        table="dim_container",
        df=df,
        columns=["container_id", "container_name", "release_date", "container_type"],
        conflict_column="container_id",
    )


def load_times(conn, df: pd.DataFrame) -> int:
    logger.info(f"[dim_time] Loading {len(df)} rows …")
    return _insert_dimension(
        conn,
        table="dim_time",
        df=df,
        columns=["date_id", "day", "month", "year"],
        conflict_column="date_id",
    )


def load_stickers(conn, df: pd.DataFrame) -> int:
    logger.info(f"[dim_sticker] Loading {len(df)} rows …")
    return _insert_dimension(
        conn,
        table="dim_sticker",
        df=df,
        columns=["sticker_name", "rarity"],
        conflict_column="sticker_name",
    )


def load_wear_ranges(conn, df: pd.DataFrame) -> int:
    logger.info(f"[dim_wear_range] Loading {len(df)} rows …")
    return _insert_dimension(conn, "dim_wear_range", df, ["wear_range"], "wear_range")


def load_price_ranges(conn, df: pd.DataFrame) -> int:
    logger.info(f"[dim_price_range] Loading {len(df)} rows …")
    return _insert_dimension(conn, "dim_price_range", df, ["price_range"], "price_range")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_prices_dimensions(
    skins_csv_path: str = "data/processed/skins.csv",
    price_frames: Optional[List[pd.DataFrame]] = None,
    max_price_files: Optional[int] = None,
    weapons_txt_path: str = "data/raw/weapons.txt",
    knives_csv_path: str = "data/processed/from_market_data/knives.csv",
    weapons_csv_path: str = "data/processed/from_market_data/weapons.csv",
    gloves_csv_path: str = "data/processed/from_market_data/gloves.csv",
    load_to_db: bool = True,
) -> None:
    """Build all dimensions and facts, save each to CSV, then (optionally) load to DB."""
    skins_df = pd.read_csv(skins_csv_path)
    price_frames = (
        price_frames
        if price_frames is not None
        else extract_prices(max_files=max_price_files)
    )
    logger.info(f"extract_prices complete — {len(price_frames)} frames loaded.")

    taxonomy = load_weapon_taxonomy(weapons_txt_path)

    # ------------------------------------------------------------------
    # Build independent dimensions in parallel
    # ------------------------------------------------------------------
    def _build_weapons():  return _build_weapon_dim(price_frames, taxonomy, knives_csv_path)
    def _build_times():    return _build_time_dim(price_frames)
    def _build_wear():     return _build_wear_dim(price_frames)
    def _build_pr():       return _build_price_range_dim()
    def _build_skins():    return _build_skin_dim(skins_df, price_frames)
    def _build_cont():     return _build_containers(skins_df, price_frames)
    def _build_itype():    return _build_item_type_dim()

    builders = {
        "weapons":    _build_weapons,
        "times":      _build_times,
        "wear":       _build_wear,
        "price_range":_build_pr,
        "skins":      _build_skins,
        "containers": _build_cont,
        "item_type":  _build_itype,
    }

    results: Dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=len(builders)) as pool:
        future_map = {pool.submit(fn): key for key, fn in builders.items()}
        for fut in as_completed(future_map):
            key = future_map[fut]
            try:
                results[key] = fut.result()
            except Exception as exc:
                logger.error(f"[build] '{key}' failed: {exc}")
                raise

    item_type_dim_df = results["item_type"]
    containers_df    = results["containers"]
    weapons_dim_df   = results["weapons"]
    times_dim_df     = results["times"]
    wear_dim_df      = results["wear"]
    price_range_df   = results["price_range"]
    skins_dim_df     = results["skins"]

    # ------------------------------------------------------------------
    # Save all dimension CSVs in parallel  ← comment this out to skip CSV output
    # ------------------------------------------------------------------
    save_all_csvs({
        "dim_itemtype":  item_type_dim_df,
        "dim_skin":      skins_dim_df,
        "dim_weapon":    weapons_dim_df,
        "dim_container": containers_df,
        "dim_time":      times_dim_df,
        "dim_wear_range":wear_dim_df,
        "dim_price_range":price_range_df,
    })

    if not load_to_db:
        logger.info("load_to_db=False – skipping database inserts.")
        return

    conn = get_connection()
    try:
        load_weapons(conn, weapons_dim_df)
        load_containers(conn, containers_df)
        load_times(conn, times_dim_df)
        load_wear_ranges(conn, wear_dim_df)
        load_price_ranges(conn, price_range_df)

        with conn.cursor() as cur:
            cur.execute('SELECT skin_id, skin_name FROM "dim_skin"')
            skins_db = pd.DataFrame(cur.fetchall(), columns=["skin_id", "skin_name"])

            cur.execute('SELECT weapon_id, weapon_name FROM "dim_weapon"')
            weapons_db = pd.DataFrame(cur.fetchall(), columns=["weapon_id", "weapon_name"])

            cur.execute('SELECT wear_range_id, wear_range FROM "dim_wear_range"')
            wear_db = pd.DataFrame(cur.fetchall(), columns=["wear_range_id", "wear_range"])

            cur.execute('SELECT price_range_id, price_range FROM "dim_price_range"')
            pr_db = pd.DataFrame(cur.fetchall(), columns=["price_range_id", "price_range"])

        item_type_map = _fetch_item_type_map(conn)

        price_range_db = pr_db.merge(
            price_range_df[["price_range", "min_price", "max_price"]],
            on="price_range",
            how="left",
        )

        load_facts_streaming(
            conn, price_frames, price_range_db, skins_db, weapons_db, wear_db,
            item_type_map, weapons_csv_path, knives_csv_path, gloves_csv_path,
            batch_size=50,
        )
    finally:
        conn.close()
        logger.info("Database connection closed.")


def load_all(
    transformed: dict,
    weapons_txt_path: str = "data/raw/weapons.txt",
    knives_csv_path: str = "data/processed/from_market_data/knives.csv",
    price_frames: Optional[List[pd.DataFrame]] = None,
) -> None:
    """Load the core entity dimensions produced by the transform stage."""
    taxonomy = load_weapon_taxonomy(weapons_txt_path)

    if price_frames:
        weapons_dim = _build_weapon_dim(price_frames, taxonomy, knives_csv_path)
    elif "weapons" in transformed:
        w_df = transformed["weapons"].copy()
        if "weapon" in w_df.columns and "weapon_name" not in w_df.columns:
            w_df = w_df.rename(columns={"weapon": "weapon_name"})
        w_df["weapon_type"] = w_df["weapon_name"].map(taxonomy).fillna("Unknown")
        knives_path = Path(knives_csv_path)
        if knives_path.exists():
            knives_df = pd.read_csv(knives_path)
            knife_names = knives_df["weapon"].str.strip().dropna().unique()
            knives_extra = pd.DataFrame({
                "weapon_name": knife_names,
                "weapon_type": [taxonomy.get(n, "Knife") for n in knife_names],
            })
            w_df = (
                pd.concat([w_df[["weapon_name", "weapon_type"]], knives_extra], ignore_index=True)
                .drop_duplicates(subset="weapon_name")
            )
        weapons_dim = w_df[["weapon_name", "weapon_type"]].sort_values("weapon_name").reset_index(drop=True)
    else:
        weapons_dim = pd.DataFrame(columns=["weapon_name", "weapon_type"])

    conn = get_connection()
    try:
        item_type_dim_df = _build_item_type_dim()
        skins_dim = _build_skin_dim(transformed["skins"], price_frames)

        # ← comment out this block to skip CSV output in load_all
        save_all_csvs({
            "dim_itemtype": item_type_dim_df,
            "dim_skin":     skins_dim,
            **({"dim_weapon": weapons_dim} if not weapons_dim.empty else {}),
            **({"dim_sticker": transformed["stickers"]} if "stickers" in transformed else {}),
        })

        load_item_types(conn, item_type_dim_df)
        load_skins(conn, skins_dim)

        if not weapons_dim.empty:
            load_weapons(conn, weapons_dim)
        else:
            logger.warning("No weapons dataset available; skipping dim_weapon load.")

        if "stickers" in transformed:
            load_stickers(conn, transformed["stickers"])
        else:
            logger.warning("No transformed stickers dataset; skipping dim_sticker load.")
    finally:
        conn.close()
        logger.info("Database connection closed.")