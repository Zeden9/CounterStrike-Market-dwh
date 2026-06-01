"""
Load
----
Inserts cleaned DataFrames into the PostgreSQL data warehouse
dimension tables.

Strategy: INSERT … ON CONFLICT DO NOTHING
  - Idempotent: safe to re-run without duplicating rows.
  - Relies on a UNIQUE constraint on the name column of each dim table
    (see add_unique_constraints.sql if not already present).
"""

from pathlib import Path
from typing import List, Optional
import os
import sys

import pandas as pd
import psycopg2

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.config import DB_CONFIG
from config.logger import get_logger
from etl.extract.extract_prices import extract_prices

logger = get_logger(__name__)


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
# Generic upsert helper
# ---------------------------------------------------------------------------

def _find_conflict_constraint(conn, table: str, column: str) -> Optional[str]:
    sql = """
        SELECT conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ordinality) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = cols.attnum
        WHERE n.nspname = 'public'
          AND t.relname = %s
          AND c.contype IN ('u', 'p')
        GROUP BY conname
        HAVING array_agg(a.attname ORDER BY cols.ordinality) = array[%s]::name[]
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table, column))
        result = cur.fetchone()
    return result[0] if result else None


def _insert_without_conflict(
    conn,
    table: str,
    df: pd.DataFrame,
    columns: list[str],
    conflict_column: str,
) -> int:
    col_str = ", ".join([f'"{col}"' for col in columns])
    placeholders = ", ".join(["%s"] * len(columns))
    rows = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]

    with conn.cursor() as cur:
        for row in rows:
            conflict_value = row[columns.index(conflict_column)]
            if conflict_value is None:
                cur.execute(
                    f'INSERT INTO "{table}" ({col_str}) VALUES ({placeholders})',
                    row,
                )
                continue

            cur.execute(
                f'SELECT 1 FROM "{table}" WHERE "{conflict_column}" = %s LIMIT 1',
                (conflict_value,),
            )
            if cur.fetchone():
                continue
            cur.execute(
                f'INSERT INTO "{table}" ({col_str}) VALUES ({placeholders})',
                row,
            )
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        total = cur.fetchone()[0]

    logger.info(
        f"[{table}] Loaded with fallback insert because no unique constraint was found on {conflict_column}."
    )
    return total


def _insert_dimension(
    conn,
    table: str,
    df: pd.DataFrame,
    columns: list[str],
    conflict_column: str,
) -> int:
    """
    Bulk-insert rows from df into `table`.
    Skips rows that already exist (ON CONFLICT DO NOTHING).
    Returns the number of rows actually inserted.
    """
    if df.empty:
        logger.warning(f"[{table}] DataFrame is empty – nothing to load.")
        return 0

    col_str = ", ".join([f'"{col}"' for col in columns])
    placeholders = ", ".join(["%s"] * len(columns))
    rows = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]

    constraint_name = _find_conflict_constraint(conn, table, conflict_column)
    if constraint_name:
        sql = (
            f'INSERT INTO "{table}" ({col_str}) '
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ON CONSTRAINT {constraint_name} DO NOTHING"
        )
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
            conn.commit()
    else:
        logger.warning(
            f"No unique or primary-key constraint found on {table}({conflict_column}). "
            "Falling back to manual existence checks."
        )
        return _insert_without_conflict(conn, table, df, columns, conflict_column)

    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        total = cur.fetchone()[0]

    logger.info(f"[{table}] Load complete. Total rows in table: {total}")
    return total


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------

def _normalize_market_date(date_series: pd.Series) -> pd.Series:
    return pd.to_datetime(
        date_series.astype(str).str.replace(r"\s*:\s*\+0$", "", regex=True),
        errors="coerce",
    )


def _build_skin_dim(skins_df: pd.DataFrame) -> pd.DataFrame:
    df = skins_df[["skin_name", "rarity"]].drop_duplicates().sort_values("skin_name")
    df = df.reset_index(drop=True)
    df["skin_id"] = df.index + 1
    return df[["skin_id", "skin_name", "rarity"]]


def _build_weapon_dim(price_frames: List[pd.DataFrame]) -> pd.DataFrame:
    all_weapons = pd.concat(price_frames, ignore_index=True)["weapon_name"].dropna().astype(str)
    weapon_names = sorted(all_weapons.unique())
    df = pd.DataFrame({"weapon_name": weapon_names})
    df["weapon_id"] = df.index + 1
    df["weapon_type"] = None
    return df[["weapon_id", "weapon_name", "weapon_type"]]


def _build_time_dim(price_frames: List[pd.DataFrame]) -> pd.DataFrame:
    dates = []
    for df in price_frames:
        if "date" not in df.columns:
            continue
        dates.append(_normalize_market_date(df["date"]))

    if not dates:
        return pd.DataFrame(columns=["date_id", "day", "month", "year"])

    all_dates = pd.concat(dates, ignore_index=True).dropna().dt.date.drop_duplicates().sort_values()
    df = pd.DataFrame({"date_id": all_dates})
    df["day"] = df["date_id"].apply(lambda d: d.day)
    df["month"] = df["date_id"].apply(lambda d: d.month)
    df["year"] = df["date_id"].apply(lambda d: d.year)
    return df[["date_id", "day", "month", "year"]]


def _build_wear_dim(price_frames: List[pd.DataFrame]) -> pd.DataFrame:
    """Extract unique wear conditions from price data, or use standard ones."""
    wear_conditions = set()
    for df in price_frames:
        if "wear" in df.columns:
            wear_conditions.update(df["wear"].dropna().unique())

    # Use standard wear conditions if none found
    if not wear_conditions:
        wear_conditions = {"Factory New", "Minimal Wear", "Field-Tested", "Well-Worn", "Battle-Scarred"}

    wear_list = sorted(wear_conditions)
    df = pd.DataFrame({"wear_range": wear_list})
    df["wear_range_id"] = df.index + 1
    return df[["wear_range_id", "wear_range"]]


def _build_price_range_dim() -> pd.DataFrame:
    """Create price range brackets."""
    price_ranges = [
        (0, 10, "0-10"),
        (10, 50, "10-50"),
        (50, 100, "50-100"),
        (100, 500, "100-500"),
        (500, float('inf'), "500+"),
    ]
    df = pd.DataFrame({
        "price_range": [label for _, _, label in price_ranges],
        "min_price": [min_p for min_p, _, _ in price_ranges],
        "max_price": [max_p for _, max_p, _ in price_ranges],
    })
    df["price_range_id"] = df.index + 1
    return df[["price_range_id", "price_range", "min_price", "max_price"]]


def _build_containers(skins_df: pd.DataFrame, price_frames: List[pd.DataFrame]) -> pd.DataFrame:
    skins_ref = skins_df[["weapon_name", "skin_name", "collection"]].drop_duplicates()

    price_rows = []
    for df in price_frames:
        if {"weapon_name", "skin_name", "date"}.issubset(df.columns):
            price_rows.append(df[["weapon_name", "skin_name", "date"]].copy())

    if not price_rows:
        return pd.DataFrame(columns=["container_id", "container_name", "release_date", "container_type"])

    price_all = pd.concat(price_rows, ignore_index=True)
    price_all["date_parsed"] = _normalize_market_date(price_all["date"])
    joined = price_all.merge(skins_ref, on=["weapon_name", "skin_name"], how="inner")

    if joined.empty:
        logger.warning("No price rows could be matched to skins.csv for container release-date computation.")
        return pd.DataFrame(columns=["container_id", "container_name", "release_date", "container_type"])

    release_dates = (
        joined.groupby("collection", dropna=False)["date_parsed"]
        .min()
        .reset_index(name="release_date")
    )

    container_types = (
        skins_ref.assign(
            container_type=skins_ref["skin_name"].str.lower().str.startswith("sticker").map(
                {True: "Sticker Capsule", False: "Case"}
            )
        )
        .groupby("collection", dropna=False)["container_type"]
        .first()
        .reset_index()
    )

    containers = release_dates.merge(container_types, on="collection", how="left")
    containers = containers.rename(columns={"collection": "container_name"})
    containers = containers.sort_values("container_name").reset_index(drop=True)
    containers["container_id"] = containers.index + 1
    return containers[["container_id", "container_name", "release_date", "container_type"]]


# ---------------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------------

def load_skins(conn, df: pd.DataFrame) -> int:
    logger.info(f"[dim_skin] Loading {len(df)} rows …")
    columns = ["skin_id", "skin_name", "rarity"] if "skin_id" in df.columns else ["skin_name", "rarity"]
    conflict_column = "skin_id" if "skin_id" in df.columns else "skin_name"
    return _insert_dimension(conn, "dim_skin", df, columns, conflict_column)


def load_weapons(conn, df: pd.DataFrame) -> int:
    logger.info(f"[dim_weapon] Loading {len(df)} rows …")
    columns = ["weapon_id", "weapon_name", "weapon_type"] if "weapon_id" in df.columns else ["weapon_name", "weapon_type"]
    conflict_column = "weapon_id" if "weapon_id" in df.columns else "weapon_name"
    return _insert_dimension(conn, "dim_weapon", df, columns, conflict_column)


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
    return _insert_dimension(
        conn,
        table="dim_wear_range",
        df=df,
        columns=["wear_range_id", "wear_range"],
        conflict_column="wear_range_id",
    )


def load_price_ranges(conn, df: pd.DataFrame) -> int:
    logger.info(f"[dim_price_range] Loading {len(df)} rows …")
    return _insert_dimension(
        conn,
        table="dim_price_range",
        df=df,
        columns=["price_range_id", "price_range"],
        conflict_column="price_range_id",
    )


def load_facts(conn, skins_df: pd.DataFrame, price_frames: List[pd.DataFrame], price_range_dim: pd.DataFrame) -> int:
    """Load market price facts by joining with dimensions."""
    logger.info("[fact_marketprice] Starting fact load process …")
    logger.info(f"[fact_marketprice] Received {len(price_frames)} price frames")

    # Combine all price frames
    if not price_frames or all(df.empty for df in price_frames):
        logger.warning("No price data available for fact loading.")
        return 0

    price_all = pd.concat(price_frames, ignore_index=True)
    logger.info(f"[fact_marketprice] Total price records: {len(price_all)}")

    # Save null weapons to CSV before filtering
    null_weapons = price_all[price_all["weapon_name"].isna()].copy()
    if not null_weapons.empty:
        null_weapons.to_csv("null_weapons_load.csv", index=False)
        logger.info(f"[fact_marketprice] Saved {len(null_weapons)} rows with null weapons to null_weapons_load.csv")

    # Filter out rows with missing weapon_name (from example file, etc.)
    price_all = price_all[price_all["weapon_name"].notna()].copy()
    logger.info(f"[fact_marketprice] After filtering NULL weapons: {len(price_all)}")
    if price_all.empty:
        logger.warning("No valid price data after filtering out NULL weapon names.")
        return 0

    price_all["date_parsed"] = _normalize_market_date(price_all["date"])

    # Remove ★ prefix from knife names
    price_all["weapon_name"] = price_all["weapon_name"].str.replace("★ ", "", regex=False)

    # Use all prices (latest price filter removed)
    price_latest = price_all

    # Fetch dimension IDs
    with conn.cursor() as cur:
        cur.execute('SELECT skin_id, skin_name FROM "dim_skin"')
        skins_db = pd.DataFrame(cur.fetchall(), columns=["skin_id", "skin_name"])

        cur.execute('SELECT weapon_id, weapon_name FROM "dim_weapon"')
        weapons_db = pd.DataFrame(cur.fetchall(), columns=["weapon_id", "weapon_name"])

        cur.execute('SELECT wear_range_id, wear_range FROM "dim_wear_range"')
        wear_db = pd.DataFrame(cur.fetchall(), columns=["wear_range_id", "wear_range"])

    # Use passed price_range_dim instead of querying DB
    price_range_db = price_range_dim

    # Join with weapon dimension first
    facts = price_latest.merge(weapons_db, on="weapon_name", how="left")

    # Join with skin dimension
    facts = facts.merge(skins_db, on="skin_name", how="left")

    # Keep only rows where both skin and weapon matched
    facts = facts[facts["skin_id"].notna() & facts["weapon_id"].notna()].copy()

    if facts.empty:
        logger.warning("No matches between price data and dimensions after joins.")
        return 0

    # Map date
    facts["date_id"] = facts["date_parsed"].dt.date

    # Join with wear dimension
    facts = facts.merge(wear_db, left_on="wear", right_on="wear_range", how="left")

    # Map price ranges
    def get_price_range(price_val, ranges_df):
        if pd.isna(price_val):
            return None
        for _, row in ranges_df.iterrows():
            if row["min_price"] <= price_val < row["max_price"]:
                return row["price_range_id"]
        return None

    facts["price_range_id"] = facts["price"].apply(lambda p: get_price_range(p, price_range_db))

    # Build final fact table
    facts_final = facts[[
        "price", "quantity", "date_id", "skin_id", "weapon_id",
        "wear_range_id", "price_range_id"
    ]].copy()
    facts_final.columns = ["price", "volume", "date_id", "skin_id", "weapon_id", "wear_range_id", "price_range_id"]

    # Add NULL columns for unavailable dimensions
    facts_final["container_id"] = None
    facts_final["sticker_id"] = None
    facts_final["team_id"] = None
    facts_final["match_id"] = None

    logger.info(f"[fact_marketprice] Loading {len(facts_final)} rows …")

    # Insert facts directly (no conflict handling needed for facts)
    col_str = ", ".join([f'"{col}"' for col in ["price", "volume", "date_id", "skin_id", "weapon_id", "wear_range_id", "price_range_id", "container_id", "sticker_id", "team_id", "match_id"]])
    placeholders = ", ".join(["%s"] * 11)

    with conn.cursor() as cur:
        for _, row in facts_final.iterrows():
            values = (row["price"], row["volume"], row["date_id"], row["skin_id"],
                     row["weapon_id"], row["wear_range_id"], row["price_range_id"],
                     row["container_id"], row["sticker_id"], row["team_id"], row["match_id"])
            try:
                cur.execute(f'INSERT INTO "fact_marketprice" ({col_str}) VALUES ({placeholders})', values)
            except Exception as e:
                logger.debug(f"Could not insert fact row: {e}")
        conn.commit()

    return len(facts_final)

def save_facts_to_csv(skins_df, price_frames, price_range_dim, output_path="data/processed/facts.csv"):
    """Build the fact DataFrame and write to CSV without touching the DB."""
    if not price_frames or all(df.empty for df in price_frames):
        logger.warning("No price data to save.")
        return None

    price_all = pd.concat(price_frames, ignore_index=True)
    price_all = price_all[price_all["weapon_name"].notna()].copy()
    price_all["date_parsed"] = _normalize_market_date(price_all["date"])
    
    # Remove ★ prefix from knife names
    price_all["weapon_name"] = price_all["weapon_name"].str.replace("★ ", "", regex=False)

    # Vectorized price range mapping (replaces slow apply loop)
    def map_price_ranges_vectorized(prices, ranges_df):
        result = pd.Series(pd.NA, index=prices.index)
        for _, row in ranges_df.iterrows():
            mask = (prices >= row["min_price"]) & (prices < row["max_price"])
            result[mask] = row["price_range_id"]
        return result

    price_all["price_range_id"] = map_price_ranges_vectorized(price_all["price"], price_range_dim)
    price_all["date_id"] = price_all["date_parsed"].dt.date

    # Build weapon dimension from price data
    all_weapons = price_all["weapon_name"].dropna().astype(str)
    weapon_names = sorted(all_weapons.unique())
    weapons_ref = pd.DataFrame({"weapon_name": weapon_names})
    weapons_ref["weapon_id"] = weapons_ref.index + 1

    # Build skin dimension from skins_df
    skins_ref = skins_df[["skin_name", "rarity"]].drop_duplicates()
    skins_ref["skin_id"] = skins_ref.index + 1

    # Join with weapons
    facts = price_all.merge(weapons_ref[["weapon_id", "weapon_name"]], on="weapon_name", how="left")
    
    # Join with skins
    facts = facts.merge(skins_ref[["skin_id", "skin_name"]], on="skin_name", how="left")

    # Keep only rows where both skin and weapon matched
    facts = facts[facts["skin_id"].notna() & facts["weapon_id"].notna()].copy()

    if facts.empty:
        logger.warning("No matches between price data and dimensions.")
        return None

    facts_final = facts[["price", "quantity", "date_id", "skin_id", "weapon_id",
                          "wear", "price_range_id"]].copy()
    facts_final.columns = ["price", "volume", "date_id", "skin_id", "weapon_id",
                            "wear", "price_range_id"]
    facts_final["container_id"] = None
    facts_final["sticker_id"] = None
    facts_final["team_id"] = None
    facts_final["match_id"] = None

    facts_final.to_csv(output_path, index=False)
    logger.info(f"Saved {len(facts_final)} fact rows to {output_path}")
    return output_path

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_prices_dimensions(
    skins_csv_path: str = "data/processed/skins.csv",
    price_frames: Optional[List[pd.DataFrame]] = None,
    max_price_files: Optional[int] = None,
) -> None:
    skins_df = pd.read_csv(skins_csv_path)
    price_frames = price_frames if price_frames is not None else extract_prices(max_files=max_price_files)

    containers_df = _build_containers(skins_df, price_frames)
    # Don't rebuild skins_dim_df - skins were already loaded by load_all
    # skins_dim_df = _build_skin_dim(skins_df)
    # Don't rebuild weapons_dim_df - weapons were already loaded by load_all
    # weapons_dim_df = _build_weapon_dim(price_frames)
    times_dim_df = _build_time_dim(price_frames)
    wear_dim_df = _build_wear_dim(price_frames)
    price_range_dim_df = _build_price_range_dim()

    conn = get_connection()
    try:
        load_containers(conn, containers_df)
        # Skip loading skins and weapons - already done by load_all
        # load_skins(conn, skins_dim_df)
        # load_weapons(conn, weapons_dim_df)
        load_times(conn, times_dim_df)
        load_wear_ranges(conn, wear_dim_df)
        load_price_ranges(conn, price_range_dim_df)
        #load_facts(conn, skins_df, price_frames, price_range_dim_df)
        save_facts_to_csv(skins_df, price_frames, price_range_dim_df)

    finally:
        conn.close()
        logger.info("Database connection closed.")


def load_all(transformed: dict[str, object]) -> None:
    conn = get_connection()
    try:
        load_skins(conn, transformed["skins"])

        if "weapons" in transformed:
            load_weapons(conn, transformed["weapons"])
        else:
            logger.warning("No transformed weapons dataset available; skipping Dim_Weapon load.")

        if "stickers" in transformed:
            load_stickers(conn, transformed["stickers"])
        else:
            logger.warning("No transformed stickers dataset available; skipping Dim_Sticker load.")
    finally:
        conn.close()
        logger.info("Database connection closed.")
