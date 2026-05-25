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
    col_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    rows = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]

    with conn.cursor() as cur:
        for row in rows:
            conflict_value = row[columns.index(conflict_column)]
            if conflict_value is None:
                cur.execute(
                    f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})",
                    row,
                )
                continue

            cur.execute(
                f"SELECT 1 FROM {table} WHERE {conflict_column} = %s LIMIT 1",
                (conflict_value,),
            )
            if cur.fetchone():
                continue
            cur.execute(
                f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})",
                row,
            )
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
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

    col_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    rows = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]

    constraint_name = _find_conflict_constraint(conn, table, conflict_column)
    if constraint_name:
        sql = (
            f"INSERT INTO {table} ({col_str}) "
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
        cur.execute(f"SELECT COUNT(*) FROM {table}")
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
    logger.info(f"[Dim_Skin] Loading {len(df)} rows …")
    columns = ["skin_id", "skin_name", "rarity"] if "skin_id" in df.columns else ["skin_name", "rarity"]
    conflict_column = "skin_id" if "skin_id" in df.columns else "skin_name"
    return _insert_dimension(conn, "Dim_Skin", df, columns, conflict_column)


def load_weapons(conn, df: pd.DataFrame) -> int:
    logger.info(f"[Dim_Weapon] Loading {len(df)} rows …")
    columns = ["weapon_id", "weapon_name", "weapon_type"] if "weapon_id" in df.columns else ["weapon_name", "weapon_type"]
    conflict_column = "weapon_id" if "weapon_id" in df.columns else "weapon_name"
    return _insert_dimension(conn, "Dim_Weapon", df, columns, conflict_column)


def load_containers(conn, df: pd.DataFrame) -> int:
    logger.info(f"[Dim_Container] Loading {len(df)} rows …")
    return _insert_dimension(
        conn,
        table="Dim_Container",
        df=df,
        columns=["container_id", "container_name", "release_date", "container_type"],
        conflict_column="container_id",
    )


def load_times(conn, df: pd.DataFrame) -> int:
    logger.info(f"[Dim_Time] Loading {len(df)} rows …")
    return _insert_dimension(
        conn,
        table="Dim_Time",
        df=df,
        columns=["date_id", "day", "month", "year"],
        conflict_column="date_id",
    )


def load_stickers(conn, df: pd.DataFrame) -> int:
    logger.info(f"[Dim_Sticker] Loading {len(df)} rows …")
    return _insert_dimension(
        conn,
        table="Dim_Sticker",
        df=df,
        columns=["sticker_name", "rarity"],
        conflict_column="sticker_name",
    )


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
    skins_dim_df = _build_skin_dim(skins_df)
    weapons_dim_df = _build_weapon_dim(price_frames)
    times_dim_df = _build_time_dim(price_frames)

    conn = get_connection()
    try:
        load_containers(conn, containers_df)
        load_skins(conn, skins_dim_df)
        load_weapons(conn, weapons_dim_df)
        load_times(conn, times_dim_df)
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
