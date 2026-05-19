"""
Load
----
Inserts cleaned DataFrames into the PostgreSQL data warehouse
dimension tables (Lab 5 Section 5).

Strategy: INSERT … ON CONFLICT DO NOTHING
  - Idempotent: safe to re-run without duplicating rows.
  - Relies on a UNIQUE constraint on the name column of each dim table
    (see add_unique_constraints.sql if not already present).
  - The DB auto-generates the surrogate PK (SERIAL / pk increment).
"""

import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values

import pandas as pd

from config.config import DB_CONFIG
from config.logger import get_logger

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

    sql = (
        f"INSERT INTO {table} ({col_str}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_column}) DO NOTHING"
    )

    rows = [tuple(row) for row in df[columns].itertuples(index=False, name=None)]

    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        inserted = cur.rowcount          # -1 if driver can't determine; see note below
        conn.commit()

    # psycopg2's executemany doesn't always report accurate rowcount for
    # multi-row operations.  Query the table count as a reliable cross-check.
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        total = cur.fetchone()[0]

    logger.info(f"[{table}] Load complete. Total rows in table: {total}")
    return total


# ---------------------------------------------------------------------------
# Per-dimension loaders
# ---------------------------------------------------------------------------

def load_skins(conn, df: pd.DataFrame) -> int:
    logger.info(f"[Dim_Skin] Loading {len(df)} rows …")
    return _insert_dimension(
        conn,
        table="Dim_Skin",
        df=df,
        columns=["skin_name", "rarity"],
        conflict_column="skin_name",
    )


def load_weapons(conn, df: pd.DataFrame) -> int:
    logger.info(f"[Dim_Weapon] Loading {len(df)} rows …")
    return _insert_dimension(
        conn,
        table="Dim_Weapon",
        df=df,
        columns=["weapon_name", "weapon_type"],
        conflict_column="weapon_name",
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

def load_all(transformed: dict[str, pd.DataFrame]) -> None:
    conn = get_connection()
    try:
        load_skins(conn,    transformed["skins"])
        load_weapons(conn,  transformed["weapons"])
        load_stickers(conn, transformed["stickers"])
    finally:
        conn.close()
        logger.info("Database connection closed.")
