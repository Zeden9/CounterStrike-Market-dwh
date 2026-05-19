"""
Transform
---------
Cleans and normalises the three dimension DataFrames.
Every problem detected in the Extract profiling step is addressed here.

Operations performed (Lab 5 Section 4):
  - Strip whitespace from string columns
  - Standardise rarity casing  (Title Case)
  - Replace unknown / invalid rarities with "Unknown"
  - Drop fully-duplicate rows
  - Drop rows where the primary name column is NULL
  - Reset index so it is clean before loading
"""

import pandas as pd

from config.config import VALID_RARITIES
from config.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())
    return df


def _normalise_rarity(series: pd.Series) -> pd.Series:
    """
    Title-case the rarity value and replace anything not in the known
    set with 'Unknown'.  Handles NULLs gracefully.
    """
    normalised = series.str.strip().str.title()
    invalid_mask = ~normalised.isin(VALID_RARITIES) | normalised.isna()

    if invalid_mask.any():
        bad_vals = series[invalid_mask].unique()
        logger.warning(
            f"Replacing {invalid_mask.sum()} invalid/missing rarity values "
            f"{bad_vals} → 'Unknown'"
        )
    normalised[invalid_mask] = "Unknown"
    return normalised


def _drop_duplicates(df: pd.DataFrame, source: str) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    if dropped:
        logger.info(f"[{source}] Dropped {dropped} duplicate rows.")
    return df


def _drop_null_names(df: pd.DataFrame, name_col: str, source: str) -> pd.DataFrame:
    before = len(df)
    df = df.dropna(subset=[name_col])
    dropped = before - len(df)
    if dropped:
        logger.warning(
            f"[{source}] Dropped {dropped} rows with NULL '{name_col}'."
        )
    return df


# ---------------------------------------------------------------------------
# Per-dimension transforms
# ---------------------------------------------------------------------------

def transform_skins(df: pd.DataFrame) -> pd.DataFrame:
    """
    Target schema:
        skin_name   VARCHAR(255)  NOT NULL
        rarity      VARCHAR(50)
    """
    source = "Dim_Skin"
    logger.info(f"[{source}] Starting transform. Input rows: {len(df)}")

    df = _strip_strings(df)
    df = _drop_null_names(df, "skin_name", source)
    df = _drop_duplicates(df, source)

    df["rarity"] = _normalise_rarity(df["rarity"])

    # Keep only the columns that go into the DB
    df = df[["skin_name", "rarity"]].reset_index(drop=True)

    logger.info(f"[{source}] Transform complete. Output rows: {len(df)}")
    return df


def transform_weapons(df: pd.DataFrame) -> pd.DataFrame:
    """
    Target schema:
        weapon_name  VARCHAR(255)  NOT NULL
        weapon_type  VARCHAR(100)
    """
    source = "Dim_Weapon"
    logger.info(f"[{source}] Starting transform. Input rows: {len(df)}")

    df = _strip_strings(df)
    df = _drop_null_names(df, "weapon_name", source)
    df = _drop_duplicates(df, source)

    # Standardise weapon_type capitalisation
    if "weapon_type" in df.columns:
        df["weapon_type"] = df["weapon_type"].str.title().fillna("Unknown")
    else:
        logger.warning(f"[{source}] 'weapon_type' column not found – filling Unknown.")
        df["weapon_type"] = "Unknown"

    df = df[["weapon_name", "weapon_type"]].reset_index(drop=True)

    logger.info(f"[{source}] Transform complete. Output rows: {len(df)}")
    return df


def transform_stickers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Target schema:
        sticker_name  VARCHAR(255)  NOT NULL
        rarity        VARCHAR(50)
    """
    source = "Dim_Sticker"
    logger.info(f"[{source}] Starting transform. Input rows: {len(df)}")

    df = _strip_strings(df)
    df = _drop_null_names(df, "sticker_name", source)
    df = _drop_duplicates(df, source)

    df["rarity"] = _normalise_rarity(df["rarity"])

    df = df[["sticker_name", "rarity"]].reset_index(drop=True)

    logger.info(f"[{source}] Transform complete. Output rows: {len(df)}")
    return df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def transform_all(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    return {
        "skins":    transform_skins(raw["skins"]),
        "weapons":  transform_weapons(raw["weapons"]),
        "stickers": transform_stickers(raw["stickers"]),
    }
