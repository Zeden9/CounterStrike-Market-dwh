from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Weapon loading & indexing
# ---------------------------------------------------------------------------

def load_weapons(
    weapons_csv_path: str = "data/processed/from_market_data/weapons.csv",
    knives_csv_path: str = "data/processed/from_market_data/knives.csv",
    weapons_taxonomy_path: str = "data/raw/weapons.txt",
) -> List[Tuple[List[str], str]]:
    """Load weapons and knives from market-data CSVs with type info from taxonomy.

    Returns a list of (weapon_tokens, weapon_type) sorted longest-first so that
    multi-word names (e.g. "Desert Eagle") are matched before single-word ones.
    """
    # Build taxonomy: weapon_name -> weapon_type
    taxonomy: Dict[str, str] = {}
    with open(weapons_taxonomy_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = line.split(",", 1)
            if len(parts) == 2:
                taxonomy[parts[0].strip()] = parts[1].strip()

    weapons: List[Tuple[List[str], str]] = []
    seen: set = set()

    for csv_path, default_type in (
        (weapons_csv_path, "Weapon"),
        (knives_csv_path, "Knife"),
    ):
        df = pd.read_csv(csv_path)
        for weapon_name in df["weapon"].str.strip().unique():
            if weapon_name not in seen:
                seen.add(weapon_name)
                wtype = taxonomy.get(weapon_name, default_type)
                weapons.append((weapon_name.split(), wtype))

    # Longest token list first so multi-word weapons match before substrings
    weapons.sort(key=lambda x: len(x[0]), reverse=True)
    return weapons


def build_weapon_index(
    weapons: List[Tuple[List[str], str]],
) -> Dict[str, List[Tuple[List[str], str]]]:
    """Index weapons by their first token for O(1) candidate lookup.

    The full token list is still compared to guarantee correctness, but only
    candidates that share the same first token are ever checked.
    """
    index: Dict[str, List[Tuple[List[str], str]]] = defaultdict(list)
    for w_tokens, w_type in weapons:
        index[w_tokens[0]].append((w_tokens, w_type))
    return dict(index)


def find_weapon(
    tokens: List[str],
    weapon_index: Dict[str, List[Tuple[List[str], str]]],
) -> Tuple[Optional[List[str]], int, Optional[str]]:
    """Find a weapon match at the start of `tokens`.

    Returns (weapon_tokens, num_tokens_consumed, weapon_type).
    Returns (None, 0, None) when no match is found.
    """
    if not tokens:
        return None, 0, None

    for w_tokens, w_type in weapon_index.get(tokens[0], []):
        n = len(w_tokens)
        if tokens[:n] == w_tokens:
            return w_tokens, n, w_type

    return None, 0, None


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------

def extract_weapons(
    weapons_csv_path: str = "data/processed/from_market_data/weapons.csv",
    knives_csv_path: str = "data/processed/from_market_data/knives.csv",
    skin_list_path: str = "data/raw/skin_list.txt",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract weapons from the skin list using weapons/knives from market-data CSVs.

    The skin list is read with pandas (tab-separated) instead of manual line
    iteration, which is both faster and more robust to encoding edge-cases.

    Returns: (skins_df, unknown_skins_df)
    """
    weapons = load_weapons(weapons_csv_path, knives_csv_path)
    weapon_index = build_weapon_index(weapons)

    # Read the skin list as a TSV; unnamed columns get integer headers
    skin_list = pd.read_csv(
        skin_list_path,
        sep="\t",
        header=None,
        encoding="utf-8",
        on_bad_lines="skip",
        dtype=str,
    ).fillna("")

    if skin_list.shape[1] < 2:
        return pd.DataFrame(), pd.DataFrame()

    weapons_records: List[dict] = []
    unknown_records: List[dict] = []

    # Column references (positional)
    col_name = 0
    col_rarity = 1
    col_collection = skin_list.shape[1] - 2  # second-to-last column

    for row in skin_list.itertuples(index=False):
        name_field: str = row[col_name].strip()
        if not name_field:
            continue

        tokens = name_field.split()
        weapon_tokens, weapon_len, weapon_type = find_weapon(tokens, weapon_index)

        if weapon_tokens:
            weapon_name = " ".join(weapon_tokens)
            skin_name = " ".join(tokens[weapon_len:])
        else:
            weapon_name = None
            skin_name = name_field

        record = {
            "weapon_name": weapon_name,
            "weapon_type": weapon_type,
            "skin_name": skin_name,
            "collection": row[col_collection] if skin_list.shape[1] >= 2 else None,
            "rarity": row[col_rarity] if skin_list.shape[1] >= 2 else None,
        }

        if weapon_name is not None:
            weapons_records.append(record)
        else:
            unknown_records.append(record)

    return pd.DataFrame(weapons_records), pd.DataFrame(unknown_records)


# ---------------------------------------------------------------------------
# Public API used by downstream pipeline
# ---------------------------------------------------------------------------

def extract_all() -> dict:
    """Return all extracted datasets as a dict of DataFrames.

    Keys are used by the downstream transform stage.
    """
    skins_df, unknown_skins_df = extract_weapons()
    return {"skins": skins_df, "unknown_skins": unknown_skins_df}


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    skins_df, unknown_skins_df = extract_weapons()
    skins_df.to_csv("data/processed/skins.csv", index=False)
    unknown_skins_df.to_csv("data/processed/unknown_skins.csv", index=False)
    print(f"Wrote data/processed/skins.csv ({len(skins_df)} rows)")
    print(f"Wrote data/processed/unknown_skins.csv ({len(unknown_skins_df)} rows)")
