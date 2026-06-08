import os
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Compiled once at module level
_STRIP_WEAR_RE = re.compile(r"\s*\([^)]*\)$")
_STICKER_TYPE_RE = re.compile(r"\(([^)]+)\)")
_STICKER_PARENS_RE = re.compile(r"\s*\([^)]+\)")

_PREFIXES_TO_STRIP = ("StatTrak™", "StatTrak", "Souvenir")


# ---------------------------------------------------------------------------
# Weapon loading & indexing
# ---------------------------------------------------------------------------

def load_weapons(weapons_path: str = "data/raw/weapons.txt") -> List[Tuple[str, str]]:
    """Load weapon names and types from taxonomy file."""
    weapons: List[Tuple[str, str]] = []
    with open(weapons_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(",", maxsplit=1)]
            weapon_name = parts[0]
            weapon_type = parts[1] if len(parts) == 2 else ""
            weapons.append((weapon_name, weapon_type))
    return weapons


def build_weapon_prefix_index(
    weapons: List[Tuple[str, str]],
) -> Dict[str, List[Tuple[str, str, str]]]:
    """Index weapons by lowercased first word for fast candidate lookup.

    Each bucket value is (lower_name, original_name, weapon_type).
    Longer names come first within each bucket so "Desert Eagle" beats "Desert".
    """
    buckets: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
    for orig_name, wtype in weapons:
        first_word = orig_name.lower().split()[0] if orig_name else ""
        buckets[first_word].append((orig_name.lower(), orig_name, wtype))

    # Sort each bucket: longest name first to avoid prefix false-matches
    for key in buckets:
        buckets[key].sort(key=lambda t: len(t[0]), reverse=True)

    return dict(buckets)


# ---------------------------------------------------------------------------
# Name normalisation helpers
# ---------------------------------------------------------------------------

def normalize_item_name(name: str) -> str:
    """Strip leading ★, StatTrak™/StatTrak, and Souvenir prefixes."""
    normalized = name.strip().lstrip("★").strip()
    for prefix in _PREFIXES_TO_STRIP:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):].strip()
            break  # only one prefix can appear at a time
    return normalized


def strip_wear(text: str) -> str:
    """Remove trailing wear/quality parenthetical from a skin name."""
    return _STRIP_WEAR_RE.sub("", text).strip()


def detect_item_type(name: str) -> Optional[str]:
    """Detect if the item is StatTrak, Souvenir, or Standard (None).
    
    Returns: "StatTrak", "Souvenir", or None (which represents "Standard")
    """
    if not name:
        return None
    normalized = name.strip().lstrip("★").strip()
    if normalized.startswith("StatTrak™") or normalized.startswith("StatTrak"):
        return "StatTrak"
    elif normalized.startswith("Souvenir"):
        return "Souvenir"
    return None  # Standard


# ---------------------------------------------------------------------------
# Item type detection & parsing
# ---------------------------------------------------------------------------

def is_gloves(name: str) -> bool:
    return "gloves" in name.lower()


def is_knife(weapon_name: str, weapon_type: str) -> bool:
    return (weapon_type.lower() == "knife") or weapon_name.lower().endswith("knife")


def find_weapon(
    name: str,
    weapon_index: Dict[str, List[Tuple[str, str, str]]],
) -> Optional[Tuple[str, str]]:
    """Return (original_weapon_name, weapon_type) if name starts with a known weapon."""
    normalized_lower = normalize_item_name(name).lower()
    if not normalized_lower:
        return None
    first_word = normalized_lower.split()[0]
    for lower_name, orig_name, wtype in weapon_index.get(first_word, []):
        if normalized_lower.startswith(lower_name):
            return orig_name, wtype
    return None


def parse_weapon_name(name: str) -> Tuple[str, str]:
    """Return (weapon, skin_name) from a normalised item name."""
    normalized = normalize_item_name(name)
    if " | " in normalized:
        weapon, skin = normalized.split(" | ", 1)
    else:
        weapon, skin = normalized, ""
    return weapon.strip(), strip_wear(skin).strip()


# Alias — same logic applies to knives
parse_knife_name = parse_weapon_name


def parse_sticker(name: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Parse a sticker entry.

    Returns: (sticker_name, event, sticker_type)
    """
    name_without_prefix = name[len("Sticker | "):]

    type_match = _STICKER_TYPE_RE.search(name_without_prefix)
    sticker_type = type_match.group(1) if type_match else None

    name_without_type = _STICKER_PARENS_RE.sub("", name_without_prefix).strip()

    parts = name_without_type.split("|", 1)
    sticker_name = parts[0].strip()
    event = parts[1].strip() if len(parts) > 1 else None

    return sticker_name, event, sticker_type


# ---------------------------------------------------------------------------
# Vectorized row parsers (used via Series.apply on pre-filtered subsets)
# ---------------------------------------------------------------------------

def _parse_glove_row(decoded_name: str) -> dict:
    weapon, skin_name = parse_weapon_name(decoded_name)
    item_type = detect_item_type(decoded_name)
    return {"weapon": weapon, "skin_name": skin_name, "type": item_type}


def _parse_sticker_row(decoded_name: str) -> dict:
    sticker_name, event, sticker_type = parse_sticker(decoded_name)
    item_type = detect_item_type(decoded_name)
    return {"sticker_name": sticker_name, "event": event, "rarity": sticker_type, "type": item_type}


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------

def extract_items_from_price(
    conversion_table_path: str = "data/raw/name_conversion_table.csv",
    weapons_path: str = "data/raw/weapons.txt",
    output_dir: str = "data/processed/from_market_data",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract items from the price-history conversion table.

    Uses vectorized pandas operations for category detection and per-category
    apply() for parsing, replacing the slow iterrows() loop.

    Returns: (weapons_df, knives_df, gloves_df, stickers_df, unknown_df)
    """
    os.makedirs(output_dir, exist_ok=True)

    weapons = load_weapons(weapons_path)
    weapon_index = build_weapon_prefix_index(weapons)

    df = pd.read_csv(conversion_table_path, header=None, names=["encoded", "decoded"])
    decoded: pd.Series = df["decoded"].str.strip()

    # ------------------------------------------------------------------
    # Vectorized category masks (fast string ops on the whole series)
    # ------------------------------------------------------------------
    glove_mask = decoded.str.contains("gloves", case=False, na=False)
    sticker_mask = decoded.str.startswith("Sticker | ") & ~glove_mask

    # Everything else is a potential weapon/knife/unknown
    other_mask = ~glove_mask & ~sticker_mask

    # ------------------------------------------------------------------
    # Gloves
    # ------------------------------------------------------------------
    gloves_parsed = decoded[glove_mask].apply(_parse_glove_row)
    gloves_df = (
        pd.DataFrame(gloves_parsed.tolist()).drop_duplicates()
        if not gloves_parsed.empty
        else pd.DataFrame(columns=["weapon", "skin_name", "type"])
    )

    # ------------------------------------------------------------------
    # Stickers
    # ------------------------------------------------------------------
    stickers_parsed = decoded[sticker_mask].apply(_parse_sticker_row)
    stickers_df = (
        pd.DataFrame(stickers_parsed.tolist())
        if not stickers_parsed.empty
        else pd.DataFrame(columns=["sticker_name", "event", "rarity", "type"])
    )

    # ------------------------------------------------------------------
    # Weapons, knives, unknowns — still needs per-row weapon lookup but
    # now only runs on the filtered subset, not the entire table
    # ------------------------------------------------------------------
    weapons_records: List[dict] = []
    knives_records: List[dict] = []
    unknown_records: List[dict] = []

    knife_seen: set = set()
    weapon_seen: set = set()

    for decoded_name in decoded[other_mask]:
        weapon_match = find_weapon(decoded_name, weapon_index)

        if weapon_match is not None:
            weapon_name, weapon_type = weapon_match
            weapon, skin_name = parse_weapon_name(decoded_name)
            item_type = detect_item_type(decoded_name)
            key = (weapon, skin_name)

            if is_knife(weapon_name, weapon_type):
                if key not in knife_seen:
                    knife_seen.add(key)
                    knives_records.append({"weapon": weapon, "skin_name": skin_name, "type": item_type})
            else:
                if key not in weapon_seen:
                    weapon_seen.add(key)
                    weapons_records.append({"weapon": weapon, "skin_name": skin_name, "type": item_type})
        else:
            unknown_records.append({"name": decoded_name})

    weapons_df = pd.DataFrame(weapons_records) if weapons_records else pd.DataFrame(columns=["weapon", "skin_name", "type"])
    knives_df = pd.DataFrame(knives_records) if knives_records else pd.DataFrame(columns=["weapon", "skin_name", "type"])
    unknown_df = pd.DataFrame(unknown_records) if unknown_records else pd.DataFrame(columns=["name"])

    return weapons_df, knives_df, gloves_df, stickers_df, unknown_df


# ---------------------------------------------------------------------------
# Public API used by downstream pipeline
# ---------------------------------------------------------------------------

def extract_all() -> dict:
    """Return all extracted datasets as a dict of DataFrames."""
    weapons_df, knives_df, gloves_df, stickers_df, unknown_df = extract_items_from_price()
    return {
        "weapons": weapons_df,
        "knives": knives_df,
        "gloves": gloves_df,
        "stickers": stickers_df,
        "unknown": unknown_df,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    weapons_df, knives_df, gloves_df, stickers_df, unknown_df = extract_items_from_price()

    output_dir = "data/processed/from_market_data"
    for name, frame in (
        ("weapons", weapons_df),
        ("knives", knives_df),
        ("gloves", gloves_df),
        ("stickers", stickers_df),
        ("unknown", unknown_df),
    ):
        path = f"{output_dir}/{name}.csv"
        frame.to_csv(path, index=False)
        print(f"Wrote {path} ({len(frame)} items)")
