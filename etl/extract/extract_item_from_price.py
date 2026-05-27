import pandas as pd
from typing import List, Tuple, Optional
import os


def load_weapons(weapons_path: str = "data/raw/weapons.txt") -> List[Tuple[str, str]]:
    """Load weapon names and types."""
    weapons = []
    with open(weapons_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split(",", maxsplit=1)]
            weapon_name = parts[0]
            weapon_type = parts[1] if len(parts) == 2 else ""
            weapons.append((weapon_name, weapon_type))
    return weapons


def normalize_item_name(name: str) -> str:
    """Normalize market item names for weapon matching."""
    normalized = name.strip()
    # Remove leading star symbol and whitespace
    while normalized.startswith("★"):
        normalized = normalized[1:].strip()
    for prefix in ["StatTrak™", "StatTrak", "Souvenir"]:
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):].strip()
    return normalized


def strip_wear(text: str) -> str:
    """Remove wear or quality strings from a skin name."""
    import re
    return re.sub(r"\s*\([^)]*\)$", "", text).strip()


def find_weapon(name: str, weapons: List[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    """Return matched weapon name and type if the decoded name begins with one."""
    normalized = normalize_item_name(name)
    normalized_lower = normalized.lower()
    for weapon_name, weapon_type in weapons:
        if normalized_lower.startswith(weapon_name.lower()):
            return weapon_name, weapon_type
    return None


def is_gloves(name: str) -> bool:
    return "gloves" in name.lower()


def is_knife(weapon_name: str, weapon_type: str) -> bool:
    if weapon_type and weapon_type.lower() == "knife":
        return True
    return weapon_name.lower().endswith("knife")


def parse_weapon_name(name: str) -> Tuple[str, str]:
    """Return normalized weapon and skin_name."""
    normalized = normalize_item_name(name)
    if " | " in normalized:
        weapon, skin = normalized.split(" | ", 1)
    else:
        weapon, skin = normalized, ""
    skin = strip_wear(skin)
    return weapon.strip(), skin.strip()


# parse_knife_name is now an alias for parse_weapon_name — same logic applies to all weapons.
parse_knife_name = parse_weapon_name


def parse_sticker(name: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Parse sticker name.

    Returns: (sticker_name, event, type)
    """
    import re

    # Remove "Sticker | " prefix
    name_without_prefix = name[len("Sticker | "):]

    # Extract type from parentheses
    sticker_type = None
    type_match = re.search(r'\(([^)]+)\)', name_without_prefix)
    if type_match:
        sticker_type = type_match.group(1)

    # Remove any parenthetical type indicators
    name_without_type = re.sub(r'\s*\([^)]+\)', '', name_without_prefix).strip()

    # Split by pipe to extract event (if exists)
    parts = name_without_type.split("|")

    if len(parts) == 1:
        sticker_name = parts[0].strip()
        event = None
    else:
        sticker_name = parts[0].strip()
        event = parts[1].strip() if len(parts) > 1 else None

    return sticker_name, event, sticker_type


def extract_items_from_price(
    conversion_table_path: str = "data/raw/name_conversion_table.csv",
    weapons_path: str = "data/raw/weapons.txt",
    output_dir: str = "data/processed/from_market_data"
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract items from price history conversion table.

    Returns: (weapons_df, knives_df, gloves_df, stickers_df, unknown_df)
    """
    os.makedirs(output_dir, exist_ok=True)

    weapons = load_weapons(weapons_path)
    weapons_dicts = []
    knives_dicts = []
    gloves_dicts = []
    stickers_dicts = []
    unknown_dicts = []
    knife_seen = set()
    weapon_seen = set()
    glove_seen = set()

    df = pd.read_csv(conversion_table_path, header=None, names=["encoded", "decoded"])

    for _, row in df.iterrows():
        decoded_name = row["decoded"].strip()

        if is_gloves(decoded_name):
            weapon, skin_name = parse_weapon_name(decoded_name)
            if (weapon, skin_name) not in glove_seen:
                glove_seen.add((weapon, skin_name))
                gloves_dicts.append({"weapon": weapon, "skin_name": skin_name})
            continue

        weapon_match = find_weapon(decoded_name, weapons)
        if weapon_match is not None:
            weapon_name, weapon_type = weapon_match
            if is_knife(weapon_name, weapon_type):
                weapon, skin_name = parse_knife_name(decoded_name)
                if (weapon, skin_name) not in knife_seen:
                    knife_seen.add((weapon, skin_name))
                    knives_dicts.append({"weapon": weapon, "skin_name": skin_name})
                continue
            weapon, skin_name = parse_weapon_name(decoded_name)
            if (weapon, skin_name) not in weapon_seen:
                weapon_seen.add((weapon, skin_name))
                weapons_dicts.append({"weapon": weapon, "skin_name": skin_name})
            continue

        if decoded_name.startswith("Sticker | "):
            sticker_name, event, sticker_type = parse_sticker(decoded_name)
            stickers_dicts.append({
                "name": sticker_name,
                "event": event,
                "type": sticker_type,
            })
            continue

        unknown_dicts.append({"name": decoded_name})

    weapons_df = pd.DataFrame(weapons_dicts)
    knives_df = pd.DataFrame(knives_dicts)
    gloves_df = pd.DataFrame(gloves_dicts)
    stickers_df = pd.DataFrame(stickers_dicts)
    unknown_df = pd.DataFrame(unknown_dicts)

    return weapons_df, knives_df, gloves_df, stickers_df, unknown_df


if __name__ == "__main__":
    weapons_df, knives_df, gloves_df, stickers_df, unknown_df = extract_items_from_price()

    output_dir = "data/processed/from_market_data"

    weapons_df.to_csv(f"{output_dir}/weapons.csv", index=False)
    print(f"Wrote {output_dir}/weapons.csv ({len(weapons_df)} items)")

    knives_df.to_csv(f"{output_dir}/knives.csv", index=False)
    print(f"Wrote {output_dir}/knives.csv ({len(knives_df)} items)")

    gloves_df.to_csv(f"{output_dir}/gloves.csv", index=False)
    print(f"Wrote {output_dir}/gloves.csv ({len(gloves_df)} items)")

    stickers_df.to_csv(f"{output_dir}/stickers.csv", index=False)
    print(f"Wrote {output_dir}/stickers.csv ({len(stickers_df)} items)")

    unknown_df.to_csv(f"{output_dir}/unknown.csv", index=False)
    print(f"Wrote {output_dir}/unknown.csv ({len(unknown_df)} items)")


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