import pandas as pd
from typing import List, Tuple, Optional


def load_weapons(weapons_csv_path: str = "data/processed/from_market_data/weapons.csv",
                 knives_csv_path: str = "data/processed/from_market_data/knives.csv",
                 weapons_taxonomy_path: str = "data/raw/weapons.txt") -> List[Tuple[List[str], str]]:
    """Load weapons and knives from market data CSVs with type info from taxonomy.

    Returns a list of tuples: (weapon_tokens, weapon_type)
    """
    # Load taxonomy for weapon types
    taxonomy = {}
    with open(weapons_taxonomy_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) == 2:
                weapon_name = parts[0].strip()
                weapon_type = parts[1].strip()
                taxonomy[weapon_name] = weapon_type

    weapons = []
    seen_weapons = set()

    # Load weapons from weapons.csv
    weapons_df = pd.read_csv(weapons_csv_path)
    for weapon_name in weapons_df["weapon"].unique():
        weapon_name = weapon_name.strip()
        if weapon_name not in seen_weapons:
            weapon_type = taxonomy.get(weapon_name, "Weapon")
            weapon_tokens = weapon_name.split()
            weapons.append((weapon_tokens, weapon_type))
            seen_weapons.add(weapon_name)

    # Load knives from knives.csv
    knives_df = pd.read_csv(knives_csv_path)
    for weapon_name in knives_df["weapon"].unique():
        weapon_name = weapon_name.strip()
        if weapon_name not in seen_weapons:
            weapon_type = taxonomy.get(weapon_name, "Knife")
            weapon_tokens = weapon_name.split()
            weapons.append((weapon_tokens, weapon_type))
            seen_weapons.add(weapon_name)

    # Sort by length of weapon tokens in reverse (longest first)
    weapons.sort(key=lambda x: len(x[0]), reverse=True)
    return weapons


def find_weapon(tokens: List[str], weapons: List[Tuple[List[str], str]]) -> Tuple[Optional[List[str]], int, Optional[str]]:
    """Find weapon in tokens.
    
    Returns: (weapon_tokens, num_tokens_consumed, weapon_type)
    """
    for w_tokens, w_type in weapons:
        if tokens[:len(w_tokens)] == w_tokens:
            return w_tokens, len(w_tokens), w_type
    return None, 0, None


def extract_weapons(weapons_csv_path: str = "data/processed/from_market_data/weapons.csv",
                    knives_csv_path: str = "data/processed/from_market_data/knives.csv",
                    skin_list_path: str = "data/raw/skin_list.txt") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract weapons from skin list using weapons/knives from market data CSVs.

    Returns: (skins_df, unknown_skins_df)
    """
    weapons = load_weapons(weapons_csv_path, knives_csv_path)
    weapons_dicts = []
    unknown_skins_dicts = []

    with open(skin_list_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                continue

            tokens = parts[0].split()
            
            # Find weapon in tokens
            weapon_tokens, weapon_len, weapon_type = find_weapon(tokens, weapons)

            weapon_name = " ".join(weapon_tokens) if weapon_tokens else None
            
            # Build skin_name from remaining tokens
            if weapon_tokens:
                remaining_tokens = tokens[weapon_len:]
                skin_name = " ".join(remaining_tokens)
            else:
                # No weapon found, skin_name is all tokens
                skin_name = " ".join(tokens)

            item_dict = {
                "weapon_name": weapon_name,
                "weapon_type": weapon_type,
                "skin_name": skin_name,
                "collection": parts[-2] if len(parts) >= 2 else None,
                "rarity": parts[1] if len(parts) >= 2 else None,
            }

            if weapon_name is not None:
                weapons_dicts.append(item_dict)
            else:
                unknown_skins_dicts.append(item_dict)

    skins_df = pd.DataFrame(weapons_dicts)
    unknown_skins_df = pd.DataFrame(unknown_skins_dicts)
    
    return skins_df, unknown_skins_df


if __name__ == "__main__":
    skins_df, unknown_skins_df = extract_weapons()
    skins_df.to_csv("data/processed/skins.csv", index=False)
    unknown_skins_df.to_csv("data/processed/unknown_skins.csv", index=False)
    print("Wrote data/processed/skins.csv")
    print("Wrote data/processed/unknown_skins.csv")


def extract_all() -> dict:
    """Return all extracted datasets as a dict of DataFrames.

    Keys are used by the downstream transform stage.
    """
    skins_df, unknown_skins_df = extract_weapons()
    return {"skins": skins_df, "unknown_skins": unknown_skins_df}