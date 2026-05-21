import pandas as pd
from typing import List, Tuple, Optional


def load_weapons(weapons_path: str = "data/raw/weapons.txt") -> List[List[str]]:
    with open(weapons_path, "r", encoding="utf-8") as f:
        weapons = [line.strip().split() for line in f if line.strip()]
    weapons.sort(key=len, reverse=True)
    return weapons


def find_weapon(tokens: List[str], weapons: List[List[str]]) -> Tuple[Optional[List[str]], int]:
    for w_tokens in weapons:
        if tokens[:len(w_tokens)] == w_tokens:
            return w_tokens, len(w_tokens)
    return None, 0


def extract_weapons(weapons_path: str = "data/raw/weapons.txt", skin_list_path: str = "data/raw/skin_list.txt") -> pd.DataFrame:
    weapons = load_weapons(weapons_path)
    weapons_dicts = []

    with open(skin_list_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                continue

            tokens = parts[0].split()
            weapon_tokens, weapon_len = find_weapon(tokens, weapons)

            weapon_name = " ".join(weapon_tokens) if weapon_tokens else None
            skin_name = " ".join(tokens[weapon_len:])
            weapons_dicts.append({
                "weapon_name": weapon_name,
                "skin_name": skin_name,
                "collection": parts[-2],
                "rarity": parts[1],
            })

    return pd.DataFrame(weapons_dicts)


if __name__ == "__main__":
    df = extract_weapons()
    df.to_csv("data/processed/skins_extracted.csv", index=False)
    print("Wrote data/processed/skins_extracted.csv")


def extract_all() -> dict:
    """Return all extracted datasets as a dict of DataFrames.

    Keys are used by the downstream transform stage.
    """
    skins_df = extract_weapons()
    return {"skins": skins_df}