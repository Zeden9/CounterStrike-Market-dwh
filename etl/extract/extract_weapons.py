import pandas as pd
import os 

print(os.getcwd())

# -------------------------
# 1. Load weapons (preprocess once)
# -------------------------
with open("data/raw/weapons.txt", "r", encoding="utf-8") as f:
    weapons = [line.strip().split() for line in f]

# sort longest first (improves matching quality)
weapons.sort(key=len, reverse=True)


# -------------------------
# 2. Weapon matching function
# -------------------------
def find_weapon(tokens):
    for w_tokens in weapons:
        if tokens[:len(w_tokens)] == w_tokens:
            return w_tokens, len(w_tokens)
    return None, 0


# -------------------------
# 3. Parse skin list
# -------------------------
weapons_dicts = []

with open("data/raw/skin_list.txt", "r", encoding="utf-8", errors="ignore") as f:
    for line in f:

        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) < 2:
            continue

        # tokens of full name (weapon + skin)
        tokens = parts[0].split()

        weapon_tokens, weapon_len = find_weapon(tokens)

        weapon_name = " ".join(weapon_tokens) if weapon_tokens else None
        skin_name = " ".join(tokens[weapon_len:])
        weapons_dicts.append({
            "weapon_name": weapon_name,
            "skin_name": skin_name,
            "collection": parts[-2],
            "rarity": parts[1]
            #"weapon_type" = weapon_type
        })


# -------------------------
# 4. DataFrame
# -------------------------
skins_df = pd.DataFrame(weapons_dicts)

skins_df.to_csv("data/processed/skins.csv", index=False)