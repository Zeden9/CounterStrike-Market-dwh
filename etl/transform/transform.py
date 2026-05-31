from typing import Dict, List, Optional

import pandas as pd

from etl.transform.transform_weapons import transform_weapons
from etl.transform.transform_prices import transform_prices


def _transform_weapons_items(weapons_df: pd.DataFrame) -> pd.DataFrame:
    """Transform weapons DataFrame to required format for loading.

    Input: DataFrame with columns ['weapon', 'skin_name']
    Output: DataFrame with columns ['weapon_name', 'weapon_type']
    """
    if weapons_df is None or weapons_df.empty:
        return pd.DataFrame(columns=['weapon_name', 'weapon_type'])

    # Get unique weapons and add weapon_type
    unique_weapons = weapons_df[['weapon']].drop_duplicates().copy()
    unique_weapons.columns = ['weapon_name']

    # Determine weapon type based on content (all from weapons.csv are regular weapons)
    unique_weapons['weapon_type'] = 'Weapon'

    return unique_weapons


def _transform_stickers(stickers_df: pd.DataFrame) -> pd.DataFrame:
    """Transform stickers DataFrame to required format for loading.

    Input: DataFrame with columns ['name', 'event', 'type']
    Output: DataFrame with columns ['sticker_name', 'rarity']
    """
    if stickers_df is None or stickers_df.empty:
        return pd.DataFrame(columns=['sticker_name', 'rarity'])

    result = stickers_df[['name']].copy()
    result.columns = ['sticker_name']
    # Use 'type' as rarity if available, otherwise set to 'Unknown'
    result['rarity'] = stickers_df['type'].fillna('Unknown')

    return result


def transform_all(raw: Dict[str, object], price_frames: Optional[List[pd.DataFrame]] = None) -> Dict[str, object]:
    """Apply all transforms to the extracted data and return a dict of DataFrames.

    Expects `raw` to contain keys "skins", "weapons", "stickers".
    """
    skins_raw = raw.get("skins")
    skins_transformed = transform_weapons(skins_raw)
    prices_transformed = transform_prices(price_frames or [])

    transformed = {"skins": skins_transformed, "prices": prices_transformed}

    # Transform weapons and stickers
    if "weapons" in raw and raw["weapons"] is not None:
        transformed["weapons"] = _transform_weapons_items(raw["weapons"])
    if "stickers" in raw and raw["stickers"] is not None:
        transformed["stickers"] = _transform_stickers(raw["stickers"])

    return transformed


if __name__ == "__main__":
    # simple CLI: run transform using extractor
    from etl.extract.extract_weapons import extract_all
    from etl.extract.extract_prices import extract_prices

    raw = extract_all()
    prices = extract_prices()
    transform_all(raw, prices)
    print("Transform complete")
