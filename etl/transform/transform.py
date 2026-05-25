from typing import Dict, List, Optional

import pandas as pd

from etl.transform.transform_weapons import transform_weapons
from etl.transform.transform_prices import transform_prices


def transform_all(raw: Dict[str, object], price_frames: Optional[List[pd.DataFrame]] = None) -> Dict[str, object]:
    """Apply all transforms to the extracted data and return a dict of DataFrames.

    Expects `raw` to contain the key "skins" with a DataFrame value.
    """
    skins_raw = raw.get("skins")
    skins_transformed = transform_weapons(skins_raw)
    prices_transformed = transform_prices(price_frames or [])
    return {"skins": skins_transformed, "prices": prices_transformed}


if __name__ == "__main__":
    # simple CLI: run transform using extractor
    from etl.extract.extract_weapons import extract_all
    from etl.extract.extract_prices import extract_prices

    raw = extract_all()
    prices = extract_prices()
    transform_all(raw, prices)
    print("Transform complete")
