from typing import Dict

from etl.transform.transform_weapons import transform_weapons


def transform_all(raw: Dict[str, object]) -> Dict[str, object]:
    """Apply all transforms to the extracted data and return a dict of DataFrames.

    Expects `raw` to contain the key "skins" with a DataFrame value.
    """
    skins_raw = raw.get("skins")
    skins_transformed = transform_weapons(skins_raw)
    return {"skins": skins_transformed}


if __name__ == "__main__":
    # simple CLI: run transform using extractor
    from etl.extract.extract_weapons import extract_all

    raw = extract_all()
    transform_all(raw)
    print("Transform complete")