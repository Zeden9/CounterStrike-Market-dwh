import pandas as pd
from etl.extract.extract_weapons import extract_weapons


def transform_weapons(skins_df: pd.DataFrame | None = None, output_path: str = "data/processed/skins.csv") -> pd.DataFrame:
    """Transform weapon extraction into final processed CSV.

    If `skins_df` is None the extractor will be run.
    """
    if skins_df is None:
        skins_df = extract_weapons()

    # Placeholder for additional transforms (normalization, type mapping, etc.)
    # For now we just persist the extracted dataframe.
    skins_df.to_csv(output_path, index=False)
    return skins_df


if __name__ == "__main__":
    out = "data/processed/skins.csv"
    df = transform_weapons(output_path=out)
    print(f"Wrote {out}")
