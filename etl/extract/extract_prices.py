import re
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd


def load_name_conversion_table(name_conversion_table_path: str = "data/raw/name_conversion_table.csv") -> Dict[str, str]:
    df = pd.read_csv(name_conversion_table_path)
    if df.shape[1] < 2:
        raise ValueError("Name conversion table must have at least two columns: encoded and decoded names")
    return dict(zip(df.iloc[:, 0].astype(str), df.iloc[:, 1].astype(str)))


def parse_skin_name(decoded_name: str) -> Tuple[Optional[str], str, Optional[str]]:
    decoded_name = decoded_name.strip()
    parts = decoded_name.split("|", 1)
    if len(parts) != 2:
        return None, decoded_name, None

    weapon_name = parts[0].strip()
    skin_part = parts[1].strip()

    match = re.match(r"^(?P<skin_name>.+?)\s*\((?P<wear>[^)]+)\)\s*$", skin_part)
    if match:
        skin_name = match.group("skin_name").strip()
        wear = match.group("wear").strip()
    else:
        skin_name = skin_part
        wear = None

    return weapon_name, skin_name, wear


def extract_prices(
    market_dir: str = "data/raw/market",
    max_files: Optional[int] = None,
    name_conversion_table_path: str = "data/raw/name_conversion_table.csv",
) -> List[pd.DataFrame]:
    """Load up to `max_files` market CSVs as normalized pandas DataFrames.

    If `max_files` is None, all available market CSV files are loaded.
    The file stem is used as the URL-encoded key into the conversion table.
    Each returned DataFrame has columns:
    weapon_name, skin_name, wear, price, quantity, date, timestamp
    """
    market_path = Path(market_dir)
    if not market_path.exists():
        raise FileNotFoundError(f"Market directory not found: {market_path}")

    conversion_table = load_name_conversion_table(name_conversion_table_path)
    csv_files = sorted([path for path in market_path.rglob("*.csv") if path.is_file()])
    if not csv_files:
        return []

    frames: List[pd.DataFrame] = []
    selected_files = csv_files if max_files is None else csv_files[:max_files]
    for csv_path in selected_files:
        encoded_name = csv_path.stem
        decoded_name = conversion_table.get(encoded_name, urllib.parse.unquote(encoded_name))
        weapon_name, skin_name, wear = parse_skin_name(decoded_name)

        df = pd.read_csv(csv_path)
        if "unix timestamp" in df.columns:
            df = df.rename(columns={"unix timestamp": "timestamp"})

        for required_column in ["weapon_name", "skin_name", "wear"]:
            if required_column not in df.columns:
                df[required_column] = None

        df["weapon_name"] = weapon_name
        df["skin_name"] = skin_name
        df["wear"] = wear

        for column in ["price", "quantity", "date", "timestamp"]:
            if column not in df.columns:
                df[column] = None

        df = df[["weapon_name", "skin_name", "wear", "price", "quantity", "date", "timestamp"]]
        frames.append(df)

    return frames


if __name__ == "__main__":
    dfs = extract_prices()
    print(f"Loaded {len(dfs)} price DataFrames")
    for index, df in enumerate(dfs, start=1):
        skin_name = df["skin_name"].iat[0] if not df.empty else "unknown"
        print(f"{index}: {df.shape[0]} rows from {skin_name}")
        print(df.head())
