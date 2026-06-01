import re
import urllib.parse
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Compiled once at module level — not re-compiled per file/call
_WEAR_RE = re.compile(r"^(?P<skin_name>.+?)\s*\((?P<wear>[^)]+)\)\s*$")

_OUTPUT_COLUMNS = ["weapon_name", "skin_name", "wear", "price", "quantity", "date", "timestamp"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_name_conversion_table(
    name_conversion_table_path: str = "data/raw/name_conversion_table.csv",
) -> Dict[str, str]:
    df = pd.read_csv(name_conversion_table_path)
    if df.shape[1] < 2:
        raise ValueError(
            "Name conversion table must have at least two columns: encoded and decoded names"
        )
    return dict(zip(df.iloc[:, 0].astype(str), df.iloc[:, 1].astype(str)))


def parse_skin_name(decoded_name: str) -> Tuple[Optional[str], str, Optional[str]]:
    """Split a decoded market name into (weapon_name, skin_name, wear).

    Uses a module-level compiled regex — safe to call from worker processes.
    """
    decoded_name = decoded_name.strip()
    parts = decoded_name.split("|", 1)
    if len(parts) != 2:
        return None, decoded_name, None

    weapon_name = parts[0].strip()
    skin_part = parts[1].strip()

    match = _WEAR_RE.match(skin_part)
    if match:
        return weapon_name, match.group("skin_name").strip(), match.group("wear").strip()

    return weapon_name, skin_part, None


# ---------------------------------------------------------------------------
# Worker — must be a module-level function for ProcessPoolExecutor pickling
# ---------------------------------------------------------------------------

def _load_single_file(args: Tuple[Path, Dict[str, str]]) -> pd.DataFrame:
    """Load and normalise one market CSV. Runs in a worker process."""
    csv_path, conversion_table = args

    encoded_name = csv_path.stem
    decoded_name = conversion_table.get(encoded_name, urllib.parse.unquote(encoded_name))
    weapon_name, skin_name, wear = parse_skin_name(decoded_name)

    df = pd.read_csv(csv_path)

    if "unix timestamp" in df.columns:
        df = df.rename(columns={"unix timestamp": "timestamp"})

    # Overwrite / ensure the three parsed columns exist
    df["weapon_name"] = weapon_name
    df["skin_name"] = skin_name
    df["wear"] = wear

    # Fill any missing value columns with None
    for col in ("price", "quantity", "date", "timestamp"):
        if col not in df.columns:
            df[col] = None

    return df[_OUTPUT_COLUMNS]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_prices(
    market_dir: str = "data/raw/every_5th",
    max_files: Optional[int] = None,
    name_conversion_table_path: str = "data/raw/name_conversion_table.csv",
    workers: int = 8,
) -> List[pd.DataFrame]:
    """Load up to `max_files` market CSVs as normalised pandas DataFrames.

    Files are loaded in parallel across `workers` processes.

    If `max_files` is None, all available market CSV files are loaded.
    The file stem is used as the URL-encoded key into the conversion table.

    Each returned DataFrame has columns:
        weapon_name, skin_name, wear, price, quantity, date, timestamp
    """
    market_path = Path(market_dir)
    if not market_path.exists():
        raise FileNotFoundError(f"Market directory not found: {market_path}")

    conversion_table = load_name_conversion_table(name_conversion_table_path)
    csv_files = sorted(p for p in market_path.rglob("*.csv") if p.is_file())
    if not csv_files:
        return []

    selected_files = csv_files if max_files is None else csv_files[:max_files]
    args = [(p, conversion_table) for p in selected_files]

    # chunksize=50 amortises IPC overhead when there are thousands of small files
    with ProcessPoolExecutor(max_workers=workers) as executor:
        frames = list(executor.map(_load_single_file, args, chunksize=50))

    return frames


def extract_prices_df(
    market_dir: str = "data/raw/every_5th",
    max_files: Optional[int] = None,
    name_conversion_table_path: str = "data/raw/name_conversion_table.csv",
    workers: int = 8,
) -> pd.DataFrame:
    """Same as extract_prices but returns a single concatenated DataFrame.

    Prefer this over calling pd.concat yourself downstream.
    """
    frames = extract_prices(
        market_dir=market_dir,
        max_files=max_files,
        name_conversion_table_path=name_conversion_table_path,
        workers=workers,
    )
    if not frames:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    dfs = extract_prices()
    print(f"Loaded {len(dfs)} price DataFrames")
    for index, df in enumerate(dfs, start=1):
        skin_name = df["skin_name"].iat[0] if not df.empty else "unknown"
        print(f"{index}: {df.shape[0]} rows from {skin_name}")
        print(df.head())
