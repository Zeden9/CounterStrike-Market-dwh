import pandas as pd
from typing import List


def transform_prices(price_frames: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """Normalize extracted price frames for downstream loading."""
    required_columns = [
        "weapon_name",
        "skin_name",
        "wear",
        "price",
        "quantity",
        "date",
        "timestamp",
    ]

    normalized_frames: List[pd.DataFrame] = []
    for df in price_frames:
        frame = df.copy()
        if "unix timestamp" in frame.columns:
            frame = frame.rename(columns={"unix timestamp": "timestamp"})

        for col in required_columns:
            if col not in frame.columns:
                frame[col] = None

        normalized_frames.append(frame[required_columns])

    return normalized_frames
