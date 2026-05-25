"""
ETL Configuration
-----------------
Central place for all settings: DB connection, file paths, column mappings.
Edit this file to match your local setup before running anything.
"""

import os

# ---------------------------------------------------------------------------
# PostgreSQL connection settings
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host":     os.getenv("DW_HOST",     "localhost"),
    "port":     int(os.getenv("DW_PORT", "5432")),
    "dbname":   os.getenv("DW_NAME",     "CSGO_MARKETPLACE"),
    "user":     os.getenv("DW_USER",     "postgres"),
    "password": os.getenv("DW_PASSWORD", "admin"),
}

# ---------------------------------------------------------------------------
# Raw data file paths  (put your CSVs here)
# ---------------------------------------------------------------------------
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

RAW_FILES = {
    "skins":    os.path.join(DATA_DIR, "skins.csv"),
    "weapons":  os.path.join(DATA_DIR, "weapons.txt"),
}

# ---------------------------------------------------------------------------
# Column mappings:  CSV column name  →  DB column name
# Adjust the left-hand side to match your actual CSV headers.
# ---------------------------------------------------------------------------
# COLUMN_MAP = {
#     "skins": {
#         "skin_name": "skin_name",   # name of the skin
#         "rarity":    "rarity",      # e.g. "Covert", "Classified" …
#     },
#     "weapons": {
#         "weapon_name": "weapon_name",
#         "weapon_type": "weapon_type",  # e.g. "Rifle", "Pistol" …
#     },
#     "stickers": {
#         "sticker_name": "sticker_name",
#         "rarity":       "rarity",
#     },
# }

# ---------------------------------------------------------------------------
# Known rarity values (used for validation / normalisation)
# # ---------------------------------------------------------------------------
# VALID_RARITIES = {
#     "Consumer Grade", "Industrial Grade", "Mil-Spec",
#     "Restricted", "Classified", "Covert", "Contraband",
#     "High Grade", "Remarkable", "Exotic", "Extraordinary",   # sticker tiers
#     "Unknown",   # fallback after cleaning
# }

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR  = os.path.join(os.path.dirname(__file__), "..", "logs")
LOG_FILE = os.path.join(LOG_DIR, "etl.log")
