"""
post_load_transform.py
----------------------
Post-load transform stage: updates item_type_id based on weapon names.

This module runs after all data is loaded and refines classifications
for StatTrak and Souvenir items by examining weapon names.

Usage
-----
    from etl.transform.post_load_transform import run_post_load_transform
    run_post_load_transform()
"""

import psycopg2

from config.config import DB_CONFIG
from config.logger import get_logger

logger = get_logger(__name__)


def run_post_load_transform():
    """Update item_type_id based on weapon names (Souvenir, StatTrak).
    
    This post-load transform runs after all data is loaded and updates
    the fact_marketprice table with the correct item type classifications.
    
    Raises
    ------
    psycopg2.Error
        If database connection or query execution fails.
    """
    logger.info("=== POST-LOAD TRANSFORM: Item Type Classification ===")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        # SQL statements to update item_type_id based on weapon names
        statements = [
            {
                "name": "Souvenir",
                "item_type_id": 3,
                "sql": """
                    UPDATE fact_marketprice AS fp
                    SET item_type_id = 3
                    FROM dim_weapon AS dw
                    WHERE dw.weapon_id = fp.weapon_id
                      AND dw.weapon_name LIKE '%Souvenir%'
                """
            },
            {
                "name": "StatTrak",
                "item_type_id": 2,
                "sql": """
                    UPDATE fact_marketprice AS fp
                    SET item_type_id = 2
                    FROM dim_weapon AS dw
                    WHERE dw.weapon_id = fp.weapon_id
                      AND dw.weapon_name LIKE '%StatTrak%'
                """
            }
        ]
        
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt["sql"])
                rows_updated = cur.rowcount
                logger.info(
                    f"  [{stmt['name']}] Updated {rows_updated} rows "
                    f"(item_type_id = {stmt['item_type_id']})"
                )
        
        conn.commit()
        logger.info("Post-load transform completed successfully.")
        
    except psycopg2.Error as e:
        logger.error(f"Database error during post-load transform: {e}")
        raise
    finally:
        conn.close()