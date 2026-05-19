-- add_unique_constraints.sql
-- Run this ONCE against your PostgreSQL DW before the first ETL load.
-- These UNIQUE constraints allow the ON CONFLICT DO NOTHING strategy
-- in load.py to work correctly (idempotent re-runs).

ALTER TABLE Dim_Skin
    ADD CONSTRAINT uq_dim_skin_name UNIQUE (skin_name);

ALTER TABLE Dim_Weapon
    ADD CONSTRAINT uq_dim_weapon_name UNIQUE (weapon_name);

ALTER TABLE Dim_Sticker
    ADD CONSTRAINT uq_dim_sticker_name UNIQUE (sticker_name);
