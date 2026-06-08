-- 1. Dimension Tables

CREATE TABLE dim_time (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT
);

CREATE TABLE dim_skin_addon (
    addon_id SERIAL PRIMARY KEY,
    addon_name VARCHAR(50)
);

CREATE TABLE dim_skin (
    skin_id SERIAL PRIMARY KEY,
    skin_name VARCHAR(255),
    rarity VARCHAR(50),
    addon_id INT,
    FOREIGN KEY (addon_id) REFERENCES dim_skin_addon(addon_id)
);

CREATE TABLE dim_sticker (
    sticker_id SERIAL PRIMARY KEY,
    sticker_name VARCHAR(255),
    rarity VARCHAR(50)
);

CREATE TABLE dim_weapon (
    weapon_id SERIAL PRIMARY KEY,
    weapon_name VARCHAR(255),
    weapon_type VARCHAR(100)
);

CREATE TABLE dim_container (
    container_id SERIAL PRIMARY KEY,
    container_name VARCHAR(255),
    container_price FLOAT,
    release_date TIMESTAMP,
    container_type VARCHAR(100)
);

CREATE TABLE dim_team (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(255)
);

CREATE TABLE dim_price_range (
    price_range_id SERIAL PRIMARY KEY,
    price_range VARCHAR(50)
);

CREATE TABLE dim_wear_range (
    wear_range_id SERIAL PRIMARY KEY,
    wear_range VARCHAR(50)
);

CREATE TABLE dim_itemtype (
    item_type_id SERIAL PRIMARY KEY,
    item_type VARCHAR(10)
);

-- 2. Dependent Dimension Table

CREATE TABLE dim_matchoutcome (
    match_id SERIAL PRIMARY KEY,
    match_date DATE,
    winner_team INT,
    loser_team INT,
    FOREIGN KEY (winner_team) REFERENCES dim_team(team_id),
    FOREIGN KEY (loser_team) REFERENCES dim_team(team_id)
);

-- 3. Central Fact Table

CREATE TABLE fact_marketprice (
    fact_id SERIAL PRIMARY KEY,

    item_type_id INT NOT NULL,
    price FLOAT,
    volume INT,
    date_id DATE NOT NULL,

    sticker_id INT,
    team_id INT,
    skin_id INT,
    container_id INT,
    match_id INT,
    wear_range_id INT,
    price_range_id INT,
    weapon_id INT,

    FOREIGN KEY (date_id) REFERENCES dim_time(date_id),
    FOREIGN KEY (sticker_id) REFERENCES dim_sticker(sticker_id),
    FOREIGN KEY (team_id) REFERENCES dim_team(team_id),
    FOREIGN KEY (skin_id) REFERENCES dim_skin(skin_id),
    FOREIGN KEY (container_id) REFERENCES dim_container(container_id),
    FOREIGN KEY (match_id) REFERENCES dim_matchoutcome(match_id),
    FOREIGN KEY (wear_range_id) REFERENCES dim_wear_range(wear_range_id),
    FOREIGN KEY (price_range_id) REFERENCES dim_price_range(price_range_id),
    FOREIGN KEY (weapon_id) REFERENCES dim_weapon(weapon_id),
    FOREIGN KEY (item_type_id) REFERENCES dim_itemtype(item_type_id),

    -- Exactly one of skin, sticker, or container must be populated
    CHECK (
        (CASE WHEN skin_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN sticker_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN container_id IS NOT NULL THEN 1 ELSE 0 END)
        = 1
    )
);