-- 1. Dimension Tables (No external dependencies)
CREATE TABLE Dim_Time (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT
);

CREATE TABLE Dim_Skin (
    skin_id SERIAL PRIMARY KEY, -- Automatically handles SEQUENCE and DEFAULT
    skin_name VARCHAR(255),
    rarity VARCHAR(50)
);

CREATE TABLE Dim_Sticker (
    sticker_id SERIAL PRIMARY KEY,
    sticker_name VARCHAR(255),
    rarity VARCHAR(50)
);

CREATE TABLE Dim_Weapon (
    weapon_id SERIAL PRIMARY KEY,
    weapon_name VARCHAR(255),
    weapon_type VARCHAR(100)
);

CREATE TABLE Dim_Container (
    container_id SERIAL PRIMARY KEY,
    container_name VARCHAR(255),
    container_price FLOAT,
    release_date TIMESTAMP,
    container_type VARCHAR(100)
);

CREATE TABLE Dim_Team (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(255)
);

CREATE TABLE Dim_Price_range (
    price_range_id SERIAL PRIMARY KEY,
    price_range VARCHAR(50)
);

CREATE TABLE Dim_Wear_range (
    wear_range_id SERIAL PRIMARY KEY,
    wear_range VARCHAR(50)
);

CREATE TABLE Dim_ItemType (
    item_type_id SERIAL PRIMARY KEY,
    item_type VARCHAR(10)
);

-- 2. Dependent Dimension Tables (Contains Foreign Keys)
CREATE TABLE Dim_MatchOutcome (
    match_id SERIAL PRIMARY KEY,
    match_date DATE,
    winner_team INT,
    loser_team INT,
    FOREIGN KEY (winner_team) REFERENCES Dim_Team(team_id),
    FOREIGN KEY (loser_team) REFERENCES Dim_Team(team_id)
);

-- 3. Central Fact Table
CREATE TABLE Fact_MarketPrice (
    fact_id SERIAL PRIMARY KEY,
    item_type INT,
    price FLOAT,
    volume INT,
    date_id DATE,
    sticker_id INT,
    team_id INT,
    skin_id INT,
    container_id INT,
    match_id INT,
    wear_range_id INT,
    price_range_id INT,
    weapon_id INT,
    
    -- Foreign Key Constraints
    FOREIGN KEY (date_id) REFERENCES Dim_Time(date_id),
    FOREIGN KEY (sticker_id) REFERENCES Dim_Sticker(sticker_id),
    FOREIGN KEY (team_id) REFERENCES Dim_Team(team_id),
    FOREIGN KEY (skin_id) REFERENCES Dim_Skin(skin_id),
    FOREIGN KEY (container_id) REFERENCES Dim_Container(container_id),
    FOREIGN KEY (match_id) REFERENCES Dim_MatchOutcome(match_id),
    FOREIGN KEY (wear_range_id) REFERENCES Dim_Wear_range(wear_range_id),
    FOREIGN KEY (price_range_id) REFERENCES Dim_Price_range(price_range_id),
    FOREIGN KEY (weapon_id) REFERENCES Dim_Weapon(weapon_id),
    FOREIGN KEY (item_type) REFERENCES Dim_ItemType(item_type_id),

    -- Business Rule: Fact record must belong to exactly one structural item category
    CHECK (
        (
            CASE WHEN skin_id IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN sticker_id IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN container_id IS NOT NULL THEN 1 ELSE 0 END
        ) = 1
    )
);