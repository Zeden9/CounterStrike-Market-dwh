CREATE TABLE Fact_MarketPrice (
    fact_id SERIAL PRIMARY KEY,

    item_type VARCHAR(20),

    price FLOAT,
    volume INT,

    date_id DATE,

    skin_id INT,
    sticker_id INT,
    container_id INT,

    weapon_id INT,
    wear_range_id INT,
    team_id INT,
    match_id INT,
    price_range_id INT,

    FOREIGN KEY (date_id) REFERENCES Dim_Time(date_id),
    FOREIGN KEY (skin_id) REFERENCES Dim_Skin(skin_id),
    FOREIGN KEY (sticker_id) REFERENCES Dim_Sticker(sticker_id),
    FOREIGN KEY (container_id) REFERENCES Dim_Container(container_id),

    CHECK (
        (
            CASE WHEN skin_id IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN sticker_id IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN container_id IS NOT NULL THEN 1 ELSE 0 END
        ) = 1
    )
);

