CREATE TABLE IF NOT EXISTS broker_instruments (
    symbol VARCHAR(32) NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    simulated_price_cent BIGINT UNSIGNED NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (symbol),
    CONSTRAINT chk_simulated_price_cent
        CHECK (simulated_price_cent > 0),
    CONSTRAINT chk_enabled
        CHECK (enabled IN (0, 1))
) ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_bin;