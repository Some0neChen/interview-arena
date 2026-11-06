CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    account_id BIGINT UNSIGNED NOT NULL,
    client_order_id VARCHAR(64) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    limit_price_cent BIGINT UNSIGNED NOT NULL,
    quantity BIGINT UNSIGNED NOT NULL,
    reserved_cent BIGINT UNSIGNED NOT NULL,
    state TINYINT UNSIGNED NOT NULL default 0,
    version BIGINT UNSIGNED NOT NULL default 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id),
    UNIQUE KEY uk_orders_account_client (account_id, client_order_id),
    KEY idx_order_state (state)
) ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_bin;