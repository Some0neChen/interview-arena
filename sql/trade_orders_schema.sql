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
    broker_order_id BIGINT UNSIGNED DEFAULT NULL,
    broker_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
    broker_state TINYINT UNSIGNED NOT NULL DEFAULT 0,
    filled_quantity BIGINT UNSIGNED NOT NULL DEFAULT 0,
    fill_price_cent BIGINT UNSIGNED NOT NULL DEFAULT 0,
    reject_reason VARCHAR(255) NOT NULL DEFAULT '',
    retry_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMP(3) DEFAULT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id),
    UNIQUE KEY uk_orders_account_client (account_id, client_order_id),
    KEY idx_order_state (state)
) ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_bin;