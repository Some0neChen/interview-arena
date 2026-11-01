CREATE TABLE IF NOT EXISTS broker_orders (
    broker_order_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    gateway_namespace VARCHAR(255) NOT NULL,
    gateway_order_id BIGINT UNSIGNED NOT NULL,
    account_id BIGINT UNSIGNED NOT NULL,
    symbol VARCHAR(255) NOT NULL,
    quantity BIGINT UNSIGNED NOT NULL,
    limit_price_cent BIGINT UNSIGNED NOT NULL,
    state TINYINT UNSIGNED NOT NULL,
    filled_quantity BIGINT UNSIGNED NOT NULL DEFAULT 0,
    fill_price_cent BIGINT UNSIGNED NOT NULL DEFAULT 0,
    broker_version TINYINT UNSIGNED NOT NULL DEFAULT 1,
    reject_reason VARCHAR(255) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (broker_order_id),
    UNIQUE KEY uk_orders_account_client (gateway_namespace, gateway_order_id)
) ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_bin;