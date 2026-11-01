CREATE TABLE IF NOT EXISTS outbox_events (
    event_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_id BIGINT UNSIGNED NOT NULL,
    order_version BIGINT UNSIGNED NOT NULL,
    event_type TINYINT UNSIGNED NOT NULL,
    payload JSON NOT NULL,
    publish_state TINYINT UNSIGNED default 0 NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP DEFAULT NULL,
    PRIMARY KEY (event_id),
    UNIQUE KEY uk_outbox_events_order (order_id, order_version),
    INDEX publish_state_index (publish_state, event_id)
) ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_ai_ci;