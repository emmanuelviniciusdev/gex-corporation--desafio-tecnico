CREATE TABLE IF NOT EXISTS raw_payloads (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    correlation_id VARCHAR(36) NOT NULL,
    gateway VARCHAR(20) NOT NULL,
    received_at DATETIME(6) NOT NULL,
    headers JSON NOT NULL,
    original_body TEXT NOT NULL,
    decrypted_body TEXT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    INDEX idx_correlation_id (correlation_id),
    INDEX idx_gateway (gateway),
    INDEX idx_received_at (received_at)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS processed_webhooks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(255) NOT NULL,
    event VARCHAR(50) NOT NULL,
    correlation_id VARCHAR(36) NOT NULL,
    processed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    UNIQUE KEY uk_transaction_event (
        transaction_id,
        event
    ),

    INDEX idx_processed_webhooks_correlation_id (correlation_id)
    INDEX idx_processed_webhooks_transaction_id (transaction_id)
    INDEX idx_processed_webhooks_event (event)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS leads (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    email_raw VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL DEFAULT 'Customer',
    last_name VARCHAR(100) NOT NULL,
    phone VARCHAR(30) NULL,
    phone_raw VARCHAR(100) NULL,
    phone_valid TINYINT(1) NOT NULL DEFAULT 0,
    country CHAR(2) NULL,
    created_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),

    UNIQUE KEY uq_leads_email (email),

    INDEX idx_leads_country (country)
    INDEX idx_leads_email (email)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS orders (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    lead_id BIGINT UNSIGNED NOT NULL,
    raw_payload_id BIGINT NULL,
    gateway VARCHAR(20) NOT NULL,
    transaction_id VARCHAR(255) NOT NULL,
    transaction_time DATETIME(6) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    product_niche VARCHAR(100) NULL,
    quantity INT UNSIGNED NOT NULL,
    amount_usd DECIMAL(10,2) NOT NULL,
    payment_method VARCHAR(30) NOT NULL,
    payment_status VARCHAR(20) NOT NULL,
    created_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),

    UNIQUE KEY uq_orders_gateway_tx (
        gateway,
        transaction_id
    ),

    INDEX idx_orders_lead (lead_id),
    INDEX idx_orders_product (product_id),
    INDEX idx_orders_raw_payload (raw_payload_id),

    CONSTRAINT fk_orders_lead
        FOREIGN KEY (lead_id)
        REFERENCES leads(id),

    CONSTRAINT fk_orders_raw_payload
        FOREIGN KEY (raw_payload_id)
        REFERENCES raw_payloads(id)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS lead_events (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT UNSIGNED NOT NULL,
    transaction_id VARCHAR(255) NOT NULL,
    correlation_id VARCHAR(36) NOT NULL,
    event VARCHAR(50) NOT NULL,
    gateway_time DATETIME(6) NOT NULL,
    persisted_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),
    lag_seconds INT NOT NULL,

    UNIQUE KEY uq_order_event (
        order_id,
        event
    ),

    INDEX idx_transaction_event (
        transaction_id,
        event
    ),

    INDEX idx_correlation_id (correlation_id),

    CONSTRAINT fk_events_order
        FOREIGN KEY (order_id)
        REFERENCES orders(id)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS distribution_status (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT UNSIGNED NOT NULL,
    channel ENUM(
        'SMS',
        'EMAIL',
        'CALL_CENTER',
        'WHATSAPP'
    ) NOT NULL,
    status ENUM(
        'pending',
        'delivered',
        'failed'
    ) NOT NULL DEFAULT 'pending',
    created_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),
    delivered_at DATETIME(6) NULL,
    lag_db_channel_seconds INT NULL,

    UNIQUE KEY uq_dist_order_channel (
        order_id,
        channel
    ),

    INDEX idx_dist_status_channel (
        channel,
        status
    ),

    INDEX idx_dist_created (created_at),

    CONSTRAINT fk_dist_order
        FOREIGN KEY (order_id)
        REFERENCES orders(id)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS lead_dead_letter (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    correlation_id VARCHAR(36) NULL,
    origin VARCHAR(50) NOT NULL,
    raw_payload_id BIGINT NULL,
    payload LONGTEXT NOT NULL,
    error_message TEXT NOT NULL,
    created_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),

    INDEX idx_dlq_origin_created (
        origin,
        created_at
    ),

    INDEX idx_dlq_correlation (
        correlation_id
    ),

    INDEX idx_dlq_raw_payload (
        raw_payload_id
    ),

    CONSTRAINT fk_dlq_raw_payload
        FOREIGN KEY (raw_payload_id)
        REFERENCES raw_payloads(id)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
