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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS processed_webhooks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(255) NOT NULL,
    event VARCHAR(50) NOT NULL,
    correlation_id VARCHAR(36) NOT NULL,
    processed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uk_transaction_event (transaction_id, event),
    INDEX idx_correlation_id (correlation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
