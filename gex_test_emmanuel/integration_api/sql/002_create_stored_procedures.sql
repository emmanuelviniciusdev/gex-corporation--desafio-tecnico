DELIMITER $$

DROP PROCEDURE IF EXISTS sp_insert_lead $$

CREATE PROCEDURE sp_insert_lead(
    -- lead fields
    IN p_email         VARCHAR(255),
    IN p_email_raw     VARCHAR(255),
    IN p_first_name    VARCHAR(100),
    IN p_last_name     VARCHAR(100),
    IN p_phone         VARCHAR(30),
    IN p_phone_raw     VARCHAR(100),
    IN p_phone_valid   TINYINT(1),
    IN p_country       CHAR(2),
    -- order fields
    IN p_raw_payload_id BIGINT,
    IN p_gateway        VARCHAR(20),
    IN p_transaction_id VARCHAR(255),
    IN p_transaction_time DATETIME(6),
    IN p_product_id     VARCHAR(20),
    IN p_product_name   VARCHAR(200),
    IN p_product_niche  VARCHAR(100),
    IN p_quantity       INT UNSIGNED,
    IN p_amount_usd     DECIMAL(10,2),
    IN p_payment_method VARCHAR(30),
    IN p_payment_status VARCHAR(20),
    -- lead_event fields
    IN p_correlation_id  VARCHAR(36),
    IN p_event           VARCHAR(50),
    IN p_gateway_time    DATETIME(6),
    IN p_persisted_at    DATETIME(6),
    IN p_lag_seconds     INT,
    -- output
    OUT p_lead_id    BIGINT UNSIGNED,
    OUT p_order_id   BIGINT UNSIGNED,
    OUT p_event_id   BIGINT UNSIGNED
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    -- Upsert lead: insert if not exists, otherwise get existing id
    INSERT INTO leads (
        email, email_raw, first_name, last_name,
        phone, phone_raw, phone_valid, country,
        created_at, updated_at
    )
    VALUES (
        p_email, p_email_raw, p_first_name, p_last_name,
        p_phone, p_phone_raw, p_phone_valid, p_country,
        NOW(6), NOW(6)
    )
    ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id);

    SET p_lead_id = LAST_INSERT_ID();

    -- Upsert order: insert if not exists, otherwise get existing id
    INSERT INTO orders (
        lead_id, raw_payload_id, gateway, transaction_id,
        transaction_time, product_id, product_name, product_niche,
        quantity, amount_usd, payment_method, payment_status,
        created_at
    )
    VALUES (
        p_lead_id, p_raw_payload_id, p_gateway, p_transaction_id,
        p_transaction_time, p_product_id, p_product_name, p_product_niche,
        p_quantity, p_amount_usd, p_payment_method, p_payment_status,
        NOW(6)
    )
    ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id);

    SET p_order_id = LAST_INSERT_ID();

    -- Insert lead event (unique per order + event)
    INSERT INTO lead_events (
        order_id, transaction_id, correlation_id,
        event, gateway_time, persisted_at, lag_seconds
    )
    VALUES (
        p_order_id, p_transaction_id, p_correlation_id,
        p_event, p_gateway_time, p_persisted_at, p_lag_seconds
    )
    ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id);

    SET p_event_id = LAST_INSERT_ID();

    COMMIT;
END $$

DELIMITER ;
