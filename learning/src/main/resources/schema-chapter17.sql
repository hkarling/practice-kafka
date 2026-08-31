DROP TABLE IF EXISTS idempotent_order_log;
CREATE TABLE idempotent_order_log (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL UNIQUE,
    processed_at TIMESTAMP NOT NULL DEFAULT now()
);