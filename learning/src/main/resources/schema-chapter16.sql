DROP TABLE IF EXISTS order_processing_log;
CREATE TABLE order_processing_log (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP NOT NULL default now()
)