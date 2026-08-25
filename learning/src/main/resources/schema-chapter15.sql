DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
                        id BIGSERIAL PRIMARY KEY,
                        order_id VARCHAR(50) NOT NULL UNIQUE,
                        status VARCHAR(20) NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT now()
);
