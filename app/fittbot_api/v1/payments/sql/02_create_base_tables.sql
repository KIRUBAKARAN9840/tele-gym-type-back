-- Create base payment tables
USE payments;

-- 1. Catalog Products (Base reference table)
CREATE TABLE catalog_products (
    sku VARCHAR(100) PRIMARY KEY,
    item_type VARCHAR(50) NOT NULL,
    title VARCHAR(200) NOT NULL,
    base_amount_minor BIGINT NOT NULL DEFAULT 0,
    description TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_catalog_products_item_type (item_type),
    INDEX idx_catalog_products_active (active)
);

-- 2. Orders (Main order entity)
CREATE TABLE orders (
    id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    provider VARCHAR(50) NOT NULL,
    provider_order_id VARCHAR(100),
    gross_amount_minor BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_orders_customer_id (customer_id),
    INDEX idx_orders_customer_created (customer_id, created_at),
    INDEX idx_orders_provider_status (provider, status),
    INDEX idx_orders_provider_order_id (provider_order_id)
);

-- 3. Order Items (Individual items within orders)
CREATE TABLE order_items (
    id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    item_type VARCHAR(50) NOT NULL,
    sku VARCHAR(100),
    gym_id VARCHAR(100),
    trainer_id VARCHAR(100),
    title VARCHAR(200),
    unit_price_minor BIGINT NOT NULL,
    qty INT NOT NULL DEFAULT 1,
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (sku) REFERENCES catalog_products(sku) ON DELETE SET NULL,
    
    INDEX idx_order_items_order_id (order_id),
    INDEX idx_order_items_sku (sku),
    INDEX idx_order_items_gym_trainer (gym_id, trainer_id),
    INDEX idx_order_items_item_type (item_type)
);

-- 4. Payments (Payment records from providers)
CREATE TABLE payments (
    id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    amount_minor BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    provider VARCHAR(50) NOT NULL,
    provider_payment_id VARCHAR(100),
    status VARCHAR(20) NOT NULL,
    authorized_at TIMESTAMP NULL,
    captured_at TIMESTAMP NULL,
    failed_at TIMESTAMP NULL,
    payment_metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    
    INDEX idx_payments_order_id (order_id),
    INDEX idx_payments_customer_status (customer_id, status),
    INDEX idx_payments_provider_payment_id (provider_payment_id),
    INDEX idx_payments_provider_status (provider, status),
    INDEX idx_payments_captured_at (captured_at)
);