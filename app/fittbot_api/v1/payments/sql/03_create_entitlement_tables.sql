-- Create entitlement and fulfillment tables
USE payments;

-- 5. Entitlements (Customer access rights)
CREATE TABLE entitlements (
    id VARCHAR(100) PRIMARY KEY,
    order_item_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    gym_id VARCHAR(100),
    trainer_id VARCHAR(100),
    entitlement_type VARCHAR(50) NOT NULL,
    scheduled_for DATE,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    active_from TIMESTAMP NULL,
    active_until TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (order_item_id) REFERENCES order_items(id) ON DELETE CASCADE,
    
    INDEX idx_entitlements_customer_id (customer_id),
    INDEX idx_entitlements_customer_status (customer_id, status),
    INDEX idx_entitlements_gym_scheduled (gym_id, scheduled_for),
    INDEX idx_entitlements_trainer_scheduled (trainer_id, scheduled_for),
    INDEX idx_entitlements_type_status (entitlement_type, status)
);

-- 6. Check-ins (Gym visit tracking)
CREATE TABLE checkins (
    id VARCHAR(100) PRIMARY KEY,
    entitlement_id VARCHAR(100) NOT NULL UNIQUE,
    gym_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    scanned_at TIMESTAMP NOT NULL,
    in_time TIMESTAMP NULL,
    out_time TIMESTAMP NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ok',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (entitlement_id) REFERENCES entitlements(id) ON DELETE RESTRICT,
    
    INDEX idx_checkins_gym_scanned (gym_id, scanned_at),
    INDEX idx_checkins_customer_scanned (customer_id, scanned_at),
    INDEX idx_checkins_entitlement (entitlement_id)
);

-- 7. Subscriptions (App subscription management)
CREATE TABLE subscriptions (
    id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    provider VARCHAR(50) NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    rc_original_txn_id VARCHAR(100),
    latest_txn_id VARCHAR(100),
    active_from TIMESTAMP NULL,
    active_until TIMESTAMP NULL,
    trial_start TIMESTAMP NULL,
    trial_end TIMESTAMP NULL,
    auto_renew BOOLEAN,
    cancel_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_subscriptions_customer_status (customer_id, status),
    INDEX idx_subscriptions_provider_product (provider, product_id),
    INDEX idx_subscriptions_active_period (active_from, active_until),
    INDEX idx_subscriptions_rc_txn (rc_original_txn_id)
);