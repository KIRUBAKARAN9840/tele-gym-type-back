-- Create supporting tables (fees, adjustments, idempotency)
USE payments;

-- 17. Fees Actuals (Actual fees charged by providers)
CREATE TABLE fees_actuals (
    id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100),
    payment_id VARCHAR(100),
    gateway_fee_minor BIGINT NOT NULL DEFAULT 0,
    payout_fee_minor BIGINT NOT NULL DEFAULT 0,
    tax_on_fees_minor BIGINT NOT NULL DEFAULT 0,
    notes TEXT,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL,
    FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE SET NULL,
    
    INDEX idx_fees_actuals_order_id (order_id),
    INDEX idx_fees_actuals_payment_id (payment_id),
    INDEX idx_fees_actuals_recorded_at (recorded_at)
);

-- 18. Adjustments (Manual accounting corrections)
CREATE TABLE adjustments (
    id VARCHAR(100) PRIMARY KEY,
    scope VARCHAR(20) NOT NULL, -- order|payment|payout_batch|general
    scope_id VARCHAR(100),
    amount_minor BIGINT NOT NULL, -- +/- values
    reason TEXT NOT NULL,
    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_adjustments_scope (scope, scope_id),
    INDEX idx_adjustments_created_by (created_by),
    INDEX idx_adjustments_created_at (created_at)
);

-- 19. Idempotency Keys (Request deduplication)
CREATE TABLE idempotency_keys (
    `key` VARCHAR(100) PRIMARY KEY,
    request_hash VARCHAR(64), -- SHA256 hash
    response_status INT,
    response_body LONGBLOB,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_idempotency_keys_expires_at (expires_at),
    INDEX idx_idempotency_keys_request_hash (request_hash)
);