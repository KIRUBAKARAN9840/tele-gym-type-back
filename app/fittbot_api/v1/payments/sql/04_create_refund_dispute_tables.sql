-- Create refund and dispute tables
USE payments;

-- 8. Refunds (Payment refund tracking)
CREATE TABLE refunds (
    id VARCHAR(100) PRIMARY KEY,
    payment_id VARCHAR(100) NOT NULL,
    entitlement_id VARCHAR(100),
    amount_minor BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    provider VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    provider_ref VARCHAR(100),
    reason TEXT,
    processed_at TIMESTAMP NULL,
    refund_metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE CASCADE,
    FOREIGN KEY (entitlement_id) REFERENCES entitlements(id) ON DELETE SET NULL,
    
    INDEX idx_refunds_payment_id (payment_id),
    INDEX idx_refunds_entitlement_id (entitlement_id),
    INDEX idx_refunds_status (status),
    INDEX idx_refunds_provider (provider),
    INDEX idx_refunds_processed_at (processed_at)
);

-- 9. Disputes (Payment dispute management)
CREATE TABLE disputes (
    id VARCHAR(100) PRIMARY KEY,
    payment_id VARCHAR(100) NOT NULL,
    provider VARCHAR(50) NOT NULL,
    amount_minor BIGINT NOT NULL,
    reason TEXT,
    status VARCHAR(20) NOT NULL,
    opened_at TIMESTAMP NOT NULL,
    closed_at TIMESTAMP NULL,
    payload_json JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE CASCADE,
    
    INDEX idx_disputes_payment_id (payment_id),
    INDEX idx_disputes_status (status),
    INDEX idx_disputes_provider (provider),
    INDEX idx_disputes_opened_at (opened_at),
    INDEX idx_disputes_closed_at (closed_at)
);