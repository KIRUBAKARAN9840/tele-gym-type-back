-- Create settlement and reconciliation tables
USE payments;

-- 15. Settlements (Settlement batches from payment providers)
CREATE TABLE settlements (
    id VARCHAR(100) PRIMARY KEY,
    provider VARCHAR(50) NOT NULL,
    settlement_date DATE NOT NULL,
    provider_ref VARCHAR(100),
    gross_captured_minor BIGINT NOT NULL,
    mdr_amount_minor BIGINT NOT NULL,
    tax_on_mdr_minor BIGINT NOT NULL,
    net_settled_minor BIGINT NOT NULL,
    payload_json JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_settlements_provider_date (provider, settlement_date),
    INDEX idx_settlements_provider_ref (provider_ref)
);

-- 16. Settlement Items (Individual payments within settlements)
CREATE TABLE settlement_items (
    id VARCHAR(100) PRIMARY KEY,
    settlement_id VARCHAR(100) NOT NULL,
    provider_payment_id VARCHAR(100) NOT NULL UNIQUE,
    payment_id VARCHAR(100),
    gross_captured_minor BIGINT NOT NULL,
    mdr_amount_minor BIGINT NOT NULL,
    tax_on_mdr_minor BIGINT NOT NULL,
    net_settled_minor BIGINT NOT NULL,
    settled_on DATE NOT NULL,
    
    FOREIGN KEY (settlement_id) REFERENCES settlements(id) ON DELETE CASCADE,
    FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE SET NULL,
    
    INDEX idx_settlement_items_settlement_id (settlement_id),
    INDEX idx_settlement_items_provider_payment_id (provider_payment_id),
    INDEX idx_settlement_items_payment_id (payment_id),
    INDEX idx_settlement_items_settled_on (settled_on)
);