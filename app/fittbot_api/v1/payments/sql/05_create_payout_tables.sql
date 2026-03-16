-- Create payout and commission tables
USE payments;

-- 10. Commission Schedules (Dynamic commission rates)
CREATE TABLE commission_schedules (
    id VARCHAR(100) PRIMARY KEY,
    scope VARCHAR(20) NOT NULL, -- global|gym|product
    scope_id VARCHAR(100),      -- gym_id or sku
    commission_pct DECIMAL(5,2) DEFAULT 0,
    commission_fixed_minor BIGINT DEFAULT 0,
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_commission_schedules_scope (scope, scope_id),
    INDEX idx_commission_schedules_effective (effective_from, effective_to),
    INDEX idx_commission_schedules_active (effective_from, effective_to, scope)
);

-- 11. Payout Lines (Individual payouts per entitlement)
CREATE TABLE payout_lines (
    id VARCHAR(100) PRIMARY KEY,
    entitlement_id VARCHAR(100) NOT NULL UNIQUE,
    gym_id VARCHAR(100) NOT NULL,
    gross_amount_minor BIGINT NOT NULL,
    commission_amount_minor BIGINT NOT NULL,
    net_amount_minor BIGINT NOT NULL,
    applied_commission_pct DECIMAL(5,2) DEFAULT 0,
    applied_commission_fixed_minor BIGINT DEFAULT 0,
    payout_fee_allocated_minor BIGINT DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    scheduled_for DATE NOT NULL,
    batch_id VARCHAR(100),
    provider_ref VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (entitlement_id) REFERENCES entitlements(id) ON DELETE RESTRICT,
    
    INDEX idx_payout_lines_entitlement_id (entitlement_id),
    INDEX idx_payout_lines_gym_scheduled (gym_id, scheduled_for),
    INDEX idx_payout_lines_status (status),
    INDEX idx_payout_lines_batch_id (batch_id)
);

-- 12. Payout Batches (Batch payouts to gyms)
CREATE TABLE payout_batches (
    id VARCHAR(100) PRIMARY KEY,
    batch_date DATE NOT NULL,
    gym_id VARCHAR(100) NOT NULL,
    total_net_amount_minor BIGINT NOT NULL,
    payout_mode VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL,
    provider_ref VARCHAR(100),
    fee_actual_minor BIGINT NOT NULL DEFAULT 0,
    tax_on_fee_minor BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_payout_batches_gym_date (gym_id, batch_date),
    INDEX idx_payout_batches_status (status),
    INDEX idx_payout_batches_provider_ref (provider_ref)
);

-- Add foreign key for batch_id in payout_lines
ALTER TABLE payout_lines 
ADD FOREIGN KEY (batch_id) REFERENCES payout_batches(id) ON DELETE SET NULL;

-- 13. Payout Events (Payout processing events)
CREATE TABLE payout_events (
    id VARCHAR(100) PRIMARY KEY,
    payout_batch_id VARCHAR(100) NOT NULL,
    provider VARCHAR(50) NOT NULL DEFAULT 'razorpayx',
    event_type VARCHAR(50) NOT NULL, -- created|processed|failed|utr
    provider_ref VARCHAR(100),
    event_time TIMESTAMP NOT NULL,
    
    FOREIGN KEY (payout_batch_id) REFERENCES payout_batches(id) ON DELETE CASCADE,
    
    INDEX idx_payout_events_batch_id (payout_batch_id),
    INDEX idx_payout_events_event_time (event_time),
    INDEX idx_payout_events_provider_ref (provider_ref)
);

-- 14. Beneficiaries (Bank account details for payouts)
CREATE TABLE beneficiaries (
    id VARCHAR(100) PRIMARY KEY,
    gym_id VARCHAR(100) NOT NULL,
    contact_id VARCHAR(100) NOT NULL,
    fund_account_id VARCHAR(100) NOT NULL,
    account_type VARCHAR(20) NOT NULL, -- bank|upi
    masked_account VARCHAR(50),
    ifsc VARCHAR(20),
    upi VARCHAR(100),
    kyc_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_beneficiaries_gym_id (gym_id),
    INDEX idx_beneficiaries_contact_id (contact_id),
    INDEX idx_beneficiaries_fund_account_id (fund_account_id)
);