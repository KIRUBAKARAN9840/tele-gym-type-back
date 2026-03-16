-- Complete payment system tables creation script
-- Execute this single file to create all tables in the payments schema

-- Create payments schema
CREATE SCHEMA IF NOT EXISTS payments CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE payments;

-- ===========================================
-- STEP 1: BASE TABLES
-- ===========================================

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

-- ===========================================
-- STEP 2: ENTITLEMENT TABLES
-- ===========================================

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

-- ===========================================
-- STEP 3: REFUND & DISPUTE TABLES
-- ===========================================

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

-- ===========================================
-- STEP 4: PAYOUT & COMMISSION TABLES
-- ===========================================

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

-- 11. Payout Batches (Batch payouts to gyms)
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

-- 12. Payout Lines (Individual payouts per entitlement)
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
    FOREIGN KEY (batch_id) REFERENCES payout_batches(id) ON DELETE SET NULL,
    
    INDEX idx_payout_lines_entitlement_id (entitlement_id),
    INDEX idx_payout_lines_gym_scheduled (gym_id, scheduled_for),
    INDEX idx_payout_lines_status (status),
    INDEX idx_payout_lines_batch_id (batch_id)
);

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

-- ===========================================
-- STEP 5: SETTLEMENT TABLES
-- ===========================================

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

-- ===========================================
-- STEP 6: SUPPORT TABLES
-- ===========================================

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

-- ===========================================
-- VERIFICATION QUERIES
-- ===========================================

-- Show all created tables
SHOW TABLES;

-- Show table structures for key tables
DESCRIBE orders;
DESCRIBE payments;
DESCRIBE entitlements;
DESCRIBE payout_lines;