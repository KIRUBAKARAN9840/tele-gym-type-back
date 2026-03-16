-- Migration: Create Payment System Tables
-- Version: 001
-- Description: Initial payment system tables for production deployment

-- Create catalog_products table
CREATE TABLE IF NOT EXISTS catalog_products (
    sku VARCHAR(100) PRIMARY KEY,
    item_type VARCHAR(50) NOT NULL,
    title VARCHAR(200) NOT NULL,
    base_amount_minor BIGINT NOT NULL DEFAULT 0,
    description TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_catalog_products_item_type (item_type),
    INDEX idx_catalog_products_active (active)
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    provider VARCHAR(50) NOT NULL,
    provider_order_id VARCHAR(100),
    gross_amount_minor BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_orders_customer_created (customer_id, created_at),
    INDEX idx_orders_provider_status (provider, status),
    INDEX idx_orders_provider_order_id (provider_order_id)
);

-- Create order_items table
CREATE TABLE IF NOT EXISTS order_items (
    id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    item_type VARCHAR(50) NOT NULL,
    sku VARCHAR(100),
    gym_id VARCHAR(100),
    trainer_id VARCHAR(100),
    title VARCHAR(200),
    unit_price_minor BIGINT NOT NULL,
    qty INTEGER NOT NULL DEFAULT 1,
    metadata JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (sku) REFERENCES catalog_products(sku) ON DELETE SET NULL,
    
    INDEX idx_order_items_order_id (order_id),
    INDEX idx_order_items_sku (sku),
    INDEX idx_order_items_gym_trainer (gym_id, trainer_id)
);

-- Create entitlements table
CREATE TABLE IF NOT EXISTS entitlements (
    id VARCHAR(100) PRIMARY KEY,
    order_item_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    gym_id VARCHAR(100),
    trainer_id VARCHAR(100),
    entitlement_type VARCHAR(50) NOT NULL,
    scheduled_for DATE,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    active_from DATETIME,
    active_until DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (order_item_id) REFERENCES order_items(id) ON DELETE CASCADE,
    
    INDEX idx_entitlements_customer_status (customer_id, status),
    INDEX idx_entitlements_gym_scheduled (gym_id, scheduled_for),
    INDEX idx_entitlements_trainer_scheduled (trainer_id, scheduled_for),
    INDEX idx_entitlements_type_status (entitlement_type, status)
);

-- Create checkins table
CREATE TABLE IF NOT EXISTS checkins (
    id VARCHAR(100) PRIMARY KEY,
    entitlement_id VARCHAR(100) NOT NULL UNIQUE,
    gym_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    scanned_at DATETIME NOT NULL,
    in_time DATETIME,
    out_time DATETIME,
    status VARCHAR(20) NOT NULL DEFAULT 'ok',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (entitlement_id) REFERENCES entitlements(id) ON DELETE RESTRICT,
    
    INDEX idx_checkins_gym_scanned (gym_id, scanned_at),
    INDEX idx_checkins_customer_scanned (customer_id, scanned_at),
    INDEX idx_checkins_entitlement (entitlement_id)
);

-- Create payments table
CREATE TABLE IF NOT EXISTS payments (
    id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    amount_minor BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    provider VARCHAR(50) NOT NULL,
    provider_payment_id VARCHAR(100),
    status VARCHAR(20) NOT NULL,
    authorized_at DATETIME,
    captured_at DATETIME,
    failed_at DATETIME,
    payment_metadata JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    
    INDEX idx_payments_order_id (order_id),
    INDEX idx_payments_customer_status (customer_id, status),
    INDEX idx_payments_provider_payment_id (provider_payment_id),
    INDEX idx_payments_provider_status (provider, status),
    INDEX idx_payments_captured_at (captured_at)
);

-- Create settlements table
CREATE TABLE IF NOT EXISTS settlements (
    id VARCHAR(100) PRIMARY KEY,
    provider VARCHAR(50) NOT NULL,
    settlement_date DATE NOT NULL,
    provider_ref VARCHAR(100),
    gross_captured_minor BIGINT NOT NULL,
    mdr_amount_minor BIGINT NOT NULL,
    tax_on_mdr_minor BIGINT NOT NULL,
    net_settled_minor BIGINT NOT NULL,
    payload_json JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_settlements_provider_date (provider, settlement_date),
    INDEX idx_settlements_provider_ref (provider_ref)
);

-- Create settlement_items table
CREATE TABLE IF NOT EXISTS settlement_items (
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

-- Create fees_actuals table
CREATE TABLE IF NOT EXISTS fees_actuals (
    id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100),
    payment_id VARCHAR(100),
    gateway_fee_minor BIGINT NOT NULL DEFAULT 0,
    payout_fee_minor BIGINT NOT NULL DEFAULT 0,
    tax_on_fees_minor BIGINT NOT NULL DEFAULT 0,
    notes TEXT,
    recorded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL,
    FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE SET NULL,
    
    INDEX idx_fees_actuals_order_id (order_id),
    INDEX idx_fees_actuals_payment_id (payment_id),
    INDEX idx_fees_actuals_recorded_at (recorded_at)
);

-- Create commission_schedules table
CREATE TABLE IF NOT EXISTS commission_schedules (
    id VARCHAR(100) PRIMARY KEY,
    scope VARCHAR(20) NOT NULL,
    scope_id VARCHAR(100),
    commission_pct DECIMAL(5,2) DEFAULT 0,
    commission_fixed_minor BIGINT DEFAULT 0,
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_commission_schedules_scope (scope, scope_id),
    INDEX idx_commission_schedules_effective (effective_from, effective_to),
    INDEX idx_commission_schedules_active (effective_from, effective_to, scope)
);

-- Create payout_batches table
CREATE TABLE IF NOT EXISTS payout_batches (
    id VARCHAR(100) PRIMARY KEY,
    batch_date DATE NOT NULL,
    gym_id VARCHAR(100) NOT NULL,
    total_net_amount_minor BIGINT NOT NULL,
    payout_mode VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL,
    provider_ref VARCHAR(100),
    fee_actual_minor BIGINT NOT NULL DEFAULT 0,
    tax_on_fee_minor BIGINT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_payout_batches_gym_date (gym_id, batch_date),
    INDEX idx_payout_batches_status (status),
    INDEX idx_payout_batches_provider_ref (provider_ref)
);

-- Create payout_events table
CREATE TABLE IF NOT EXISTS payout_events (
    id VARCHAR(100) PRIMARY KEY,
    payout_batch_id VARCHAR(100) NOT NULL,
    provider VARCHAR(50) NOT NULL DEFAULT 'razorpayx',
    event_type VARCHAR(50) NOT NULL,
    provider_ref VARCHAR(100),
    event_time DATETIME NOT NULL,
    
    FOREIGN KEY (payout_batch_id) REFERENCES payout_batches(id) ON DELETE CASCADE,
    
    INDEX idx_payout_events_batch_id (payout_batch_id),
    INDEX idx_payout_events_event_time (event_time),
    INDEX idx_payout_events_provider_ref (provider_ref)
);

-- Create payout_lines table
CREATE TABLE IF NOT EXISTS payout_lines (
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
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (entitlement_id) REFERENCES entitlements(id) ON DELETE RESTRICT,
    FOREIGN KEY (batch_id) REFERENCES payout_batches(id) ON DELETE SET NULL,
    
    INDEX idx_payout_lines_entitlement_id (entitlement_id),
    INDEX idx_payout_lines_gym_scheduled (gym_id, scheduled_for),
    INDEX idx_payout_lines_status (status),
    INDEX idx_payout_lines_batch_id (batch_id)
);

-- Create beneficiaries table
CREATE TABLE IF NOT EXISTS beneficiaries (
    id VARCHAR(100) PRIMARY KEY,
    gym_id VARCHAR(100) NOT NULL,
    contact_id VARCHAR(100) NOT NULL,
    fund_account_id VARCHAR(100) NOT NULL,
    account_type VARCHAR(20) NOT NULL,
    masked_account VARCHAR(50),
    ifsc VARCHAR(20),
    upi VARCHAR(100),
    kyc_status VARCHAR(20),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_beneficiaries_gym_id (gym_id),
    INDEX idx_beneficiaries_contact_id (contact_id),
    INDEX idx_beneficiaries_fund_account_id (fund_account_id)
);

-- Create subscriptions table
CREATE TABLE IF NOT EXISTS subscriptions (
    id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    provider VARCHAR(50) NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    rc_original_txn_id VARCHAR(100),
    latest_txn_id VARCHAR(100),
    active_from DATETIME,
    active_until DATETIME,
    trial_start DATETIME,
    trial_end DATETIME,
    auto_renew BOOLEAN,
    cancel_reason TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_subscriptions_customer_status (customer_id, status),
    INDEX idx_subscriptions_provider_product (provider, product_id),
    INDEX idx_subscriptions_active_period (active_from, active_until),
    INDEX idx_subscriptions_rc_txn (rc_original_txn_id)
);

-- Create refunds table
CREATE TABLE IF NOT EXISTS refunds (
    id VARCHAR(100) PRIMARY KEY,
    payment_id VARCHAR(100) NOT NULL,
    entitlement_id VARCHAR(100),
    amount_minor BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'INR',
    provider VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    provider_ref VARCHAR(100),
    reason TEXT,
    processed_at DATETIME,
    refund_metadata JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE CASCADE,
    FOREIGN KEY (entitlement_id) REFERENCES entitlements(id) ON DELETE SET NULL,
    
    INDEX idx_refunds_payment_id (payment_id),
    INDEX idx_refunds_entitlement_id (entitlement_id),
    INDEX idx_refunds_status (status),
    INDEX idx_refunds_provider (provider),
    INDEX idx_refunds_processed_at (processed_at)
);

-- Create disputes table
CREATE TABLE IF NOT EXISTS disputes (
    id VARCHAR(100) PRIMARY KEY,
    payment_id VARCHAR(100) NOT NULL,
    provider VARCHAR(50) NOT NULL,
    amount_minor BIGINT NOT NULL,
    reason TEXT,
    status VARCHAR(20) NOT NULL,
    opened_at DATETIME NOT NULL,
    closed_at DATETIME,
    payload_json JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (payment_id) REFERENCES payments(id) ON DELETE CASCADE,
    
    INDEX idx_disputes_payment_id (payment_id),
    INDEX idx_disputes_status (status),
    INDEX idx_disputes_provider (provider),
    INDEX idx_disputes_opened_at (opened_at),
    INDEX idx_disputes_closed_at (closed_at)
);

-- Create adjustments table
CREATE TABLE IF NOT EXISTS adjustments (
    id VARCHAR(100) PRIMARY KEY,
    scope VARCHAR(20) NOT NULL,
    scope_id VARCHAR(100),
    amount_minor BIGINT NOT NULL,
    reason TEXT NOT NULL,
    created_by VARCHAR(100) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_adjustments_scope (scope, scope_id),
    INDEX idx_adjustments_created_by (created_by),
    INDEX idx_adjustments_created_at (created_at)
);

-- Create webhook_events table
CREATE TABLE IF NOT EXISTS webhook_events (
    id VARCHAR(100) PRIMARY KEY,
    provider VARCHAR(50) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    external_event_id VARCHAR(100),
    signature VARCHAR(500),
    verified BOOLEAN NOT NULL DEFAULT FALSE,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    payload_json JSON,
    error_message TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_webhook_events_provider (provider),
    INDEX idx_webhook_events_event_type (event_type),
    INDEX idx_webhook_events_external_id (external_event_id),
    INDEX idx_webhook_events_processed (processed),
    INDEX idx_webhook_events_verified (verified),
    INDEX idx_webhook_events_created_at (created_at)
);

-- Create idempotency_keys table
CREATE TABLE IF NOT EXISTS idempotency_keys (
    `key` VARCHAR(100) PRIMARY KEY,
    request_hash VARCHAR(64),
    response_status INTEGER,
    response_body LONGBLOB,
    expires_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_idempotency_keys_expires_at (expires_at),
    INDEX idx_idempotency_keys_request_hash (request_hash)
);