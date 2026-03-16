-- Migration: Seed Initial Data for Payment System
-- Version: 002
-- Description: Insert default configuration and sample data

-- Insert default catalog products
INSERT IGNORE INTO catalog_products (sku, item_type, title, base_amount_minor, description, active) VALUES
('DAILY_PASS_BASIC', 'daily_pass', 'Basic Daily Pass', 5000, 'Single day gym access', TRUE),
('DAILY_PASS_PREMIUM', 'daily_pass', 'Premium Daily Pass', 8000, 'Premium gym access with amenities', TRUE),
('PT_SESSION_60MIN', 'pt_session', '60 Minute Personal Training', 150000, 'One hour personal training session', TRUE),
('PT_SESSION_30MIN', 'pt_session', '30 Minute Personal Training', 80000, 'Half hour personal training session', TRUE),
('APP_SUB_MONTHLY', 'app_subscription', 'Monthly App Subscription', 29900, 'Monthly premium app subscription', TRUE),
('APP_SUB_YEARLY', 'app_subscription', 'Yearly App Subscription', 299900, 'Annual premium app subscription', TRUE);

-- Insert default global commission schedule (5% commission)
INSERT IGNORE INTO commission_schedules (
    id, scope, scope_id, commission_pct, commission_fixed_minor, 
    effective_from, effective_to, created_at, updated_at
) VALUES (
    'cs_global_default', 'global', NULL, 5.00, 0,
    '2024-01-01', NULL, 
    NOW(), NOW()
);

-- Insert sample gym-specific commission (lower rate for preferred gym)
INSERT IGNORE INTO commission_schedules (
    id, scope, scope_id, commission_pct, commission_fixed_minor,
    effective_from, effective_to, created_at, updated_at
) VALUES (
    'cs_gym_premium_001', 'gym', 'gym_001', 3.00, 0,
    '2024-01-01', NULL,
    NOW(), NOW()
);

-- Insert sample product-specific commission (higher rate for PT sessions)
INSERT IGNORE INTO commission_schedules (
    id, scope, scope_id, commission_pct, commission_fixed_minor,
    effective_from, effective_to, created_at, updated_at
) VALUES (
    'cs_product_pt_premium', 'product', 'PT_SESSION_60MIN', 8.00, 0,
    '2024-01-01', NULL,
    NOW(), NOW()
);