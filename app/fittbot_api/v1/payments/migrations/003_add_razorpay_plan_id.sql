-- Migration: Add razorpay_plan_id column to catalog_products
-- This column will store the actual Razorpay Plan IDs for subscription creation

-- Add the new column
ALTER TABLE payments.catalog_products
ADD COLUMN razorpay_plan_id VARCHAR(100);

-- Add index for faster lookups
CREATE INDEX IF NOT EXISTS idx_catalog_products_razorpay_plan_id
ON payments.catalog_products(razorpay_plan_id);

-- Insert your actual Platinum Plan
INSERT INTO payments.catalog_products (
    sku,
    item_type,
    title,
    base_amount_minor,
    description,
    active,
    razorpay_plan_id,
    created_at,
    updated_at
) VALUES (
    'platinum_plan_yearly',
    'app_subscription',
    'Platinum Plan',
    190800,  -- ₹1,908.00 in paise
    'Twelve Months Plan of Fittbot. Includes the premium features like KyraAI, AI Food Detection, Smart Diet and Workout Tracker, Water Tracker, Live Gym, Gym Buddy, 2 Session Free Nutrition Consultation.',
    true,
    'plan_RGfiC75bgBctNV',  -- Your actual Plan ID from Razorpay Dashboard
    NOW(),
    NOW()
) ON CONFLICT (sku) DO UPDATE SET
    razorpay_plan_id = EXCLUDED.razorpay_plan_id,
    base_amount_minor = EXCLUDED.base_amount_minor,
    title = EXCLUDED.title,
    description = EXCLUDED.description,
    updated_at = NOW();

-- Verify the insertion
SELECT
    sku,
    title,
    base_amount_minor,
    razorpay_plan_id,
    active
FROM payments.catalog_products
WHERE sku = 'platinum_plan_yearly';