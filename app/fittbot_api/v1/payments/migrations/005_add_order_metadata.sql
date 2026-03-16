-- Migration: Add order_metadata column to orders table
-- Date: 2025-10-30
-- Description: Adds JSON column to store additional metadata for orders

USE payments_production;

-- Add order_metadata column to orders table
ALTER TABLE orders
ADD COLUMN IF NOT EXISTS order_metadata JSON DEFAULT NULL
COMMENT 'Additional order metadata in JSON format'
AFTER status;

-- Verify the column was added
SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'payments_production'
  AND TABLE_NAME = 'orders'
  AND COLUMN_NAME = 'order_metadata';
