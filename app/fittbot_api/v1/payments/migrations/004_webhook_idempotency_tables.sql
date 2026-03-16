-- Migration: Add webhook idempotency tables to payments schema
-- Version: 004
-- Description: Create tables for webhook processing idempotency and recovery tracking

USE payments;

-- Execute the webhook idempotency table creation
SOURCE sql/08_create_webhook_idempotency_tables.sql;

-- Verify tables were created
SELECT 
    table_name,
    table_comment
FROM information_schema.tables 
WHERE table_schema = 'payments' 
AND table_name LIKE '%webhook%'
ORDER BY table_name;

-- Insert migration record (if you have a migrations tracking table)
-- INSERT INTO schema_migrations (version, description, executed_at) 
-- VALUES ('004', 'Create webhook idempotency and recovery tables', NOW())
-- ON DUPLICATE KEY UPDATE executed_at = NOW();