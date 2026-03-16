-- Migration: Create webhook processing logs table for idempotency
-- Run this to create the table for tracking webhook processing

CREATE TABLE webhook_processing_logs (
    id VARCHAR(255) PRIMARY KEY,
    event_id VARCHAR(255) UNIQUE NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    customer_id VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP NULL,
    raw_event_data TEXT,
    result_summary TEXT,
    error_message TEXT,
    processing_duration_ms INT,
    retry_count INT DEFAULT 0,
    is_recovery_event BOOLEAN DEFAULT FALSE,
    
    INDEX idx_event_id (event_id),
    INDEX idx_event_type (event_type),
    INDEX idx_customer_id (customer_id),
    INDEX idx_status (status),
    INDEX idx_started_at (started_at),
    INDEX idx_completed_at (completed_at),
    INDEX idx_is_recovery (is_recovery_event)
);

-- Insert a comment to track when this was created
INSERT INTO schema_migrations (version, description, executed_at) 
VALUES ('003', 'Create webhook processing logs table for idempotency', NOW())
ON DUPLICATE KEY UPDATE executed_at = NOW();