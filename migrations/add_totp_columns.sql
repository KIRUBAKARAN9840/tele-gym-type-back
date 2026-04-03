-- TOTP (Google Authenticator) Support Migration for MySQL
-- Run this migration to add TOTP columns to admins and employees tables

-- Add TOTP columns to admins table (MySQL compatible)
ALTER TABLE fittbot_admins.admins
ADD COLUMN totp_secret VARCHAR(255) NULL,
ADD COLUMN totp_enabled BOOLEAN DEFAULT FALSE,
ADD COLUMN backup_codes TEXT NULL,
ADD COLUMN totp_verified_at TIMESTAMP NULL;

-- Add TOTP columns to employees table (MySQL compatible)
ALTER TABLE fittbot_admins.employees
ADD COLUMN totp_secret VARCHAR(255) NULL,
ADD COLUMN totp_enabled BOOLEAN DEFAULT FALSE,
ADD COLUMN backup_codes TEXT NULL,
ADD COLUMN totp_verified_at TIMESTAMP NULL;

-- Add index for TOTP queries
CREATE INDEX idx_admins_totp_enabled ON fittbot_admins.admins(totp_enabled);
CREATE INDEX idx_employees_totp_enabled ON fittbot_admins.employees(totp_enabled);
