-- Migration: Add referral_code column to clients table
-- Description: Adds unique referral code field for user referral system
-- Format: FIT + 7 alphanumeric characters (supports 78+ billion unique codes)

-- Step 1: Add the column (nullable initially)
ALTER TABLE clients
ADD COLUMN referral_code VARCHAR(10) NULL;

-- Step 2: Add unique index for performance and constraint
CREATE UNIQUE INDEX idx_clients_referral_code ON clients(referral_code);

-- Step 3: Add comment for documentation
ALTER TABLE clients
MODIFY COLUMN referral_code VARCHAR(10) NULL
COMMENT 'Unique referral code format: FIT + 7 alphanumeric chars (e.g., FITA1B2C3D)';

-- Note: After migration, update application code to:
-- 1. Generate referral codes for new users during registration
-- 2. Backfill existing users with referral codes using script below

-- Optional: Backfill existing users (run separately after deployment)
-- This will be handled by the Python script
