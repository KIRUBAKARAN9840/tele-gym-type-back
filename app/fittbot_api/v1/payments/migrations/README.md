# Payment System Database Migrations

This directory contains SQL migration files for the FittBot Payment System.

## Migration Files

- `001_create_payment_tables.sql` - Creates all payment system tables with proper indexes
- `002_seed_initial_data.sql` - Inserts default configuration and sample data

## Running Migrations

### Manual Execution
Execute migrations in order using your MySQL client:

```bash
mysql -u root -p latest < 001_create_payment_tables.sql
mysql -u root -p latest < 002_seed_initial_data.sql
```

### Python/Alembic Integration
If using Alembic for migrations:

```bash
# Generate migration
alembic revision --autogenerate -m "Create payment tables"

# Run migration
alembic upgrade head
```

## Database Schema Overview

### Core Tables
- `catalog_products` - Product catalog with pricing
- `orders` - Customer orders
- `order_items` - Items within orders
- `entitlements` - Customer entitlements from purchases
- `checkins` - Gym visit check-ins
- `payments` - Payment records
- `settlements` - Settlement reconciliation
- `settlement_items` - Individual payment settlements

### Payout System
- `payout_batches` - Grouped payouts to gyms
- `payout_lines` - Individual payout line items
- `payout_events` - Payout processing events
- `beneficiaries` - Gym bank account details

### Commission & Fees
- `commission_schedules` - Commission rate configuration
- `fees_actuals` - Actual fees charged by providers

### Subscriptions & Refunds
- `subscriptions` - App subscription management
- `refunds` - Payment refunds
- `disputes` - Payment disputes
- `adjustments` - Manual accounting adjustments

### System Tables
- `webhook_events` - Incoming webhook events
- `idempotency_keys` - Request deduplication

## Important Notes

1. All monetary amounts are stored in **minor units (paise)** for precision
2. Timestamps use UTC timezone
3. Foreign key constraints ensure data integrity
4. Comprehensive indexing for performance
5. JSON columns for flexible metadata storage

## Production Deployment Checklist

- [ ] Review connection pool settings
- [ ] Verify backup strategy
- [ ] Set up monitoring for long-running queries
- [ ] Configure proper MySQL timezone settings
- [ ] Test rollback procedures
- [ ] Validate index performance with production data volumes