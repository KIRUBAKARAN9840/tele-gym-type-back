# Payment System Database Setup

## Quick Setup (Recommended)

Execute the complete setup with a single file:

```sql
source D:\AWS and Local\app\fittbot_api\v1\payments\sql\08_create_all_tables.sql
```

Or using mysql command:

```bash
mysql -u root -p < "D:\AWS and Local\app\fittbot_api\v1\payments\sql\08_create_all_tables.sql"
```

## Step-by-Step Setup

If you prefer to execute step by step:

1. **Create Schema**: `01_create_schema.sql`
2. **Base Tables**: `02_create_base_tables.sql`
3. **Entitlement Tables**: `03_create_entitlement_tables.sql`
4. **Refund/Dispute Tables**: `04_create_refund_dispute_tables.sql`
5. **Payout Tables**: `05_create_payout_tables.sql`
6. **Settlement Tables**: `06_create_settlement_tables.sql`
7. **Support Tables**: `07_create_support_tables.sql`

## Database Structure

### Core Flow Tables (19 total)

1. **catalog_products** - Product catalog
2. **orders** - Main orders
3. **order_items** - Order line items
4. **payments** - Payment transactions
5. **entitlements** - Customer access rights
6. **checkins** - Gym visit tracking
7. **subscriptions** - App subscriptions
8. **refunds** - Refund tracking
9. **disputes** - Payment disputes
10. **commission_schedules** - Commission rates
11. **payout_lines** - Individual payouts
12. **payout_batches** - Batch payouts
13. **payout_events** - Payout events
14. **beneficiaries** - Bank accounts
15. **settlements** - Provider settlements
16. **settlement_items** - Settlement details
17. **fees_actuals** - Actual fees
18. **adjustments** - Manual corrections
19. **idempotency_keys** - Deduplication

### Key Features

- ✅ **Foreign key constraints** for data integrity
- ✅ **Comprehensive indexing** for performance
- ✅ **UTC timestamps** with timezone support
- ✅ **JSON columns** for metadata
- ✅ **Money in minor units** (paise) for precision
- ✅ **Multi-provider support** (Razorpay, RevenueCat, Google Play)
- ✅ **Audit trail** with created_at/updated_at
- ✅ **Flexible commission model**

### Verification

After running the setup, verify with:

```sql
USE payments;
SHOW TABLES;
SELECT COUNT(*) as table_count FROM information_schema.tables 
WHERE table_schema = 'payments';
```

Should show 19 tables in the payments schema.