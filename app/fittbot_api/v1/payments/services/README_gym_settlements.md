# Gym Membership Settlement System

## Overview

The gym membership settlement system provides comprehensive tracking and automated payouts for gym membership payments. It integrates with Razorpay's settlement and payout APIs to:

1. **Track Settlement Events** - Monitor when payments are settled by Razorpay
2. **Manage Gym Earnings** - Calculate net earnings per gym after fees
3. **Automate Payouts** - Batch and process payouts to gyms
4. **Handle Reconciliation** - Track and resolve discrepancies

## Architecture

### Models
- **Gym** - Gym entities with payout policies
- **SettlementEvent** - Settlement records from Razorpay
- **LedgerEarning** - Gym earnings ledger
- **PayoutFundAccount** - Gym bank account details
- **PayoutBatch** - Grouped payouts for processing
- **Payout** - Individual payouts to gyms
- **ReconciliationGap** - Discrepancy tracking

### Flow
1. **Payment Capture** → Creates pending ledger earning
2. **Settlement Webhook** → Updates earning with actual fees
3. **Payout Batching** → Groups earnings for efficient transfer
4. **Payout Processing** → Triggers Razorpay payouts
5. **Payout Webhooks** → Updates earning states

## Integration Points

### Existing System Integration
- **gym_membership.py** - Creates ledger earnings on payment capture
- **webhook handlers** - Can be extended for settlement webhooks
- **settings.py** - Added payout-specific configuration

### APIs Endpoints

#### Fund Account Management
- `POST /gym-settlements/fund-accounts/upsert` - Setup gym bank accounts

#### Analytics
- `GET /gym-settlements/gym/{gym_id}/earnings` - Earnings summary
- `GET /gym-settlements/gym/{gym_id}/payouts` - Payout history

#### Webhooks
- `POST /gym-settlements/webhooks/razorpay/payouts` - Payout status updates

#### Cron/Admin
- `POST /gym-settlements/cron/ingest-settlements` - Pull settlement data
- `POST /gym-settlements/cron/run-payout-batch` - Trigger payouts

## Configuration

Add these settings to your main app configuration:

```python
# Razorpay Payout Settings
razorpay_payout_account_number = "2323230084229691"  # Your payout account
razorpay_payouts_webhook_secret = "your_webhook_secret"
```

## Usage

### 1. Setup Gym Fund Account
```bash
curl -X POST "/gym-settlements/fund-accounts/upsert" \
  -H "Content-Type: application/json" \
  -d '{
    "gym_id": "123",
    "account_type": "bank",
    "details": {
      "name": "Gym Account",
      "account_number": "1234567890",
      "ifsc": "SBIN0001234"
    }
  }'
```

### 2. Schedule Settlement Ingestion
Run daily to pull settlement data:
```bash
curl -X POST "/gym-settlements/cron/ingest-settlements"
```

### 3. Process Payouts
Run to trigger gym payouts (typically daily/weekly):
```bash
curl -X POST "/gym-settlements/cron/run-payout-batch"
```

### 4. Monitor Gym Earnings
```bash
curl "/gym-settlements/gym/123/earnings"
```

## Settlement States

### Ledger Earning States
- `pending_settlement` - Payment captured, awaiting settlement
- `eligible_for_payout` - Settlement received, ready for payout
- `in_payout` - Included in payout batch
- `paid_out` - Successfully paid to gym

### Payout States
- `queued` - Created, awaiting processing
- `processing` - Being processed by Razorpay
- `processed` - Successfully completed
- `failed` - Failed to process
- `reversed` - Reversed/refunded

## Error Handling

The system includes comprehensive error handling:

- **Idempotency** - Safe to retry operations
- **Reconciliation** - Tracks and reports gaps
- **Webhook Verification** - Validates webhook signatures
- **Retry Logic** - Handles temporary failures

## Monitoring

Monitor these key metrics:

1. **Pending settlements** - Payments awaiting settlement
2. **Eligible payouts** - Earnings ready for payout
3. **Failed payouts** - Payouts requiring attention
4. **Reconciliation gaps** - Data discrepancies

## Security

- Webhook signature verification
- Sensitive data masking
- Secure fund account creation
- Idempotency key validation

## Testing

The system can be tested in development mode:

1. Use test Razorpay credentials
2. Webhook verification is relaxed for `secret = "replace_me"`
3. Manual settlement ingestion via API
4. Mock payout processing

## Maintenance

Regular maintenance tasks:

1. **Daily settlement sync** - Pull latest settlements
2. **Weekly payout processing** - Batch gym payouts
3. **Monthly reconciliation** - Review and resolve gaps
4. **Quarterly account verification** - Validate fund accounts