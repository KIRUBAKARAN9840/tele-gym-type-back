# FittBot Payments System

A production-ready, unified payment gateway system supporting multiple payment providers and payout mechanisms.

## Features

### Payment Processing
- **Razorpay Payment Gateway** - Subscriptions and alternative billing
- **RevenueCat/Google Play** - App subscription lifecycle management
- **Multi-currency support** - Default INR with extensibility
- **Idempotency protection** - Request deduplication with TTL

### Payout System
- **RazorpayX Integration** - Automated bulk payouts
- **Commission Management** - Hierarchical rate configuration (gym > product > global)
- **Real-time Payout Lines** - Generated on gym check-ins
- **Fee Allocation** - Pro-rata payout fee distribution

### Settlement & Reconciliation
- **Automated Settlement Import** - MDR and fee reconciliation
- **Payment Matching** - Link settlements to payments
- **Accurate Fee Tracking** - Gateway fees with tax breakdown

### Business Logic
- **Entitlement System** - Purchase-to-usage lifecycle
- **Check-in Processing** - QR code scanning with fraud detection
- **Refund Handling** - Automated payout reversals
- **Subscription Management** - RevenueCat webhook integration

## Architecture

```
├── models/          # SQLAlchemy ORM models
├── services/        # Business logic layer
├── routes/          # FastAPI route handlers
├── webhooks/        # Provider webhook handlers
├── schemas/         # Pydantic request/response models
├── config/          # Configuration and database setup
├── utils/           # Utilities (idempotency, signatures)
├── migrations/      # Database migration scripts
└── main.py         # Main payment router
```

## Quick Start

### 1. Environment Setup
```bash
# Copy environment template
cp .env.example .env

# Update with your credentials
nano .env
```

### 2. Database Setup
```bash
# Run migrations
mysql -u root -p your_db < migrations/001_create_payment_tables.sql
mysql -u root -p your_db < migrations/002_seed_initial_data.sql
```

### 3. Integration
The payment system is automatically integrated into the main FastAPI app via the `main.py` imports.

## API Endpoints

### Orders
```http
POST /payments/orders/                    # Create order
GET  /payments/orders/{order_id}          # Get order
GET  /payments/orders/customer/{customer_id} # Customer orders
POST /payments/orders/{order_id}/cancel   # Cancel order
```

### Payments
```http
POST /payments/payments/verify            # Verify payment
GET  /payments/payments/{payment_id}      # Get payment
GET  /payments/payments/customer/{customer_id} # Customer payments
```

### Check-ins
```http
POST /payments/checkins/scan              # Process check-in
GET  /payments/checkins/gym/{gym_id}      # Gym check-ins
GET  /payments/checkins/customer/{customer_id} # Customer check-ins
```

### Payouts
```http
POST /payments/payouts/run                # Run payout processing
GET  /payments/payouts/batch/{batch_id}   # Get payout batch
GET  /payments/payouts/gym/{gym_id}       # Gym payouts
```

### Webhooks
```http
POST /payments/webhooks/razorpayx         # RazorpayX webhooks
POST /payments/webhooks/revenuecat        # RevenueCat webhooks
```

### Admin
```http
GET /payments/admin/dashboard/summary     # Dashboard analytics
GET /payments/admin/analytics/gmv         # GMV analytics
GET /payments/admin/analytics/commissions # Commission analytics
```

## Data Models

### Core Entities
- **Order** - Customer purchase orders
- **OrderItem** - Individual items in orders
- **Entitlement** - Customer rights from purchases
- **Payment** - Payment transaction records
- **Checkin** - Gym visit check-ins

### Payout Entities
- **PayoutLine** - Individual gym earnings
- **PayoutBatch** - Grouped payouts
- **Beneficiary** - Gym bank accounts

### Configuration
- **CatalogProduct** - Product pricing catalog
- **CommissionSchedule** - Commission rate rules

## Business Flows

### 1. Purchase Flow
```
Order Creation → Payment Verification → Entitlement Generation
```

### 2. Gym Visit Flow
```
QR Code Scan → Entitlement Validation → Check-in → Payout Line Creation
```

### 3. Payout Flow
```
Daily Batch → Gym Grouping → RazorpayX API → Webhook Confirmation
```

### 4. Settlement Flow
```
Provider Settlement → Import Reconciliation → Fee Allocation → Payment Matching
```

## Configuration

### Commission Hierarchy
1. **Gym-specific** - `scope: "gym"`, `scope_id: "gym_123"`
2. **Product-specific** - `scope: "product"`, `scope_id: "SKU_ABC"`
3. **Global default** - `scope: "global"`, `scope_id: null`

### Environment Variables
All configuration uses `PAYMENT_` prefix. See `.env.example` for full list.

## Security

### Webhook Verification
- **HMAC-SHA256** signature validation
- **Request deduplication** via idempotency keys
- **Rate limiting** and authentication

### Data Protection
- **Encrypted secrets** in environment
- **Minimal PII storage** 
- **Audit trails** for all transactions

## Monitoring

### Key Metrics
- **GMV** (Gross Merchandise Value)
- **Commission earned**
- **Gateway fees**
- **Payout fees**
- **Net operational profit**
- **Cash flow** (in/out)

### Health Checks
```http
GET /payments/health                      # Basic health check
```

## Production Deployment

### Database
- Use connection pooling
- Configure proper indexes
- Set up automated backups
- Monitor query performance

### Security
- Use proper secrets management
- Enable HTTPS only
- Configure CORS appropriately
- Set up monitoring/alerting

### Scaling
- Database read replicas for analytics
- Redis for webhook deduplication
- Horizontal API scaling
- Queue-based payout processing

## Development

### Running Tests
```bash
pytest app/fittbot_api/v1/payments/tests/
```

### Code Style
- Black formatter
- isort import sorting
- Type hints required
- Docstrings for public APIs

## Support

For production issues:
1. Check logs in `/payments/admin/` endpoints
2. Verify webhook signatures
3. Check database constraints
4. Review commission schedules

## License

Proprietary - FittBot Team