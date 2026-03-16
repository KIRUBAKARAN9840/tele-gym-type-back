# Gym Membership Payment Integration

## Flow Overview

### 1. Frontend Flow
1. **Client provides**: `gym_id` and `plan_id`
2. **Backend lookup**: Scans `gym_plans` table for `amount` and `duration`
3. **Order creation**: Backend creates order with authoritative pricing
4. **Razorpay checkout**: Frontend launches Razorpay with order details
5. **Payment verification**: Backend verifies and activates membership
6. **Membership active**: From payment time + duration months

### 2. Backend Integration

#### API Endpoints
- `POST /payments/gym/checkout/create-order`
  - Input: `{gym_id: int, plan_id: int, start_on?: "YYYY-MM-DD"}`
  - Backend looks up `gym_plans` table for pricing
  - Returns Razorpay order details

- `POST /payments/gym/checkout/verify`
  - Input: `{razorpay_payment_id, razorpay_order_id, razorpay_signature}`
  - Verifies payment and activates membership
  - Returns membership details with dates

- `POST /payments/gym/webhook/razorpay`
  - Handles `payment.captured` webhooks
  - Idempotent - safe to retry
  - Same activation logic as verify endpoint

#### Database Tables Used
- `gym_plans` - Authoritative pricing and duration
- `orders` + `order_items` - Payment order tracking
- `payments` - Payment records
- `entitlements` - Membership records with start/end dates
- `payout_lines` - Gym revenue tracking (full amount)
- `gym_fees` - Legacy table mirroring

### 3. Membership Activation Logic

```javascript
// If start_on provided
membership_start = start_on

// Otherwise (default)
membership_start = payment_captured_time

// Duration from gym_plans table
membership_end = membership_start + duration_months
```

### 4. Frontend Usage

```jsx
import GymMembershipPayButton from './GymMembershipPayButton';

<GymMembershipPayButton
  gymId={101}
  planId={3}  // Backend will lookup gym_plans.amount & gym_plans.duration
  startOn={undefined}  // Optional - defaults to payment time
  prefill={{
    name: "John Doe",
    email: "john@example.com",
    contact: "9999999999"
  }}
  label="Buy Membership"
/>
```

### 5. JWT Authentication
- Requires valid JWT token with "client" or "owner" role
- Token must contain valid `sub` (user_id) claim
- Uses your existing auth system from `app.utils.security`

### 6. Error Handling
- Invalid gym_id/plan_id → 404 Plan not found
- Invalid JWT → 401 Unauthorized
- Payment failures → Retry with backoff
- Webhook signature verification
- Idempotent processing (safe retries)

### 7. Testing
Run the included test script:
```bash
python test_gym_plans.py
```

This verifies:
- ✅ `gym_plans` table structure
- ✅ Price/duration lookup queries
- ✅ Sample data validation

### 8. Webhook Configuration
Configure Razorpay webhook URL:
```
POST https://your-api.com/payments/gym/webhook/razorpay
```

Events to subscribe:
- `payment.captured` - For membership activation

### 9. Commission Logic
**Removed as requested** - PayoutLines created with:
- `commission_amount_minor = 0`
- `net_amount_minor = full_amount`
- Full payment amount goes to gym