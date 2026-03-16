"""
Gym Membership Settlement & GymPayout System
Comprehensive settlement tracking and automated payouts for gym memberships
"""

import json
import logging
import secrets
import time
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Dict, Optional, List, Tuple

import requests
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy import JSON
from sqlalchemy.orm import Session, relationship

# Import from existing codebase
from ..config.database import get_db_session
from app.models.database import Base
from ..config.settings import get_payment_settings
from ..models.payments import Payment
from ..models.orders import Order, OrderItem
from ..models.enums import StatusEnt
from ..utils.webhook_verifier import verify_razorpay_signature
from ..utils.id_generator import generate_id
from ..utils.crypto import verify_webhook_sig

logger = logging.getLogger("payments.gym_settlements")
router = APIRouter(prefix="/gym-settlements", tags=["Gym Settlements & GymPayouts"])

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc


# ============================================================================
#                                 MODELS
# ============================================================================

class PaymentGym(Base):
    """
    Gym entity for payments - integrates with existing santy_fittbot.gyms or creates placeholder
    """
    __tablename__ = "gym_entities"
    __table_args__ = {"schema": "payments", "extend_existing": True}

    id = Column(String(64), primary_key=True)
    name = Column(String(200), nullable=False)
    status = Column(String(32), nullable=False, default="active")  # active/suspended
    payout_policy = Column(JSON, nullable=True)  # {min_threshold_minor, cadence, ...}
    external_gym_id = Column(Integer, nullable=True)  # Link to santy_fittbot.gyms.id
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    def __repr__(self) -> str:
        return f"<Gym(id={self.id}, name={self.name})>"


class GymPayoutFundAccount(Base):
    """
    Fund account details for gym payouts via Razorpay
    """
    __tablename__ = "payout_fund_accounts"
    __table_args__ = (
        UniqueConstraint("gym_id", "provider", "fund_account_id", name="uq_pfa_gym_provider_faid"),
        Index("ix_pfa_gym_active", "gym_id", "active"),
        {"schema": "payments", "extend_existing": True}
    )

    id = Column(String(40), primary_key=True, default=lambda: generate_id("pfa"))
    gym_id = Column(String(64), ForeignKey("payments.gym_entities.id"), nullable=False)
    provider = Column(String(32), nullable=False, default="razorpay")
    fund_account_id = Column(String(64), nullable=False)  # Razorpay fund account id
    account_type = Column(String(16), nullable=False)  # bank | vpa
    masked_details = Column(JSON, nullable=True)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    gym = relationship("PaymentGym")

    def __repr__(self) -> str:
        return f"<GymPayoutFundAccount(id={self.id}, gym_id={self.gym_id})>"


class SettlementEvent(Base):
    """
    Settlement events from Razorpay - tracks when payments are settled
    """
    __tablename__ = "settlement_events"
    __table_args__ = (
        UniqueConstraint("provider", "provider_settlement_id", name="uq_settlement_provider_id"),
        Index("ix_settlement_payment", "payment_id"),
        {"schema": "payments", "extend_existing": True}
    )

    id = Column(String(40), primary_key=True, default=lambda: generate_id("setl"))
    provider = Column(String(32), nullable=False, default="razorpay")
    provider_settlement_id = Column(String(64), nullable=False)  # settlement id/tx id from RZP
    payment_id = Column(String(64), nullable=False)  # rzp payment id
    amount_minor = Column(Integer, nullable=False)   # gross
    fees_minor = Column(Integer, nullable=False, default=0)
    tax_minor = Column(Integer, nullable=False, default=0)
    settled_at = Column(DateTime(timezone=True), nullable=False)
    raw = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

    def __repr__(self) -> str:
        return f"<SettlementEvent(id={self.id}, payment_id={self.payment_id})>"

    @property
    def net_amount_minor(self) -> int:
        """Net amount after fees and tax"""
        return self.amount_minor - self.fees_minor - self.tax_minor


class LedgerEarning(Base):
    """
    Gym earnings ledger - tracks earnings per gym from settlements
    """
    __tablename__ = "ledger_earnings"
    __table_args__ = (
        UniqueConstraint("payment_id", name="uq_ledger_payment"),
        Index("ix_ledger_gym_state", "gym_id", "state"),
        CheckConstraint("amount_net_minor >= 0", name="ck_ledger_net_nonneg"),
        {"schema": "payments", "extend_existing": True}
    )

    id = Column(String(40), primary_key=True, default=lambda: generate_id("leg"))
    gym_id = Column(String(64), ForeignKey("payments.gym_entities.id"), nullable=False)
    payment_id = Column(String(64), nullable=False)  # rzp payment id
    order_id = Column(String(40), ForeignKey("payments.orders.id"), nullable=True)  # internal order
    amount_gross_minor = Column(Integer, nullable=False)
    fees_minor = Column(Integer, nullable=False, default=0)
    tax_minor = Column(Integer, nullable=False, default=0)
    amount_net_minor = Column(Integer, nullable=False)
    state = Column(String(32), nullable=False, default="pending_settlement")
    settlement_event_id = Column(String(40), ForeignKey("payments.settlement_events.id"), nullable=True)
    payout_id = Column(String(40), ForeignKey("payments.gym_payouts.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    gym = relationship("PaymentGym")
    settlement_event = relationship("SettlementEvent")
    order = relationship("Order", foreign_keys=[order_id])

    def __repr__(self) -> str:
        return f"<LedgerEarning(id={self.id}, gym_id={self.gym_id}, state={self.state})>"

    @property
    def net_amount_rupees(self) -> float:
        """Net amount in rupees"""
        return self.amount_net_minor / 100.0


class GymPayoutBatch(Base):
    """
    Gym GymPayout batch - groups multiple gym payouts for processing
    """
    __tablename__ = "gym_payout_batches"
    __table_args__ = {"schema": "payments", "extend_existing": True}

    id = Column(String(40), primary_key=True, default=lambda: generate_id("pob"))
    batch_ref = Column(String(120), nullable=False, unique=True)
    scheduled_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    status = Column(String(24), nullable=False, default="created")  # created|processing|completed|failed|partial
    created_by = Column(String(64), nullable=False, default="system_cron")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    def __repr__(self) -> str:
        return f"<GymPayoutBatch(id={self.id}, status={self.status})>"


class GymPayout(Base):
    """
    Individual payout to a gym
    """
    __tablename__ = "gym_payouts"
    __table_args__ = (
        UniqueConstraint("provider", "provider_payout_id", name="uq_gym_payout_provider_ppid"),
        Index("ix_gym_payout_status", "status"),
        CheckConstraint("amount_minor > 0", name="ck_gym_payout_amount_pos"),
        {"schema": "payments", "extend_existing": True}
    )

    id = Column(String(40), primary_key=True, default=lambda: generate_id("pyo"))
    batch_id = Column(String(40), ForeignKey("payments.gym_payout_batches.id"), nullable=True)
    gym_id = Column(String(64), ForeignKey("payments.gym_entities.id"), nullable=False)
    fund_account_id = Column(String(40), ForeignKey("payments.payout_fund_accounts.id"), nullable=False)
    amount_minor = Column(Integer, nullable=False)
    currency = Column(String(8), nullable=False, default="INR")
    provider = Column(String(32), nullable=False, default="razorpay")
    provider_payout_id = Column(String(64), nullable=True)
    status = Column(String(24), nullable=False, default="queued")
    failure_reason = Column(Text, nullable=True)
    idempotency_key = Column(String(128), nullable=False, unique=True)
    meta = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    batch = relationship("GymPayoutBatch")
    gym = relationship("PaymentGym")
    fund_account = relationship("GymPayoutFundAccount")

    def __repr__(self) -> str:
        return f"<GymPayout(id={self.id}, gym_id={self.gym_id}, status={self.status})>"

    @property
    def amount_rupees(self) -> float:
        """Amount in rupees"""
        return self.amount_minor / 100.0


class ReconciliationGap(Base):
    """
    Tracks reconciliation gaps and discrepancies
    """
    __tablename__ = "reconciliation_gaps"
    __table_args__ = {"schema": "payments", "extend_existing": True}

    id = Column(String(40), primary_key=True, default=lambda: generate_id("gap"))
    topic = Column(String(64), nullable=False)  # settlement_missing|payout_mismatch|etc
    ref_ids = Column(JSON, nullable=True)      # {"payment_id": "...", "payout_id": "..."}
    details = Column(JSON, nullable=True)
    resolved = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    def __repr__(self) -> str:
        return f"<ReconciliationGap(id={self.id}, topic={self.topic})>"


# ============================================================================
#                           RAZORPAY CLIENT HELPERS
# ============================================================================

def _rzp_auth(settings):
    """Get Razorpay authentication tuple"""
    return (settings.razorpay_key_id, settings.razorpay_key_secret)


def _mask(s: Optional[str], keep=4) -> str:
    """Mask sensitive information"""
    if not s:
        return "masked"
    if len(s) <= keep:
        return "*" * len(s)
    return f"{s[:2]}...{s[-keep:]}"


def rzp_create_or_get_contact(settings, gym: PaymentGym, contact_payload: Optional[dict] = None) -> dict:
    """
    Create or get Razorpay Contact for gym
    """
    url = "https://api.razorpay.com/v1/contacts"
    payload = {
        "name": gym.name,
        "type": "vendor",
        "reference_id": gym.id,
    }
    if contact_payload:
        payload.update(contact_payload)

    # Try search by reference_id first
    try:
        qs = {"reference_id": gym.id}
        rr = requests.get(url, params=qs, auth=_rzp_auth(settings), timeout=20)
        if rr.status_code == 200:
            arr = rr.json().get("items", [])
            if arr:
                return arr[0]
    except Exception:
        pass

    # Create new contact
    r = requests.post(url, auth=_rzp_auth(settings), json=payload, timeout=20)
    r.raise_for_status()
    return r.json()


def rzp_create_or_get_fund_account(settings, contact_id: str, account_type: str, details: dict) -> dict:
    """
    Create or get fund account for payout
    """
    url = "https://api.razorpay.com/v1/fund_accounts"

    # Try to find existing fund account
    try:
        ls = requests.get(url, params={"contact_id": contact_id}, auth=_rzp_auth(settings), timeout=20)
        if ls.status_code == 200:
            for item in ls.json().get("items", []):
                if account_type == "bank" and item.get("account_type") == "bank_account":
                    d = item.get("bank_account", {})
                    if (d.get("ifsc") == details.get("ifsc") and
                        d.get("last4") == details.get("account_number", "")[-4:]):
                        return item
                if account_type == "vpa" and item.get("account_type") == "vpa":
                    if item.get("vpa", {}).get("address") == details.get("vpa"):
                        return item
    except Exception:
        pass

    # Create new fund account
    payload = {"contact_id": contact_id}
    if account_type == "bank":
        payload["account_type"] = "bank_account"
        payload["bank_account"] = {
            "name": details.get("name") or "Beneficiary",
            "ifsc": details["ifsc"],
            "account_number": details["account_number"],
        }
    else:
        payload["account_type"] = "vpa"
        payload["vpa"] = {"address": details["vpa"]}

    r = requests.post(url, auth=_rzp_auth(settings), json=payload, timeout=20)
    r.raise_for_status()
    return r.json()


def rzp_trigger_payout(settings, fund_account_id: str, amount_minor: int, currency: str, reference_id: str, narration: str) -> dict:
    """
    Trigger payout via Razorpay
    """
    url = "https://api.razorpay.com/v1/payouts"
    payload = {
        "account_number": settings.razorpay_payout_account_number,
        "fund_account_id": fund_account_id,
        "amount": amount_minor,
        "currency": currency,
        "mode": "IMPS",
        "purpose": "payout",
        "queue_if_low_balance": True,
        "reference_id": reference_id,
        "narration": narration[:40],
    }
    r = requests.post(url, auth=_rzp_auth(settings), json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def rzp_list_settlements(settings, from_ts: int, to_ts: int) -> List[dict]:
    """
    Pull settlement transactions from Razorpay
    """
    url = "https://api.razorpay.com/v1/settlements"
    params = {"from": from_ts, "to": to_ts}
    r = requests.get(url, auth=_rzp_auth(settings), params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    items = data.get("items") or data.get("settlements") or []
    return items


# ============================================================================
#                           DOMAIN HELPERS
# ============================================================================

def now_utc() -> datetime:
    """Get current UTC time"""
    return datetime.now(UTC)


def mask_details(details: dict) -> dict:
    """Mask sensitive account details"""
    if not details:
        return {}
    md = {}
    if "account_number" in details:
        acc = details["account_number"]
        md["account_number"] = f"****{acc[-4:]}" if acc else None
    if "ifsc" in details:
        md["ifsc"] = _mask(details["ifsc"], keep=3)
    if "vpa" in details:
        v = details["vpa"]
        if v and len(v) > 5:
            md["vpa"] = f"{v[:2]}***{v[-2:]}"
        else:
            md["vpa"] = _mask(v)
    if "name" in details:
        md["name"] = details["name"]
    return md


def resolve_gym_for_payment(db: Session, payment: Payment) -> Optional[str]:
    """
    Resolve gym ID from payment - adapt this to your system
    """
    if payment.order_id:
        # Check if order has gym_membership item type
        order_item = db.query(OrderItem).filter(
            OrderItem.order_id == payment.order_id,
            OrderItem.item_type == "gym_membership"
        ).first()

        if order_item and order_item.item_metadata:
            gym_id = order_item.item_metadata.get("gym_id")
            if gym_id:
                return str(gym_id)

    # Fallback to payment metadata
    meta = payment.payment_metadata or {}
    gym_id = meta.get("gym_id")
    return str(gym_id) if gym_id else None


def compute_idempotency_key(*parts: str) -> str:
    """Generate idempotency key from parts"""
    s = "|".join(str(p) for p in parts)
    return sha256(s.encode("utf-8")).hexdigest()


def ensure_gym_exists(db: Session, gym_id: str, gym_name: str = None) -> PaymentGym:
    """Ensure gym entity exists in our system"""
    gym = db.query(PaymentGym).filter(PaymentGym.id == gym_id).first()
    if not gym:
        gym = PaymentGym(
            id=gym_id,
            name=gym_name or f"Gym {gym_id}",
            status="active",
            external_gym_id=int(gym_id) if gym_id.isdigit() else None
        )
        db.add(gym)
        db.flush()
    return gym


# ============================================================================
#                   SETTLEMENT INGESTION (poller-based)
# ============================================================================

def ingest_settlement_events_for_window(db: Session, settings, start_dt: datetime, end_dt: datetime) -> int:
    """
    Poll Razorpay settlements for time window and create settlement events
    """
    count = 0
    from_ts = int(start_dt.timestamp())
    to_ts = int(end_dt.timestamp())

    try:
        items = rzp_list_settlements(settings, from_ts, to_ts)
    except requests.RequestException as e:
        logger.error(f"[SETTLEMENT] list_settlements failed: {e}")
        return 0

    for item in items:
        # Extract settlement data
        provider_settlement_id = str(item.get("id") or item.get("entity_id") or "")
        payment_id = item.get("payment_id") or item.get("payment", {}).get("id")
        if not provider_settlement_id or not payment_id:
            continue

        amount_minor = int(item.get("amount", 0))
        fees_minor = int(item.get("fees", 0))
        tax_minor = int(item.get("tax", 0))
        settled_at = item.get("created_at") or item.get("settled_at") or int(time.time())
        settled_at_dt = datetime.fromtimestamp(int(settled_at), tz=UTC)

        # Upsert SettlementEvent
        ev = db.query(SettlementEvent).filter(
            SettlementEvent.provider == "razorpay",
            SettlementEvent.provider_settlement_id == provider_settlement_id
        ).first()

        if not ev:
            ev = SettlementEvent(
                provider="razorpay",
                provider_settlement_id=provider_settlement_id,
                payment_id=payment_id,
                amount_minor=amount_minor,
                fees_minor=fees_minor,
                tax_minor=tax_minor,
                settled_at=settled_at_dt,
                raw=item,
            )
            db.add(ev)
            db.flush()

        # Find corresponding payment in our system
        pay = db.query(Payment).filter(
            Payment.provider == "razorpay",
            Payment.provider_payment_id == payment_id,
            Payment.status == "captured"
        ).first()

        if not pay:
            # Settlement for unknown payment
            gap = ReconciliationGap(
                topic="settlement_missing_payment",
                ref_ids={"payment_id": payment_id, "provider_settlement_id": provider_settlement_id},
                details={"item": item},
            )
            db.add(gap)
            continue

        # Resolve gym for this payment
        gym_id = resolve_gym_for_payment(db, pay)
        if not gym_id:
            gap = ReconciliationGap(
                topic="settlement_no_gym_mapping",
                ref_ids={"payment_id": payment_id},
                details={"order_id": pay.order_id},
            )
            db.add(gap)
            continue

        # Ensure gym exists
        ensure_gym_exists(db, gym_id)

        # Calculate net amount
        net = max(0, amount_minor - fees_minor - tax_minor)

        # Upsert LedgerEarning
        le = db.query(LedgerEarning).filter(LedgerEarning.payment_id == payment_id).first()
        if not le:
            le = LedgerEarning(
                gym_id=gym_id,
                payment_id=payment_id,
                order_id=pay.order_id,
                amount_gross_minor=amount_minor,
                fees_minor=fees_minor,
                tax_minor=tax_minor,
                amount_net_minor=net,
                state="eligible_for_payout",
                settlement_event_id=ev.id,
            )
            db.add(le)
        else:
            # Update if previously pending
            if le.state == "pending_settlement":
                le.amount_gross_minor = amount_minor
                le.fees_minor = fees_minor
                le.tax_minor = tax_minor
                le.amount_net_minor = net
                le.state = "eligible_for_payout"
                le.settlement_event_id = ev.id
                le.updated_at = now_utc()

        count += 1

    db.commit()
    return count


# ============================================================================
#                            PAYOUT PROCESSING
# ============================================================================

def create_payout_for_gym(db: Session, gym_id: str, fund_account_id: str, amount_minor: int, currency="INR") -> GymPayout:
    """Create payout for gym with idempotency"""
    bucket = datetime.utcnow().strftime("%Y-%m-%d")
    idem_key = compute_idempotency_key("payout", gym_id, bucket, fund_account_id, str(amount_minor))

    existing = db.query(GymPayout).filter_by(idempotency_key=idem_key).first()
    if existing:
        return existing

    payout = GymPayout(
        gym_id=gym_id,
        fund_account_id=fund_account_id,
        amount_minor=amount_minor,
        currency=currency,
        provider="razorpay",
        status="queued",
        idempotency_key=idem_key,
    )
    db.add(payout)
    db.flush()

    # Attach eligible earnings to this payout
    rows = db.query(LedgerEarning).filter(
        LedgerEarning.gym_id == gym_id,
        LedgerEarning.state == "eligible_for_payout"
    ).order_by(LedgerEarning.created_at.asc()).all()

    running = 0
    attach = []
    for r in rows:
        if running + r.amount_net_minor > amount_minor:
            break
        r.state = "in_payout"
        r.payout_id = payout.id
        r.updated_at = now_utc()
        running += r.amount_net_minor
        attach.append(r.id)

    payout.meta = {"attached_earnings": attach, "sum_attached": running}
    db.commit()
    return payout


def refresh_batch_status(db: Session, batch_id: str):
    """Update batch status based on payout statuses"""
    payouts = db.query(GymPayout).filter(GymPayout.batch_id == batch_id).all()
    if not payouts:
        b = db.query(GymPayoutBatch).get(batch_id)
        if b:
            b.status = "completed"
            b.updated_at = now_utc()
        db.commit()
        return

    states = {p.status for p in payouts}
    b = db.query(GymPayoutBatch).get(batch_id)
    if not b:
        return

    if states == {"processed"}:
        b.status = "completed"
    elif "failed" in states:
        b.status = "partial" if len(states) > 1 else "failed"
    elif "processing" in states or "queued" in states:
        b.status = "processing"
    else:
        b.status = "partial"

    b.updated_at = now_utc()
    db.commit()


def run_payout_batch(db: Session, settings) -> str:
    """
    Main payout batch processing - called by cron
    """
    # Create batch
    batch = GymPayoutBatch(
        batch_ref=f"pob_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{secrets.token_hex(4)}",
        status="created",
        created_by="system_cron",
    )
    db.add(batch)
    db.flush()

    # Process all active gyms
    gyms = db.query(PaymentGym).filter(PaymentGym.status == "active").all()

    for g in gyms:
        # Find active fund account
        fa = db.query(GymPayoutFundAccount).filter(
            GymPayoutFundAccount.gym_id == g.id,
            GymPayoutFundAccount.provider == "razorpay",
            GymPayoutFundAccount.active == True
        ).order_by(GymPayoutFundAccount.created_at.desc()).first()

        if not fa:
            continue

        # Sum eligible earnings
        total = db.query(func.coalesce(func.sum(LedgerEarning.amount_net_minor), 0)).filter(
            LedgerEarning.gym_id == g.id,
            LedgerEarning.state == "eligible_for_payout"
        ).scalar()

        policy = g.payout_policy or {}
        threshold = int(policy.get("min_threshold_minor", 10000))  # ₹100 default
        if total < threshold:
            continue

        # Create payout
        payout = create_payout_for_gym(db, g.id, fa.id, total)
        payout.batch_id = batch.id
        db.commit()

        # Trigger Razorpay payout
        try:
            resp = rzp_trigger_payout(
                settings=settings,
                fund_account_id=fa.fund_account_id,
                amount_minor=payout.amount_minor,
                currency=payout.currency,
                reference_id=payout.idempotency_key,
                narration=f"FB Gym {g.id[:10]}",
            )
            payout.provider_payout_id = resp.get("id")
            payout.status = resp.get("status", "processing")
            payout.updated_at = now_utc()
            db.commit()
            logger.info(f"✅ GymPayout triggered for gym {g.id}: {payout.amount_rupees}₹")
        except requests.RequestException as e:
            logger.error(f"[PAYOUT] API error gym={g.id}: {e}")

    refresh_batch_status(db, batch.id)
    return batch.batch_ref


# ============================================================================
#                        WEBHOOKS: PAYOUT EVENTS
# ============================================================================

@router.post("/webhooks/razorpay/payouts")
async def webhook_razorpay_payouts(request: Request, db: Session = Depends(get_db_session)):
    """Handle Razorpay payout webhooks"""
    settings = get_payment_settings()

    try:
        raw = await request.body()
    except Exception:
        return Response(status_code=400, content=b"")

    sig = request.headers.get("X-Razorpay-Signature", "")
    if not verify_razorpay_signature(raw.decode(), sig, settings.razorpay_payouts_webhook_secret):
        raise HTTPException(status_code=401, detail="Invalid signature")

    payload = json.loads(raw.decode("utf-8"))
    event = payload.get("event", "")
    if event not in ("payout.processed", "payout.failed", "payout.reversed"):
        return {"status": "ignored", "event": event}

    p = (payload.get("payload") or {}).get("payout", {}).get("entity", {})
    provider_payout_id = p.get("id")
    reference_id = p.get("reference_id")  # our idempotency key

    # Find payout
    payout = None
    if provider_payout_id:
        payout = db.query(GymPayout).filter(
            GymPayout.provider == "razorpay",
            GymPayout.provider_payout_id == provider_payout_id
        ).first()
    if not payout and reference_id:
        payout = db.query(GymPayout).filter(GymPayout.idempotency_key == reference_id).first()

    if not payout:
        # Unknown payout
        gap = ReconciliationGap(
            topic="payout_webhook_unknown",
            ref_ids={"provider_payout_id": provider_payout_id, "reference_id": reference_id},
            details={"payload": payload},
        )
        db.add(gap)
        db.commit()
        return {"status": "ignored"}

    # Update payout status
    new_status = event.split(".")[1]  # processed|failed|reversed
    if new_status == "processed":
        payout.status = "processed"
    elif new_status == "failed":
        payout.status = "failed"
        payout.failure_reason = p.get("failure_reason") or p.get("failure_reason_code")
    elif new_status == "reversed":
        payout.status = "reversed"

    payout.updated_at = now_utc()

    # Update ledger earnings
    earnings = db.query(LedgerEarning).filter(LedgerEarning.payout_id == payout.id).all()
    if new_status == "processed":
        for e in earnings:
            e.state = "paid_out"
            e.updated_at = now_utc()
    else:
        # Failed/reversed - make earnings eligible again
        for e in earnings:
            e.state = "eligible_for_payout"
            e.payout_id = None
            e.updated_at = now_utc()

    db.commit()
    logger.info(f"✅ GymPayout webhook processed: {payout.id} -> {payout.status}")

    return {"status": "ok", "payout_id": payout.id, "new_status": payout.status}


# ============================================================================
#                         API: FUND ACCOUNT MANAGEMENT
# ============================================================================

@router.post("/fund-accounts/upsert")
async def upsert_fund_account(request: Request, db: Session = Depends(get_db_session)):
    """
    Upsert fund account for gym
    Body: {
      "gym_id": "gym_123",
      "account_type": "bank" | "vpa",
      "details": { "name": "...", "account_number":"...", "ifsc":"..." }  # or {"vpa":"..."}
      "contact_payload": {...}  # optional Razorpay contact fields
    }
    """
    settings = get_payment_settings()
    body = await request.json()
    gym_id = body.get("gym_id")
    account_type = body.get("account_type")
    details = body.get("details") or {}
    contact_payload = body.get("contact_payload") or {}

    if not gym_id or not account_type or not details:
        raise HTTPException(status_code=400, detail="gym_id, account_type, details are required")

    # Ensure gym exists
    gym = ensure_gym_exists(db, gym_id)

    # Create Razorpay contact and fund account
    contact = rzp_create_or_get_contact(settings, gym, contact_payload=contact_payload)
    fa = rzp_create_or_get_fund_account(settings, contact_id=contact["id"], account_type=account_type, details=details)

    # Upsert local fund account record
    local = db.query(GymPayoutFundAccount).filter_by(
        gym_id=gym.id,
        provider="razorpay",
        fund_account_id=fa["id"]
    ).first()

    if not local:
        local = GymPayoutFundAccount(
            gym_id=gym.id,
            provider="razorpay",
            fund_account_id=fa["id"],
            account_type="bank" if fa.get("account_type") == "bank_account" else "vpa",
            masked_details=mask_details(details),
            active=True,
        )
    else:
        local.active = True
        local.updated_at = now_utc()

    db.add(local)
    db.commit()

    return {
        "gym_id": gym.id,
        "fund_account_id": fa["id"],
        "active": local.active,
        "masked_details": local.masked_details,
    }


# ============================================================================
#                     CRON/ADMIN TRIGGER ENDPOINTS
# ============================================================================

@router.post("/cron/run-payout-batch")
async def cron_run_payout_batch(db: Session = Depends(get_db_session)):
    """Trigger payout batch processing"""
    settings = get_payment_settings()
    ref = run_payout_batch(db, settings)
    return {"batch_ref": ref}


@router.post("/cron/ingest-settlements")
async def cron_ingest_settlements(request: Request, db: Session = Depends(get_db_session)):
    """
    Ingest settlements for date range
    Body: { "from": "2025-09-15", "to": "2025-09-17" }
    """
    settings = get_payment_settings()

    try:
        body = await request.json()
    except:
        body = {}

    today_ist = datetime.now(IST).date()
    start_date = body.get("from") or str(today_ist - timedelta(days=1))
    end_date = body.get("to") or str(today_ist)

    start_dt = datetime.fromisoformat(start_date).replace(tzinfo=IST).astimezone(UTC)
    end_dt = datetime.fromisoformat(end_date).replace(tzinfo=IST).astimezone(UTC)

    n = ingest_settlement_events_for_window(db, settings, start_dt, end_dt)
    return {"ingested": n, "from": start_dt.isoformat(), "to": end_dt.isoformat()}


# ============================================================================
#                         ANALYTICS ENDPOINTS
# ============================================================================

@router.get("/gym/{gym_id}/earnings")
async def get_gym_earnings(gym_id: str, db: Session = Depends(get_db_session)):
    """Get earnings summary for gym"""
    earnings = db.query(LedgerEarning).filter(LedgerEarning.gym_id == gym_id).all()

    total_gross = sum(e.amount_gross_minor for e in earnings)
    total_fees = sum(e.fees_minor + e.tax_minor for e in earnings)
    total_net = sum(e.amount_net_minor for e in earnings)

    pending = sum(e.amount_net_minor for e in earnings if e.state == "eligible_for_payout")
    paid = sum(e.amount_net_minor for e in earnings if e.state == "paid_out")

    return {
        "gym_id": gym_id,
        "total_earnings": len(earnings),
        "gross_amount_minor": total_gross,
        "fees_minor": total_fees,
        "net_amount_minor": total_net,
        "pending_payout_minor": pending,
        "paid_out_minor": paid,
        "gross_amount_rupees": total_gross / 100.0,
        "net_amount_rupees": total_net / 100.0,
        "pending_payout_rupees": pending / 100.0,
    }


@router.get("/gym/{gym_id}/payouts")
async def get_gym_payouts(gym_id: str, db: Session = Depends(get_db_session)):
    """Get payout history for gym"""
    payouts = db.query(GymPayout).filter(GymPayout.gym_id == gym_id).order_by(GymPayout.created_at.desc()).all()

    return {
        "gym_id": gym_id,
        "payouts": [
            {
                "id": p.id,
                "amount_minor": p.amount_minor,
                "amount_rupees": p.amount_rupees,
                "status": p.status,
                "provider_payout_id": p.provider_payout_id,
                "created_at": p.created_at.isoformat(),
                "failure_reason": p.failure_reason,
            }
            for p in payouts
        ]
    }
