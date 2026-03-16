
import os, json, logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus
from typing import List, Optional

from exponent_server_sdk import PushClient, PushMessage
import boto3
from sqlalchemy import (
    create_engine, select, update, delete,
    Column, Integer, String, Float, Boolean, Time, JSON, DateTime, Index, ForeignKey, Text, Date, Enum
)
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.mutable import MutableList
import uuid


REGION = os.getenv("AWS_REGION", "ap-south-2")
SECRET_NAME = "fittbot/secrets"


def fetch_secret():
    """Fetch secrets from AWS Secrets Manager"""
    sm = boto3.client("secretsmanager", region_name=REGION)
    val = sm.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(val["SecretString"])


secrets = fetch_secret()
DB_USERNAME = secrets.get("DB_USERNAME")
DB_PASSWORD = secrets.get("DB_PASSWORD")
DB_HOST = secrets.get("DB_HOST")
DB_NAME = secrets.get("DB_NAME")


if DB_PASSWORD:
    conn = f"mysql+pymysql://{DB_USERNAME}:{quote_plus(DB_PASSWORD)}@{DB_HOST}/{DB_NAME}"
else:
    conn = f"mysql+pymysql://{DB_USERNAME}@{DB_HOST}/{DB_NAME}"


engine = create_engine(
    conn, pool_pre_ping=True, pool_size=4,
    max_overflow=0, pool_recycle=300,
)
Session = sessionmaker(bind=engine, autoflush=False)
push_client = PushClient()


# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────

log = logging.getLogger("lambda_worker")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s"))
    log.addHandler(h)


# ─────────────────────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────────────────────

Base = declarative_base()


class Reminder(Base):
    __tablename__ = "reminders"

    reminder_id = Column(Integer, primary_key=True, index=True, nullable=False)
    client_id = Column(Integer, ForeignKey("clients.client_id", ondelete="CASCADE", onupdate="CASCADE"), index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), index=True)

    reminder_time = Column(Time)
    details = Column(String(500), nullable=False)
    vibration_pattern = Column(JSON)
    reminder_type = Column(String(45))
    is_recurring = Column(Boolean, default=False, nullable=False)

    reminder_Sent = Column(Boolean, default=False, nullable=False)
    queued = Column(Boolean, default=False, nullable=False)
    sent_at = Column(DateTime)

    reminder_mode = Column(String(45))
    intimation_start_time = Column(Time)
    intimation_end_time = Column(Time)
    water_timing = Column(Float)
    water_amount = Column(Integer)
    gym_count = Column(Integer)
    diet_type = Column(String(45))
    title = Column(String(45))
    others_time = Column(DateTime)

    __table_args__ = (
        Index("idx_due_scan", reminder_Sent, queued, reminder_time),
        Index("idx_mode_time", reminder_mode, reminder_time),
    )


class Client(Base):
    __tablename__ = "clients"

    client_id = Column(Integer, primary_key=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    name = Column(String(100), nullable=False)
    profile = Column(String(255))
    location = Column(String(255), nullable=True)
    email = Column(String(100), unique=True, nullable=False)
    contact = Column(String(15), nullable=False)
    password = Column(String(255), nullable=False)
    lifestyle = Column(Text)
    medical_issues = Column(Text)
    batch_id = Column(Integer, nullable=True)
    training_id = Column(Integer, nullable=True)
    age = Column(Integer, nullable=True)
    goals = Column(Text)
    gender = Column(String(20), nullable=True, default="Other")
    height = Column(Float, nullable=True)
    weight = Column(Float, nullable=True)
    bmi = Column(Float, nullable=True)
    access = Column(Boolean)
    joined_date = Column(Date, default=datetime.now().date)
    status = Column(Enum("active", "inactive"), default="active")
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    dob = Column(Date, nullable=True)
    expiry = Column(Enum("joining_date", "start_of_the_month"))
    refresh_token = Column(String(255))
    verification = Column(JSON)
    uuid_client = Column(String, unique=True, default=lambda: str(uuid.uuid4()), nullable=False)
    incomplete = Column(Boolean, nullable=False, default=False)
    expo_token = Column(MutableList.as_mutable(JSON))


def _now_ist() -> datetime:
    """Current time in Asia/Kolkata (no DST)."""
    return datetime.now(tz=ZoneInfo("Asia/Kolkata"))


PAGE_BY_MODE = {
    "water": "Water",
    "gym": "My Gym",
    "diet": "Diet",
    "others": "Others",
}


DEFAULT_PAGE = "Water"


REMINDER_TITLE = {
    "water": "Water Reminder",
    "gym": "Gym Crowd Alert",
    "diet": "Diet Reminder",
    "others": "Reminder",
}

CHANNEL_BY_MODE = {
    "water": "default",
    "gym": "workout_channel",
    "diet": "diet_channel",
    "others": "default",
}


# ─────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────

def lambda_handler(event, _context):
    """
    Process reminder IDs from SQS queue.
    Sends push notifications and updates reminder status.
    """
    sess = Session()
    processed = 0
    failed = 0
    skipped = 0

    # Stats tracking
    stats = {
        "total_records": len(event.get("Records", [])),
        "push_success": 0,
        "push_failed": 0,
        "invalid_tokens_removed": 0,
        "skipped_no_reminder": 0,
        "skipped_already_sent": 0,
        "skipped_no_client": 0,
        "skipped_no_token": 0,
    }

    log.info("═" * 60)
    log.info("LAMBDA START | Time: %s IST | Records: %d",
             _now_ist().strftime("%Y-%m-%d %H:%M:%S"), stats["total_records"])
    log.info("═" * 60)

    try:
        for idx, record in enumerate(event["Records"], 1):
            rid = json.loads(record["body"])["rid"]
            log.info("─" * 40)
            log.info("[%d/%d] Processing reminder_id=%s", idx, stats["total_records"], rid)

            # Fetch reminder with lock
            reminder: Optional[Reminder] = (
                sess.execute(
                    select(Reminder)
                    .where(Reminder.reminder_id == rid)
                    .with_for_update(skip_locked=True)
                )
                .scalar_one_or_none()
            )

            # Skip if reminder not found
            if not reminder:
                log.warning("  SKIP: Reminder %s not found (deleted?)", rid)
                stats["skipped_no_reminder"] += 1
                skipped += 1
                continue

            # Log reminder details
            log.info("  Reminder: mode=%s | recurring=%s | client_id=%s",
                     reminder.reminder_mode, reminder.is_recurring, reminder.client_id)
            log.info("  Times: reminder_time=%s | others_time=%s | sent_at=%s",
                     reminder.reminder_time, reminder.others_time, reminder.sent_at)
            log.info("  Title: '%s' | Details: '%s'",
                     reminder.title or "N/A", (reminder.details or "")[:50])

            # Skip if already sent (duplicate SQS message) - reset queued flag
            if reminder.reminder_Sent:
                sess.execute(
                    update(Reminder)
                    .where(Reminder.reminder_id == rid)
                    .values(queued=False)
                )
                log.warning("  SKIP: Already sent (duplicate SQS?), reset queued=False")
                stats["skipped_already_sent"] += 1
                skipped += 1
                continue

            # Fetch client (handle if deleted)
            client: Optional[Client] = (
                sess.execute(
                    select(Client).where(Client.client_id == reminder.client_id)
                )
                .scalar_one_or_none()
            )

            if not client:
                # Client deleted - reset queued so cleanse_reminders can handle it
                sess.execute(
                    update(Reminder)
                    .where(Reminder.reminder_id == rid)
                    .values(queued=False)
                )
                log.warning("  SKIP: Client %s not found (deleted?), reset queued=False", reminder.client_id)
                stats["skipped_no_client"] += 1
                skipped += 1
                continue

            log.info("  Client: name='%s' | contact=%s | status=%s",
                     client.name, client.contact, client.status)

            # Get Expo tokens
            tokens = client.expo_token if isinstance(client.expo_token, list) else [client.expo_token]
            tokens = [t for t in tokens if t]

            if not tokens:
                # No token - reset queued flag
                sess.execute(
                    update(Reminder)
                    .where(Reminder.reminder_id == rid)
                    .values(queued=False)
                )
                log.warning("  SKIP: No Expo token for client %s, reset queued=False", client.client_id)
                stats["skipped_no_token"] += 1
                skipped += 1
                continue

            log.info("  Tokens: %d token(s) found", len(tokens))
            for t_idx, tok in enumerate(tokens, 1):
                log.info("    Token %d: %s...%s", t_idx, tok[:25] if tok else "None", tok[-10:] if tok and len(tok) > 35 else "")

            # Build push notification
            mode = (reminder.reminder_mode or "").lower()
            page = PAGE_BY_MODE.get(mode, DEFAULT_PAGE)
            title = reminder.title or REMINDER_TITLE.get(mode, "Reminder")
            channel = CHANNEL_BY_MODE.get(mode, "default")

            messages: List[PushMessage] = [
                PushMessage(
                    to=t,
                    sound="default",
                    priority="high",
                    title=title,
                    body=reminder.details,
                    data={
                        "reminder_id": rid,
                        "vibrationPattern": reminder.vibration_pattern,
                        "page": page,
                    },
                    channel_id=channel,
                    display_in_foreground=True,
                )
                for t in tokens
            ]

            log.info("  Sending push: title='%s' | channel=%s | mode=%s", title, channel, mode)

            # Send push notification and handle invalid tokens
            try:
                log.info("  Calling Expo push API for %d message(s)...", len(messages))
                responses = push_client.publish_multiple(messages)

                # Log each response
                success_count = 0
                error_count = 0
                invalid_tokens = []

                for t_idx, (token, response) in enumerate(zip(tokens, responses), 1):
                    token_short = f"{token[:20]}...{token[-8:]}" if token and len(token) > 30 else token

                    if response.status == "ok":
                        success_count += 1
                        log.info("    [%d] SUCCESS | Token: %s | ID: %s",
                                 t_idx, token_short, getattr(response, 'id', 'N/A'))
                    else:
                        error_count += 1
                        error_type = getattr(response.details, "error", None) if response.details else None
                        error_msg = getattr(response.details, "message", None) if response.details else str(response.details)
                        log.error("    [%d] FAILED | Token: %s | Error: %s | Message: %s",
                                  t_idx, token_short, error_type, error_msg)

                        if error_type == "DeviceNotRegistered":
                            invalid_tokens.append(token)
                            log.warning("    -> Token marked for removal (DeviceNotRegistered)")

                stats["push_success"] += success_count
                stats["push_failed"] += error_count
                log.info("  Push result: %d success, %d failed", success_count, error_count)

                # Remove invalid tokens from client's expo_token array
                if invalid_tokens:
                    stats["invalid_tokens_removed"] += len(invalid_tokens)
                    current_tokens = client.expo_token if isinstance(client.expo_token, list) else [client.expo_token]
                    updated_tokens = [t for t in current_tokens if t and t not in invalid_tokens]

                    sess.execute(
                        update(Client)
                        .where(Client.client_id == client.client_id)
                        .values(expo_token=updated_tokens if updated_tokens else None)
                    )
                    log.info("Removed %d invalid token(s) from client %s, %d remaining",
                             len(invalid_tokens), client.client_id, len(updated_tokens))

            except Exception as exc:
                log.error("  EXPO API ERROR (rid=%s): %s", rid, exc)
                failed += 1
                raise  # Let SQS retry

            # Update reminder based on mode
            delete_after_send = False
            log.info("  Updating reminder in DB...")

            if mode == "water":
                # Water: advance time for next reminder
                next_dt = (
                    datetime.combine(
                        _now_ist().date(),
                        reminder.reminder_time,
                        tzinfo=ZoneInfo("Asia/Kolkata")
                    )
                    + timedelta(hours=reminder.water_timing or 1)
                )
                nxt = next_dt.time()

                end_overflow = (
                    reminder.intimation_end_time
                    and nxt > reminder.intimation_end_time
                )

                update_values = {
                    "reminder_time": nxt,
                    "queued": False,
                    "sent_at": _now_ist(),
                }

                if end_overflow or not reminder.is_recurring:
                    update_values["reminder_Sent"] = True
                    delete_after_send = not reminder.is_recurring

                sess.execute(
                    update(Reminder)
                    .where(Reminder.reminder_id == rid)
                    .values(**update_values)
                )
                log.info("  [WATER] Next time: %s | End overflow: %s | Will mark sent: %s",
                         nxt, end_overflow, update_values.get("reminder_Sent", False))

            else:
                # Diet/Gym/Others: mark as sent
                sess.execute(
                    update(Reminder)
                    .where(Reminder.reminder_id == rid)
                    .values(
                        reminder_Sent=True,
                        queued=False,
                        sent_at=_now_ist(),
                    )
                )
                delete_after_send = not reminder.is_recurring
                log.info("  [%s] Marked as sent | recurring=%s | will_delete=%s",
                         mode.upper(), reminder.is_recurring, delete_after_send)

            # Delete non-recurring reminders after sending
            if delete_after_send:
                sess.execute(delete(Reminder).where(Reminder.reminder_id == rid))
                log.info("  DELETED: Non-recurring reminder %s removed from DB", rid)

            log.info("  DONE: Reminder %s processed successfully", rid)
            processed += 1

        sess.commit()

        # Final summary
        log.info("═" * 60)
        log.info("LAMBDA COMPLETE | Summary:")
        log.info("  Total records: %d", stats["total_records"])
        log.info("  Processed: %d | Skipped: %d | Failed: %d", processed, skipped, failed)
        log.info("  Push success: %d | Push failed: %d", stats["push_success"], stats["push_failed"])
        log.info("  Invalid tokens removed: %d", stats["invalid_tokens_removed"])
        log.info("  Skip reasons: no_reminder=%d | already_sent=%d | no_client=%d | no_token=%d",
                 stats["skipped_no_reminder"], stats["skipped_already_sent"],
                 stats["skipped_no_client"], stats["skipped_no_token"])
        log.info("═" * 60)

        return {
            "processed": processed,
            "skipped": skipped,
            "failed": failed,
            "stats": stats
        }

    except Exception as e:
        sess.rollback()
        log.error("═" * 60)
        log.error("LAMBDA FAILED | Error: %s", str(e))
        log.error("  Processed before failure: %d | Skipped: %d", processed, skipped)
        log.exception("Full traceback:")
        log.error("═" * 60)
        raise

    finally:
        sess.close()
        log.info("DB session closed")
