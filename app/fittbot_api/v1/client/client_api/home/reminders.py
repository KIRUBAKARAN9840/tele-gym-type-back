# app/routers/reminder_router.py

import uuid
from datetime import datetime, time, timedelta
from typing import Optional, List

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models.fittbot_models import Client, Reminder
from app.models.database import get_db

# Logging & errors
from app.utils.logging_setup import jlog
from app.utils.logging_utils import (
    FittbotHTTPException,
    SecuritySeverity,   # kept for consistency, though not used as "security" here
    EventType,
)

router = APIRouter(prefix="/reminder", tags=["Reminders"])


# -----------------------------
# Domain logger (non-auth)
# -----------------------------
class _RemindersLogger:
    """
    Lightweight structured logger facade for the Reminders domain.
    Mirrors _AuthLogger style but domain-agnostic. Keeps logs concise JSON.
    """

    def __init__(self):
        self.request_id = None

    def set_request_context(self, context: Optional[object] = None) -> str:
        # Attach/derive a request-id. You can also pull from headers if needed.
        # If you later add a middleware to inject X-Request-ID, read it here.
        self.request_id = str(uuid.uuid4())
        return self.request_id

    def _log(self, level: str, **payload):
        payload.setdefault("timestamp", datetime.utcnow().isoformat() + "Z")
        payload.setdefault("request_id", self.request_id)
        payload.setdefault("domain", "reminders")
        jlog(level, payload)

    # Debug/Info
    def debug(self, msg: str, **kv): self._log("debug", type="debug", msg=msg, **kv)
    def info(self, msg: str, **kv):  self._log("info",  type="info",  msg=msg, **kv)

    # Warnings/Errors
    def warning(self, msg: str, **kv): self._log("warning", type="warn", msg=msg, **kv)
    def error(self, msg: str, **kv):   self._log("error",   type="error", msg=msg, **kv)

    # Domain helpers
    def business_event(self, name: str, **kv):
        self._log("info", type=EventType.BUSINESS, event=name, **kv)

    def api_access(self, method: str, endpoint: str, response_time_ms: int, **kv):
        self._log("info", type=EventType.API, method=method, endpoint=endpoint,
                  response_time_ms=int(response_time_ms), **kv)


reminders_logger = _RemindersLogger()


# -----------------------------
# Schemas
# -----------------------------
class ReminderRequest(BaseModel):
    gym_id: Optional[int]=None
    client_id: int
    reminder_time: Optional[time] = None
    reminder_type: str
    is_recurring: str
    reminder_mode: Optional[str] = None
    intimation_start_time: Optional[time] = None
    intimation_end_time: Optional[time] = None
    water_timing: Optional[float] = None
    water_amount: Optional[int] = None
    gym_count: Optional[int] = None
    diet_type: Optional[str] = None
    title: str
    details: str
    others_time: Optional[datetime] = None


# -----------------------------
# Helpers
# -----------------------------
def format_time_12hr(t: time) -> str:
    """Convert time to 12-hour format string"""
    hour = t.hour
    minute = t.minute
    period = "AM" if hour < 12 else "PM"
    if hour == 0:
        hour = 12
    elif hour > 12:
        hour = hour - 12
    return f"{hour}:{minute:02d} {period}"


def format_reminder_description(reminder_req: ReminderRequest) -> str:
    """Format reminder description based on reminder mode"""
    mode = reminder_req.reminder_mode.lower() if reminder_req.reminder_mode else "others"

    if mode == "water":
        water_amount = reminder_req.water_amount or 250
        return f"Drink {water_amount}ml of water"

    elif mode == "gym":
        return "Time for your workout session"

    elif mode == "diet":
        diet_type = reminder_req.diet_type or "meal"
        return f"Time for your {diet_type.lower()}"

    else:
        # For "others" or any custom reminder, use the provided details
        return reminder_req.details if reminder_req.details else reminder_req.title


def compute_next_water_time(start_time: time, water_timing: float, end_time: time) -> datetime:
    """
    Compute the next reminder time for water mode.
    Preserves your original logic/behavior; only logs added by caller.
    """
    current_dt = datetime.now()
    if current_dt.minute < 30:
        half_boundary = current_dt.replace(minute=30, second=0, microsecond=0)
    else:
        half_boundary = (current_dt.replace(minute=0, second=0, microsecond=0)
                         + timedelta(hours=1))

    end_dt = datetime.combine(current_dt.date(), end_time)
    if half_boundary > end_dt:
        half_boundary = end_dt

    if water_timing < 1:  # e.g., 0.5 hr
        return half_boundary
    else:
        next_dt = (half_boundary if half_boundary.minute == 0
                   else half_boundary + timedelta(minutes=30))
        if next_dt > end_dt:
            next_dt = end_dt
        return next_dt


# -----------------------------
# Routes
# -----------------------------

@router.get("/get_reminders")
def get_reminders_by_client(client_id: int, db: Session = Depends(get_db)):
    request_id = reminders_logger.set_request_context({"client_id": client_id})
    reminders_logger.info("Fetching reminders", client_id=client_id)

    try:
        reminders = (
            db.query(Reminder)
            .filter(Reminder.client_id == client_id)
            .order_by(Reminder.reminder_id.desc())
            .all()
        )

        if not reminders:
            reminders_logger.info("No reminders found", client_id=client_id, count=0)
            return {"status": 200, "data": []}

        reminders_logger.info("Reminders fetched", client_id=client_id, count=len(reminders))
        return {"status": 200, "data": reminders}

    except FittbotHTTPException:
        raise  # already logged via constructor
    except Exception as e:
        db.rollback()
        # Enterprise-grade error with concise context
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch reminders",
            error_code="REMINDER_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id, "request_id": request_id},
        )
    finally:
        try:
            db.close()
        except Exception:
            # If your DB dependency already handles lifecycle, this is harmless.
            pass


@router.post("/create_reminders")
def create_reminder(reminder_req: ReminderRequest, db: Session = Depends(get_db)):
    request_id = reminders_logger.set_request_context(reminder_req.model_dump())


    try:
        is_recurring = True if reminder_req.is_recurring.lower() == "daily" else False
        vibration_pattern = [0, 250, 250, 0] if reminder_req.reminder_type.lower() == "alarm" else None

        # Validate client
        client = db.query(Client).filter(Client.client_id == reminder_req.client_id).first()
        if not client:
            reminders_logger.warning("Client not found", client_id=reminder_req.client_id)
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
                log_level="warning",
                log_data={"client_id": reminder_req.client_id, "request_id": request_id},
            )

        # Validation windows for non-recurring + water/diet/gym
        if not is_recurring and reminder_req.reminder_mode in ("water",):
            if (not reminder_req.intimation_start_time) or (not reminder_req.intimation_end_time):
                reminders_logger.warning("Missing intimation window for water reminder",
                                         client_id=reminder_req.client_id)
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Intimation start/end times must be provided for water/diet reminders",
                    error_code="REMINDER_VALIDATION_ERROR",
                    log_level="warning",
                    log_data={"mode": "water", "request_id": request_id},
                )
            current_t = datetime.now().time()
            if not (reminder_req.intimation_start_time <= current_t <= reminder_req.intimation_end_time):
                reminders_logger.warning("Current time outside intimation window (water)",
                                         start=str(reminder_req.intimation_start_time),
                                         end=str(reminder_req.intimation_end_time),
                                         now=str(current_t))
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Current time is not within the allowed intimation window.",
                    error_code="REMINDER_OUTSIDE_WINDOW",
                    log_level="warning",
                    log_data={"mode": "water", "request_id": request_id},
                )

        if not is_recurring and reminder_req.reminder_mode in ("diet",):
            if reminder_req.reminder_time and reminder_req.reminder_time < datetime.now().time():
                reminders_logger.warning("Diet reminder time set in the past",
                                         reminder_time=str(reminder_req.reminder_time))
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Reminder Time has been set in past please change",
                    error_code="REMINDER_TIME_IN_PAST",
                    log_level="warning",
                    log_data={"mode": "diet", "request_id": request_id},
                )

        if not is_recurring and reminder_req.reminder_mode in ("gym",) and reminder_req.gym_id is not None:
            current_t = datetime.now().time()
            if not (
                reminder_req.intimation_start_time
                and reminder_req.intimation_end_time
                and (reminder_req.intimation_start_time <= current_t <= reminder_req.intimation_end_time)
            ):
                reminders_logger.warning("Current time outside intimation window (gym)",
                                         start=str(reminder_req.intimation_start_time),
                                         end=str(reminder_req.intimation_end_time),
                                         now=str(current_t))
                raise FittbotHTTPException(
                    status_code=400,
                    detail="Current time is not in between the intimation window",
                    error_code="REMINDER_OUTSIDE_WINDOW",
                    log_level="warning",
                    log_data={"mode": "gym", "request_id": request_id},
                )

        # Compute scheduling (preserving original behavior)
        computed_reminder_dt: Optional[datetime] = None
        reminder_sent = False

        if reminder_req.reminder_mode == "water" and reminder_req.water_timing is not None:
            current_t = datetime.now().time()
            if is_recurring and not (
                reminder_req.intimation_start_time
                and reminder_req.intimation_end_time
                and (reminder_req.intimation_start_time <= current_t <= reminder_req.intimation_end_time)
            ):
                # Schedule at start window for recurring when outside window
                computed_reminder_dt = datetime.combine(
                    datetime.today(), reminder_req.intimation_start_time
                ) if reminder_req.intimation_start_time else None
                reminder_sent = True
            else:
                # Compute the next water reminder time within window
                computed_reminder_dt = compute_next_water_time(
                    reminder_req.intimation_start_time,  # type: ignore[arg-type]
                    reminder_req.water_timing,
                    reminder_req.intimation_end_time      # type: ignore[arg-type]
                )
                reminder_sent = False

            reminders_logger.debug("Water reminder timing computed",
                                   computed_at=str(computed_reminder_dt) if computed_reminder_dt else None,
                                   reminder_sent=reminder_sent)

        # Final reminder datetime per original logic
        if computed_reminder_dt is not None:
            final_reminder_dt = computed_reminder_dt
        elif reminder_req.reminder_time:
            final_reminder_dt = datetime.combine(datetime.today(), reminder_req.reminder_time)
        else:
            final_reminder_dt = datetime.now()

        reminders_logger.info("Final reminder schedule computed",
                              final=str(final_reminder_dt.isoformat()),
                              is_recurring=is_recurring,
                              mode=reminder_req.reminder_mode)

        # Format the description in a readable way
        formatted_details = format_reminder_description(reminder_req)
        print(f"DEBUG FORMATTING: mode={reminder_req.reminder_mode}, original='{reminder_req.details}', formatted='{formatted_details}'")

        # Persist
        new_reminder = Reminder(
            gym_id=reminder_req.gym_id if reminder_req.gym_id is not None else None,
            client_id=reminder_req.client_id,
            reminder_time=final_reminder_dt.time(),
            title=reminder_req.title,
            details=formatted_details,
            vibration_pattern=vibration_pattern,
            reminder_type=reminder_req.reminder_type.lower(),
            is_recurring=is_recurring,
            reminder_mode=reminder_req.reminder_mode,
            intimation_start_time=reminder_req.intimation_start_time,
            intimation_end_time=reminder_req.intimation_end_time,
            water_timing=reminder_req.water_timing,
            water_amount=reminder_req.water_amount,
            gym_count=reminder_req.gym_count,
            diet_type=reminder_req.diet_type,
            reminder_Sent=reminder_sent,
            others_time=reminder_req.others_time,
        )
        db.add(new_reminder)
        db.commit()
        db.refresh(new_reminder)



        return {
            "status": 200,
            "message": "Reminder created successfully",
            "reminder_id": new_reminder.reminder_id,
            "scheduled_reminder_time": final_reminder_dt.isoformat(),
        }

    except FittbotHTTPException:
        raise  # already logged via constructor
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to create reminder",
            error_code="REMINDER_CREATE_ERROR",
            log_data={
                "exc": repr(e),
                "client_id": reminder_req.client_id,
                "request_id": request_id,
            },
        )
    finally:
        try:
            db.close()
        except Exception:
            pass


@router.delete("/delete_reminders")
async def delete_reminder(reminder_id: int, db: Session = Depends(get_db)):
    request_id = reminders_logger.set_request_context({"reminder_id": reminder_id})
    reminders_logger.info("Delete reminder requested", reminder_id=reminder_id)

    try:
        reminder = db.query(Reminder).filter(Reminder.reminder_id == reminder_id).first()
        if not reminder:
            reminders_logger.warning("Reminder not found", reminder_id=reminder_id)
            raise FittbotHTTPException(
                status_code=404,
                detail="Reminder not found",
                error_code="REMINDER_NOT_FOUND",
                log_level="warning",
                log_data={"reminder_id": reminder_id, "request_id": request_id},
            )

        db.delete(reminder)
        db.commit()

        reminders_logger.business_event("reminder_deleted", reminder_id=reminder_id)
        return {
            "status": 200,
            "message": "Reminder deleted successfully",
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to delete reminder",
            error_code="REMINDER_DELETE_ERROR",
            log_data={"exc": repr(e), "reminder_id": reminder_id, "request_id": request_id},
        )
    finally:
        try:
            db.close()
        except Exception:
            pass
