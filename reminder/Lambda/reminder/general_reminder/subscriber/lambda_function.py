# subscriber.py
# ────────────────────────────────────────────────────────────
# Runtime: Python 3.12 | 128 MB | 60 s
# Trigger: SQS → Lambda (Event Source Mapping on BROADCAST_QUEUE_URL)

import json, logging, random, os
from typing import List, Dict, Any
from urllib.parse import quote_plus

import boto3
from exponent_server_sdk import PushClient, PushMessage, PushServerError
from sqlalchemy import create_engine, Column, Integer, JSON, update
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.mutable import MutableList


# ─── CONFIGURATION ────────────────────────────────────────
ENVIRONMENT = os.getenv("ENVIRONMENT", "production").lower()
REGION = os.getenv("AWS_REGION", "ap-south-2")
SECRET_NAME = "fittbot/secrets"


def load_env_file():
    """Load environment variables from .env file for local development"""
    from pathlib import Path
    env_file = Path(__file__).resolve().parent / ".env"
    if not env_file.exists():
        return {}

    env_vars = {}
    with open(env_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            env_vars[key] = value
    return env_vars


def get_db_credentials():
    """Get database credentials based on environment (matches ephemeral_tasks)"""
    # Local/dev: load from .env file
    if ENVIRONMENT in ("local", "development", "dev"):
        env_vars = load_env_file()

        db_username = env_vars.get("DB_USERNAME") or os.getenv("DB_USERNAME", "root")
        db_password = env_vars.get("DB_PASSWORD") or os.getenv("DB_PASSWORD", "")
        db_host = env_vars.get("DB_HOST") or os.getenv("DB_HOST", "localhost")
        db_name = env_vars.get("DB_NAME") or os.getenv("DB_NAME", "fittbot_local")

        return {
            "DB_USERNAME": db_username,
            "DB_PASSWORD": db_password,
            "DB_HOST": db_host,
            "DB_NAME": db_name,
        }

    # Check if credentials already in environment (from ECS task definition)
    if os.getenv("DB_USERNAME") and os.getenv("DB_HOST"):
        return {
            "DB_USERNAME": os.getenv("DB_USERNAME"),
            "DB_PASSWORD": os.getenv("DB_PASSWORD"),
            "DB_HOST": os.getenv("DB_HOST"),
            "DB_NAME": os.getenv("DB_NAME"),
        }

    # Production: fetch from AWS Secrets Manager
    sm = boto3.client("secretsmanager", region_name=REGION)
    val = sm.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(val["SecretString"])


def build_connection_string(creds):
    """Build MySQL connection string with proper URL encoding"""
    username = creds.get("DB_USERNAME")
    password = creds.get("DB_PASSWORD")
    host = creds.get("DB_HOST")
    db_name = creds.get("DB_NAME")
    if password:
        return f"mysql+pymysql://{username}:{quote_plus(password)}@{host}/{db_name}"
    return f"mysql+pymysql://{username}@{host}/{db_name}"


# ────────────────────────── DB INITIALISATION ──────────────────────────────
creds = get_db_credentials()
conn = build_connection_string(creds)

engine = create_engine(conn, pool_pre_ping=True, pool_size=2, max_overflow=0, pool_recycle=300)
Session = sessionmaker(bind=engine, autoflush=False)

Base = declarative_base()

class Client(Base):
    __tablename__ = "clients"
    client_id = Column(Integer, primary_key=True)
    expo_token = Column(MutableList.as_mutable(JSON))

push = PushClient()
log  = logging.getLogger()
log.setLevel(logging.INFO)


TEMPLATES: Dict[str, List[Dict[str, str]]] = {

    "birthday": [
        {
            "title": "🎂 Happy Birthday, {name}!",
            "body":  "The whole FittBot crew wishes you a year full of strength, health & PR-breaking workouts. Enjoy your day! 🥳",
            "channel": "default"
        }
    ],
    "breakfast": [
        {"title": "🌞 Good morning, {name}!",
         "body":  "Kick-start your metabolism with a protein-packed breakfast 🥣💪",
         "channel": "default"},
        {"title": "🥞 Rise & dine, {name}",
         "body":  "Fuel your gains – 30 g protein + slow carbs.",
         "channel": "default"},
        {"title": "🍳 Breakfast check-in,{name}",
         "body":  "Don’t skip the most important meal of the day!",
         "channel": "default"},
        {"title": "🌅 {name}, time to eat!",
         "body":  "Quality breakfast keeps cravings away all morning.",
         "channel": "default"},
    ],
    "mid_morning_snack": [
        {"title": "🍏 Snack O’Clock,{name}",
         "body":  "Grab a fruit or nuts to keep energy steady.",
         "channel": "default"},
        {"title": "🥜 Quick bite, {name}?",
         "body":  "A handful of almonds beats a sugary bar every time.",
         "channel": "default"},
        {"title": "🚀 Mini-refuel,{name}",
         "body":  "Protein yoghurt or banana – pick one and power on!",
         "channel": "default"},
    ],
    "lunch": [
        {"title": "🍱 Lunch time,{name}",
         "body":  "½ veggies, ¼ protein, ¼ carbs – plate it right.",
         "channel": "default"},
        {"title": "🥗 Balanced lunch alert,{name}",
         "body":  "Add colour: greens + lean protein boost recovery.",
         "channel": "default"},
        {"title": "🍛 Lunchtime, {name}",
         "body":  "Slow down and chew – mindful eating aids digestion.",
         "channel": "default"},
    ],
    "dinner": [
        {"title": "🍽️ Dinner call,{name}",
         "body":  "Light & early helps recovery – what’s on your plate?",
         "channel": "default"},
        {"title": "🌙 Wind-down meal,{name}",
         "body":  "Protein + veggies, skip heavy carbs for better sleep.",
         "channel": "default"},
        {"title": "🥦 Evening fuel,{name}",
         "body":  "Remember to hydrate and keep portions moderate.",
         "channel": "default"},
    ],
    "water": [
        {"title": "💧 Hydration check, {name}",
         "body":  "Even 1% dehydration drops your focus & energy. Drink a full glass now — your brain needs it!",
         "channel": "default"},
        {"title": "🥛 Water break, {name}",
         "body":  "Water fuels every muscle contraction. A quick glass now keeps your workout performance on point.",
         "channel": "default"},
        {"title": "💙 Stay hydrated, {name}",
         "body":  "Thirst means you're already dehydrated. Sip 200 ml right now and feel the difference.",
         "channel": "default"},
        {"title": "💪 Refill your glass, {name}",
         "body":  "Hydrated muscles recover faster. Pour yourself a glass and keep those gains coming!",
         "channel": "default"},
        {"title": "🔥 Time to hydrate, {name}",
         "body":  "Water flushes out toxins and reduces soreness. Take a few sips — your body is counting on you.",
         "channel": "default"},
        {"title": "🔔 Quick sip, {name}?",
         "body":  "Staying hydrated curbs cravings and keeps your metabolism firing. Grab that bottle!",
         "channel": "default"},
    ],

    "stretch_break": [
        {"title": "{name},🧘‍♂️ Stretch break",
         "body":  "Stand up, roll shoulders, 30-sec hamstring stretch—go!",
         "channel": "default"},
        {"title": "🙆 Mobility minute,{name}",
         "body":  "Neck circles + chest opener = instant refresh.",
         "channel": "default"},
        {"title": "🦵 Leg stretch time,{name}",
         "body":  "Desk posture fix: quad stretch & ankle rolls.",
         "channel": "default"},
         {"title": "📏 Posture check,{name}",
         "body":  "Straighten your back, relax shoulders.",
         "channel": "default"},
        {"title": "🪑 Sit tall,{name}",
         "body":  "Ear-hip-ankle line = painless spine.",
         "channel": "default"},
        {"title": "🔔 Back alert,{name}",
         "body":  "Roll shoulder blades down & away from ears.",
         "channel": "default"},
    ],
    "session_nudge": [
        {"title":"⏱  Still working out, {name}?",
         "body":"Hope you’re crushing it! Don’t forget to punch-out when done 💪",
         "channel":"workout_channel"}
    ],
  "punchout_intimation": [
  {
    "title": "🏁 Session complete, {name}!",
    "body": "You’ve been going strong for over 2 hours—let us handle the punch-out so your progress stays on track 💪✅",
    "channel": "workout_channel"
  }
]

}



MAX_BATCH = 100

# ─── LAMBDA HANDLER ─────────────────────────────────────────
def lambda_handler(event: Dict[str, Any], _ctx):
    # SQS wraps messages under Records[]
    records = event.get("Records") or []
    for rec in records:
        try:
            payload = json.loads(rec["body"])
            tpl_key = payload["template"]
        except Exception as e:
            log.error("Invalid SQS record: %s", e)
            continue

        variants = TEMPLATES.get(tpl_key)
        if not variants:
            log.error("Unknown template '%s'", tpl_key)
            continue

        tpl = random.choice(variants)  # ← pick 1 of the 3-4 variants

        # payload['recipients'] is a list of {token, name}
        recs = payload.get("recipients", [])
        if not isinstance(recs, list) or not recs:
            log.warning("No recipients for template '%s'", tpl_key)
            continue

        # Chunk into MAX_BATCH and send
        for i in range(0, len(recs), MAX_BATCH):
            _send_batch(tpl_key, tpl, recs[i : i + MAX_BATCH])

# ─── INTERNAL: SEND ONE BATCH ───────────────────────────────
def _send_batch(tpl_key: str, tpl: Dict[str, str], batch: List[Dict[str, str]]):
    messages = []
    token_info = []  # Track token -> client_id mapping

    for r in batch:
        token = r.get("token")
        name  = r.get("name", "there")
        client_id = r.get("client_id")
        if not token:
            continue
        messages.append(
            PushMessage(
                to        = token,
                title     = tpl["title"].format(name=name),
                body      = tpl["body"].format(name=name, steps_left=r.get("steps_left", "")),
                sound     = "default",
                priority  = "high",
                channel_id= tpl["channel"],
                data      = {"template": tpl_key},
                display_in_foreground=True,
            )
        )
        token_info.append({"token": token, "client_id": client_id})

    if not messages:
        log.warning("Skipping empty batch for '%s'", tpl_key)
        return

    try:
        resp = push.publish_multiple(messages)
        ok   = sum(1 for r in resp if r.status == "ok")
        err  = len(resp) - ok
        log.info("Template '%s' → Sent %d ok, %d errors", tpl_key, ok, err)

        # Collect invalid tokens grouped by client_id
        invalid_by_client: Dict[int, List[str]] = {}
        for info, response in zip(token_info, resp):
            if response.status == "error":
                error_type = getattr(response.details, "error", None) if response.details else None
                if error_type == "DeviceNotRegistered" and info["client_id"]:
                    cid = info["client_id"]
                    if cid not in invalid_by_client:
                        invalid_by_client[cid] = []
                    invalid_by_client[cid].append(info["token"])
                    log.info("Token %s is unregistered (client %s)", info["token"][:20] + "...", cid)

        # Remove invalid tokens from database
        if invalid_by_client:
            _cleanup_invalid_tokens(invalid_by_client)

    except PushServerError as exc:
        log.error("Expo push failed for '%s': %s", tpl_key, exc)
        raise  # let SQS/Lambda retry


def _cleanup_invalid_tokens(invalid_by_client: Dict[int, List[str]]):
    """Remove invalid tokens from clients' expo_token arrays"""
    sess = Session()
    try:
        for client_id, invalid_tokens in invalid_by_client.items():
            client = sess.query(Client).filter(Client.client_id == client_id).first()
            if not client or not client.expo_token:
                continue

            current_tokens = client.expo_token if isinstance(client.expo_token, list) else [client.expo_token]
            updated_tokens = [t for t in current_tokens if t and t not in invalid_tokens]

            sess.execute(
                update(Client)
                .where(Client.client_id == client_id)
                .values(expo_token=updated_tokens if updated_tokens else None)
            )
            log.info("Removed %d invalid token(s) from client %s, %d remaining",
                     len(invalid_tokens), client_id, len(updated_tokens))

        sess.commit()
    except Exception as exc:
        sess.rollback()
        log.error("Failed to cleanup invalid tokens: %s", exc)
    finally:
        sess.close()
