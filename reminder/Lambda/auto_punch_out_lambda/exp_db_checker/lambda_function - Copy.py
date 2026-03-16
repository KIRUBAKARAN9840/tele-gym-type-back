import os, json, logging, boto3
from datetime import datetime, date
from zoneinfo import ZoneInfo
from redis import Redis
from sqlalchemy import create_engine, select, or_
import uuid
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy import Column, Integer,BigInteger, String, Float, Enum, Text, DateTime, ForeignKey, Date,Time,Boolean,JSON,Numeric
from sqlalchemy.orm import sessionmaker, Session,declarative_base

Base = declarative_base()



class Client(Base):
    __tablename__ = "clients"
 
    client_id = Column(Integer, primary_key=True, index=True)
    gym_id = Column(Integer, ForeignKey("gyms.gym_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=True)
    name = Column(String(100), nullable=False)
    profile=Column(String(255))
    location = Column(String(255),nullable=True)
    email = Column(String(100), unique=True, nullable=False)
    contact = Column(String(15), nullable=False)
    password = Column(String(255), nullable=False)
    lifestyle = Column(Text)
    medical_issues = Column(Text)
    batch_id = Column(Integer, nullable=True)
    training_id = Column(Integer, nullable=True)
    age = Column(Integer, nullable=True)
    goals = Column(Text)
    gender = Column(String(20), nullable=True,default="Other")
    height = Column(Float, nullable=True)
    weight = Column(Float, nullable=True)
    bmi = Column(Float, nullable=True)
    access=Column(Boolean)
    joined_date = Column(Date, default=datetime.now().date)
    status = Column(Enum("active", "inactive"), default="active")
    created_at = Column(DateTime, default=datetime.now())
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())
    dob=Column(Date, nullable=True)
    expiry=Column(Enum("joining_date", "start_of_the_month"))
    refresh_token=Column(String(255))
    verification=Column(JSON)
    uuid_client = Column(String, unique=True, default=lambda: str(uuid.uuid4()), nullable=False)
    incomplete = Column(Boolean, nullable=False, default=False)
    expo_token = Column(MutableList.as_mutable(JSON))



class Attendance(Base):
    __tablename__ = "attendance"
    
    record_id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, nullable=False)
    gym_id = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    in_time = Column(Time, nullable=False)
    out_time = Column(Time)
    muscle=Column(JSON)
    in_time_2 = Column(Time)
    out_time_2= Column(Time)
    muscle_2  = Column(JSON)
    in_time_3 = Column(Time)
    out_time_3= Column(Time)
    muscle_3  = Column(JSON)





REDIS_HOST       = "fittbot-dev-cluster-new.azdytp.0001.aps2.cache.amazonaws.com"
NUDGE_QUEUE      = "https://sqs.ap-south-2.amazonaws.com/182399696098/GeneralReminderQueue"
PUNCH_QUEUE      = "os.environ"
MAX_BATCH        = 100
TEMPLATE_KEY     = "session_nudge"
TZ               = ZoneInfo("Asia/Kolkata")
REGION       = "ap-south-2"
SECRET_NAME  = "fittbot/mysqldb"
RDS_HOST     = "devfittbotdb.c5ayks2cmx9u.ap-south-2.rds.amazonaws.com"
DB_NAME      = "fittbot" 



log = logging.getLogger()
log.setLevel(logging.INFO)



sqs = boto3.client("sqs", region_name=REGION)
sm  = boto3.client("secretsmanager", region_name=REGION)


log = logging.getLogger()
log.setLevel(logging.INFO)



def fetch_secret():
    sm = boto3.client("secretsmanager", region_name=REGION)
    val = sm.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(val["SecretString"])


creds = fetch_secret()
conn  = (
    f"mysql+pymysql://{creds['username']}:{creds['password']}@"
    f"{RDS_HOST}:3306/{DB_NAME}"
)

engine = create_engine(
    conn, pool_pre_ping=True, pool_size=4,
    max_overflow=0, pool_recycle=300,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False)
redis = Redis(host=REDIS_HOST, port=6379, decode_responses=True)


def _active_slot(att):
    if att.out_time is None:                     return att.in_time, 0
    if att.in_time_2 and att.out_time_2 is None: return att.in_time_2, 1
    if att.in_time_3 and att.out_time_3 is None: return att.in_time_3, 2
    return None, None

# def _flag_once(key):       
#     return redis.setnx(key, "1") and redis.expire(key, 86400)

# ─── main ───
def lambda_handler(event, _ctx):
    now   = datetime.now(tz=TZ)
    today = now.date()

    sess: Session = SessionLocal()
    nudges, autos = [], []

    try:
        rows = sess.execute(
            select(Attendance, Client.expo_token, Client.name)
            .join(Client, Client.client_id == Attendance.client_id)
            .where(Attendance.date == today)
            .where(
                or_(
                    Attendance.out_time     == None,                                   # first slot open
                    (Attendance.in_time_2 != None) & (Attendance.out_time_2 == None),  # second slot open
                    (Attendance.in_time_3 != None) & (Attendance.out_time_3 == None)   # third slot open
                )
            )
        ).all()


        for att, token, full_name in rows:
            in_t, slot_idx = _active_slot(att)
            if not in_t or not token:
                continue

            mins = (now - datetime.combine(today, in_t, TZ)).total_seconds()/60
            rid  = att.record_id
            name = (full_name or "there").split()[0]

            # if 60 <= mins < 90 and _flag_once(f"60:{rid}"):
            if 60 <= mins < 90:
                nudges += _tok(token, name)

            # elif 90 <= mins < 120 and _flag_once(f"90:{rid}"):
            elif 90 <= mins < 120:
                nudges += _tok(token, name)

            # elif mins >= 120 and _flag_once(f"120:{rid}"):
            elif mins >= 120:
                nudges += _tok(token, name)

                # autos.append({"gym_id": att.gym_id, "client_id": att.client_id})
                # nudges += _tok(token, name)
                # _close_slot(att, slot_idx, now.time(), sess)

        sess.commit()
    finally:
        sess.close()

    _enqueue(nudges, NUDGE_QUEUE, {"template": TEMPLATE_KEY})
    # _enqueue(autos,  PUNCH_QUEUE, {"action": "auto_punch"})

    return {"nudges": len(nudges), "auto_out": len(autos)}

def _tok(tok_field, name):
    if isinstance(tok_field, list):
        return [{"token": t, "name": name} for t in tok_field if t]
    return [{"token": tok_field, "name": name}]

def _enqueue(items, queue_url, base):
    for i in range(0, len(items), MAX_BATCH):
        body = base | {"recipients" if "template" in base else "list": items[i:i+MAX_BATCH]}
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))

def _close_slot(att, idx, out_t, sess):
    if idx == 0:   att.out_time   = out_t
    elif idx == 1: att.out_time_2 = out_t
    else:          att.out_time_3 = out_t
    sess.flush()
