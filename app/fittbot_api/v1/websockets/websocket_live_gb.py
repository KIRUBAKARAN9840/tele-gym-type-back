# app/routers/websocket_live.py
from __future__ import annotations

import os
import asyncio
import json
from collections import defaultdict
from contextlib import suppress
from datetime import datetime, date
from typing import Dict, List

from fastapi import APIRouter
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState
from redis.asyncio import Redis
from jose import jwt, JWTError
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, asc

from app.models.database import SessionLocal
from app.models.fittbot_models import (
    GBMessage,
    FittbotMuscleGroup,
    Client,
    LiveCount,
    GymPlans,
    New_Session,
    Participant,
    JoinProposal,
    RejectedProposal,
    Attendance,
    ManualAttendance,
    ManualClient,
    ImportClientAttendance,
    GymImportData,
)
from app.utils.security import SECRET_KEY, ALGORITHM

router = APIRouter(prefix="/websocket_live", tags=["WebSocket_contents"])

# ──────────────────────────────────────────────────────────────
# Redis / Hubs bootstrap
# ──────────────────────────────────────────────────────────────
REDIS_DSN = os.getenv(
    "WEBSOCKET_REDIS_DSN",
    "redis://fittbot-dev-cluster-new.azdytp.0001.aps2.cache.amazonaws.com:6379/0",
)

redis_pool: Redis | None = None
session_hub: "PatternHub" | None = None
live_hub: "PatternHub" | None = None
chat_hub: "PatternHub" | None = None


async def _create_redis() -> Redis:
    return Redis.from_url(REDIS_DSN, decode_responses=True)


async def _ensure_hubs() -> tuple["PatternHub", "PatternHub", "PatternHub"]:
    global redis_pool, session_hub, live_hub, chat_hub
    if redis_pool is None:
        redis_pool = await _create_redis()
    if session_hub is None:
        session_hub = PatternHub(redis_pool, prefix="live_sessions:")
        await session_hub.start()
    if live_hub is None:
        live_hub = PatternHub(redis_pool, prefix="live:")
        await live_hub.start()
    if chat_hub is None:
        chat_hub = PatternHub(redis_pool, prefix="chat:")
        await chat_hub.start()
    return session_hub, live_hub, chat_hub


# ──────────────────────────────────────────────────────────────
# Hub
# ──────────────────────────────────────────────────────────────
class PatternHub:
    def __init__(self, redis: Redis, prefix: str):
        self._redis = redis
        self._prefix = prefix if prefix.endswith(":") else f"{prefix}:"
        self._conns: Dict[int, List[WebSocket]] = defaultdict(list)
        self._rx_task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._rx_task and not self._rx_task.done():
            return
        pubsub = self._redis.pubsub()
        await pubsub.psubscribe(f"{self._prefix}*")
        self._rx_task = asyncio.create_task(self._fan_in(pubsub))

    async def _fan_in(self, pubsub):
        async for m in pubsub.listen():
            if m.get("type") != "pmessage":
                continue
            channel: str | None = m.get("channel")
            payload = m.get("data")
            if not isinstance(channel, str):
                continue
            key = self._extract_key(channel)
            if key is not None:
                await self._fan_out(key, payload)

    def _extract_key(self, channel: str) -> int | None:
        try:
            # "<prefix>:<int>"
            return int(channel.split(":", 1)[1])
        except Exception:
            return None

    async def _fan_out(self, key: int, payload: str) -> None:
        stale: List[WebSocket] = []
        for ws in list(self._conns[key]):
            try:
                if ws.application_state == WebSocketState.CONNECTED:
                    await ws.send_text(payload)
            except Exception:
                stale.append(ws)
        for ws in stale:
            await self.leave(key, ws)

    async def join(self, key: int, ws: WebSocket) -> None:
        conns = self._conns[key]
        if ws not in conns:
            conns.append(ws)

    async def leave(self, key: int, ws: WebSocket) -> None:
        with suppress(ValueError):
            self._conns[key].remove(ws)

    async def publish(self, key: int, obj) -> None:
        payload = obj if isinstance(obj, str) else json.dumps(obj, default=str)
        await self._redis.publish(f"{self._prefix}{key}", payload)


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def get_sessions_data(gym_id: int, db: Session) -> list:
    now = datetime.now()
    today_date = now.date()
    current_time = now.time()

    sessions_query = (
        db.query(
            New_Session,
            Client.name.label("host_name"),
            Client.profile.label("host_profile"),
            Client.gender.label("host_gender"),
        )
        .join(Client, New_Session.host_id == Client.client_id)
        .filter(New_Session.gym_id == gym_id)
        .filter(
            or_(
                New_Session.session_date > today_date,
                and_(
                    New_Session.session_date == today_date,
                    New_Session.session_time >= current_time,
                ),
            )
        )
        .order_by(asc(New_Session.session_date), asc(New_Session.session_time))
        .all()
    )

    sessions_data = []
    for session, host_name, host_profile, host_gender in sessions_query:
        participants_query = (
            db.query(
                Participant,
                Client.name.label("participant_name"),
                Client.gender.label("participant_gender"),
                Client.profile.label("participant_profile"),
            )
            .join(Client, Participant.user_id == Client.client_id)
            .filter(Participant.session_id == session.session_id)
            .all()
        )
        participants_list = [
            {
                "participant_id": p.participant_id,
                "user_id": p.user_id,
                "participant_name": n,
                "gender": g,
                "participant_profile": prof,
                "proposed_time": p.proposed_time,
            }
            for p, n, g, prof in participants_query
        ]

        proposals_query = (
            db.query(
                JoinProposal,
                Client.name.label("proposer_name"),
                Client.profile.label("proposer_profile"),
            )
            .join(Client, JoinProposal.proposer_id == Client.client_id)
            .filter(JoinProposal.session_id == session.session_id)
            .all()
        )
        join_proposals_list = [
            {
                "proposal_id": pr.proposal_id,
                "proposer_id": pr.proposer_id,
                "proposer_name": n,
                "proposer_profile": prof,
                "proposal_time": pr.proposed_time,
            }
            for pr, n, prof in proposals_query
        ]

        rejected_ids = [
            row[0]
            for row in db.query(RejectedProposal.user_id)
            .filter(RejectedProposal.session_id == session.session_id)
            .all()
        ]

        workout_images: List[str] = []
        workout_type_list = (
            session.workout_type if isinstance(session.workout_type, list) else [session.workout_type]
        )
        for muscle_group in workout_type_list:
            muscle_images = (
                db.query(FittbotMuscleGroup.url)
                .filter(
                    FittbotMuscleGroup.muscle_group == muscle_group,
                    FittbotMuscleGroup.gender == host_gender,
                )
                .all()
            )
            workout_images.extend([img[0] for img in muscle_images])

        sessions_data.append(
            {
                "session_id": session.session_id,
                "gym_id": session.gym_id,
                "workout_type": session.workout_type,
                "workout_images": workout_images,
                "session_date": session.session_date,
                "session_time": session.session_time,
                "host_id": session.host_id,
                "participant_limit": session.participant_limit,
                "gender_preference": session.gender_preference,
                "host_name": host_name,
                "host_profile": host_profile,
                "participant_count": len(participants_list),
                "participants": participants_list,
                "requests": join_proposals_list,
                "rejected": rejected_ids,
            }
        )
    return sessions_data


async def _verify_jwt(ws: WebSocket) -> bool:
    token = ws.headers.get("authorization") or ws.query_params.get("token")
    if not token:
        await ws.close(code=1008)
        return False
    if token.startswith("Bearer "):
        token = token.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        ws.scope["user"] = payload.get("sub")
        return True
    except (jwt.ExpiredSignatureError, JWTError):
        await ws.close(code=4401, reason="Unauthorized")
        return False


# ──────────────────────────────────────────────────────────────
# WebSocket endpoints
# ──────────────────────────────────────────────────────────────
@router.websocket("/ws/sessions/{gym_id}")
async def ws_sessions(ws: WebSocket, gym_id: int):
    await ws.accept()
    s_hub, _, _ = await _ensure_hubs()
    await s_hub.join(gym_id, ws)

    db = SessionLocal()
    try:
        sessions_data = get_sessions_data(gym_id, db)
        await ws.send_text(
            json.dumps({"action": "session_data", "data": sessions_data}, default=str)
        )
    finally:
        db.close()

    try:
        while True:
            await ws.receive_text()  # keep alive; ignore inbound messages
    except WebSocketDisconnect:
        await s_hub.leave(gym_id, ws)


@router.websocket("/ws/live/{gym_id}")
async def ws_live_gym(ws: WebSocket, gym_id: int):
    await ws.accept()
    _, l_hub, _ = await _ensure_hubs()
    await l_hub.join(gym_id, ws)

    db = SessionLocal()
    try:
        current_clients = (
            db.query(
                Attendance.client_id,
                Attendance.in_time,
                Attendance.in_time_2,
                Attendance.in_time_3,
                Attendance.out_time,
                Attendance.out_time_2,
                Attendance.out_time_3,
                Client.name,
                Client.training_id,
                Client.goals,
                Client.profile,
                Client.client_id,
                Attendance.muscle,
                Attendance.muscle_2,
                Attendance.muscle_3,
            )
            .join(Client, Attendance.client_id == Client.client_id)
            .filter(
                Attendance.date == date.today(),
                Attendance.gym_id == gym_id,
                Client.gym_id == gym_id,
                or_(
                    and_(Attendance.in_time.isnot(None), Attendance.out_time.is_(None)),
                    and_(Attendance.in_time_2.isnot(None), Attendance.out_time_2.is_(None)),
                    and_(Attendance.in_time_3.isnot(None), Attendance.out_time_3.is_(None)),
                ),
            )
            .all()
        )

        # Query manual clients currently in gym
        manual_present = (
            db.query(
                ManualAttendance.manual_client_id,
                ManualClient.name,
                ManualClient.dp.label("profile"),
                ManualClient.goal,
            )
            .join(ManualClient, ManualAttendance.manual_client_id == ManualClient.id)
            .filter(
                ManualAttendance.date == date.today(),
                ManualAttendance.gym_id == gym_id,
                ManualAttendance.in_time.isnot(None),
                ManualAttendance.out_time.is_(None),
            )
            .all()
        )

        # Query import clients currently in gym
        import_present = (
            db.query(
                ImportClientAttendance.import_client_id,
                GymImportData.client_name.label("name"),
            )
            .join(GymImportData, ImportClientAttendance.import_client_id == GymImportData.import_id)
            .filter(
                ImportClientAttendance.date == date.today(),
                ImportClientAttendance.gym_id == gym_id,
                ImportClientAttendance.in_time.isnot(None),
                ImportClientAttendance.out_time.is_(None),
            )
            .all()
        )

        # Calculate live_count: number of unique clients currently in gym (regular + manual + import)
        live_count = len(current_clients) + len(manual_present) + len(import_present)

        goals_summary: Dict[str, Dict] = {}
        training_type_summary: Dict[str, Dict] = {}
        muscle_summary: Dict[str, Dict] = {}
        present_clients: List[Dict] = []

        for client in current_clients:
            goal_key = client.goals or "Unknown"
            goals_summary.setdefault(goal_key, {"count": 0, "clients": []})
            goals_summary[goal_key]["count"] += 1
            goals_summary[goal_key]["clients"].append(client.name)

            training_type = (
                db.query(GymPlans.plans).filter(GymPlans.id == client.training_id).scalar()
            )
            training_key = training_type or "Unknown"
            training_type_summary.setdefault(training_key, {"count": 0, "clients": []})
            training_type_summary[training_key]["count"] += 1
            training_type_summary[training_key]["clients"].append(client.name)

            # Determine which punch-in is active and use the corresponding muscle group
            # Check from latest to earliest to find the active (not punched out) session
            active_muscle = None
            if client.in_time_3 is not None and client.out_time_3 is None:
                active_muscle = client.muscle_3
            elif client.in_time_2 is not None and client.out_time_2 is None:
                active_muscle = client.muscle_2
            elif client.in_time is not None and client.out_time is None:
                active_muscle = client.muscle

            if active_muscle:
                for muscle in active_muscle:
                    muscle_summary.setdefault(muscle, {"count": 0, "clients": []})
                    muscle_summary[muscle]["count"] += 1
                    muscle_summary[muscle]["clients"].append(client.name)

            present_clients.append(
                {"name": client.name, "profile": client.profile, "client_id": client.client_id}
            )

        # Add manual clients to present_clients list
        for m in manual_present:
            # Add manual client goal to summary if available
            if m.goal:
                goals_summary.setdefault(m.goal, {"count": 0, "clients": []})
                goals_summary[m.goal]["count"] += 1
                goals_summary[m.goal]["clients"].append(m.name)

            present_clients.append(
                {
                    "client_id": f"manual_{m.manual_client_id}",
                    "manual_client_id": m.manual_client_id,
                    "name": m.name,
                    "profile": m.profile,
                }
            )

        # Add import clients to present_clients list
        for i in import_present:
            present_clients.append(
                {
                    "client_id": f"import_{i.import_client_id}",
                    "import_client_id": i.import_client_id,
                    "name": i.name,
                    "profile": None,
                }
            )

        top_goal = max(
            goals_summary.items(), key=lambda x: x[1]["count"], default=(None, {})
        )[0]
        top_training_type = max(
            training_type_summary.items(), key=lambda x: x[1]["count"], default=(None, {})
        )[0]
        top_muscle = max(
            muscle_summary.items(), key=lambda x: x[1]["count"], default=(None, {})
        )[0]

        male_url = female_url = ""
        if top_muscle:
            pics = (
                db.query(FittbotMuscleGroup.gender, FittbotMuscleGroup.url)
                .filter(FittbotMuscleGroup.muscle_group == top_muscle)
                .all()
            )
            if pics:
                pics_map = {g: u for g, u in pics}
                male_url = pics_map.get("male", "")
                female_url = pics_map.get("female", "")

        await ws.send_text(
            json.dumps(
                {
                    "action": "get_initial_data",
                    "live_count": live_count,
                    "total_present": live_count,
                    "goals_summary": goals_summary,
                    "training_type_summary": training_type_summary,
                    "muscle_summary": muscle_summary,
                    "top_goal": top_goal,
                    "top_training_type": top_training_type,
                    "top_muscle": top_muscle,
                    "present_clients": present_clients,
                    "male_url": male_url,
                    "female_url": female_url,
                },
                default=str,
            )
        )
    finally:
        db.close()

    try:
        while True:
            await ws.receive_text()  # ignore inbound; keep connection alive
    except WebSocketDisconnect:
        await l_hub.leave(gym_id, ws)


@router.websocket("/ws/chat/{session_id}")
async def ws_chat(ws: WebSocket, session_id: int):
    await ws.accept()


    _, _, c_hub = await _ensure_hubs()
    await c_hub.join(session_id, ws)

    db = SessionLocal()
    try:
        msgs = (
            db.query(GBMessage, Client.name.label("client_name"))
            .join(Client, GBMessage.client_id == Client.client_id)
            .filter(GBMessage.session_id == session_id)
            .order_by(GBMessage.sent_at.asc())
            .all()
        )
        history = [
            {
                "id": m.GBMessage.id,
                "client_id": m.GBMessage.client_id,
                "client_name": m.client_name,
                "session_id": m.GBMessage.session_id,
                "message": m.GBMessage.message,
                "sent_at": m.GBMessage.sent_at.isoformat(),
            }
            for m in msgs
        ]
        await ws.send_text(json.dumps({"action": "old_messages", "data": history}))
    except Exception as exc:
        await ws.send_text(json.dumps({"action": "error", "message": f"Error fetching history: {exc}"}))

    try:
        while True:
            data = await ws.receive_text()
            payload = json.loads(data)
            action = payload.get("action")

            if action == "send":
                try:
                    new_msg = GBMessage(
                        client_id=payload["client_id"],
                        session_id=session_id,
                        message=payload["message"],
                        sent_at=datetime.now(),
                    )
                    db.add(new_msg)
                    db.commit()
                    db.refresh(new_msg)

                    client = db.query(Client).filter(Client.client_id == payload["client_id"]).first()
                    resp = {
                        "action": "new_message",
                        "data": {
                            "id": new_msg.id,
                            "client_id": new_msg.client_id,
                            "client_name": client.name if client else None,
                            "session_id": new_msg.session_id,
                            "message": new_msg.message,
                            "sent_at": new_msg.sent_at.isoformat(),
                        },
                    }
                    await c_hub.publish(session_id, resp)
                except Exception as exc:
                    db.rollback()
                    await ws.send_text(json.dumps({"action": "error", "message": f"send failed: {exc}"}))

            elif action == "edit":
                try:
                    msg_id = payload["message_id"]
                    new_text = payload["message"]
                    rec = db.query(GBMessage).filter(GBMessage.id == msg_id).first()
                    if not rec:
                        raise ValueError("Message not found")
                    rec.message = new_text
                    db.commit()
                    resp = {
                        "action": "edit_message",
                        "data": {
                            "id": rec.id,
                            "client_id": rec.client_id,
                            "session_id": rec.session_id,
                            "message": rec.message,
                            "edited_at": datetime.now().isoformat(),
                        },
                    }
                    await c_hub.publish(session_id, resp)
                except Exception as exc:
                    db.rollback()
                    await ws.send_text(json.dumps({"action": "error", "message": f"edit failed: {exc}"}))

            elif action == "delete":
                try:
                    ids = payload.get("message_ids", [])
                    if not ids:
                        raise ValueError("No IDs supplied")
                    db.query(GBMessage).filter(GBMessage.id.in_(ids)).delete(synchronize_session=False)
                    db.commit()
                    resp = {"action": "delete_message", "data": {"message_ids": ids}}
                    await c_hub.publish(session_id, resp)
                except Exception as exc:
                    db.rollback()
                    await ws.send_text(json.dumps({"action": "error", "message": f"delete failed: {exc}"}))
            else:
                await ws.send_text(json.dumps({"action": "error", "message": "Unknown action"}))
    except WebSocketDisconnect:
        await c_hub.leave(session_id, ws)
    finally:
        db.close()
