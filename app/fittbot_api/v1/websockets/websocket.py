# websocket_endpoints.py
# Refactored to remove logger usage and use FittbotHTTPException
# where applicable (HTTP routes). WebSocket paths handle errors
# by sending messages/closing the socket.

from __future__ import annotations

import os
import json
from collections import defaultdict
from datetime import datetime, date
from typing import Dict, List

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Header
from jose import jwt, JWTError
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session
from starlette.websockets import WebSocketState

from app.models.database import SessionLocal
from app.models.fittbot_models import (
    GBMessage,
    Client,
    LiveCount,
    New_Session,
    Participant,
    JoinProposal,
    RejectedProposal,
    Attendance,
    FittbotMuscleGroup,
    GymPlans,
)
from app.utils.security import SECRET_KEY, ALGORITHM
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/websocket", tags=["Websocket_contents"])


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _decode_bearer(token_or_header: str | None) -> str | None:
    if not token_or_header:
        return None
    tok = token_or_header
    if tok.startswith("Bearer "):
        tok = tok.split(" ", 1)[1]
    return tok or None


def _jwt_verify_to_ws_scope(ws: WebSocket) -> bool:
    """Verify Authorization header or token query param; set ws.scope['user']."""
    token = ws.headers.get("authorization") or ws.query_params.get("token")
    token = _decode_bearer(token)
    if not token:
        return False
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        ws.scope["user"] = payload.get("sub")
        return True
    except (jwt.ExpiredSignatureError, JWTError):
        return False


# ──────────────────────────────────────────────────────────────
# Session data builder
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
        .order_by(New_Session.session_date.asc(), New_Session.session_time.asc())
        .all()
    )

    sessions_data = []
    for session, host_name, host_profile in sessions_query:
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

        rejected_rows = (
            db.query(RejectedProposal.user_id)
            .filter(RejectedProposal.session_id == session.session_id)
            .all()
        )
        rejected_user_ids = [row[0] for row in rejected_rows] if rejected_rows else []

        sessions_data.append(
            {
                "session_id": session.session_id,
                "gym_id": session.gym_id,
                "workout_type": session.workout_type,
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
                "rejected": rejected_user_ids,
            }
        )
    return sessions_data


# ──────────────────────────────────────────────────────────────
# Connection Managers
# ──────────────────────────────────────────────────────────────
class _BaseConnMgr:
    def __init__(self):
        self.active_connections: Dict[int, List[WebSocket]] = defaultdict(list)

    async def connect(self, key: int, websocket: WebSocket):
        conns = self.active_connections[key]
        if websocket not in conns:
            conns.append(websocket)

    def disconnect(self, key: int, websocket: WebSocket):
        if key in self.active_connections and websocket in self.active_connections[key]:
            self.active_connections[key].remove(websocket)

    async def _safe_send(self, ws: WebSocket, message: str) -> bool:
        try:
            if ws.application_state == WebSocketState.CONNECTED:
                await ws.send_text(message)
                return True
            return False
        except Exception:
            return False

    async def broadcast(self, key: int, message: str):
        conns = self.active_connections.get(key, [])
        if not conns:
            return
        stale: List[WebSocket] = []
        for ws in conns:
            ok = await self._safe_send(ws, message)
            if not ok:
                stale.append(ws)
        for ws in stale:
            self.disconnect(key, ws)


class SessionUpdateManager(_BaseConnMgr):
    pass


class LiveGymConnectionManager(_BaseConnMgr):
    pass


class ChatConnectionManager(_BaseConnMgr):
    pass


class PostConnectionManager(_BaseConnMgr):
    pass


session_update_manager = SessionUpdateManager()
live_gym_manager = LiveGymConnectionManager()
chat_manager = ChatConnectionManager()
post_mgr = PostConnectionManager()


# ──────────────────────────────────────────────────────────────
# WebSocket: Sessions
# ──────────────────────────────────────────────────────────────
@router.websocket("/ws/sessions/{gym_id}")
async def session_updates_endpoint(websocket: WebSocket, gym_id: int):
    await websocket.accept()

    # Auth
    if not _jwt_verify_to_ws_scope(websocket):
        await websocket.close(code=4401, reason="Unauthorized")
        return

    await session_update_manager.connect(gym_id, websocket)

    # DB session
    db_gen = SessionLocal()
    db = db_gen
    try:
        sessions_data = get_sessions_data(gym_id, db)
        await websocket.send_text(
            json.dumps({"action": "session_data", "data": sessions_data}, default=str)
        )
    except Exception as e:
        await websocket.send_text(json.dumps({"status": 500, "error": str(e)}))
    finally:
        db.close()

    try:
        while True:
            await websocket.receive_text()  # keep alive / ignore incoming
    except WebSocketDisconnect:
        session_update_manager.disconnect(gym_id, websocket)


# ──────────────────────────────────────────────────────────────
# WebSocket: Live Gym
# ──────────────────────────────────────────────────────────────
@router.websocket("/ws/live/{gym_id}")
async def live_gym_endpoint(websocket: WebSocket, gym_id: int):
    await websocket.accept()

    if not _jwt_verify_to_ws_scope(websocket):
        await websocket.close(code=4401, reason="Unauthorized")
        return

    await live_gym_manager.connect(gym_id, websocket)

    db = SessionLocal()
    try:
        rec = db.query(LiveCount).filter(LiveCount.gym_id == gym_id).first()
        if not rec:
            rec = LiveCount(gym_id=gym_id, count=0)
            db.add(rec)
            db.commit()
            db.refresh(rec)
        else:
            db.refresh(rec)
        live_count = rec.count

        # Present clients + muscle summary
        rows = (
            db.query(
                Attendance.client_id,
                Attendance.in_time,
                Attendance.in_time_2,
                Attendance.in_time_3,
                Attendance.muscle,
                Attendance.muscle_2,
                Attendance.muscle_3,
                Client.name,
                Client.training_id,
                Client.goals,
                Client.profile,
            )
            .join(Client, Attendance.client_id == Client.client_id)
            .filter(
                Attendance.date == date.today(),
                Client.gym_id == gym_id,
                or_(
                    and_(Attendance.in_time.isnot(None), Attendance.out_time.is_(None)),
                    and_(
                        Attendance.in_time_2.isnot(None),
                        Attendance.out_time_2.is_(None),
                    ),
                    and_(
                        Attendance.in_time_3.isnot(None),
                        Attendance.out_time_3.is_(None),
                    ),
                ),
            )
            .all()
        )

        muscle_summary: Dict[str, Dict[str, list | int]] = {}
        present_clients: List[dict] = []
        seen_client_ids = set()

        def _iter_muscles(m1, m2, m3):
            for v in (m1, m2, m3):
                if not v:
                    continue
                if isinstance(v, list):
                    for it in v:
                        yield it
                else:
                    yield v

        for r in rows:
            for muscle in _iter_muscles(r.muscle, r.muscle_2, r.muscle_3):
                d = muscle_summary.setdefault(muscle, {"count": 0, "clients": []})
                d["count"] += 1
                d["clients"].append(r.name)

            if r.client_id not in seen_client_ids:
                seen_client_ids.add(r.client_id)
                present_clients.append({"name": r.name, "profile": r.profile})

        top_muscle = max(
            muscle_summary.items(), key=lambda x: x[1]["count"], default=(None, {})
        )[0]

        print("muscle summary is",muscle_summary)
        print("top muscle is",top_muscle)

        await websocket.send_text(
            json.dumps(
                {
                    "action": "get_initial_data",
                    "live_count": live_count,
                    "muscle_summary": muscle_summary,
                    "top_muscle": top_muscle,
                    "present_clients": present_clients,
                },
                default=str,
            )
        )
    except Exception as e:
        await websocket.send_text(
            json.dumps({"action": "error", "message": f"Error retrieving data: {e}"})
        )
    finally:
        db.close()

    try:
        while True:
            await websocket.receive_text()  # echo disabled; keep alive
    except WebSocketDisconnect:
        live_gym_manager.disconnect(gym_id, websocket)
    except Exception:
        await websocket.close()


# ──────────────────────────────────────────────────────────────
# WebSocket: Chat
# ──────────────────────────────────────────────────────────────
@router.websocket("/ws/chat/{session_id}")
async def chat_endpoint(websocket: WebSocket, session_id: int):
    await websocket.accept()

    if not _jwt_verify_to_ws_scope(websocket):
        await websocket.close(code=4401, reason="Unauthorized")
        return

    await chat_manager.connect(session_id, websocket)

    db = SessionLocal()
    try:
        old_messages_query = (
            db.query(GBMessage, Client.name.label("client_name"))
            .join(Client, GBMessage.client_id == Client.client_id)
            .filter(GBMessage.session_id == session_id)
            .order_by(GBMessage.sent_at.asc())
            .all()
        )
        old_messages = [
            {
                "id": msg.GBMessage.id,
                "client_id": msg.GBMessage.client_id,
                "client_name": msg.client_name,
                "session_id": msg.GBMessage.session_id,
                "message": msg.GBMessage.message,
                "sent_at": msg.GBMessage.sent_at.isoformat(),
            }
            for msg in old_messages_query
        ]
        await websocket.send_text(
            json.dumps({"action": "old_messages", "data": old_messages})
        )
    except Exception as e:
        await websocket.send_text(
            json.dumps(
                {"action": "error", "message": f"Error fetching old messages: {e}"}
            )
        )

    try:
        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
            except Exception:
                await websocket.send_text(
                    json.dumps(
                        {"action": "error", "message": "Invalid JSON payload"}
                    )
                )
                continue

            action = payload.get("action")
            if action == "send":
                try:
                    new_message = GBMessage(
                        client_id=payload["client_id"],
                        session_id=session_id,
                        message=payload["message"],
                        sent_at=datetime.now(),
                    )
                    db.add(new_message)
                    db.commit()
                    db.refresh(new_message)

                    client = (
                        db.query(Client)
                        .filter(Client.client_id == payload["client_id"])
                        .first()
                    )
                    client_name = client.name if client else None

                    response = {
                        "action": "new_message",
                        "data": {
                            "id": new_message.id,
                            "client_id": new_message.client_id,
                            "client_name": client_name,
                            "session_id": new_message.session_id,
                            "message": new_message.message,
                            "sent_at": new_message.sent_at.isoformat(),
                        },
                    }
                    await chat_manager.broadcast(session_id, json.dumps(response))
                except Exception as e:
                    db.rollback()
                    await websocket.send_text(
                        json.dumps(
                            {
                                "action": "error",
                                "message": f"Error sending message: {e}",
                            }
                        )
                    )

            elif action == "edit":
                try:
                    message_id = payload.get("message_id")
                    new_text = payload.get("message")
                    msg_record = (
                        db.query(GBMessage).filter(GBMessage.id == message_id).first()
                    )
                    if not msg_record:
                        await websocket.send_text(
                            json.dumps(
                                {"action": "error", "message": "Message not found"}
                            )
                        )
                        continue
                    msg_record.message = new_text
                    db.commit()
                    response = {
                        "action": "edit_message",
                        "data": {
                            "id": msg_record.id,
                            "client_id": msg_record.client_id,
                            "session_id": msg_record.session_id,
                            "message": msg_record.message,
                            "edited_at": datetime.now().isoformat(),
                        },
                    }
                    await chat_manager.broadcast(session_id, json.dumps(response))
                except Exception as e:
                    db.rollback()
                    await websocket.send_text(
                        json.dumps(
                            {"action": "error", "message": f"Error editing message: {e}"}
                        )
                    )

            elif action == "delete":
                try:
                    message_ids = payload.get("message_ids", [])
                    if not message_ids:
                        await websocket.send_text(
                            json.dumps(
                                {"action": "error", "message": "No IDs supplied"}
                            )
                        )
                        continue
                    db.query(GBMessage).filter(GBMessage.id.in_(message_ids)).delete(
                        synchronize_session=False
                    )
                    db.commit()
                    response = {
                        "action": "delete_message",
                        "data": {"message_ids": message_ids},
                    }
                    await chat_manager.broadcast(session_id, json.dumps(response))
                except Exception as e:
                    db.rollback()
                    await websocket.send_text(
                        json.dumps(
                            {
                                "action": "error",
                                "message": f"Error deleting messages: {e}",
                            }
                        )
                    )
            else:
                await websocket.send_text(
                    json.dumps({"action": "error", "message": "Unknown action"})
                )
    except WebSocketDisconnect:
        chat_manager.disconnect(session_id, websocket)
    finally:
        db.close()


# ──────────────────────────────────────────────────────────────
# HTTP: internal new post → notify WS clients
# ──────────────────────────────────────────────────────────────
@router.post("/internal/new_post")
async def internal_new_post(
    payload: dict,
    x_api_key: str = Header(...),
):
    try:
        expected = os.getenv("LAMBDA_HEADER", "lambda_header_feed_not_out")
        if x_api_key != expected:
            raise FittbotHTTPException(
                status_code=401,
                detail="Invalid API key",
                error_code="WS_INVALID_API_KEY",
                log_data={"provided": "***masked***"},
                security_event=True,
            )

        try:
            gym_id = int(payload["gym_id"])
            post_id = int(payload["post_id"])
        except Exception:
            raise FittbotHTTPException(
                status_code=422,
                detail="Invalid payload format",
                error_code="WS_INVALID_PAYLOAD",
                log_data={"keys": list(payload.keys()) if isinstance(payload, dict) else "non-dict"},
            )

        message = json.dumps(
            {"action": "new_post", "gym_id": gym_id, "post_id": post_id}
        )
        await post_mgr.broadcast(gym_id, message)
        return {"ok": True}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Internal error while posting websocket notification",
            error_code="WS_POST_NOTIFY_ERROR",
            log_data={"error": repr(e)},
        )


# ──────────────────────────────────────────────────────────────
# WebSocket: posts feed
# ──────────────────────────────────────────────────────────────
@router.websocket("/ws/posts/{gym_id}")
async def posts_ws(ws: WebSocket, gym_id: int):
    await ws.accept()
    await post_mgr.connect(gym_id, ws)

    # send a probe so the client can verify wire-up
    await ws.send_text('{"action":"probe","msg":"hello"}')

    try:
        while True:
            await ws.receive_text()  # keep alive
    except WebSocketDisconnect:
        post_mgr.disconnect(gym_id, ws)
