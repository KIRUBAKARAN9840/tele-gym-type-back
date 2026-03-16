# app/api/v1/gym_buddy/sessions.py

import json
from datetime import datetime, date
from datetime import time as dt_time
from typing import List, Dict, Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_,asc

from app.models.database import get_db
from app.utils.logging_utils import FittbotHTTPException
from app.fittbot_api.v1.websockets.websocket import session_update_manager  # broadcast helper

# ---- Models referenced in queries (adjust import path to your project) ----
from app.models.fittbot_models import (
    FittbotMuscleGroup,
    New_Session,
    Client,
    Participant,
    JoinProposal,
    RejectedProposal,
)

router = APIRouter(prefix="/gym_buddy", tags=["Clients"])


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

    # === BATCH LOAD ALL DATA UPFRONT (N+1 FIX) ===
    session_ids = [s.session_id for s, _, _, _ in sessions_query]

    # BATCH 1: Load all participants for all sessions at once
    all_participants = {}
    if session_ids:
        participants_data = (
            db.query(
                Participant,
                Client.name.label("participant_name"),
                Client.gender.label("participant_gender"),
                Client.profile.label("participant_profile"),
            )
            .join(Client, Participant.user_id == Client.client_id)
            .filter(Participant.session_id.in_(session_ids))
            .all()
        )
        for p, n, g, prof in participants_data:
            if p.session_id not in all_participants:
                all_participants[p.session_id] = []
            all_participants[p.session_id].append({
                "participant_id": p.participant_id,
                "user_id": p.user_id,
                "participant_name": n,
                "gender": g,
                "participant_profile": prof,
                "proposed_time": p.proposed_time,
            })

    # BATCH 2: Load all proposals for all sessions at once
    all_proposals = {}
    if session_ids:
        proposals_data = (
            db.query(
                JoinProposal,
                Client.name.label("proposer_name"),
                Client.profile.label("proposer_profile"),
            )
            .join(Client, JoinProposal.proposer_id == Client.client_id)
            .filter(JoinProposal.session_id.in_(session_ids))
            .all()
        )
        for pr, n, prof in proposals_data:
            if pr.session_id not in all_proposals:
                all_proposals[pr.session_id] = []
            all_proposals[pr.session_id].append({
                "proposal_id": pr.proposal_id,
                "proposer_id": pr.proposer_id,
                "proposer_name": n,
                "proposer_profile": prof,
                "proposal_time": pr.proposed_time,
            })

    # BATCH 3: Load all rejected proposals for all sessions at once
    all_rejected = {}
    if session_ids:
        rejected_data = (
            db.query(RejectedProposal.session_id, RejectedProposal.user_id)
            .filter(RejectedProposal.session_id.in_(session_ids))
            .all()
        )
        for session_id, user_id in rejected_data:
            if session_id not in all_rejected:
                all_rejected[session_id] = []
            all_rejected[session_id].append(user_id)

    # BATCH 4: Load all muscle group images at once
    all_workout_types = set()
    all_genders = set()
    for session, _, _, host_gender in sessions_query:
        workout_type_list = session.workout_type if isinstance(session.workout_type, list) else [session.workout_type]
        for wt in workout_type_list:
            if wt:
                all_workout_types.add(wt)
        if host_gender:
            all_genders.add(host_gender)

    muscle_images_map = {}
    if all_workout_types and all_genders:
        muscle_data = (
            db.query(FittbotMuscleGroup.muscle_group, FittbotMuscleGroup.gender, FittbotMuscleGroup.url)
            .filter(
                FittbotMuscleGroup.muscle_group.in_(all_workout_types),
                FittbotMuscleGroup.gender.in_(all_genders),
            )
            .all()
        )
        for mg, gender, url in muscle_data:
            key = (mg, gender)
            if key not in muscle_images_map:
                muscle_images_map[key] = []
            muscle_images_map[key].append(url)

    # === PROCESS SESSIONS IN MEMORY (NO MORE QUERIES IN LOOP) ===
    sessions_data = []
    for session, host_name, host_profile, host_gender in sessions_query:
        # Get from pre-loaded maps (was N+1 queries)
        participants_list = all_participants.get(session.session_id, [])
        join_proposals_list = all_proposals.get(session.session_id, [])
        rejected_ids = all_rejected.get(session.session_id, [])

        workout_images: List[str] = []
        workout_type_list = (
            session.workout_type if isinstance(session.workout_type, list) else [session.workout_type]
        )
        for muscle_group in workout_type_list:
            # Get images from pre-loaded map (was N+1 query)
            images = muscle_images_map.get((muscle_group, host_gender), [])
            workout_images.extend(images)

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




# ------------------------ Schemas (unchanged) ------------------------
class SessionCreate(BaseModel):
    gym_id: int
    workout_type: List[str]
    session_date: date
    session_time: dt_time
    host_id: int
    participant_limit: int
    gender_preference: str


class JoinProposalCreate(BaseModel):
    session_id: int
    gym_id: int
    proposer_id: int
    proposed_time: dt_time


class AcceptProposalCreate(BaseModel):
    session_id: int
    proposal_id: int
    gym_id: int


class SessionRescheduleRequest(BaseModel):
    session_id: int
    gym_id: int
    new_session_date: date
    new_session_time: dt_time


class SessionParticipantRemovalRequest(BaseModel):
    session_id: int
    gym_id: int
    client_ids: List[int]


# ------------------------ Endpoints ------------------------
@router.post("/create_session")
async def create_session(
    http_request: Request,
    session: SessionCreate,
    db: Session = Depends(get_db),
):
    try:
        if not session.workout_type or len(session.workout_type) == 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="At least one muscle group must be selected",
                error_code="NO_WORKOUT_TYPES",
                log_data={"host_id": session.host_id, "gym_id": session.gym_id},
            )

        db_session = New_Session(
            workout_type=session.workout_type,
            gym_id=session.gym_id,
            session_time=session.session_time,
            session_date=session.session_date,
            host_id=session.host_id,
            participant_limit=session.participant_limit,
            gender_preference=session.gender_preference,
        )
        db.add(db_session)
        db.commit()
        db.refresh(db_session)

        sessions_data = get_sessions_data(session.gym_id, db)

        message = json.dumps({"action": "update_sessions", "data": sessions_data}, default=str)
        await http_request.app.state.session_hub.publish(session.gym_id, message)

        return {"status": 200, "data": db_session}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error creating session: {str(e)}",
            error_code="CREATE_SESSION_ERROR",
            log_data={"host_id": session.host_id, "gym_id": session.gym_id, "error": str(e)},
        )


@router.get("/get_session")
async def get_sessions(gym_id: int, db: Session = Depends(get_db)):
    try:
        now = datetime.now()  # kept (even if not reused) to preserve original logic
        today_date = now.date()
        current_time = now.time()
        sessionkey = f"gym:{gym_id}:buddysessions"  # retained (unused) to keep logic untouched

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

            participants_list = []
            for (
                participant,
                participant_name,
                participant_gender,
                participant_profile,
            ) in participants_query:
                participants_list.append(
                    {
                        "participant_id": participant.participant_id,
                        "user_id": participant.user_id,
                        "participant_name": participant_name,
                        "gender": participant_gender,
                        "participant_profile": participant_profile,
                        "proposed_time": participant.proposed_time,
                    }
                )

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
            join_proposals_list = []
            for proposal, proposer_name, proposer_profile in proposals_query:
                join_proposals_list.append(
                    {
                        "proposal_id": proposal.proposal_id,
                        "proposer_id": proposal.proposer_id,
                        "proposer_name": proposer_name,
                        "proposer_profile": proposer_profile,
                        "proposal_time": proposal.proposed_time,
                    }
                )

            participant_count = len(participants_list)
            rejected_rows = (
                db.query(RejectedProposal.user_id)
                .filter(RejectedProposal.session_id == session.session_id)
                .all()
            )
            rejected_user_ids = [row[0] for row in rejected_rows] if rejected_rows else []

            session_data = {
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
                "participant_count": participant_count,
                "participants": participants_list,
                "requests": join_proposals_list,
                "rejected": rejected_user_ids,
            }

            sessions_data.append(session_data)

        print(sessions_data)
        return {"status": 200, "data": sessions_data}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error fetching sessions: {str(e)}",
            error_code="GET_SESSIONS_ERROR",
            log_data={"gym_id": gym_id, "error": str(e)},
        )


@router.post("/join_session")
async def create_join_proposal(
    http_request: Request,
    proposal: JoinProposalCreate,
    db: Session = Depends(get_db),
):
    try:
        proposer = db.query(Client).filter(Client.client_id == proposal.proposer_id).first()
        if not proposer:
            raise FittbotHTTPException(
                status_code=404,
                detail="Proposer not found",
                error_code="PROPOSER_NOT_FOUND",
                log_data={"proposer_id": proposal.proposer_id},
            )

        session_obj = db.query(New_Session).filter(New_Session.session_id == proposal.session_id).first()
        if not session_obj:
            raise FittbotHTTPException(
                status_code=404,
                detail="Session not found",
                error_code="SESSION_NOT_FOUND",
                log_data={"session_id": proposal.session_id},
            )

        if session_obj.gender_preference.lower() != "any":
            if proposer.gender.lower() != session_obj.gender_preference.lower():
                raise FittbotHTTPException(
                    status_code=400,
                    detail="You cannot participate: Your gender does not match the session's preference.",
                    error_code="GENDER_MISMATCH",
                    log_data={
                        "proposer_gender": getattr(proposer, "gender", None),
                        "required": session_obj.gender_preference,
                    },
                )

        db_proposal = JoinProposal(
            session_id=proposal.session_id,
            proposer_id=proposal.proposer_id,
            proposed_time=proposal.proposed_time,
        )
        db.add(db_proposal)
        db.commit()
        db.refresh(db_proposal)

        rejected = (
            db.query(RejectedProposal)
            .filter(
                RejectedProposal.session_id == proposal.session_id,
                RejectedProposal.user_id == proposal.proposer_id,
            )
            .all()
        )
        if rejected:
            for item in rejected:
                db.delete(item)
            db.commit()

        sessions_data = get_sessions_data(proposal.gym_id, db)
        message = json.dumps({"action": "update_sessions", "data": sessions_data}, default=str)
        await http_request.app.state.session_hub.publish(proposal.gym_id, message)

        return {"status": 200, "message": "successfuly created join proposal"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error creating join proposal: {str(e)}",
            error_code="JOIN_PROPOSAL_CREATE_ERROR",
            log_data={
                "session_id": proposal.session_id,
                "proposer_id": proposal.proposer_id,
                "gym_id": proposal.gym_id,
                "error": str(e),
            },
        )


@router.post("/accept_session")
async def accept_proposal(
    http_request: Request,
    request: AcceptProposalCreate,
    db: Session = Depends(get_db),
):
    try:
        session_id = request.session_id
        proposal_id = request.proposal_id

        session = db.query(New_Session).filter(New_Session.session_id == session_id).first()
        if not session:
            raise FittbotHTTPException(
                status_code=404, detail="Session not found", error_code="SESSION_NOT_FOUND", log_data={"session_id": session_id}
            )

        current_participants = db.query(Participant).filter(Participant.session_id == session_id).count()
        if current_participants >= session.participant_limit:
            raise FittbotHTTPException(
                status_code=400, detail="Session is full", error_code="SESSION_FULL", log_data={"session_id": session_id}
            )

        proposal = db.query(JoinProposal).filter(JoinProposal.proposal_id == proposal_id).first()
        if not proposal:
            raise FittbotHTTPException(
                status_code=404, detail="Proposal not found", error_code="PROPOSAL_NOT_FOUND", log_data={"proposal_id": proposal_id}
            )

        db_participant = Participant(
            session_id=session_id, user_id=proposal.proposer_id, proposed_time=proposal.proposed_time
        )
        db.add(db_participant)
        db.delete(proposal)
        db.commit()

        rejected = (
            db.query(RejectedProposal)
            .filter(RejectedProposal.session_id == session_id, RejectedProposal.user_id == db_participant.user_id)
            .all()
        )
        if rejected:
            for item in rejected:
                db.delete(item)
            db.commit()

        sessions_data = get_sessions_data(request.gym_id, db)
        message = json.dumps({"action": "update_sessions", "data": sessions_data}, default=str)
        await http_request.app.state.session_hub.publish(request.gym_id, message)

        return {"status": 200, "message": "Proposal accepted and user added to session"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error accepting proposal:{str(e)}",
            error_code="ACCEPT_PROPOSAL_ERROR",
            log_data={
                "session_id": request.session_id,
                "proposal_id": request.proposal_id,
                "gym_id": request.gym_id,
                "error": str(e),
            },
        )


@router.put("/reschedule_session")
async def reschedule_session(
    http_request: Request,
    payload: SessionRescheduleRequest,
    db: Session = Depends(get_db),
):
    try:
        session = (
            db.query(New_Session)
            .filter(
                New_Session.session_id == payload.session_id,
                New_Session.gym_id == payload.gym_id,
            )
            .first()
        )
        if not session:
            raise FittbotHTTPException(
                status_code=404,
                detail="Session not found",
                error_code="SESSION_NOT_FOUND",
                log_data={"session_id": payload.session_id, "gym_id": payload.gym_id},
            )

        proposed_datetime = datetime.combine(payload.new_session_date, payload.new_session_time)
        if proposed_datetime <= datetime.now():
            raise FittbotHTTPException(
                status_code=400,
                detail="Reschedule time must be in the future.",
                error_code="INVALID_RESCHEDULE_TIME",
                log_data={
                    "session_id": payload.session_id,
                    "gym_id": payload.gym_id,
                    "new_session_date": str(payload.new_session_date),
                    "new_session_time": str(payload.new_session_time),
                },
            )

        session.session_date = payload.new_session_date
        session.session_time = payload.new_session_time
        db.commit()
        db.refresh(session)

        sessions_data = get_sessions_data(payload.gym_id, db)
        message = json.dumps({"action": "update_sessions", "data": sessions_data}, default=str)
        await http_request.app.state.session_hub.publish(payload.gym_id, message)

        return {"status": 200, "message": "Session rescheduled", "data": session}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error rescheduling session: {str(e)}",
            error_code="RESCHEDULE_SESSION_ERROR",
            log_data={
                "session_id": payload.session_id,
                "gym_id": payload.gym_id,
                "new_session_date": str(payload.new_session_date),
                "new_session_time": str(payload.new_session_time),
                "error": str(e),
            },
        )


@router.post("/remove_participants")
async def remove_participants(
    http_request: Request,
    payload: SessionParticipantRemovalRequest,
    db: Session = Depends(get_db),
):
    try:
        if not payload.client_ids:
            raise FittbotHTTPException(
                status_code=400,
                detail="At least one client_id is required.",
                error_code="NO_CLIENT_IDS",
                log_data={"session_id": payload.session_id, "gym_id": payload.gym_id},
            )

        session = (
            db.query(New_Session)
            .filter(
                New_Session.session_id == payload.session_id,
                New_Session.gym_id == payload.gym_id,
            )
            .first()
        )
        if not session:
            raise FittbotHTTPException(
                status_code=404,
                detail="Session not found",
                error_code="SESSION_NOT_FOUND",
                log_data={"session_id": payload.session_id, "gym_id": payload.gym_id},
            )

        participants = (
            db.query(Participant)
            .filter(
                Participant.session_id == payload.session_id,
                Participant.user_id.in_(payload.client_ids),
            )
            .all()
        )
        if not participants:
            raise FittbotHTTPException(
                status_code=404,
                detail="No matching participants found in session",
                error_code="PARTICIPANTS_NOT_FOUND",
                log_data={
                    "session_id": payload.session_id,
                    "client_ids": payload.client_ids,
                    "gym_id": payload.gym_id,
                },
            )

        for participant in participants:
            db.delete(participant)

        existing_rejections = set(
            user_id
            for (user_id,) in db.query(RejectedProposal.user_id)
            .filter(
                RejectedProposal.session_id == payload.session_id,
                RejectedProposal.user_id.in_(payload.client_ids),
            )
            .all()
        )
        for client_id in payload.client_ids:
            if client_id not in existing_rejections:
                db.add(RejectedProposal(session_id=payload.session_id, user_id=client_id))

        db.commit()

        sessions_data = get_sessions_data(payload.gym_id, db)
        message = json.dumps({"action": "update_sessions", "data": sessions_data}, default=str)
        await http_request.app.state.session_hub.publish(payload.gym_id, message)

        return {"status": 200, "message": "Participants removed from session"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error removing participants: {str(e)}",
            error_code="REMOVE_PARTICIPANTS_ERROR",
            log_data={
                "session_id": payload.session_id,
                "client_ids": payload.client_ids,
                "gym_id": payload.gym_id,
                "error": str(e),
            },
        )


@router.delete("/delete_proposal")
async def reject_proposal(
    http_request: Request,
    session_id: int,
    proposal_id: int,
    proposer_id: int,
    gym_id: int,
    db: Session = Depends(get_db),
):
    try:
        proposal = db.query(JoinProposal).filter(JoinProposal.proposal_id == proposal_id).first()
        if not proposal:
            raise FittbotHTTPException(
                status_code=404, detail="Proposal not found", error_code="PROPOSAL_NOT_FOUND", log_data={"proposal_id": proposal_id}
            )

        db.delete(proposal)
        db.commit()

        Rejected = RejectedProposal(session_id=session_id, user_id=proposer_id)
        db.add(Rejected)
        db.commit()

        sessions_data = get_sessions_data(gym_id, db)
        message = json.dumps({"action": "update_sessions", "data": sessions_data}, default=str)
        await http_request.app.state.session_hub.publish(gym_id, message)

        return {"status": 200, "message": "Proposal rejected"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error rejecting proposal: {str(e)}",
            error_code="DELETE_PROPOSAL_ERROR",
            log_data={
                "session_id": session_id,
                "proposal_id": proposal_id,
                "proposer_id": proposer_id,
                "gym_id": gym_id,
                "error": str(e),
            },
        )


@router.delete("/delete_session")
async def reject_proposal(  # keep name as in original snippet
    http_request:Request,
    session_id: int,
    gym_id: int,
    db: Session = Depends(get_db),
):
    try:
        proposal = db.query(New_Session).filter(New_Session.session_id == session_id).first()
        if not proposal:
            raise FittbotHTTPException(
                status_code=404, detail="session not found", error_code="SESSION_NOT_FOUND", log_data={"session_id": session_id}
            )

        db.delete(proposal)
        db.commit()

        sessions_data = get_sessions_data(gym_id, db)
        message = json.dumps({"action": "update_sessions", "data": sessions_data}, default=str)
        await http_request.app.state.session_hub.publish(gym_id, message)
        

        return {"status": 200, "message": "Session deleted"}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error deleting session: {str(e)}",
            error_code="DELETE_SESSION_ERROR",
            log_data={"session_id": session_id, "gym_id": gym_id, "error": str(e)},
        )


@router.get("/get_session_details")
async def get_session_details(session_id: int, db: Session = Depends(get_db)):
    try:
        participants_query = (
            db.query(Participant, Client.name.label("participant_name"))
            .join(Client, Participant.user_id == Client.client_id)
            .filter(Participant.session_id == session_id)
            .all()
        )

        participants = []
        for participant, participant_name in participants_query:
            participants.append(
                {"participant_id": participant.participant_id, "participant_name": participant_name}
            )

        proposals_query = (
            db.query(JoinProposal, Client.name.label("proposer_name"))
            .join(Client, JoinProposal.proposer_id == Client.client_id)
            .filter(JoinProposal.session_id == session_id)
            .all()
        )

        join_proposals = []
        for proposal, proposer_name in proposals_query:
            join_proposals.append({"proposal_id": proposal.proposal_id, "proposer_name": proposer_name})

        return {"status": 200, "data": {"participants": participants, "join_proposals": join_proposals}}

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail=f"Error fetching session details: {str(e)}",
            error_code="SESSION_DETAILS_ERROR",
            log_data={"session_id": session_id, "error": str(e)},
        )
