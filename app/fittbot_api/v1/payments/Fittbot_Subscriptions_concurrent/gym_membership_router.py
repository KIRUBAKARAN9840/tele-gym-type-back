from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.fittbot_api.v1.payments.routes.gym_membership import UnifiedMembershipRequest
from app.utils.request_auth import resolve_authenticated_user_id

from .dependencies import (
    get_gym_membership_command_dispatcher,
    get_gym_membership_command_store,
)
from .schemas import (
    CommandStatusResponse,
    GymMembershipCommandAccepted,
    GymMembershipVerifyRequest,
)
from .services.gym_membership_dispatcher import GymMembershipCommandDispatcher
from .stores.command_store import CommandStore

router = APIRouter(
    prefix="/pay/gym_membership_v2",
    tags=["Gym Membership Payments v2"],
)


@router.post(
    "/checkout",
    response_model=GymMembershipCommandAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_gym_membership_checkout(
    request: Request,
    body: UnifiedMembershipRequest,
    dispatcher: GymMembershipCommandDispatcher = Depends(get_gym_membership_command_dispatcher),
):
    client_id = resolve_authenticated_user_id(request, body.client_id)
    status_record = await dispatcher.enqueue_checkout(body, owner_id=client_id)
    status_url = request.url_for("get_gym_membership_command_status", command_id=status_record.request_id)

    # Track checkout initiation (non-blocking)
    from app.services.activity_tracker import track_event
    await track_event(
        int(client_id), "checkout_initiated",
        gym_id=getattr(body, "gym_id", None),
        product_type="membership",
        source="payment_membership",
        command_id=status_record.request_id,
    )

    return GymMembershipCommandAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


@router.post(
    "/verify",
    response_model=GymMembershipCommandAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_gym_membership_verify(
    request: Request,
    body: GymMembershipVerifyRequest,
    dispatcher: GymMembershipCommandDispatcher = Depends(get_gym_membership_command_dispatcher),
):
    client_id = resolve_authenticated_user_id(request)
    status_record = await dispatcher.enqueue_verify(body, owner_id=client_id)
    status_url = request.url_for("get_gym_membership_command_status", command_id=status_record.request_id)

    # Track payment verification (non-blocking)
    from app.services.activity_tracker import track_event
    await track_event(
        int(client_id), "checkout_completed",
        product_type="membership",
        source="payment_membership",
        command_id=status_record.request_id,
    )

    return GymMembershipCommandAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


@router.get(
    "/commands/{command_id}",
    response_model=CommandStatusResponse,
    name="get_gym_membership_command_status",
)
async def get_gym_membership_command_status(
    command_id: str,
    request: Request,
    store: CommandStore = Depends(get_gym_membership_command_store),
):
    client_id = resolve_authenticated_user_id(request)
    record = await store.get(command_id, owner_id=client_id)
    if not record:
        raise HTTPException(status_code=404, detail="command_not_found")
    return record.to_response()


@router.post("/webhook", status_code=status.HTTP_202_ACCEPTED)
async def enqueue_gym_membership_webhook(
    request: Request,
    dispatcher: GymMembershipCommandDispatcher = Depends(get_gym_membership_command_dispatcher),
):
    raw_body = await request.body()
    signature = request.headers.get("X-Razorpay-Signature", "")
    status_record = await dispatcher.enqueue_webhook(signature=signature, raw_body=raw_body.decode())
    return {
        "request_id": status_record.request_id,
        "status": status_record.status,
    }
