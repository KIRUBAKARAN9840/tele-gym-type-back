from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.utils.request_auth import resolve_authenticated_user_id

from .dependencies import (
    get_revenuecat_command_dispatcher,
    get_revenuecat_command_store,
)
from .schemas import (
    CommandStatusResponse,
    RevenueCatCommandAccepted,
    RevenueCatCreateOrderRequest,
    RevenueCatVerifyRequest,
)
from .services.revenuecat_dispatcher import RevenueCatCommandDispatcher
from .stores.command_store import CommandStore

router = APIRouter(prefix="/revenuecat_v2", tags=["RevenueCat Subscriptions v2"])


@router.post(
    "/subscriptions/create",
    response_model=RevenueCatCommandAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_revenuecat_order(
    request: Request,
    body: RevenueCatCreateOrderRequest,
    dispatcher: RevenueCatCommandDispatcher = Depends(get_revenuecat_command_dispatcher),
):
    client_id = resolve_authenticated_user_id(request, body.client_id)
    status_record = await dispatcher.enqueue_order(body, client_id=client_id)
    status_url = request.url_for("get_revenuecat_command_status", command_id=status_record.request_id)

    # Track checkout initiation (non-blocking)
    from app.services.activity_tracker import track_event
    await track_event(
        int(client_id), "checkout_initiated",
        product_type="subscription",
        product_details={"plan_sku": body.product_sku},
        source="payment_subscription_googleplay",
        command_id=status_record.request_id,
    )

    return RevenueCatCommandAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


@router.post(
    "/subscriptions/verify",
    response_model=RevenueCatCommandAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_revenuecat_verify(
    request: Request,
    body: RevenueCatVerifyRequest,
    dispatcher: RevenueCatCommandDispatcher = Depends(get_revenuecat_command_dispatcher),
):
    client_id = resolve_authenticated_user_id(request, body.client_id)
    status_record = await dispatcher.enqueue_verify(body, client_id=client_id)
    status_url = request.url_for("get_revenuecat_command_status", command_id=status_record.request_id)

    # Track payment verification (non-blocking)
    from app.services.activity_tracker import track_event
    await track_event(
        int(client_id), "checkout_completed",
        product_type="subscription",
        source="payment_subscription_googleplay",
        command_id=status_record.request_id,
    )

    return RevenueCatCommandAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


# Reuse same status endpoint for any RevenueCat command
@router.get(
    "/commands/{command_id}",
    response_model=CommandStatusResponse,
    name="get_revenuecat_command_status",
)
async def get_revenuecat_command_status(
    command_id: str,
    request: Request,
    store: CommandStore = Depends(get_revenuecat_command_store),
):
    client_id = resolve_authenticated_user_id(request)
    record = await store.get(command_id, owner_id=client_id)
    if not record:
        raise HTTPException(status_code=404, detail="command_not_found")
    return record.to_response()


@router.post("/webhooks", status_code=status.HTTP_202_ACCEPTED)
async def enqueue_revenuecat_webhook(
    request: Request,
    dispatcher: RevenueCatCommandDispatcher = Depends(get_revenuecat_command_dispatcher),
):
    raw_body = await request.body()
    signature = request.headers.get("Authorization", "").replace("Bearer ", "")
    status_record = await dispatcher.enqueue_webhook(signature=signature, raw_body=raw_body.decode())
    return {
        "request_id": status_record.request_id,
        "status": status_record.status,
    }
