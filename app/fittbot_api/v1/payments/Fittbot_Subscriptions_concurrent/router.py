import logging
from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.utils.request_auth import resolve_authenticated_user_id

from .dependencies import (
    get_command_dispatcher,
    get_command_store,
    get_config,
)
from .schemas import (
    CommandStatusResponse,
    SubscriptionCheckoutAccepted,
    SubscriptionCheckoutRequest,
    SubscriptionVerifyAccepted,
    SubscriptionVerifyRequest,
)
from .services.command_dispatcher import CommandDispatcher
from .stores.command_store import CommandStore

logger = logging.getLogger("payments.razorpay.v2.api")

router = APIRouter(
    prefix="/razorpay_payments_v2",
    tags=["Razorpay Subscriptions v2"],
)


@router.post(
    "/checkout",
    response_model=SubscriptionCheckoutAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_checkout(
    request: Request,
    body: SubscriptionCheckoutRequest,
    dispatcher: CommandDispatcher = Depends(get_command_dispatcher),
):
    user_id = resolve_authenticated_user_id(request, body.user_id)
    payload = body.copy(
        update={
            "idempotency_key": body.idempotency_key
            or request.headers.get("Idempotency-Key")
        }
    )
    status_record = await dispatcher.enqueue_checkout(payload, user_id=user_id)
    logger.info(
        "RAZORPAY_CHECKOUT_ENQUEUED",
        extra={
            "request_id": status_record.request_id,
            "user_id": user_id,
            "plan_sku": body.plan_sku,
        },
    )
    status_url = request.url_for("get_checkout_command_status", command_id=status_record.request_id)

    # Track checkout initiation (non-blocking)
    # from app.services.activity_tracker import track_event
    # await track_event(
    #     int(user_id), "checkout_initiated",
    #     product_type="subscription",
    #     product_details={"plan_sku": body.plan_sku},
    #     source="payment_subscription_razorpay",
    #     command_id=status_record.request_id,
    # )

    return SubscriptionCheckoutAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


@router.post(
    "/verify",
    response_model=SubscriptionVerifyAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def enqueue_verify(
    request: Request,
    body: SubscriptionVerifyRequest,
    dispatcher: CommandDispatcher = Depends(get_command_dispatcher),
):
    user_id = resolve_authenticated_user_id(request, body.user_id)
    payload = body.copy(
        update={
            "user_id": user_id,
            "idempotency_key": body.idempotency_key
            or request.headers.get("Idempotency-Key")
        }
    )
    status_record = await dispatcher.enqueue_verify(payload)
    logger.info(
        "RAZORPAY_VERIFY_ENQUEUED",
        extra={
            "request_id": status_record.request_id,
            "payment_id": body.razorpay_payment_id,
            "subscription_id": body.razorpay_subscription_id,
        },
    )
    status_url = request.url_for("get_checkout_command_status", command_id=status_record.request_id)

    # Track payment verification (non-blocking)
    # from app.services.activity_tracker import track_event
    # await track_event(
    #     int(user_id), "checkout_completed",
    #     product_type="subscription",
    #     source="payment_subscription_razorpay",
    #     command_id=status_record.request_id,
    # )

    return SubscriptionVerifyAccepted(
        request_id=status_record.request_id,
        status=status_record.status,
        status_url=str(status_url),
    )


@router.get(
    "/commands/{command_id}",
    response_model=CommandStatusResponse,
    name="get_checkout_command_status",
)
async def get_command_status(
    command_id: str,
    request: Request,
    store: CommandStore = Depends(get_command_store),
):
    user_id = resolve_authenticated_user_id(request)
    record = await store.get(command_id, owner_id=user_id)
    if not record:
        raise HTTPException(status_code=404, detail="command_not_found")
    return record.to_response()


@router.post("/webhook", status_code=status.HTTP_202_ACCEPTED)
async def enqueue_webhook(
    request: Request,
    dispatcher: CommandDispatcher = Depends(get_command_dispatcher),
):
    raw_body = await request.body()
    payload = await request.json()
    payload["raw_body"] = raw_body.decode()
    payload["signature"] = request.headers.get("X-Razorpay-Signature")
    payload["webhook_id"] = payload.get("id")
    status_record = await dispatcher.enqueue_webhook(payload)
    return {
        "request_id": status_record.request_id,
        "status": status_record.status,
    }
