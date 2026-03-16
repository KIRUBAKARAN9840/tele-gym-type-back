from typing import Optional

from app.celery_app import celery_app
from app.fittbot_api.v1.payments.Fittbot_Subscriptions_concurrent.schemas import (
    SessionCheckoutRequest,
    SessionVerifyRequest,
)

from ..config import HighConcurrencyConfig
from ..schemas import CommandStatusResponse
from ..stores.command_store import CommandStore


class SessionCommandDispatcher:
    """Queues session booking checkout/verify commands for Celery processing."""

    def __init__(self, store: CommandStore, config: HighConcurrencyConfig):
        self.store = store
        self.config = config

    async def enqueue_checkout(
        self, payload: SessionCheckoutRequest, *, owner_id: Optional[str] = None
    ) -> CommandStatusResponse:
        record = await self.store.create(
            command_type="session_checkout",
            payload=payload.dict(),
            owner_id=owner_id or str(payload.client_id),
        )
        self._send_task(self.config.sessions_checkout_queue_name, record.command_id)
        return record.to_response()

    async def enqueue_verify(
        self, payload: SessionVerifyRequest, *, owner_id: Optional[str] = None
    ) -> CommandStatusResponse:
        payload_dict = payload.dict()
        if owner_id is not None and "client_id" not in payload_dict:
            payload_dict["client_id"] = owner_id
        record = await self.store.create(
            command_type="session_verify",
            payload=payload_dict,
            owner_id=owner_id or payload_dict.get("client_id"),
        )
        self._send_task(self.config.sessions_verify_queue_name, record.command_id)
        return record.to_response()

    async def get_status(self, command_id: str, *, owner_id: Optional[str] = None) -> CommandStatusResponse:
        record = await self.store.get(command_id, owner_id=owner_id)
        if not record:
            raise KeyError("command_not_found")
        return record.to_response()

    async def enqueue_webhook(self, signature: str, raw_body: str) -> CommandStatusResponse:
        """
        Enqueue a Razorpay webhook for session payments.
        Stores signature + raw_body for verification in the processor.
        """
        record = await self.store.create(
            command_type="session_webhook",
            payload={"signature": signature, "raw_body": raw_body},
        )
        self._send_task(self.config.sessions_webhook_queue_name, record.command_id)
        return record.to_response()

    def _send_task(self, task_name: str, command_id: str) -> None:
        celery_app.send_task(task_name, args=[command_id], queue="payments")
