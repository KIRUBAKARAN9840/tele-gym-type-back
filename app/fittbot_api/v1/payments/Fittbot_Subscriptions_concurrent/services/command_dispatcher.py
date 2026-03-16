from typing import Dict, Optional

from app.celery_app import celery_app

from ..config import HighConcurrencyConfig
from ..schemas import (
    CommandStatusResponse,
    SubscriptionCheckoutRequest,
    SubscriptionVerifyRequest,
)
from ..stores.command_store import CommandStore


class CommandDispatcher:
    """
    Responsible for translating HTTP payloads into persistent commands +
    fanning them out to Celery. Keeping this logic centralized ensures
    we can swap the transport (SQS, EventBridge, Kafka) in one place.
    """

    def __init__(self, store: CommandStore, config: HighConcurrencyConfig):
        self.store = store
        self.config = config

    async def enqueue_checkout(
        self, payload: SubscriptionCheckoutRequest, *, user_id: str
    ) -> CommandStatusResponse:
        command = {
            "user_id": user_id,
            "plan_sku": payload.plan_sku,
            "metadata": payload.metadata,
        }
        record = await self.store.create(
            command_type="checkout",
            payload=command,
            idempotency_key=payload.idempotency_key,
            owner_id=user_id,
        )
        self._send_task(self.config.checkout_queue_name, record.command_id)
        return record.to_response()

    async def enqueue_verify(
        self, payload: SubscriptionVerifyRequest
    ) -> CommandStatusResponse:
        record = await self.store.create(
            command_type="verify",
            payload=payload.dict(),
            idempotency_key=payload.idempotency_key,
            owner_id=payload.user_id,
        )
        self._send_task(self.config.verify_queue_name, record.command_id)
        return record.to_response()

    async def enqueue_webhook(self, body: Dict) -> CommandStatusResponse:
        record = await self.store.create(command_type="webhook", payload=body)
        self._send_task(self.config.webhook_queue_name, record.command_id)
        return record.to_response()

    async def get_status(self, command_id: str, *, owner_id: Optional[str] = None) -> CommandStatusResponse:
        record = await self.store.get(command_id, owner_id=owner_id)
        if not record:
            raise KeyError("command_not_found")
        return record.to_response()

    def _send_task(self, task_name: str, command_id: str) -> None:
        celery_app.send_task(task_name, args=[command_id], queue="payments")
