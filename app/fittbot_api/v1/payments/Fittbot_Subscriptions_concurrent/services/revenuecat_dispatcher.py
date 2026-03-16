from typing import Optional

from app.celery_app import celery_app

from ..config import HighConcurrencyConfig
from ..schemas import (
    CommandStatusResponse,
    RevenueCatCreateOrderRequest,
    RevenueCatVerifyRequest,
)
from ..stores.command_store import CommandStore


class RevenueCatCommandDispatcher:
    """Queues RevenueCat commands and hands them to Celery."""

    def __init__(self, store: CommandStore, config: HighConcurrencyConfig):
        self.store = store
        self.config = config

    async def enqueue_order(
        self, payload: RevenueCatCreateOrderRequest, *, client_id: str
    ) -> CommandStatusResponse:
        record = await self.store.create(
            command_type="revenuecat_order",
            payload={
                "client_id": client_id,
                "product_sku": payload.product_sku,
                "currency": payload.currency,
            },
            idempotency_key=payload.idempotency_key,
            owner_id=client_id,
        )
        self._send_task(self.config.revenuecat_order_queue_name, record.command_id)
        return record.to_response()

    async def enqueue_verify(
        self, payload: RevenueCatVerifyRequest, *, client_id: str
    ) -> CommandStatusResponse:
        record = await self.store.create(
            command_type="revenuecat_verify",
            payload={"client_id": client_id},
            idempotency_key=payload.idempotency_key,
            owner_id=client_id,
        )
        self._send_task(self.config.revenuecat_verify_queue_name, record.command_id)
        return record.to_response()

    async def enqueue_webhook(self, signature: str, raw_body: str) -> CommandStatusResponse:
        record = await self.store.create(
            command_type="revenuecat_webhook",
            payload={"signature": signature, "raw_body": raw_body},
        )
        self._send_task(self.config.revenuecat_webhook_queue_name, record.command_id)
        return record.to_response()

    async def get_status(self, command_id: str, *, owner_id: Optional[str] = None) -> CommandStatusResponse:
        record = await self.store.get(command_id, owner_id=owner_id)
        if not record:
            raise KeyError("command_not_found")
        return record.to_response()

    def _send_task(self, task_name: str, command_id: str) -> None:
        celery_app.send_task(task_name, args=[command_id], queue="payments")
