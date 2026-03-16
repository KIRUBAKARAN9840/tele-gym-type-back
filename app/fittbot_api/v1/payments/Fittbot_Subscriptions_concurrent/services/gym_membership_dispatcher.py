from typing import Dict, Any, Optional

from app.celery_app import celery_app
from app.fittbot_api.v1.payments.routes.gym_membership import UnifiedMembershipRequest

from ..config import HighConcurrencyConfig
from ..schemas import CommandStatusResponse, GymMembershipVerifyRequest
from ..stores.command_store import CommandStore


class GymMembershipCommandDispatcher:
    """Queues gym membership checkout/verify/webhook commands."""

    def __init__(self, store: CommandStore, config: HighConcurrencyConfig):
        self.store = store
        self.config = config

    async def enqueue_checkout(
        self, payload: UnifiedMembershipRequest, *, owner_id: Optional[str] = None
    ) -> CommandStatusResponse:
        record = await self.store.create(
            command_type="gym_membership_checkout",
            payload=payload.dict(),
            owner_id=owner_id or getattr(payload, "client_id", None),
        )
        self._send_task(self.config.gym_membership_checkout_queue_name, record.command_id)
        return record.to_response()

    async def enqueue_verify(
        self, payload: GymMembershipVerifyRequest, *, owner_id: Optional[str] = None
    ) -> CommandStatusResponse:
        payload_dict = payload.dict()
        if owner_id is not None and "client_id" not in payload_dict:
            payload_dict["client_id"] = owner_id
        record = await self.store.create(
            command_type="gym_membership_verify",
            payload=payload_dict,
            owner_id=owner_id,
        )
        self._send_task(self.config.gym_membership_verify_queue_name, record.command_id)
        return record.to_response()

    async def enqueue_webhook(self, signature: str, raw_body: str) -> CommandStatusResponse:
        record = await self.store.create(
            command_type="gym_membership_webhook",
            payload={"signature": signature, "raw_body": raw_body},
        )
        self._send_task(self.config.gym_membership_webhook_queue_name, record.command_id)
        return record.to_response()

    async def get_status(self, command_id: str, *, owner_id: Optional[str] = None) -> CommandStatusResponse:
        record = await self.store.get(command_id, owner_id=owner_id)
        if not record:
            raise KeyError("command_not_found")
        return record.to_response()

    def _send_task(self, task_name: str, command_id: str) -> None:
        celery_app.send_task(task_name, args=[command_id], queue="payments")
