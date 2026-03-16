from typing import Optional

from app.celery_app import celery_app
from app.fittbot_api.v1.payments.dailypass.routes import (
    UnifiedCheckoutRequest,
    UnifiedVerificationRequest,
)

from ..config import HighConcurrencyConfig
from ..schemas import CommandStatusResponse
from ..stores.command_store import CommandStore


class DailyPassCommandDispatcher:
    """Queues DailyPass checkout/verify commands for Celery processing."""

    def __init__(
        self,
        store: CommandStore,
        config: HighConcurrencyConfig,
        *,
        checkout_queue: str = None,
        verify_queue: str = None,
        upgrade_checkout_queue: str = None,
        upgrade_verify_queue: str = None,
        edit_topup_checkout_queue: str = None,
        edit_topup_verify_queue: str = None,
    ):
        self.store = store
        self.config = config
        self.checkout_queue = checkout_queue or config.dailypass_checkout_queue_name
        self.verify_queue = verify_queue or config.dailypass_verify_queue_name
        self.upgrade_checkout_queue = upgrade_checkout_queue or config.dailypass_upgrade_checkout_queue_name
        self.upgrade_verify_queue = upgrade_verify_queue or config.dailypass_upgrade_verify_queue_name
        self.edit_topup_checkout_queue = edit_topup_checkout_queue or config.dailypass_edit_topup_checkout_queue_name
        self.edit_topup_verify_queue = edit_topup_verify_queue or config.dailypass_edit_topup_verify_queue_name

    async def enqueue_checkout(
        self, payload: UnifiedCheckoutRequest, *, owner_id: Optional[str] = None
    ) -> CommandStatusResponse:
        record = await self.store.create(
            command_type="dailypass_checkout",
            payload=payload.dict(),
            owner_id=owner_id or getattr(payload, "client_id", None),
        )
        self._send_task(self.checkout_queue, record.command_id)
        return record.to_response()

    async def enqueue_verify(
        self, payload: UnifiedVerificationRequest, *, owner_id: Optional[str] = None
    ) -> CommandStatusResponse:
        payload_dict = payload.dict()
        if owner_id is not None and "client_id" not in payload_dict:
            payload_dict["client_id"] = owner_id
        record = await self.store.create(
            command_type="dailypass_verify",
            payload=payload_dict,
            owner_id=owner_id,
        )
        self._send_task(self.verify_queue, record.command_id)
        return record.to_response()

    async def enqueue_upgrade_checkout(self, payload, *, owner_id: Optional[str] = None) -> CommandStatusResponse:
        payload_dict = payload.dict() if hasattr(payload, "dict") else dict(payload)
        record = await self.store.create(
            command_type="dailypass_upgrade_checkout",
            payload=payload_dict,
            owner_id=owner_id or payload_dict.get("client_id"),
        )
        self._send_task(self.upgrade_checkout_queue, record.command_id)
        return record.to_response()

    async def enqueue_upgrade_verify(self, payload, *, owner_id: Optional[str] = None) -> CommandStatusResponse:
        payload_dict = payload.dict() if hasattr(payload, "dict") else dict(payload)
        if owner_id is not None and "client_id" not in payload_dict:
            payload_dict["client_id"] = owner_id
        record = await self.store.create(
            command_type="dailypass_upgrade_verify",
            payload=payload_dict,
            owner_id=owner_id,
        )
        self._send_task(self.upgrade_verify_queue, record.command_id)
        return record.to_response()

    async def enqueue_edit_topup_checkout(self, payload, *, owner_id: Optional[str] = None) -> CommandStatusResponse:
        payload_dict = payload.dict() if hasattr(payload, "dict") else dict(payload)
        record = await self.store.create(
            command_type="dailypass_edit_topup_checkout",
            payload=payload_dict,
            owner_id=owner_id or payload_dict.get("client_id"),
        )
        self._send_task(self.edit_topup_checkout_queue, record.command_id)
        return record.to_response()

    async def enqueue_edit_topup_verify(self, payload, *, owner_id: Optional[str] = None) -> CommandStatusResponse:
        payload_dict = payload.dict() if hasattr(payload, "dict") else dict(payload)
        if owner_id is not None and "client_id" not in payload_dict:
            payload_dict["client_id"] = owner_id
        record = await self.store.create(
            command_type="dailypass_edit_topup_verify",
            payload=payload_dict,
            owner_id=owner_id,
        )
        self._send_task(self.edit_topup_verify_queue, record.command_id)
        return record.to_response()

    async def get_status(self, command_id: str, *, owner_id: Optional[str] = None) -> CommandStatusResponse:
        record = await self.store.get(command_id, owner_id=owner_id)
        if not record:
            raise KeyError("command_not_found")
        return record.to_response()

    def _send_task(self, task_name: str, command_id: str) -> None:
        celery_app.send_task(task_name, args=[command_id], queue="payments")
