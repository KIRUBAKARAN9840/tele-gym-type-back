import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from redis import Redis

from app.fittbot_api.v1.payments.utils import generate_unique_id

from ..config import HighConcurrencyConfig
from ..schemas import CommandStatus, CommandStatusResponse


@dataclass
class CommandRecord:
    command_id: str
    command_type: str
    status: CommandStatus
    payload: Dict[str, Any]
    owner_id: Optional[str]
    result: Optional[Dict[str, Any]]
    error: Optional[str]
    created_at: int
    updated_at: int

    def to_response(self) -> CommandStatusResponse:
        return CommandStatusResponse(
            request_id=self.command_id,
            status=self.status,
            data=self.result,
            error=self.error,
            updated_at_epoch=self.updated_at,
        )


class CommandStore:
    """
    Redis-backed command store that works in both async FastAPI handlers
    and synchronous Celery workers. All Redis operations run in a thread
    via asyncio.to_thread so we never block the event loop nor reuse a
    closed loop across forks.
    """

    def __init__(
        self,
        redis: Redis,
        config: HighConcurrencyConfig,
        *,
        redis_prefix: Optional[str] = None,
        command_id_prefix: str = "rzp_cmd",
    ):
        self.redis = redis
        self.config = config
        self._redis_prefix = redis_prefix or config.redis_prefix
        self._command_id_prefix = command_id_prefix

    def _key(self, command_id: str) -> str:
        return f"{self._redis_prefix}:cmd:{command_id}"

    async def create(
        self,
        command_type: str,
        payload: Dict[str, Any],
        *,
        idempotency_key: Optional[str] = None,
        owner_id: Optional[str] = None,
    ) -> CommandRecord:
        now = int(time.time())
        owner = self._determine_owner_id(payload, owner_id)
        if idempotency_key:
            command_id = self._namespaced_id(idempotency_key, owner)
            existing = await self.get(command_id, owner_id=owner)
            if existing:
                return existing
        else:
            command_id = self._namespaced_id(
                generate_unique_id(self._command_id_prefix),
                owner,
            )
        record = {
            "command_id": command_id,
            "command_type": command_type,
            "status": CommandStatus.queued.value,
            "payload": payload,
            "owner_id": owner,
            "result": None,
            "error": None,
            "created_at": now,
            "updated_at": now,
        }
        await self._set(
            self._key(command_id),
            json.dumps(record),
            ex=self.config.command_ttl_seconds,
            nx=False,
        )
        return self._deserialize(record)

    async def mark_processing(self, command_id: str) -> CommandRecord:
        return await self._update(command_id, status=CommandStatus.processing.value)

    async def mark_completed(self, command_id: str, result: Dict[str, Any]) -> CommandRecord:
        return await self._update(command_id, status=CommandStatus.completed.value, result=result, error=None)

    async def mark_failed(self, command_id: str, error: str, *, result: Optional[Dict[str, Any]] = None) -> CommandRecord:
        return await self._update(command_id, status=CommandStatus.failed.value, error=error, result=result)

    async def get(self, command_id: str, *, owner_id: Optional[str] = None) -> Optional[CommandRecord]:
        raw = await self._get(self._key(command_id))
        if not raw:
            return None
        record = self._deserialize(json.loads(raw))
        if owner_id is not None:
            expected_owner = str(owner_id)
            if not record.owner_id or record.owner_id != expected_owner:
                return None
        return record

    async def _update(self, command_id: str, **updates) -> CommandRecord:
        record = await self.get(command_id)
        if not record:
            raise ValueError(f"Command {command_id} not found")
        data = {
            "command_id": record.command_id,
            "command_type": record.command_type,
            "status": record.status.value,
            "payload": record.payload,
            "owner_id": record.owner_id,
            "result": record.result,
            "error": record.error,
            "created_at": record.created_at,
            "updated_at": int(time.time()),
        }
        data.update({k: v for k, v in updates.items() if v is not None})
        await self._set(
            self._key(command_id),
            json.dumps(data),
            ex=self.config.command_ttl_seconds,
        )
        return self._deserialize(data)

    async def _set(self, *args, **kwargs):
        await asyncio.to_thread(self.redis.set, *args, **kwargs)

    async def _get(self, *args, **kwargs):
        return await asyncio.to_thread(self.redis.get, *args, **kwargs)

    def _determine_owner_id(self, payload: Dict[str, Any], owner_id: Optional[str]) -> Optional[str]:
        owner = owner_id or payload.get("user_id") or payload.get("client_id")
        return str(owner) if owner is not None else None

    def _namespaced_id(self, command_id: str, owner_id: Optional[str]) -> str:
        if owner_id is None:
            return command_id
        return f"{owner_id}:{command_id}"

    def _deserialize(self, raw: Dict[str, Any]) -> CommandRecord:
        return CommandRecord(
            command_id=raw["command_id"],
            command_type=raw["command_type"],
            status=CommandStatus(raw["status"]),
            payload=raw.get("payload") or {},
            owner_id=raw.get("owner_id"),
            result=raw.get("result"),
            error=raw.get("error"),
            created_at=raw.get("created_at", 0),
            updated_at=raw.get("updated_at", 0),
        )
