# app/api/v1/client/check_client_target.py

import json
from datetime import datetime
from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
from redis.asyncio import Redis

from app.models.database import get_db
from app.utils.redis_config import get_redis
from app.utils.logging_utils import FittbotHTTPException
from app.models.fittbot_models import Client, ClientTarget, ClientActual

router = APIRouter(prefix="/check_client_target", tags=["Client Targets"])


@router.get("/get")
async def check_client_target(
    request: Request,  # kept for parity with original signature
    client_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        client_status_key = f"client{client_id}:initialstatus"
        client_status = await redis.hget(client_status_key, "initialstatus")
        actual_weight = await redis.hget(client_status_key, "actual_weight")
        target_weight = await redis.hget(client_status_key, "target_weight")
        start_weight = await redis.hget(client_status_key, "start_weight")
        height = await redis.hget(client_status_key, "height")
        goals = await redis.hget(client_status_key, "goals")
        age = await redis.hget(client_status_key, "age")
        lifestyle = await redis.hget(client_status_key, "lifestyle")

        if client_status is None:
            client = db.query(Client).filter(Client.client_id == client_id).first()
            target_data = db.query(ClientTarget).filter(ClientTarget.client_id == client_id).first()
            if not client:
                raise FittbotHTTPException(
                    status_code=404,
                    detail="User not found",
                    error_code="CLIENT_NOT_FOUND",
                    log_data={"client_id": client_id},
                )

            client_status = client.status
            height = client.height if client.height is not None else ""
            age = client.age if client.age is not None else ""
            lifestyle = client.lifestyle if client.lifestyle is not None else ""
            goals = client.goals if client.goals is not None else ""
            actual_weight = client.weight if client.weight is not None else None
            target_weight = target_data.weight if target_data and target_data.weight is not None else 0
            start_weight = target_data.start_weight if target_data and target_data.start_weight is not None else 0

            await redis.hset(
                client_status_key,
                mapping={
                    "initialstatus": client_status,
                    "actual_weight": actual_weight,
                    "target_weight": target_weight,
                    "start_weight": start_weight,
                    "goals": goals,
                    "height": height,
                    "age": age,
                    "lifestyle": lifestyle,
                },
            )
            await redis.expire(client_status_key, 86400)

            # Keep the original scoping/behavior
            def ensure_str(value):
                if value is None:
                    return ""
                if isinstance(value, bytes):
                    return value.decode()
                return value

            client_status = ensure_str(client_status)
            goals = ensure_str(goals)
            height = ensure_str(height)
            age = ensure_str(age)
            lifestyle = ensure_str(lifestyle)

        weight = True
        if any([target_weight == "0", target_weight is None, target_weight == 0]):
            weight = False

        target_actual_key = f"client{client_id}:initial_target_actual"
        target_actual_data = await redis.get(target_actual_key)

        if target_actual_data:
            target_actual = json.loads(target_actual_data)
        else:
            today_dt = datetime.now().date()
            target_data = db.query(ClientTarget).filter(ClientTarget.client_id == client_id).first()
            actual_data = (
                db.query(ClientActual)
                .filter(ClientActual.client_id == client_id, ClientActual.date == today_dt)
                .first()
            )

            target_actual = {
                "calories": {
                    "target": target_data.calories if target_data else None,
                    "actual": actual_data.calories if actual_data else 0,
                },
                "protein": {
                    "target": target_data.protein if target_data else None,
                    "actual": actual_data.protein if actual_data else 0,
                },
                "carbs": {
                    "target": target_data.carbs if target_data else None,
                    "actual": actual_data.carbs if actual_data else 0,
                },
                "fat": {
                    "target": target_data.fat if target_data else None,
                    "actual": actual_data.fats if actual_data else 0,
                },
            }

            await redis.set(target_actual_key, json.dumps(target_actual))
            await redis.expire(target_actual_key, 86400)

        calories = True
        if any(
            [
                target_actual["calories"]["target"] is None,
                target_actual["protein"]["target"] is None,
                target_actual["carbs"]["target"] is None,
                target_actual["fat"]["target"] is None,
            ]
        ):
            calories = False

        return {
            "status": 200,
            "message": "Data fetched successfully",
            "data": {
                "client": {
                    "actual_weight": actual_weight,
                    "target_weight": target_weight,
                    "start_weight": start_weight,
                    "height": height,
                    "age": age,
                    "lifestyle": lifestyle,
                    "goals": goals,
                },
                "target_actual": target_actual,
            },
            "weight": weight,
            "calories": calories,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve client information",
            error_code="CHECK_CLIENT_TARGET_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )
