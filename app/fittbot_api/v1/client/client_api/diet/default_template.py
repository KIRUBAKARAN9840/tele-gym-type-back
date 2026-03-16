# app/routers/default_diet_template_router.py

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
from app.models.database import get_db
from app.models.fittbot_models import Client, FittbotDietTemplate
from app.utils.logging_setup import jlog
from app.utils.logging_utils import (
    FittbotHTTPException,
    EventType
)
from app.utils.redis_config import get_redis
from redis.asyncio import Redis
import json
import uuid
from datetime import datetime

router = APIRouter(
    prefix="/default_diet_template",
    tags=["Fittbot Diet Template"],
)


class _DietTemplateLogger:

    def __init__(self):
        self.request_id = None

    def set_request_context(self, request_or_ctx) -> str:
        # If you later add middleware that injects X-Request-ID, read it here.
        self.request_id = uuid.uuid4().hex
        return self.request_id

    def _log(self, level: str, **payload):
        payload.setdefault("timestamp", datetime.utcnow().isoformat() + "Z")
        payload.setdefault("request_id", self.request_id)
        payload.setdefault("domain", "diet_template")
        jlog(level, payload)

    # Debug/Info
    def debug(self, msg: str, **kv): self._log("debug", type="debug", msg=msg, **kv)
    def info(self, msg: str, **kv):  self._log("info",  type="info",  msg=msg, **kv)

    # Warnings/Errors
    def warning(self, msg: str, **kv): self._log("warning", type="warn", msg=msg, **kv)
    def error(self, msg: str, **kv):   self._log("error",   type="error", msg=msg, **kv)

    # Domain helpers
    def business_event(self, name: str, **kv):
        self._log("info", type=EventType.BUSINESS, event=name, **kv)

    def api_access(self, method: str, endpoint: str, response_time_ms: int, **kv):
        self._log("info", type=EventType.API, method=method, endpoint=endpoint,
                  response_time_ms=int(response_time_ms), **kv)

client_logger = _DietTemplateLogger()


@router.get("/get")
async def get_single_fittbot_template(
    client_id: int,
    cousine: str,
    goal_type: str,
    expertise_level: str,
    db: Session = Depends(get_db),
):


    try:


        client = (
            db.query(Client)
            .filter(Client.client_id == client_id)
            .first()
        )
        if client is None:

            raise FittbotHTTPException(
                status_code=404,
                detail=f"Client with id {client_id} not found",
                error_code="CLIENT_NOT_FOUND",
                log_level="warning",
                log_data={"client_id": client_id},
            )

        template = (
            db.query(FittbotDietTemplate)
            .filter(
                FittbotDietTemplate.gender == client.gender,
                FittbotDietTemplate.goals == goal_type,
                FittbotDietTemplate.cousine == cousine,
                FittbotDietTemplate.expertise_level == expertise_level,
            )
            .first()
        )

        if template is None:
            client_logger.business_event(
                "diet_template_viewed",
                client_id=client_id,
                cousine=cousine,
                goal_type=goal_type,
                expertise_level=expertise_level,
                gender=client.gender,
                template_found=False,
            )
            return {
                "status": 200,
                "message": "No matching template found",
                "data": {},
            }


        print("template.template_json",template.template_json)
        return {
            "status": 200,
            "message": "Data retrieved successfully",
            "data": {
                "id": template.id,
                "template_json": template.template_json,
                "template_name": template.template_name,
                "gender": (template.gender or "").lower(),
                "cousine": template.cousine,
                "expertise_level": template.expertise_level,
                "goal": template.goals,
                "tip": template.tip,
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as e:

        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve diet template",
            error_code="DIET_TEMPLATE_FETCH_ERROR",
            log_data={
                "exc": repr(e),
                "client_id": client_id,
                "cousine": cousine,
                "goal_type": goal_type,
                "expertise_level": expertise_level
            },
        )




@router.get('/fittbot_template')
async def get_fittbot_diet_template(db:Session=Depends(get_db), redis:Redis=Depends(get_redis)):
    try:
        redis_key = "fittbotDefaultDietKey"
        cached_data = await redis.get(redis_key)
        if cached_data:
            default_diet= json.loads(cached_data)
        else:
            default = db.query(FittbotDietTemplate).all()
            default_diet=[{
                'id':template.id,
                'template_json':template.template_json,
                'template_name':template.template_name
            }for template in default]
            await redis.set(redis_key, json.dumps(default_diet), ex=86400)
        return{
            'status':200,
            'message':"Data retrived Successfully",
            "data":default_diet
        }
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve diet template",
            error_code="DIET__FITTBOT_TEMPLATE_FETCH_ERROR",
            log_data={
                "exc": repr(e),

            },
        )
 


