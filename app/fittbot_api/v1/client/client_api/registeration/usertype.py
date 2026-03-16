from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import Client, ClientWeightSelection
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/client/new_registration", tags=["Client Registration"])


class UserTypeResponse(BaseModel):
    status: int
    usertype: str


@router.get("/usertype")
async def get_usertype(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> UserTypeResponse:

    try:
    
        stmt = select(Client).where(Client.client_id == client_id)
        result = await db.execute(stmt)
        client = result.scalars().first()

        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
            )

        # Check each step - handle None values safely
        dob_completed = client.dob is not None
        goal_completed = bool(client.goals and str(client.goals).strip())
        height_completed = client.height is not None
        weight_completed = client.weight is not None and client.bmi is not None

        # Check body shape using ClientWeightSelection
        stmt = select(ClientWeightSelection).where(
            ClientWeightSelection.client_id == str(client_id)
        )
        result = await db.execute(stmt)
        weight_selection = result.scalars().first()
        body_shape_completed = weight_selection is not None

        lifestyle_completed = bool(client.lifestyle and str(client.lifestyle).strip())

        registration_steps = {
            "dob": dob_completed,
            "goal": goal_completed,
            "height": height_completed,
            "weight": weight_completed,
            "body_shape": body_shape_completed,
            "lifestyle": lifestyle_completed,
        }

        # Check if all steps are completed
        all_steps_completed = all(registration_steps.values())

        # Determine usertype
        if all_steps_completed:
            usertype = "full_user"
        else:
            usertype = "guest"

        return UserTypeResponse(
            status=200,
            usertype=usertype
        )

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to get user type",
            error_code="USERTYPE_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )
