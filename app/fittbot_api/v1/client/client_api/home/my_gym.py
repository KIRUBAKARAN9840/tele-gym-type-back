# app/routers/my_gym_router.py

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
from typing import Optional
from pydantic import BaseModel
from app.models.database import get_db
from app.models.fittbot_models import TrainerProfile,ClientScheduler, TemplateDiet, Client, FeeHistory, GymPlans, TemplateWorkout
import json
from app.utils.redis_config import get_redis
from redis.asyncio import Redis
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/my_gym", tags=["MyGymData"])


@router.get("/get_assigned_dietplan")
async def get_assigned_diet_template(
    request: Request,
    client_id: int,
    db: Session = Depends(get_db),
):
    try:
        scheduler: Optional[ClientScheduler] = (
            db.query(ClientScheduler)
              .filter(ClientScheduler.client_id == client_id)
              .first()
        )
        if scheduler is None:
            return {"status": 200, "data": {}}

        template_id: Optional[int] = scheduler.assigned_dietplan
        if template_id is None:
            return {"status": 200, "data": {}}

        template: Optional[TemplateDiet] = (
            db.query(TemplateDiet)
              .filter(TemplateDiet.template_id == template_id)
              .first()
        )
        if template is None:
            return {"status": 200, "data": {}}

        response = {
            "template_id": template.template_id,
            "template_name": template.template_name,
            "template_details": template.template_details,
        }
        return {"status": 200, "data": response}

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve diet plan information",
            error_code="MYGYM_ASSIGNED_DIET_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )


async def _purge_clientdata_cache(
    redis: Redis,
    pattern: str = "gym:*:clientdata",
    batch: int = 500,
) -> None:
    """
    Clears cached clientdata entries with a SCAN loop.
    """
    cursor = 0
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=batch)
        if keys:
            await redis.unlink(*keys)
        if cursor == 0:
            break


class DataSharingRequest(BaseModel):
    client_id: int
    data_sharing: bool


@router.post("/toggling_data_sharing")
async def toggle_data_sharing(
    request: Request,
    payload: DataSharingRequest,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        client_id = payload.client_id
        data_sharing = payload.data_sharing

        client = db.query(Client).filter(Client.client_id == client_id).first()
        if client is None:
            raise FittbotHTTPException(
                status_code=404,
                detail=f"Client with id {client_id} not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        client.data_sharing = data_sharing
        db.commit()

        await _purge_clientdata_cache(redis)

        return {
            "status": 200,
            "message": "data sharing added successfully",
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to update data sharing preference",
            error_code="MYGYM_DATA_SHARING_TOGGLE_ERROR",
            log_data={"exc": repr(e), "client_id": getattr(payload, "client_id", None)},
        )


@router.get("/get_other_details")
async def get_other_details(
    request: Request,
    gym_id: int,
    client_id: int,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        # Assigned trainer details (cached)
        assigned_plans_key = f"{client_id}:{gym_id}:assigned_plans"
        assigned_plans_data = await redis.get(assigned_plans_key)
        trainer_name=None

        if assigned_plans_data:
            if isinstance(assigned_plans_data, (bytes, bytearray)):
                assigned_plans_data = assigned_plans_data.decode()
            assigned_plans = json.loads(assigned_plans_data)
        else:
            client_scheduler = (
                db.query(ClientScheduler)
                  .filter(
                      ClientScheduler.gym_id == gym_id,
                      ClientScheduler.client_id == client_id,
                  )
                  .first()
            )

            trainer_name = None
            dp = None

            if client_scheduler:
                trainer=db.query(TrainerProfile).filter(TrainerProfile.trainer_id == client_scheduler.assigned_trainer, TrainerProfile.gym_id == client_scheduler.gym_id).first()
 
                if not trainer:
                    trainer_name=""
                    dp=""
                else:
                    trainer_name=trainer.full_name
                    dp=trainer.profile_image
            
            
            assigned_plans = {
                "trainer_name": trainer_name or None,
                "trainer_dp": dp,
            }

            await redis.set(assigned_plans_key, json.dumps(assigned_plans), ex=86400)

        # Fee history (cached)
        fees_redis_key = f"{client_id}:fees"
        fee_data = await redis.get(fees_redis_key)
        if fee_data:
            if isinstance(fee_data, (bytes, bytearray)):
                fee_data = fee_data.decode()
            fee_history = json.loads(fee_data)
        else:
            fee_history_records = (
                db.query(FeeHistory)
                  .filter(FeeHistory.client_id == client_id)
                  .all()
            )
            fee_history = [
                {
                    "payment_date": record.payment_date.strftime("%Y-%m-%d"),
                    "fees_paid": record.fees_paid,
                }
                for record in fee_history_records
            ]
            await redis.set(fees_redis_key, json.dumps(fee_history), ex=86400)

        # Client's data sharing flag
        client = db.query(Client).filter(Client.client_id == client_id).first()
        if client is None:
            raise FittbotHTTPException(
                status_code=404,
                detail=f"Client with id {client_id} not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id, "gym_id": gym_id},
            )
        data_sharing = bool(client.data_sharing) if client.data_sharing is not None else False

        response = {
            "trainer_details": assigned_plans,
            "fee_history": fee_history,
            "data_sharing": data_sharing,
        }

        return {
            "status": 200,
            "message": "Data fetched successfully",
            "data": response,
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve additional information",
            error_code="MYGYM_OTHER_DETAILS_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id, "gym_id": gym_id},
        )



@router.get("/gym_workout_template")
async def get_gym_workout_template(client_id: int, db: Session = Depends(get_db)):
    try:
        schedule = (
            db.query(ClientScheduler)
            .filter(ClientScheduler.client_id == client_id)
            .first()
        )

        if schedule:

            template = (
                db.query(TemplateWorkout)
                .filter(TemplateWorkout.id == schedule.assigned_workoutplan)
                .first()
            )
            template_data = []
            if template:
                template_data = [
                    {
                        "id": template.id,
                        "name": template.name,
                        "exercise_data": template.workoutPlan,
                    }
                ]

            print("template_dataaaaaaaa",template_data)
            return {
                "status": 200,
                "message": "Data retrived sunccessfully",
                "data": template_data,
            }
        
        else:
            return {
                "status": 200,
                "message": "Data retrived sunccessfully",
                "data": []
            }


    except FittbotHTTPException:
        raise
    except Exception as e:
        print(str(e))
        raise FittbotHTTPException(
            status_code=500,
            detail=f"An error occured ,{str(e)}",
            error_code="GYM_WORKOUT_TEMPLATE_ERROR",
            log_data={"client_id": client_id, "error": str(e)},
        )




 
@router.get('/personal-training-details')
async def get_personal_training_details(gym_id: int, db: Session = Depends(get_db)):
    try:
        trainers = db.query(TrainerProfile).filter(
            TrainerProfile.gym_id == gym_id,
            TrainerProfile.personal_trainer == True
        ).all()
 
        personal_training_plans = db.query(GymPlans).filter(
            GymPlans.gym_id == gym_id,
            GymPlans.personal_training == True
        ).all()
 
 
 
        trainer_list = [
            {
                "profile_id": trainer.profile_id,
                "trainer_id": trainer.trainer_id,
                "full_name": trainer.full_name,
                "email": trainer.email,
                "specializations": trainer.specializations,
                "experience": trainer.experience,
                "certifications": trainer.certifications,
                "work_timings": trainer.work_timings,
                "profile_image": trainer.profile_image,
                "personal_trainer": trainer.personal_trainer
            } for trainer in trainers
        ]
 
        plans = [
            {
                "plan_id": plan.id,
                "plan_name": plan.plans,
                "amount": plan.amount,
                "duration": plan.duration,
                "description": plan.description,
                "services": plan.services,
                "personal_training": plan.personal_training
            } for plan in personal_training_plans  
        ]
 
        return {
            "status": 200,
            "message": "Personal trainers fetched successfully",
            "data": {
                "trainers":trainer_list,
                "plans":plans
            }
        }
    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve personal trainers",
            error_code="PERSONAL_TRAINER_FETCH_ERROR",
            log_data={"exc": repr(e), "gym_id": gym_id},
        )
 
