
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from redis.asyncio import Redis
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import json
from app.models.async_database import get_async_db
from app.utils.redis_config import get_redis
from app.models.fittbot_models import GymPlans, GymBatches, NoCostEmi

router = APIRouter(prefix="/pnb", tags=["Plans & Batches"])


def calculate_fittbot_plan_offer(gym_plan_duration: int) -> Dict[str, Any]:
    """
    Simple calculation - Fittbot offer = gym plan duration × 398
    """
    BASE_ONE_MONTH_AMOUNT = 398
    fittbot_price = gym_plan_duration * BASE_ONE_MONTH_AMOUNT

    return {
        "fittbot_plan": {
            "duration": gym_plan_duration,
            "price_rupees": fittbot_price,
        },
        "can_offer_fittbot_plan": True
    }


def calculate_nutritional_plan(duration: int) -> Dict[str, Any]:
    """Calculate nutritional plan based on duration in months"""
    if duration >= 4:
        return {"consultations": 2, "amount": 2400}
    elif duration >= 1:
        return {"consultations": 1, "amount": 1200}
    return None


@router.get("/gym/plans_and_batches")
async def get_plans_and_batches(
    gym_id: int = Query(...),
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis)
):

    try:
        response = {}

        # Check no_cost_emi for this gym
        no_cost_emi_result = await db.execute(select(NoCostEmi).where(NoCostEmi.gym_id == gym_id))
        no_cost_emi_record = no_cost_emi_result.scalars().first()
        gym_no_cost_emi_enabled = no_cost_emi_record.no_cost_emi if no_cost_emi_record else False

        redis_key_plans = f"gym:{gym_id}:plans"
        redis_key_batches = f"gym:{gym_id}:batches"
        cached_plans = await redis.get(redis_key_plans)
        if cached_plans:
            plans_data = json.loads(cached_plans)
         
            for plan in plans_data:
                if gym_no_cost_emi_enabled and plan.get("amount", 0) >= 4000:
                    plan["no_cost_emi"] = True
                else:
                    plan["no_cost_emi"] = False
            response["plans"] = plans_data

        else:
           
            plans_result = await db.execute(
                select(GymPlans).where(GymPlans.gym_id == gym_id).order_by(GymPlans.id.desc())
            )
            plans_records = plans_result.scalars().all()
            if plans_records:
                plans_data = []
                plans_data_for_cache = []
                for record in plans_records:

                    fittbot_offer = calculate_fittbot_plan_offer(gym_plan_duration=record.duration)
                    nutritional_plan = calculate_nutritional_plan(record.duration)

                    plan_no_cost_emi = gym_no_cost_emi_enabled and record.amount >= 4000

                    plan_dict = {
                        "id": record.id,
                        "plans": record.plans,
                        "gym_id": record.gym_id,
                        "amount": record.amount,
                        "duration": record.duration,
                        "description": record.description,
                        "personal_training": record.personal_training,
                        "services": record.services,
                        "original": record.original_amount if record.original_amount is not None else record.amount,
                        "bonus": record.bonus if record.bonus is not None else 0,
                        "bonus_type": record.bonus_type if record.bonus_type else None,
                        "pause": record.pause if record.pause is not None else 0,
                        "pause_type": record.pause_type if record.pause_type else None,
                        "is_couple": True if record.plan_for == "couple" else False,
                        "is_buddy": True if record.plan_for == "buddy" else False,
                        "plan_for": record.plan_for if record.plan_for else "individual",
                        "buddy_count": record.buddy_count if record.buddy_count else None,
                        "fittbot_plans": fittbot_offer,
                        "nutritional_plan": nutritional_plan,
                        "no_cost_emi": plan_no_cost_emi,
                        "sessions_count": record.sessions_count
                    }
                    plans_data.append(plan_dict)

         
                    cache_dict = {k: v for k, v in plan_dict.items() if k != "no_cost_emi"}
                    plans_data_for_cache.append(cache_dict)

                await redis.set(redis_key_plans, json.dumps(plans_data_for_cache), ex=86400)

                response["plans"] = plans_data
            else:
                response["plans"] = []

        cached_batches = await redis.get(redis_key_batches)
        if cached_batches:
            print("Batches data fetched from Redis.")
            batches_data = json.loads(cached_batches)
            response["batches"] = batches_data

        else:
            print("Batches data not found in Redis, querying the database.")
            batches_result = await db.execute(
                select(GymBatches).where(GymBatches.gym_id == gym_id).order_by(GymBatches.batch_id.desc())
            )
            batches_records = batches_result.scalars().all()
            if not batches_records:
                response["batches"] = []
            else:

                batches_data = [
                    {"id": record.batch_id, "batch_name": record.batch_name,"timing":record.timing, "description":record.description}
                    for index, record in enumerate(batches_records)
                ]
                await redis.set(redis_key_batches, json.dumps(batches_data), ex=86400)

                response["batches"] = batches_data

        return {"data":response,"status":200,"message":"Plans and batches listed successfully"}

    except Exception as e:
      
        raise HTTPException(status_code=500, detail=f"An unexpected error occured: {str(e)}")


class AddPlanRequest(BaseModel):
    gym_id: int
    plan_name: str
    amount: int
    duration:int
    personal_training:bool
    is_couple:bool
    bonus:Optional[int]=None
    pause:Optional[int]=None
    pause_type:Optional[str]=None
    bonus_type:Optional[str]=None
    original:Optional[int]=None
    services:Optional[List[str]]=None
    description:Optional[str]=None
    plan_for:Optional[str]=None
    buddy_count:Optional[int]=None
    sessions_count:Optional[int]=None

class QuickPlanItem(BaseModel):
    plan_name: str
    duration: int
    amount: int
    personal_training: bool
    is_couple: bool
    plan_for: Optional[str] = None
    buddy_count: Optional[int] = None
    sessions_count: Optional[int] = None

class AddQuickPlanRequest(BaseModel):
    gym_id: int
    plans: List[QuickPlanItem]

DEFAULT_GYM_SERVICES = [
    "Access to Gym Floor",
    "Cardio Equipment Access",
    "Strength Training Equipment",
    "Free Weight Area",
    "Functional Training Zone"
]

class AddBatchRequest(BaseModel):
    gym_id: int
    batch_name: str
    timing: str
    description:Optional[str]=None

@router.post("/gym/add_plan")
async def add_plan(
    request: AddPlanRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis)
):
    try:
        gym_id = request.gym_id
        plan_name = request.plan_name
        amount = request.original
        original_amount=request.original
        duration=request.duration
        personal_training=request.personal_training if request.personal_training else None
        services=request.services if request.services else None
        bonus=request.bonus if request.bonus else None
        bonus_type=request.bonus_type if request.bonus_type else None
        pause=request.pause if request.pause else None
        pause_type=request.pause_type if request.pause_type else None
        description = request.description if request.description else None
        is_couple=request.is_couple
        buddy_count=request.buddy_count if request.buddy_count else None
        sessions_count=request.sessions_count if request.sessions_count else None

        if request.plan_for:
            plan_for = request.plan_for
        else:
            plan_for = "couple" if is_couple else "individual"

        if request.amount:
            original_amount=request.original
            amount=request.amount

        new_plan = GymPlans(gym_id=gym_id, plans=plan_name, amount=amount,duration=duration, personal_training=personal_training,
        description=description, services=services, bonus=bonus, original_amount=original_amount, bonus_type=bonus_type, pause=pause,pause_type=pause_type,plan_for=plan_for,buddy_count=buddy_count,sessions_count=sessions_count)
        
        db.add(new_plan)
        await db.commit()
        await db.refresh(new_plan)

        redis_key_plans = f"gym:{gym_id}:plans"
        if await redis.exists(redis_key_plans):
            await redis.delete(redis_key_plans)

        return {"status": 200, "message": "Plan added successfully."}
    except Exception as e:
        await db.rollback()
        print(f"Error adding plan: {e}")
        raise HTTPException(status_code=500, detail=f"Error adding plan: {str(e)}")


@router.post("/gym/add_quick_plan")
async def add_quick_plan(
    request: AddQuickPlanRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis)
):
  
    try:
        gym_id = request.gym_id

        new_plans = [
            GymPlans(
                gym_id=gym_id,
                plans=plan_item.plan_name,
                amount=plan_item.amount,
                original_amount=plan_item.amount,
                duration=plan_item.duration,
                personal_training=plan_item.personal_training,
                services=DEFAULT_GYM_SERVICES,
                plan_for=plan_item.plan_for if plan_item.plan_for else ("couple" if plan_item.is_couple else "individual"),
                buddy_count=plan_item.buddy_count,
                sessions_count=plan_item.sessions_count if plan_item.sessions_count else None
            )
            for plan_item in request.plans
        ]

        db.add_all(new_plans)
        await db.commit()

        redis_key_plans = f"gym:{gym_id}:plans"
        if await redis.exists(redis_key_plans):
            await redis.delete(redis_key_plans)

        return {
            "status": 200,
            "message": f"{len(new_plans)} plans added successfully.",
            "plans_added": [p.plan_name for p in request.plans]
        }
    except Exception as e:
        await db.rollback()
        print(f"Error adding quick plans: {e}")
        raise HTTPException(status_code=500, detail=f"Error adding plans: {str(e)}")


@router.post("/gym/add_batch")
async def add_batch(
    request: AddBatchRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis)
):
    try:

        gym_id = request.gym_id
        batch_name = request.batch_name
        timing = request.timing
        description=request.description

        new_batch = GymBatches(gym_id=gym_id, batch_name=batch_name, timing=timing, description=description)
        db.add(new_batch)
        await db.commit()

        await db.refresh(new_batch)
        redis_key_batches = f"gym:{gym_id}:batches"
        if await redis.exists(redis_key_batches):
            await redis.delete(redis_key_batches)

        return {"status": 200, "message": "Batch added successfully."}
    except Exception as e:
        await db.rollback()
        print(f"Error adding batch: {e}")
        raise HTTPException(status_code=500, detail=f"Error adding batch: {str(e)}")

class EditPlanRequest(BaseModel):
    id: int
    plans: Optional[str] = None
    amount: Optional[int] = None
    original: Optional[int] = None
    duration: Optional[int] = None
    bonus:Optional[int]=None
    pause:Optional[int]=None
    personal_training:bool
    services:Optional[List[str]]=None
    description: Optional[str]=None
    bonus_type:Optional[str]=None
    pause_type:Optional[str]=None
    is_couple:Optional[bool]=None
    plan_for:Optional[str]=None
    buddy_count:Optional[int]=None
    sessions_count:Optional[int]=None

@router.put("/gym/edit_plan")
async def edit_plan(
    request: EditPlanRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis)
):
    try:
        result = await db.execute(select(GymPlans).where(GymPlans.id == request.id))
        plan = result.scalars().first()
        if not plan:
            raise HTTPException(status_code=404, detail="Plan not found")

        if request.plans is not None:
            plan.plans = request.plans
        if request.amount is not None:
            plan.amount = request.amount
        if request.duration is not None:
            plan.duration = request.duration
        if request.description is not None:
            plan.description = request.description
        if request.bonus is not None:
            plan.bonus = request.bonus
        if request.original is not None:
            plan.original_amount = request.original
        if request.bonus_type is not None:
            plan.bonus_type=request.bonus_type
        if request.pause_type is not None:
            plan.pause_type=request.pause_type
        if request.pause is not None:
            plan.pause = request.pause


        plan.personal_training = request.personal_training

        if request.services is not None:
            plan.services = request.services
        if request.buddy_count is not None:
            plan.buddy_count = request.buddy_count
        if request.sessions_count is not None:
            plan.sessions_count = request.sessions_count
        if request.plan_for is not None:
            plan.plan_for = request.plan_for
        elif request.is_couple is not None:
            plan.plan_for = "couple" if request.is_couple else "individual"

        await db.commit()

        gym_id = plan.gym_id
        redis_key_plans = f"gym:{gym_id}:plans"
        existing_plans = await redis.get(redis_key_plans)
        if existing_plans:
            await redis.delete(redis_key_plans)

        return {"status": 200, "message": "Plan updated successfully."}
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        print(f"Error updating plan: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating plan: {str(e)}")

@router.delete("/gym/delete_plan")
async def delete_plan(
    id: int,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis)
):
    try:
        result = await db.execute(select(GymPlans).where(GymPlans.id == id))
        plan = result.scalars().first()
        if not plan:
            raise HTTPException(status_code=404, detail="Plan not found")

        gym_id = plan.gym_id
        redis_key_plans = f"gym:{gym_id}:plans"
        existing_plans = await redis.get(redis_key_plans)
        if existing_plans:
            await redis.delete(redis_key_plans)
        await db.delete(plan)
        await db.commit()
        return {"status": 200, "message": "Plan deleted successfully."}
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        print(f"Error deleting plan: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting plan: {str(e)}")

class EditBatchRequest(BaseModel):
    batch_id: int
    batch_name: Optional[str] = None
    timing: Optional[str] = None
    description : Optional[str]=None

@router.put("/gym/edit_batch")
async def edit_batch(
    request: EditBatchRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis)
):
    try:
        result = await db.execute(select(GymBatches).where(GymBatches.batch_id == request.batch_id))
        batch = result.scalars().first()
        if not batch:
            raise HTTPException(status_code=404, detail="Batch not found")

        if request.batch_name is not None:
            batch.batch_name = request.batch_name
        if request.timing is not None:
            batch.timing = request.timing
        if request.description is not None:
            batch.description= request.description

        await db.commit()
        redis_key_batches = f"gym:{batch.gym_id}:batches"
        existing_batches = await redis.get(redis_key_batches)
        if existing_batches:
            await redis.delete(redis_key_batches)

        return {"status": 200, "message": "Batch updated successfully."}
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        print(f"Error updating batch: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating batch: {str(e)}")

@router.delete("/gym/delete_batch")
async def delete_batch(
    batch_id: int,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis)
):
    try:
        result = await db.execute(select(GymBatches).where(GymBatches.batch_id == batch_id))
        batch = result.scalars().first()
        if not batch:
            raise HTTPException(status_code=404, detail="Batch not found")

        gym_id = batch.gym_id
        redis_key_batches = f"gym:{gym_id}:batches"
        existing_batches = await redis.get(redis_key_batches)
        if existing_batches:
            await redis.delete(redis_key_batches)
        await db.delete(batch)
        await db.commit()

        return {"status": 200, "message": "Batch deleted successfully."}
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        print(f"Error deleting batch: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting batch: {str(e)}")
