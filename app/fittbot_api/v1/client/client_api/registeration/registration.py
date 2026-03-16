from typing import Optional, List
from datetime import datetime, date

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.utils.redis_config import get_redis
from redis.asyncio import Redis

from app.models.async_database import get_async_db
from app.models.fittbot_models import (
    Client,
    ClientTarget,
    ClientWeightSelection,
    ClientCharacter,
    CharactersCombination,
    ReferralCode,
    ReferralMapping,
    ReferralFittbotCash,
)
from app.utils.logging_utils import FittbotHTTPException, auth_logger
from app.utils.otp import generate_otp, async_send_verification_sms, async_send_ios_premium_sms
from app.utils.referral_code_generator import generate_referral_code_random
from app.utils.security import create_access_token, create_refresh_token

router = APIRouter(prefix="/client/new_registration", tags=["Client Registration"])


# ==================== Request/Response Models ====================

class ClientRegistrationRequest(BaseModel):
    name: str
    mobile_number: str
    gender: str
    location: str
    referral_id: Optional[str] = None
    platform: Optional[str] = None


class ClientRegistrationResponse(BaseModel):
    status: int
    message: str
    client_id: Optional[int] = None
    contact: Optional[str] = None
    full_name: Optional[str] = None


class StepResponse(BaseModel):
    status: int
    message: str
    data: Optional[dict] = None


class DOBStepRequest(BaseModel):
    client_id: int
    dob: str


class GoalStepRequest(BaseModel):
    client_id: int
    goal: str
    
class HeightStepRequest(BaseModel):
    client_id: int
    height: float

class WeightStepRequest(BaseModel):
    client_id: int
    weight: float 
    target_weight: float


class BodyShapeStepRequest(BaseModel):
    client_id: int
    current_body_shape_id: str
    target_body_shape_id: str


class LifestyleStepRequest(BaseModel):
    client_id: int
    lifestyle: str


class RegistrationStepsStatus(BaseModel):
    status: int
    message: str
    data: Optional[dict] = None




# ==================== Helper Functions ====================

async def get_client_by_id(db: AsyncSession, client_id: int) -> Client:
    stmt = select(Client).where(Client.client_id == client_id)
    result = await db.execute(stmt)
    client = result.scalars().first()
    if not client:
        raise FittbotHTTPException(
            status_code=404,
            detail="Client not found",
            error_code="CLIENT_NOT_FOUND",
        )
    return client


def calculate_age(dob: date) -> int:
    today = date.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))


def calculate_bmi(weight: float, height: float) -> float:
    """Calculate BMI from weight (kg) and height (cm)"""
    height_m = height / 100
    return round(weight / (height_m ** 2), 2)


def calculate_bmr(weight: float, height: float, age: int, gender: str = "male") -> float:
    """Calculate Basal Metabolic Rate using Mifflin-St Jeor equation"""
    if gender.lower() == "male":
        return 10 * weight + 6.25 * height - 5 * age + 5
    else:
        return 10 * weight + 6.25 * height - 5 * age - 161


activity_multipliers = {
    "sedentary": 1.2,
    "lightly_active": 1.375,
    "moderately_active": 1.55,
    "very_active": 1.725,
    "super_active": 1.9,
}


def calculate_macros(calories: float, goals: str):
    """
    Returns grams: protein, carbs, fat, fiber, sugar_cap
    - fiber uses 14 g / 1000 kcal rule
    - sugar_cap is an upper limit (<=10% kcal)
    """
    if goals == "weight_loss":
        carbs_kcal = calories * 0.30
        protein_kcal = calories * 0.45
        fat_kcal = calories * 0.20
    elif goals == "weight_gain":
        carbs_kcal = calories * 0.45
        protein_kcal = calories * 0.35
        fat_kcal = calories * 0.20
    else:  # maintenance / recomposition
        carbs_kcal = calories * 0.35
        protein_kcal = calories * 0.35
        fat_kcal = calories * 0.30

    # grams
    carbs_g = round(carbs_kcal / 4)
    protein_g = round(protein_kcal / 4)
    fat_g = round(fat_kcal / 9)

    # fiber & sugar (grams)
    fiber_g = round((calories / 1000.0) * 14)  # 14 g per 1000 kcal
    sugar_cap_g = round((calories * 0.10) / 4)  # 10% of calories

    return protein_g, carbs_g, fat_g, fiber_g, sugar_cap_g


# ==================== Step 0: Initial Registration ====================

@router.post("/register", response_model=ClientRegistrationResponse)
async def register_client(
    request: ClientRegistrationRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis)
) -> ClientRegistrationResponse:

    try:
        stmt = select(Client).where(Client.contact == request.mobile_number)
        result = await db.execute(stmt)
        existing_client = result.scalars().first()

        if existing_client:
            raise FittbotHTTPException(
                status_code=400,
                detail="Mobile number is already registered",
                error_code="CLIENT_ALREADY_EXISTS",
            )

        new_client = Client(
            name=request.name,
            contact=request.mobile_number,
            gender=request.gender,
            email="",
            password="",
            verification='{"mobile": True, "password": false}',
            profile="https://fittbot-uploads.s3.ap-south-2.amazonaws.com/default.png",
            access=False,
            incomplete=True,
            modal_shown= True
        )

        db.add(new_client)
        await db.commit()
        await db.refresh(new_client)

        # Check if client already has a referral code
        stmt = select(ReferralCode).where(ReferralCode.client_id == new_client.client_id)
        result = await db.execute(stmt)
        existing_referral = result.scalars().first()

        if existing_referral:
            new_referral_code = existing_referral.referral_code
        else:
            # Generate unique referral code for this client
            max_retries = 5
            new_referral_code = None
            for attempt in range(max_retries):
                try:
                    # Generate a random referral code using client's name
                    candidate_code = generate_referral_code_random(name=new_client.name or "User")

                    # Check if code already exists in database
                    stmt = select(ReferralCode).where(ReferralCode.referral_code == candidate_code)
                    result = await db.execute(stmt)
                    existing = result.scalars().first()

                    if existing is None:
                        # Code is unique, use it
                        new_referral_code = candidate_code
                        referral_entry = ReferralCode(
                            client_id=new_client.client_id,
                            referral_code=new_referral_code,
                            created_at=datetime.now(),
                        )
                        db.add(referral_entry)
                        await db.flush()
                        break
                    else:
                        # Code exists, try again
                        auth_logger.warning(
                            f"Referral code collision on attempt {attempt + 1} for client {new_client.client_id}, retrying...",
                            error="Code already exists",
                        )
                except Exception as e:
                    await db.rollback()
                    auth_logger.warning(
                        f"Referral code generation attempt {attempt + 1} failed for client {new_client.client_id}, retrying...",
                        error=str(e),
                    )

            if new_referral_code is None:
                auth_logger.error(
                    f"Failed to generate referral code for client {new_client.client_id} after {max_retries} attempts",
                    error="All attempts exhausted",
                )

        # Handle referral if provided
        if request.referral_id:
            referral_code_used = request.referral_id.strip()
            stmt = select(ReferralCode).where(ReferralCode.referral_code == referral_code_used)
            result = await db.execute(stmt)
            referrer_entry = result.scalars().first()

            if referrer_entry:
                # Create referral mapping with completed status
                new_referral_mapping = ReferralMapping(
                    referrer_id=referrer_entry.client_id,
                    referee_id=new_client.client_id,
                    referral_date=date.today(),
                    status="completed"
                )
                db.add(new_referral_mapping)

                # Add 100 fittbot cash to the NEW client (referee)
                new_referee_cash = ReferralFittbotCash(
                    client_id=new_client.client_id,
                    fittbot_cash=100
                )
                db.add(new_referee_cash)

                # Add 100 fittbot cash to the REFERRER
                stmt = select(ReferralFittbotCash).where(
                    ReferralFittbotCash.client_id == referrer_entry.client_id
                )
                result = await db.execute(stmt)
                referrer_cash = result.scalars().first()

                if referrer_cash:
                    referrer_cash.fittbot_cash += 100
                else:
                    new_referrer_cash = ReferralFittbotCash(
                        client_id=referrer_entry.client_id,
                        fittbot_cash=100
                    )
                    db.add(new_referrer_cash)



                await db.commit()

        # Calculate water intake based on gender
        if new_client.gender and new_client.gender.lower() == "male":
            water_intake = 3.7
        elif new_client.gender and new_client.gender.lower() == "female":
            water_intake = 2.7
        else:
            water_intake = 3.0

        # Create ClientTarget record with water intake
        new_client_target = ClientTarget(
            client_id=new_client.client_id,
            water_intake=water_intake,
        )
        db.add(new_client_target)
        await db.commit()

        mobile_otp=generate_otp()
        await redis.set(f"otp:{request.mobile_number}", mobile_otp, ex=300)

        
        if await async_send_verification_sms(request.mobile_number, mobile_otp):
            print(f"Verification OTP send successfully to {request.mobile_number}")
        else:
            raise FittbotHTTPException(
                status_code=500,
                detail="Failed to send OTP",
                error_code="SMS_SEND_FAILED"
            )

        # Send iOS-specific premium SMS
        platform_value = (request.platform or "").strip().lower()
        if platform_value == "ios":
            await async_send_ios_premium_sms(
                phone_number=request.mobile_number,
                client_name=new_client.name or "User"
            )

        return ClientRegistrationResponse(
            status=200,
            message="Client registered successfully",
            client_id=new_client.client_id,
            contact=new_client.contact,
            full_name=new_client.name,
        )

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to register client",
            error_code="CLIENT_REGISTRATION_ERROR",
            log_data={"error": repr(exc)},
        )


# ==================== Step 0 V1: Registration with Tokens ====================

@router.post("/register_v1")
async def register_client_v1(
    request: ClientRegistrationRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):
    try:
        stmt = select(Client).where(Client.contact == request.mobile_number)
        result = await db.execute(stmt)
        existing_client = result.scalars().first()

        if not existing_client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found. Please use login_v1 first.",
                error_code="CLIENT_NOT_FOUND",
            )

        new_client = existing_client
        new_client.name = request.name
        new_client.gender = request.gender
        new_client.incomplete = False
        new_client.location = request.location
        if request.platform:
            new_client.platform = request.platform
            
        await db.commit()
        await db.refresh(new_client)

        # Check if client already has a referral code
        stmt = select(ReferralCode).where(ReferralCode.client_id == new_client.client_id)
        result = await db.execute(stmt)
        existing_referral = result.scalars().first()

        if existing_referral:
            new_referral_code = existing_referral.referral_code
        else:
            # Generate unique referral code for this client
            max_retries = 5
            new_referral_code = None
            for attempt in range(max_retries):
                try:
                    candidate_code = generate_referral_code_random(name=new_client.name or "User")

                    stmt = select(ReferralCode).where(ReferralCode.referral_code == candidate_code)
                    result = await db.execute(stmt)
                    existing = result.scalars().first()

                    if existing is None:
                        new_referral_code = candidate_code
                        referral_entry = ReferralCode(
                            client_id=new_client.client_id,
                            referral_code=new_referral_code,
                            created_at=datetime.now(),
                        )
                        db.add(referral_entry)
                        await db.flush()
                        break
                    else:
                        auth_logger.warning(
                            f"Referral code collision on attempt {attempt + 1} for client {new_client.client_id}, retrying...",
                            error="Code already exists",
                        )
                except Exception as e:
                    await db.rollback()
                    auth_logger.warning(
                        f"Referral code generation attempt {attempt + 1} failed for client {new_client.client_id}, retrying...",
                        error=str(e),
                    )

            if new_referral_code is None:
                auth_logger.error(
                    f"Failed to generate referral code for client {new_client.client_id} after {max_retries} attempts",
                    error="All attempts exhausted",
                )

        # Handle referral if provided
        if request.referral_id:
            referral_code_used = request.referral_id.strip()
            stmt = select(ReferralCode).where(ReferralCode.referral_code == referral_code_used)
            result = await db.execute(stmt)
            referrer_entry = result.scalars().first()

            if referrer_entry:
                new_referral_mapping = ReferralMapping(
                    referrer_id=referrer_entry.client_id,
                    referee_id=new_client.client_id,
                    referral_date=date.today(),
                    status="completed",
                )
                db.add(new_referral_mapping)

                new_referee_cash = ReferralFittbotCash(
                    client_id=new_client.client_id,
                    fittbot_cash=100,
                )
                db.add(new_referee_cash)

                stmt = select(ReferralFittbotCash).where(
                    ReferralFittbotCash.client_id == referrer_entry.client_id
                )
                result = await db.execute(stmt)
                referrer_cash = result.scalars().first()

                if referrer_cash:
                    referrer_cash.fittbot_cash += 100
                else:
                    new_referrer_cash = ReferralFittbotCash(
                        client_id=referrer_entry.client_id,
                        fittbot_cash=100,
                    )
                    db.add(new_referrer_cash)

                await db.commit()

        # Calculate water intake based on gender
        if new_client.gender and new_client.gender.lower() == "male":
            water_intake = 3.7
        elif new_client.gender and new_client.gender.lower() == "female":
            water_intake = 2.7
        else:
            water_intake = 3.0

        # Create ClientTarget record with water intake
        new_client_target = ClientTarget(
            client_id=new_client.client_id,
            water_intake=water_intake,
        )
        db.add(new_client_target)
        await db.commit()

        # Send iOS-specific premium SMS
        platform_value = (request.platform or "").strip().lower()
        if platform_value == "ios":
            await async_send_ios_premium_sms(
                phone_number=request.mobile_number,
                client_name=new_client.name or "User",
            )

        # Generate access and refresh tokens
        access_token = create_access_token({"sub": str(new_client.client_id), "role": "client"})
        refresh_token = create_refresh_token({"sub": str(new_client.client_id)})

        new_client.refresh_token = refresh_token
        await db.commit()

        return {
            "status": 200,
            "message": "Client registered successfully",
            "data": {
                "client_id": new_client.client_id,
                "contact": new_client.contact,
                "full_name": new_client.name,
                "access_token": access_token,
                "refresh_token": refresh_token,
                "gender": new_client.gender
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to register client",
            error_code="CLIENT_REGISTRATION_V1_ERROR",
            log_data={"error": repr(exc)},
        )


# ==================== Step 1: DOB ====================

@router.post("/step/dob", response_model=StepResponse)
async def update_dob_step(
    request: DOBStepRequest,
    db: AsyncSession = Depends(get_async_db),
) -> StepResponse:

    try:
        client = await get_client_by_id(db, request.client_id)

        dob = datetime.strptime(request.dob, "%Y-%m-%d").date()
        age = calculate_age(dob)

        client.dob = dob
        client.age = age

        await db.commit()
        await db.refresh(client)

        return StepResponse(
            status=200,
            message="DOB updated successfully"
        )

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update DOB",
            error_code="DOB_UPDATE_ERROR",
            log_data={"error": repr(exc), "client_id": request.client_id},
        )


# ==================== Step 2: Goal ====================

@router.post("/step/goal", response_model=StepResponse)
async def update_goal_step(
    request: GoalStepRequest,
    db: AsyncSession = Depends(get_async_db),
) -> StepResponse:
    """
    Step 2: Update client's fitness goal.
    """
    try:
        client = await get_client_by_id(db, request.client_id)

        client.goals = request.goal

        await db.commit()
        await db.refresh(client)

        return StepResponse(
            status=200,
            message="Goal updated successfully",
            data={"goal": request.goal},
        )

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update goal",
            error_code="GOAL_UPDATE_ERROR",
            log_data={"error": repr(exc), "client_id": request.client_id},
        )


# ==================== Step 3: Height ====================

@router.post("/step/height", response_model=StepResponse)
async def update_height_step(
    request: HeightStepRequest,
    db: AsyncSession = Depends(get_async_db),
) -> StepResponse:
    """
    Step 3: Update client's height.
    """
    try:
        client = await get_client_by_id(db, request.client_id)

        client.height = request.height

        # If weight already exists, calculate BMI
        if client.weight:
            client.bmi = calculate_bmi(client.weight, request.height)

        await db.commit()
        await db.refresh(client)

        return StepResponse(
            status=200,
            message="Height updated successfully",
            data={"height": request.height, "bmi": client.bmi},
        )

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update height",
            error_code="HEIGHT_UPDATE_ERROR",
            log_data={"error": repr(exc), "client_id": request.client_id},
        )


# ==================== Step 4: Weight ====================

@router.post("/step/weight", response_model=StepResponse)
async def update_weight_step(
    request: WeightStepRequest,
    db: AsyncSession = Depends(get_async_db),
) -> StepResponse:
    """
    Step 4: Update client's weight, target weight and calculate BMI.
    Also creates/updates ClientTarget record.
    """
    try:
        client = await get_client_by_id(db, request.client_id)

        client.weight = request.weight

        # If height exists, calculate BMI
        if client.height:
            client.bmi = calculate_bmi(request.weight, client.height)

        # Set water intake based on gender
        if client.gender and client.gender.lower() == "male":
            water_intake = 3.7
        elif client.gender and client.gender.lower() == "female":
            water_intake = 2.7
        else:
            water_intake = 3.0

        # Create or update ClientTarget
        stmt = select(ClientTarget).where(ClientTarget.client_id == request.client_id)
        result = await db.execute(stmt)
        client_target = result.scalars().first()

        if not client_target:
            client_target = ClientTarget(
                client_id=request.client_id,
                water_intake=water_intake,
                weight=int(request.target_weight),
                start_weight=request.weight,
            )
            db.add(client_target)
        else:
            client_target.water_intake = water_intake
            client_target.weight = int(request.target_weight)
            client_target.start_weight = request.weight

        await db.commit()
        await db.refresh(client)

        return StepResponse(
            status=200,
            message="Weight updated successfully",
            data={
                "weight": request.weight,
                "target_weight": request.target_weight,
                "bmi": client.bmi,
            },
        )

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update weight",
            error_code="WEIGHT_UPDATE_ERROR",
            log_data={"error": repr(exc), "client_id": request.client_id},
        )


# ==================== Step 5: Current & Target Body Shape ====================

@router.post("/step/body-shape", response_model=StepResponse)
async def update_body_shape_step(
    request: BodyShapeStepRequest,
    db: AsyncSession = Depends(get_async_db),
) -> StepResponse:
    """
    Step 5: Update client's current and target body shape selection.
    """
    try:
        client = await get_client_by_id(db, request.client_id)

        # Check if combination exists
        stmt = select(CharactersCombination).where(
            CharactersCombination.characters_id == request.current_body_shape_id,
            CharactersCombination.combination_id == request.target_body_shape_id,
        )
        result = await db.execute(stmt)
        combination = result.scalars().first()

        if combination:
            # Create or update ClientCharacter
            stmt = select(ClientCharacter).where(ClientCharacter.client_id == request.client_id)
            result = await db.execute(stmt)
            client_character = result.scalars().first()

            if client_character:
                client_character.character_id = combination.id
            else:
                client_character = ClientCharacter(
                    client_id=request.client_id,
                    character_id=combination.id,
                )
                db.add(client_character)

        # Check if ClientWeightSelection exists
        stmt = select(ClientWeightSelection).where(ClientWeightSelection.client_id == str(request.client_id))
        result = await db.execute(stmt)
        weight_selection = result.scalars().first()

        if weight_selection:
            weight_selection.current_image_id = request.current_body_shape_id
            weight_selection.target_image_id = request.target_body_shape_id
            weight_selection.combination_id = f"{request.current_body_shape_id}+{request.target_body_shape_id}"
        else:
            weight_selection = ClientWeightSelection(
                client_id=str(request.client_id),
                current_image_id=request.current_body_shape_id,
                target_image_id=request.target_body_shape_id,
                combination_id=f"{request.current_body_shape_id}+{request.target_body_shape_id}",
            )
            db.add(weight_selection)

        await db.commit()

        return StepResponse(
            status=200,
            message="Body shape updated successfully",
            data={
                "current_body_shape_id": request.current_body_shape_id,
                "target_body_shape_id": request.target_body_shape_id,
            },
        )

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update body shape",
            error_code="BODY_SHAPE_UPDATE_ERROR",
            log_data={"error": repr(exc), "client_id": request.client_id},
        )


# ==================== Step 6: Lifestyle ====================

@router.post("/step/lifestyle", response_model=StepResponse)
async def update_lifestyle_step(
    request: LifestyleStepRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
) -> StepResponse:
    """
    Step 6: Update client's lifestyle.
    This is the final step - calculates nutrition targets and marks registration as complete.
    """
    try:
        client = await get_client_by_id(db, request.client_id)

        client.lifestyle = request.lifestyle
        client.incomplete = False  # Mark registration as complete

        # Calculate nutrition targets (BMR, TDEE, macros)
        if client.weight and client.height and client.age and client.goals:
            # Calculate BMR
            bmr = calculate_bmr(
                weight=client.weight,
                height=client.height,
                age=client.age,
                gender=client.gender or "male"
            )

            # Calculate TDEE using activity multiplier
            multiplier = activity_multipliers.get(request.lifestyle, 1.2)
            tdee = bmr * multiplier

            # Adjust TDEE based on goals
            if client.goals == "weight_loss":
                tdee -= 500
            elif client.goals == "weight_gain":
                tdee += 500

            # Calculate macros
            protein, carbs, fat, fiber, sugar = calculate_macros(tdee, client.goals)

            # Get or create ClientTarget and update nutrition values
            stmt = select(ClientTarget).where(ClientTarget.client_id == request.client_id)
            result = await db.execute(stmt)
            client_target = result.scalars().first()

            if client_target:
                client_target.calories = int(tdee)
                client_target.protein = protein
                client_target.carbs = carbs
                client_target.fat = fat
                client_target.fiber = fiber
                client_target.sugar = sugar
                client_target.updated_at = datetime.now()

        await db.commit()
        await db.refresh(client)

        # Clear Redis cache for client data (same patterns as my_progress.py)
        cache_patterns = [
            f"client{request.client_id}:initial_target_actual",
            f"client{request.client_id}:initialstatus",
        ]
        for key in cache_patterns:
            if await redis.exists(key):
                await redis.delete(key)

        # Clear pattern-based keys
        pattern_keys = [
            "*:status",
            "*:target_actual",
            "*:chart",
            "*:analytics",
        ]
        for pattern in pattern_keys:
            keys = await redis.keys(pattern)
            if keys:
                await redis.delete(*keys)

        return StepResponse(
            status=200,
            message="Lifestyle updated successfully. Registration complete!",
            data={
                "lifestyle": request.lifestyle,
                "registration_complete": True,
            },
        )

    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update lifestyle",
            error_code="LIFESTYLE_UPDATE_ERROR",
            log_data={"error": repr(exc), "client_id": request.client_id},
        )


# ==================== Get Registration Steps Status ====================

@router.get("/steps-status", response_model=RegistrationStepsStatus)
async def get_registration_steps_status(
    client_id: int,
    db: AsyncSession = Depends(get_async_db),
) -> RegistrationStepsStatus:
    """
    Get the completion status of all registration steps for a client.
    """
    try:
        client = await get_client_by_id(db, client_id)

        # Check each step completion - handle None values safely
        dob_completed = client.dob is not None
        goal_completed = bool(client.goals and str(client.goals).strip())
        height_completed = client.height is not None
        weight_completed = client.weight is not None and client.bmi is not None

        # Check body shape
        stmt = select(ClientWeightSelection).where(ClientWeightSelection.client_id == str(client_id))
        result = await db.execute(stmt)
        weight_selection = result.scalars().first()
        body_shape_completed = weight_selection is not None

        lifestyle_completed = bool(client.lifestyle and str(client.lifestyle).strip())

        steps_data = {
            "dob": dob_completed,
            "goal": goal_completed,
            "height": height_completed,
            "weight": weight_completed,
            "body_shape": body_shape_completed,
            "lifestyle": lifestyle_completed,
            "registration_complete": not client.incomplete,
        }

        return RegistrationStepsStatus(
            status=200,
            message="Steps status retrieved successfully",
            data=steps_data,
        )

    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to get registration steps status",
            error_code="STEPS_STATUS_ERROR",
            log_data={"error": repr(exc), "client_id": client_id},
        )



class VerifyOTPRequest(BaseModel):
    mobile_number: str
    otp: str


@router.post("/verify-otp")
async def verify_otp(
    request: VerifyOTPRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):
    """
    Verify OTP and return access/refresh tokens with client details.
    """
    try:
        # Verify OTP
        stored_otp = await redis.get(f"otp:{request.mobile_number}")
        if not stored_otp or stored_otp != str(request.otp):
            raise HTTPException(status_code=400, detail="Invalid OTP")

        # Delete OTP after successful verification
        await redis.delete(f"otp:{request.mobile_number}")

        # Get client by mobile number
        stmt = select(Client).where(Client.contact == request.mobile_number)
        result = await db.execute(stmt)
        client = result.scalars().first()

        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail="Client not found",
                error_code="CLIENT_NOT_FOUND",
            )

        # Generate tokens
        access_token = create_access_token({"sub": str(client.client_id), "role": "client"})
        refresh_token = create_refresh_token({"sub": str(client.client_id)})

        # Save refresh token to client record
        client.refresh_token = refresh_token
        await db.commit()

        return {
            "status": 200,
            "message": "OTP verified successfully",
            "data": {
                "client_id": client.client_id,
                "gender": client.gender,
                "full_name": client.name,
                "contact": client.contact,
                "access_token": access_token,
                "refresh_token": refresh_token,
            }
        }

    except HTTPException:
        raise
    except FittbotHTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Internal server error occurred during OTP verification",
            error_code="OTP_VERIFY_ERROR",
            log_data={"error": repr(exc)},
        )
