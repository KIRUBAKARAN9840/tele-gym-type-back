# app/routers/my_rewards_router.py

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session
from sqlalchemy import asc, desc
from typing import List, Optional
import datetime

from app.models.database import get_db
from app.models.fittbot_models import (
    Client,
    LeaderboardOverall,
    RewardQuest,
    LeaderboardMonthly,
    RewardGym,
    RewardBadge,
    RewardPrizeHistory,
    ReferralRedeem,
    ReferralFittbotCash,
    ReferralFittbotCashLogs,
    ReferralCode,
    RewardProgramOptIn,
)
from app.utils.logging_utils import FittbotHTTPException
from app.utils.check_subscriptions import get_client_tier
from app.utils.referral_code_generator import generate_unique_referral_code
from sqlalchemy import func
from pydantic import BaseModel

router = APIRouter(prefix="/my_rewards", tags=["show_rewards"])


@router.get("/show_rewards_page")
async def show_rewards_page(
    request: Request,
    client_id: int,
    gym_id: Optional[int] = None,
    db: Session = Depends(get_db),
):
    try:
        tier = get_client_tier(db, client_id)
        client = db.query(Client).filter(Client.client_id == client_id).first()
        if not client:
            return {"status": 404, "message": "Client not found"}

        overall_query = db.query(LeaderboardOverall).filter(
            LeaderboardOverall.client_id == client_id
        )

        overall = overall_query.order_by(desc(LeaderboardOverall.xp)).first()


        if not overall:
            badge_record = None
            client_xp = 0

        else:

            client_xp = overall.xp
            badge_record = (
                db.query(RewardBadge)
                .filter(
                    RewardBadge.min_points <= client_xp,
                    RewardBadge.max_points > client_xp,
                )
                .first()
            )

        if badge_record:
            
            next_level = (
                db.query(RewardBadge)
                .filter(RewardBadge.min_points > client_xp)
                .order_by(asc(RewardBadge.min_points))
                .first()
            )

            next_level_start = next_level.min_points if next_level else None
            next_badge_name = next_level.badge if next_level else None
            next_badge_url = next_level.image_url if next_level else None

            client_badge = {
                "badge": badge_record.badge,
                "image_url": badge_record.image_url,
                "level": badge_record.level,
                "next_level_start": next_level_start,
                "client_xp": client_xp,
                "next_badge_name": next_badge_name,
                "next_badge_url": next_badge_url,
            }
        
        else:

            next_level = (
                db.query(RewardBadge)
                .order_by(asc(RewardBadge.min_points))
                .first()
            )
            client_badge = {
                "badge": "Beginner",
                "image_url": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/New_badges/Beginner.png",
                "next_level_start": 500,
                "client_xp": client_xp,
                "next_badge_name": "Warrior",
                "next_badge_url": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/New_badges/Warrior.png",
            }


        client_history = (
            db.query(RewardPrizeHistory)
            .filter(
                RewardPrizeHistory.client_id == client_id,
                RewardPrizeHistory.is_given.is_(True),
            )
            .all()
        ) or []


        quests = db.query(RewardQuest).all()
        gym_rewards = (
            db.query(RewardGym).filter(RewardGym.gym_id == gym_id).all() or []
            if gym_id is not None
            else []
        )


        monthly_query = db.query(LeaderboardMonthly).filter(
            LeaderboardMonthly.client_id == client_id
        )

        if gym_id is not None:
            monthly_query = monthly_query.filter(LeaderboardMonthly.gym_id == gym_id)
        
        
        monthly_leaderboard = (
            monthly_query.order_by(desc(LeaderboardMonthly.xp)).limit(3).all()
        )


        def to_dict_list(items: List) -> List[dict]:
            out = []
            for item in items:
                if hasattr(item, "__dict__"):
                    d = {k: v for k, v in item.__dict__.items() if k != "_sa_instance_state"}
                    out.append(d)
            return out

        def format_month(dt: datetime.date) -> str:
            try:
                return dt.strftime("%b").upper() + dt.strftime("'%y")
            except Exception:
                return str(dt)

        monthly_leaderboard_data = [
            {
                "month": format_month(record.month),
                "gym_id": record.gym_id,
                "client_id": record.client_id,
                "xp": record.xp,
            }
            for record in monthly_leaderboard
        ]


        total_redeemed = db.query(func.sum(ReferralRedeem.points_redeemed)).filter(
            ReferralRedeem.client_id == client_id
        ).scalar() or 0

        redeemable_xp = client_xp - total_redeemed

        actual_redeemable = (redeemable_xp // 100) * 100

        fittbot_cash_entry = db.query(ReferralFittbotCash).filter(
            ReferralFittbotCash.client_id == client_id
        ).first()

        fittbot_cash = fittbot_cash_entry.fittbot_cash if fittbot_cash_entry else 0

        referral_code_entry = db.query(ReferralCode).filter(
            ReferralCode.client_id == client_id
        ).first()

        if not referral_code_entry:
            try:
                new_referral_code = generate_unique_referral_code(
                    db=db,
                    name=client.name,
                    user_id=client_id,
                    method="sequential",
                    table_name="referral_code",
                    max_retries=3
                )
                referral_code_entry = ReferralCode(
                    client_id=client_id,
                    referral_code=new_referral_code,
                    created_at=datetime.datetime.now()
                )
                db.add(referral_code_entry)
                db.commit()
                referral_code = new_referral_code
            except Exception as e:
                referral_code = None
        else:
            referral_code = referral_code_entry.referral_code

        reward_opt_in = (
            db.query(RewardProgramOptIn)
            .filter(RewardProgramOptIn.client_id == client_id)
            .first()
        )

        reward_interest_modal = reward_opt_in is not None

        

        return {
            "status": 200,
            "data": {
                "client_badge": client_badge,
                "client_history": to_dict_list(client_history),
                "quest": to_dict_list(quests),
                "gym_rewards": to_dict_list(gym_rewards),
                "monthly_leaderboard": monthly_leaderboard_data,
                "redeemable_xp": redeemable_xp,
                "actual_redeemable": actual_redeemable,
                "fittbot_cash": fittbot_cash,
                "referral_code": referral_code,
                "reward_interest_modal": reward_interest_modal,
            },
        }

    except FittbotHTTPException:
        raise
    
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to retrieve rewards information",
            error_code="MY_REWARDS_FETCH_ERROR",
            log_data={"exc": repr(e), "client_id": client_id},
        )


class RedeemRequest(BaseModel):
    client_id: int
    redeemable_points: int


@router.post("/redeem_points")
async def redeem_points(request: RedeemRequest, db: Session = Depends(get_db)):

    try:
        client_id = request.client_id
        redeemable_points = request.redeemable_points

        client = db.query(Client).filter(Client.client_id == client_id).first()
        if not client:
            raise FittbotHTTPException(
                status_code=404,
                detail=f"Client with id {client_id} not found",
                error_code="CLIENT_NOT_FOUND",
                log_data={"client_id": client_id},
            )

        if redeemable_points % 100 != 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Redeemable points must be a multiple of 100",
                error_code="INVALID_REDEEM_AMOUNT",
                log_data={"client_id": client_id, "redeemable_points": redeemable_points},
            )


        cash_to_add = redeemable_points // 100

        fittbot_cash_entry = db.query(ReferralFittbotCash).filter(
            ReferralFittbotCash.client_id == client_id
        ).first()

        if fittbot_cash_entry:

            fittbot_cash_entry.fittbot_cash += cash_to_add
        else:
        
            new_cash_entry = ReferralFittbotCash(
                client_id=client_id,
                fittbot_cash=cash_to_add
            )
            db.add(new_cash_entry)


        log_entry = ReferralFittbotCashLogs(
            client_id=client_id,
            fittbot_cash=cash_to_add,
            reason=f"Redeemed {redeemable_points} XP points"
        )
        db.add(log_entry)


        redeem_entry = ReferralRedeem(
            client_id=client_id,
            points_redeemed=redeemable_points
        )
        db.add(redeem_entry)

        db.commit()

        return {
            "status": 200,
            "message": f"Successfully redeemed {redeemable_points} points for ₹{cash_to_add}",
            "data": {
                "client_id": client_id,
                "points_redeemed": redeemable_points,
                "cash_earned": cash_to_add
            }
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Unable to redeem points",
            error_code="REDEEM_POINTS_ERROR",
            log_data={"exc": repr(e), "client_id": request.client_id},
        )
