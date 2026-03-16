# app/routers/ledger.py

import json
from datetime import datetime, date
from typing import Optional, Dict

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import extract, func
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import (
    FittbotGymMembership,
    Expenditure,
    GymMonthlyData,
    GymAnalysis,
    GymBusinessPayment,
)
from app.utils.logging_utils import FittbotHTTPException

router = APIRouter(prefix="/ledger", tags=["Analytics"])


# ---------- helpers ----------

def _ensure_expenditure_bucket(blob) -> Dict:
    """
    Ensure analysis blob is a dict with the 'expenditure_data' bucket.
    """
    if blob is None:
        blob = {}
    if isinstance(blob, str):
        try:
            blob = json.loads(blob)
        except json.JSONDecodeError:
            blob = {}
    if "expenditure_data" not in blob or not isinstance(blob["expenditure_data"], dict):
        blob["expenditure_data"] = {}
    return blob


# ---------- routes ----------

@router.get("/collection_summary")
async def collection_summary(
    gym_id: int,
    scope: str = Query(..., regex="^(current_month|current_week|custom_interval|overall|specific_month_year)$"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    month: Optional[int] = None,
    year: Optional[int] = None,
    db: Session = Depends(get_db),
):
    try:
        now = datetime.now()

        # Validate inputs
        if scope == "custom_interval":
            if not (start_date and end_date):
                raise FittbotHTTPException(
                    status_code=400,
                    detail="start_date and end_date are required for custom_interval",
                    error_code="MISSING_INTERVAL",
                    log_data={"gym_id": gym_id, "scope": scope},
                )
        elif scope == "specific_month_year":
            if not (month and year):
                raise FittbotHTTPException(
                    status_code=400,
                    detail="month and year are required for specific_month_year",
                    error_code="MISSING_MONTH_YEAR",
                    log_data={"gym_id": gym_id, "scope": scope},
                )

        # Build filters for FittbotGymMembership, Expenditure, and GymBusinessPayment
        membership_filters = [FittbotGymMembership.gym_id == str(gym_id)]
        exp_filters = [Expenditure.gym_id == gym_id]
        dailypass_filters = [
            GymBusinessPayment.gym_id == str(gym_id),
            GymBusinessPayment.mode == "dailypass"
        ]

        if scope == "current_month":
            membership_filters += [
                extract("month", FittbotGymMembership.joined_at) == now.month,
                extract("year", FittbotGymMembership.joined_at) == now.year,
            ]
            exp_filters += [
                extract("month", Expenditure.date) == now.month,
                extract("year", Expenditure.date) == now.year,
            ]
            dailypass_filters += [
                extract("month", GymBusinessPayment.date) == now.month,
                extract("year", GymBusinessPayment.date) == now.year,
            ]
        elif scope == "current_week":
            # Calculate start of current week (Monday)
            from datetime import timedelta
            start_of_week = now.date() - timedelta(days=now.weekday())
            end_of_week = start_of_week + timedelta(days=6)
            membership_filters.append(
                func.date(FittbotGymMembership.joined_at).between(start_of_week, end_of_week)
            )
            exp_filters.append(Expenditure.date.between(start_of_week, end_of_week))
            dailypass_filters.append(GymBusinessPayment.date.between(start_of_week, end_of_week))
        elif scope == "custom_interval":
            membership_filters.append(
                func.date(FittbotGymMembership.joined_at).between(start_date, end_date)
            )
            exp_filters.append(Expenditure.date.between(start_date, end_date))
            dailypass_filters.append(GymBusinessPayment.date.between(start_date, end_date))
        elif scope == "specific_month_year":
            membership_filters += [
                extract("month", FittbotGymMembership.joined_at) == month,
                extract("year", FittbotGymMembership.joined_at) == year,
            ]
            exp_filters += [
                extract("month", Expenditure.date) == month,
                extract("year", Expenditure.date) == year,
            ]
            dailypass_filters += [
                extract("month", GymBusinessPayment.date) == month,
                extract("year", GymBusinessPayment.date) == year,
            ]
        # else: overall → no additional filters

        # Query total collection from FittbotGymMembership (other revenue - memberships)
        other_revenue = (
            db.query(func.coalesce(func.sum(FittbotGymMembership.amount), 0.0))
            .filter(*membership_filters)
            .scalar()
        ) or 0.0

        # Query daily pass revenue from GymBusinessPayment
        dailypass_revenue = (
            db.query(func.coalesce(func.sum(GymBusinessPayment.amount), 0.0))
            .filter(*dailypass_filters)
            .scalar()
        ) or 0.0

        # Calculate total collection (memberships + daily passes)
        total_collection = float(other_revenue) + float(dailypass_revenue)

        # Query total expenditure
        total_expenditure = (
            db.query(func.coalesce(func.sum(Expenditure.amount), 0.0))
            .filter(*exp_filters)
            .scalar()
        ) or 0.0

        # Count memberships as receipt count
        membership_count = (
            db.query(func.count(FittbotGymMembership.id))
            .filter(*membership_filters)
            .scalar()
        ) or 0

        # Calculate profit (total collection - expenditure)
        profit = float(total_collection) - float(total_expenditure)

        return {
            "status": 200,
            "data": {
                "total_collection": float(total_collection),
                "other_revenue": float(other_revenue),
                "dailypass_revenue": float(dailypass_revenue),
                "expenditure": float(total_expenditure),
                "profit": float(profit),
                "receipt_count": int(membership_count),
            },
        }
    except FittbotHTTPException:
        raise
    except HTTPException:
        raise
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch collection summary",
            error_code="COLLECTION_SUMMARY_ERROR",
            log_data={"gym_id": gym_id, "scope": scope, "error": repr(e)},
        )


@router.get("/view_expenditure")
async def list_expenditures(
    gym_id: int,
    db: Session = Depends(get_db),
):
    try:
        records = (
            db.query(Expenditure)
            .filter(Expenditure.gym_id == gym_id)
            .order_by(Expenditure.date.desc())
            .all()
        )
        expenditures = [
            {
                "expenditure_id": exp.expenditure_id,
                "gym_id": exp.gym_id,
                "expenditure_type": exp.expenditure_type,
                "amount": float(exp.amount),
                "date": exp.date.isoformat() if hasattr(exp.date, "isoformat") else str(exp.date),
            }
            for exp in records
        ]

        return {"status": 200, "data": expenditures}
    except Exception as e:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch expenditures",
            error_code="EXPENDITURE_LIST_ERROR",
            log_data={"gym_id": gym_id, "error": repr(e)},
        )


class AddExpenditureRequest(BaseModel):
    gym_id: int
    date: str  # "YYYY-MM-DD"
    type: str
    amount: float


class UpdateExpenditureRequest(BaseModel):
    expense_id: int
    gym_id: int
    date: str  # "YYYY-MM-DD"
    type: str
    amount: float


@router.post("/add_expenditure")
async def add_expenditure(
    request: AddExpenditureRequest,
    db: Session = Depends(get_db),
):
    try:
        
        from datetime import timedelta

        recent_duplicate = (
            db.query(Expenditure)
            .filter(
                Expenditure.gym_id == request.gym_id,
                Expenditure.expenditure_type == request.type,
                Expenditure.amount == request.amount,
                Expenditure.date == request.date
            )
            .first()
        )

        if recent_duplicate:
            raise FittbotHTTPException(
                status_code=409,
                detail="Duplicate expenditure detected. This expenditure was just added.",
                error_code="DUPLICATE_EXPENDITURE",
                log_data={"gym_id": request.gym_id, "type": request.type, "amount": request.amount},
            )

        # Create new expenditure record
        exp = Expenditure(
            gym_id=request.gym_id,
            expenditure_type=request.type,
            amount=request.amount,
            date=request.date,
        )
        db.add(exp)
        # REMOVED early commit - now atomic

        # update monthly aggregate
        month_tag = datetime.now().strftime("%Y-%m")
        rec = (
            db.query(GymMonthlyData)
            .filter(
                GymMonthlyData.gym_id == request.gym_id,
                GymMonthlyData.month_year.like(f"{month_tag}%"),
            )
            .first()
        )
        if rec:
            rec.expenditure = float(rec.expenditure or 0) + float(request.amount)
        else:
            db.add(
                GymMonthlyData(
                    gym_id=request.gym_id,
                    month_year=datetime.now().strftime("%Y-%m-%d"),
                    income=0,
                    expenditure=request.amount,
                    new_entrants=0,
                )
            )

        # update analysis blob
        ga = db.query(GymAnalysis).filter(GymAnalysis.gym_id == request.gym_id).first()
        if not ga:
            ga = GymAnalysis(
                gym_id=request.gym_id,
                analysis_type="expenditure",
                analysis_name="expenditure_data",
                value=0.0,
                analysis={}
            )
            db.add(ga)
            db.flush()

        analysis = _ensure_expenditure_bucket(ga.analysis)
        analysis["expenditure_data"][request.type] = float(
            analysis["expenditure_data"].get(request.type, 0)
        ) + float(request.amount)
        ga.analysis = analysis

        # SINGLE ATOMIC COMMIT - All changes committed together
        db.commit()
        db.refresh(exp)

        return {
            "status": 200,
            "message": "Expenditure added successfully",
            "expenditure_id": exp.expenditure_id
        }
    except FittbotHTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to add expenditure",
            error_code="ADD_EXPENDITURE_ERROR",
            log_data={"gym_id": request.gym_id, "error": repr(e)},
        )


@router.post("/update_expenditure")
async def update_expenditure(
    request: UpdateExpenditureRequest,
    db: Session = Depends(get_db),
):
    try:
        exp = (
            db.query(Expenditure)
            .filter(Expenditure.expenditure_id == request.expense_id)
            .first()
        )
        if not exp:
            raise FittbotHTTPException(
                status_code=404,
                detail="Expense not found",
                error_code="EXPENSE_NOT_FOUND",
                log_data={"expense_id": request.expense_id},
            )

        old_type, old_amount, old_date = exp.expenditure_type, float(exp.amount or 0), str(exp.date)

        # Update record
        exp.expenditure_type = request.type
        exp.amount = request.amount
        exp.date = request.date
        db.flush()

        # Update monthly aggregate
        new_month_tag = datetime.strptime(request.date, "%Y-%m-%d").strftime("%Y-%m")
        old_month_tag = datetime.strptime(old_date, "%Y-%m-%d").strftime("%Y-%m")

        # Adjust old month
        old_rec = (
            db.query(GymMonthlyData)
            .filter(
                GymMonthlyData.gym_id == request.gym_id,
                GymMonthlyData.month_year.like(f"{old_month_tag}%"),
            )
            .first()
        )
        if old_rec:
            old_rec.expenditure = float(old_rec.expenditure or 0) - old_amount

        # Add to new month
        new_rec = (
            db.query(GymMonthlyData)
            .filter(
                GymMonthlyData.gym_id == request.gym_id,
                GymMonthlyData.month_year.like(f"{new_month_tag}%"),
            )
            .first()
        )
        if new_rec:
            new_rec.expenditure = float(new_rec.expenditure or 0) + float(request.amount)
        else:
            db.add(
                GymMonthlyData(
                    gym_id=request.gym_id,
                    month_year=request.date,
                    income=0,
                    expenditure=request.amount,
                    new_entrants=0,
                )
            )

        # Update analysis blob
        ga = db.query(GymAnalysis).filter(GymAnalysis.gym_id == request.gym_id).first()
        if not ga:
            ga = GymAnalysis(
                gym_id=request.gym_id,
                analysis_type="expenditure",
                analysis_name="expenditure_data",
                value=0.0,
                analysis={}
            )
            db.add(ga)
            db.flush()

        analysis = _ensure_expenditure_bucket(ga.analysis)

        # subtract from old bucket
        analysis["expenditure_data"][old_type] = float(
            analysis["expenditure_data"].get(old_type, 0)
        ) - old_amount
        # add to new bucket
        analysis["expenditure_data"][request.type] = float(
            analysis["expenditure_data"].get(request.type, 0)
        ) + float(request.amount)
        ga.analysis = analysis

        db.commit()

        return {"status": 200, "message": "Expenditure updated successfully"}
    except FittbotHTTPException:
        db.rollback()
        raise
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to update expenditure",
            error_code="UPDATE_EXPENDITURE_ERROR",
            log_data={"gym_id": request.gym_id, "expense_id": request.expense_id, "error": repr(e)},
        )


@router.delete("/delete_expenditure")
async def delete_expenditure(
    gym_id: int,
    expense_id: int,
    db: Session = Depends(get_db),
):
    try:
        exp = (
            db.query(Expenditure)
            .filter(Expenditure.expenditure_id == expense_id)
            .first()
        )
        if not exp:
            raise FittbotHTTPException(
                status_code=404,
                detail="Expense not found",
                error_code="EXPENSE_NOT_FOUND",
                log_data={"expense_id": expense_id},
            )

        # Adjust monthly aggregate for that expense month
        month_tag = str(exp.date)[:7]
        rec = (
            db.query(GymMonthlyData)
            .filter(
                GymMonthlyData.gym_id == gym_id,
                GymMonthlyData.month_year.like(f"{month_tag}%"),
            )
            .first()
        )
        if rec:
            rec.expenditure = float(rec.expenditure or 0) - float(exp.amount or 0)

        # Update analysis blob
        ga = db.query(GymAnalysis).filter(GymAnalysis.gym_id == gym_id).first()
        if not ga:
            ga = GymAnalysis(
                gym_id=gym_id,
                analysis_type="expenditure",
                analysis_name="expenditure_data",
                value=0.0,
                analysis={}
            )
            db.add(ga)
            db.flush()

        analysis = _ensure_expenditure_bucket(ga.analysis)
        old_type = exp.expenditure_type
        analysis["expenditure_data"][old_type] = float(
            analysis["expenditure_data"].get(old_type, 0)
        ) - float(exp.amount or 0)
        ga.analysis = analysis

        db.commit()

        # Now delete the record
        db.delete(exp)
        db.commit()

        return {"status": 200, "message": "Expenditure deleted successfully"}
    except FittbotHTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to delete expenditure",
            error_code="DELETE_EXPENDITURE_ERROR",
            log_data={"gym_id": gym_id, "expense_id": expense_id, "error": repr(e)},
        )
