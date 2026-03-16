from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_, extract, or_
from app.models.marketingmodels import Feedback, GymVisits, Executives, Managers
from app.models.database import get_db
from typing import Optional, List, Dict, Any
from datetime import datetime, date

router = APIRouter(prefix="/marketing/feedback", tags=["Feedback"])

class FeedbackCreate(BaseModel):
    visit_id: int
    manager_id: int
    category: str
    rating: int
    comments: str
    suggestions: Optional[str] = None
    positive_points: Optional[str] = None
    improvement_areas: Optional[str] = None

class FeedbackResponse(BaseModel):
    id: int
    visit_id: int
    manager_id: int
    executive_id: int
    category: str
    rating: int
    comments: str
    suggestions: Optional[str]
    positive_points: Optional[str]
    improvement_areas: Optional[str]
    created_at: datetime
    updated_at: datetime
    visit: Optional[Dict]
    executive: Optional[Dict]
    manager: Optional[Dict]

class FeedbackStats(BaseModel):
    total_feedback: int
    average_rating: float
    this_month: int
    executives_count: int
    category_breakdown: Dict[str, int]
    rating_distribution: Dict[str, int]

@router.post("/submit")
async def submit_feedback(
    feedback_data: FeedbackCreate,
    db: Session = Depends(get_db)
):
    try:
        visit = db.query(GymVisits).join(Executives).filter(
            and_(
                GymVisits.id == feedback_data.visit_id,
                Executives.manager_id == feedback_data.manager_id
            )
        ).first()
        
        if not visit:
            raise HTTPException(status_code=404, detail="Visit not found or unauthorized")
        
        existing_feedback = db.query(Feedback).filter(
            Feedback.visit_id == feedback_data.visit_id
        ).first()
        
        if existing_feedback:
            raise HTTPException(status_code=400, detail="Feedback already submitted for this visit")
        
        new_feedback = Feedback(
            visit_id=feedback_data.visit_id,
            manager_id=feedback_data.manager_id,
            executive_id=visit.user_id,
            category=feedback_data.category,
            rating=feedback_data.rating,
            comments=feedback_data.comments,
            suggestions=feedback_data.suggestions,
            positive_points=feedback_data.positive_points,
            improvement_areas=feedback_data.improvement_areas
        )
        
        db.add(new_feedback)
        
        visit.feedback_submitted = True
        
        db.commit()
        db.refresh(new_feedback)
        
        return {
            "status": 200,
            "message": "Feedback submitted successfully",
            "data": new_feedback
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"Error submitting feedback: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/visit/{visit_id}")
async def get_feedback_by_visit(
    visit_id: int,
    db: Session = Depends(get_db)
):
    try:
        feedback = db.query(Feedback).filter(Feedback.visit_id == visit_id).first()
        
        if not feedback:
            return {
                "status": 404,
                "message": "No feedback found for this visit",
                "data": None
            }
        
        return {
            "status": 200,
            "message": "Feedback retrieved successfully",
            "data": feedback
        }
        
    except Exception as e:
        print(f"Error getting feedback: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/executive/{executive_id}")
async def get_executive_feedback(
    executive_id: int,
    manager_id: int,
    db: Session = Depends(get_db)
):
    try:
        executive = db.query(Executives).filter(
            and_(
                Executives.id == executive_id,
                Executives.manager_id == manager_id
            )
        ).first()
        
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found")
        
        feedback_list = db.query(Feedback).filter(
            Feedback.executive_id == executive_id
        ).order_by(desc(Feedback.created_at)).all()
        
        feedback_data = []
        for feedback in feedback_list:
            visit = db.query(GymVisits).filter(GymVisits.id == feedback.visit_id).first()
            
            feedback_data.append({
                "id": feedback.id,
                "visit_id": feedback.visit_id,
                "category": feedback.category,
                "rating": feedback.rating,
                "comments": feedback.comments,
                "suggestions": feedback.suggestions,
                "positive_points": feedback.positive_points,
                "improvement_areas": feedback.improvement_areas,
                "created_at": feedback.created_at,
                "visit": {
                    "gym_name": visit.gym_name if visit else None,
                    "contact_person": visit.contact_person if visit else None,
                    "created_at": visit.created_at if visit else None
                }
            })
        
        return {
            "status": 200,
            "message": "Executive feedback retrieved successfully",
            "data": feedback_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting executive feedback: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/all")
async def get_all_feedback(
    manager_id: int,
    category: Optional[str] = Query(None),
    rating: Optional[int] = Query(None),
    executive_id: Optional[int] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        query = db.query(Feedback).join(Executives).filter(
            Executives.manager_id == manager_id
        )
        
        if category:
            query = query.filter(Feedback.category == category)
        
        if rating:
            query = query.filter(Feedback.rating == rating)
        
        if executive_id:
            query = query.filter(Feedback.executive_id == executive_id)
        
        if start_date:
            start_dt = datetime.fromisoformat(start_date)
            query = query.filter(Feedback.created_at >= start_dt)
        
        if end_date:
            end_dt = datetime.fromisoformat(end_date)
            query = query.filter(Feedback.created_at <= end_dt)
        
        feedback_list = query.order_by(desc(Feedback.created_at)).all()
        
        feedback_data = []
        for feedback in feedback_list:
            visit = db.query(GymVisits).filter(GymVisits.id == feedback.visit_id).first()
            executive = db.query(Executives).filter(Executives.id == feedback.executive_id).first()
            
            feedback_data.append({
                "id": feedback.id,
                "visit_id": feedback.visit_id,
                "category": feedback.category,
                "rating": feedback.rating,
                "comments": feedback.comments,
                "suggestions": feedback.suggestions,
                "positive_points": feedback.positive_points,
                "improvement_areas": feedback.improvement_areas,
                "created_at": feedback.created_at,
                "visit": {
                    "gym_name": visit.gym_name if visit else None,
                    "contact_person": visit.contact_person if visit else None,
                    "created_at": visit.created_at if visit else None
                },
                "executive": {
                    "name": executive.name if executive else None,
                    "employee_id": executive.emp_id if executive else None
                }
            })
        
        total_feedback = len(feedback_data)
        average_rating = sum(f["rating"] for f in feedback_data) / total_feedback if total_feedback > 0 else 0
        
        current_month = datetime.now().month
        current_year = datetime.now().year
        this_month = len([f for f in feedback_data if f["created_at"].month == current_month and f["created_at"].year == current_year])
        
        executives_count = len(set(f["executive"]["name"] for f in feedback_data if f["executive"]["name"]))
        
        stats = {
            "total_feedback": total_feedback,
            "average_rating": round(average_rating, 1),
            "this_month": this_month,
            "executives_count": executives_count
        }
        
        return {
            "status": 200,
            "message": "Feedback retrieved successfully",
            "data": {
                "feedback": feedback_data,
                "stats": stats
            }
        }
        
    except Exception as e:
        print(f"Error getting all feedback: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/stats")
async def get_feedback_stats(
    manager_id: Optional[int] = Query(None),
    executive_id: Optional[int] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        query = db.query(Feedback)
        
        if manager_id:
            query = query.join(Executives).filter(Executives.manager_id == manager_id)
        elif executive_id:
            query = query.filter(Feedback.executive_id == executive_id)
        
        feedback_list = query.all()
        
        total_feedback = len(feedback_list)
        
        if total_feedback == 0:
            return {
                "status": 200,
                "data": {
                    "total_feedback": 0,
                    "average_rating": 0,
                    "this_month": 0,
                    "executives_count": 0,
                    "category_breakdown": {},
                    "rating_distribution": {}
                }
            }
        
        average_rating = sum(f.rating for f in feedback_list) / total_feedback
        
        current_month = datetime.now().month
        current_year = datetime.now().year
        this_month = len([f for f in feedback_list if f.created_at.month == current_month and f.created_at.year == current_year])
        
        executives_count = len(set(f.executive_id for f in feedback_list))
        
        category_breakdown = {}
        for feedback in feedback_list:
            category_breakdown[feedback.category] = category_breakdown.get(feedback.category, 0) + 1
        
        rating_distribution = {}
        for feedback in feedback_list:
            rating_key = str(feedback.rating)
            rating_distribution[rating_key] = rating_distribution.get(rating_key, 0) + 1
        
        return {
            "status": 200,
            "data": {
                "total_feedback": total_feedback,
                "average_rating": round(average_rating, 1),
                "this_month": this_month,
                "executives_count": executives_count,
                "category_breakdown": category_breakdown,
                "rating_distribution": rating_distribution
            }
        }
        
    except Exception as e:
        print(f"Error getting feedback stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.put("/update/{feedback_id}")
async def update_feedback(
    feedback_id: int,
    update_data: dict,
    db: Session = Depends(get_db)
):
    try:
        feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()
        
        if not feedback:
            raise HTTPException(status_code=404, detail="Feedback not found")
        
        allowed_fields = ['category', 'rating', 'comments', 'suggestions', 'positive_points', 'improvement_areas']
        for field, value in update_data.items():
            if field in allowed_fields and value is not None:
                setattr(feedback, field, value)
        
        feedback.updated_at = datetime.now()
        
        db.commit()
        db.refresh(feedback)
        
        return {
            "status": 200,
            "message": "Feedback updated successfully",
            "data": feedback
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"Error updating feedback: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.delete("/delete/{feedback_id}")
async def delete_feedback(
    feedback_id: int,
    db: Session = Depends(get_db)
):
    try:
        feedback = db.query(Feedback).filter(Feedback.id == feedback_id).first()
        
        if not feedback:
            raise HTTPException(status_code=404, detail="Feedback not found")
        
        visit = db.query(GymVisits).filter(GymVisits.id == feedback.visit_id).first()
        if visit:
            visit.feedback_submitted = False
        
        db.delete(feedback)
        db.commit()
        
        return {
            "status": 200,
            "message": "Feedback deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"Error deleting feedback: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
