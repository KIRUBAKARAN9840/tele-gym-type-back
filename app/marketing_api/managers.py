from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.models.marketingmodels import Executives, GymVisits, Managers, Feedback, GymAssignments
from app.models.database import get_db
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from app.utils.redis_config import get_redis
from redis.asyncio import Redis
from sqlalchemy import func, desc, and_, extract
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


router = APIRouter(prefix="/marketing/managers", tags=["Managers"])

class ExecutiveResponse(BaseModel):
    id: int
    manager_id: Optional[int]
    name: str
    email: str
    contact: str
    profile: str
    dob: Optional[date]
    age: Optional[int]
    gender: Optional[str]
    role: Optional[str]
    joined_date: Optional[date]
    status: Optional[str]
    uuid: str
    employee_id: Optional[str]
    total_visits: int
    completed_visits: int
    pending_visits: int
    conversion_rate: float
    converted: float
    followups: float
    last_visit_date: Optional[datetime]
    performance_score: int
    total_assigned: int
    followup_count: int
    pending_count: int
    converted_count: int
    rejected_count: int

class ExecutiveStats(BaseModel):
    total_executives: int
    active_executives: int
    inactive_executives: int
    new_this_month: int
    total_visits: int
    total_conversions: int
    average_conversion_rate: float
    top_performer: Optional[str]

class DashboardResponse(BaseModel):
    stats: ExecutiveStats
    executives: List[ExecutiveResponse]

@router.get("/executives-dashboard")
async def get_executives_dashboard(
    user_id: int, 
    db: Session = Depends(get_db), 
    redis: Redis = Depends(get_redis)
):
    try:
        executives = db.query(Executives).filter(
            Executives.manager_id == user_id
        ).all()

        manager=db.query(Managers).filter(Managers.id == user_id).first()

        manager_profile={
            "user_id": manager.id,
            "name": manager.name,
            "email": manager.email,
            "contact": manager.contact,
            "profile": manager.profile,
            "role":manager.role
        }
        
        total_executives = len(executives)
        active_executives = len([e for e in executives if e.status == 'active'])
        inactive_executives = len([e for e in executives if e.status == 'inactive'])
        
        current_month = datetime.now().month
        current_year = datetime.now().year
        new_this_month = db.query(Executives).filter(
            and_(
                Executives.manager_id == user_id,
                extract('month', Executives.joined_date) == current_month,
                extract('year', Executives.joined_date) == current_year
            )
        ).count()
        
        total_visits = db.query(GymVisits).filter(
            GymVisits.user_id.in_([e.id for e in executives])
        ).count()
        
        total_conversions = db.query(GymVisits).filter(
            and_(
                GymVisits.user_id.in_([e.id for e in executives]),
                GymVisits.final_status == 'converted'
            )
        ).count()
        
        executives_data = []
        conversion_rates = []
        
        for executive in executives:
            exec_total_visits = db.query(GymVisits).filter(
                GymVisits.user_id == executive.id
            ).count()
            
            exec_completed_visits = db.query(GymVisits).filter(
                and_(
                    GymVisits.user_id == executive.id,
                    GymVisits.completed == True
                )
            ).count()
            
            exec_pending_visits = db.query(GymVisits).filter(
                and_(
                    GymVisits.user_id == executive.id,
                    GymVisits.completed == False
                )
            ).count()
            
            exec_conversions = db.query(GymVisits).filter(
                and_(
                    GymVisits.user_id == executive.id,
                    GymVisits.final_status == 'converted'
                )
            ).count()

            exec_followups = db.query(GymVisits).filter(
                and_(
                    GymVisits.user_id == executive.id,
                    GymVisits.final_status == 'followup'
                )
            ).count()

            total_assigned = db.query(GymVisits).filter(
                GymVisits.user_id == executive.id
            ).count()

            followup_count = db.query(GymVisits).filter(
                and_(
                    GymVisits.user_id == executive.id,
                    GymVisits.final_status == 'followup'
                )
            ).count()

            pending_count = db.query(GymVisits).filter(
                and_(
                    GymVisits.user_id == executive.id,
                    GymVisits.final_status == 'pending'
                )
            ).count()

            converted_count = db.query(GymVisits).filter(
                and_(
                    GymVisits.user_id == executive.id,
                    GymVisits.final_status == 'converted'
                )
            ).count()

            rejected_count = db.query(GymVisits).filter(
                and_(
                    GymVisits.user_id == executive.id,
                    GymVisits.final_status == 'rejected'
                )
            ).count()

            conversion_rate = (exec_conversions / exec_total_visits * 100) if exec_total_visits > 0 else 0
            conversion_rates.append(conversion_rate)
            
            last_visit = db.query(GymVisits).filter(
                GymVisits.user_id == executive.id
            ).order_by(desc(GymVisits.created_at)).first()
            
            last_visit_date = last_visit.created_at if last_visit else None
            
            visits_score = min(exec_completed_visits * 10, 40)  
            conversion_score = min(conversion_rate * 0.4, 40)  
            
            recency_score = 0
            if last_visit_date:
                days_since_last = (datetime.now() - last_visit_date).days
                if days_since_last <= 7:
                    recency_score = 20
                elif days_since_last <= 14:
                    recency_score = 15
                elif days_since_last <= 30:
                    recency_score = 10
                else:
                    recency_score = 5
            
            performance_score = int(visits_score + conversion_score + recency_score)
            
            executives_data.append(ExecutiveResponse(
                id=executive.id,
                manager_id=executive.manager_id,
                name=executive.name,
                email=executive.email,
                contact=executive.contact,
                profile=executive.profile,
                dob=executive.dob,
                age=executive.age,
                gender=executive.gender,
                role=executive.role,
                joined_date=executive.joined_date,
                status=executive.status,
                uuid=executive.uuid,
                employee_id=executive.emp_id,
                total_visits=exec_total_visits,
                completed_visits=exec_completed_visits,
                pending_visits=exec_pending_visits,
                conversion_rate=round(conversion_rate, 1),
                converted=exec_conversions,
                followups=exec_followups,
                last_visit_date=last_visit_date,
                performance_score=performance_score,
                total_assigned=total_assigned,
                followup_count=followup_count,
                pending_count=pending_count,
                converted_count=converted_count,
                rejected_count=rejected_count
            ))
        
        avg_conversion_rate = sum(conversion_rates) / len(conversion_rates) if conversion_rates else 0
        
        top_performer = None
        if executives_data:
            top_exec = max(executives_data, key=lambda x: x.performance_score)
            top_performer = top_exec.name
        
        stats = ExecutiveStats(
            total_executives=total_executives,
            active_executives=active_executives,
            inactive_executives=inactive_executives,
            new_this_month=new_this_month,
            total_visits=total_visits,
            total_conversions=total_conversions,
            average_conversion_rate=round(avg_conversion_rate, 1),
            top_performer=top_performer
        )
        
        executives_data.sort(key=lambda x: x.performance_score, reverse=True)
        
        return {
            "status":200,
            "data":{
                "stats":stats,
                "executives":executives_data,
                "manager_profile":manager_profile
            }
        }
        
        
    except Exception as e:
        print(f"Error in executives dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/get-executives")
async def get_all_executives(
    user_id: int, 
    db: Session = Depends(get_db), 
    redis: Redis = Depends(get_redis)
):
    try:
        executives = db.query(Executives).filter(Executives.manager_id == user_id).all()
        executives_data = []
        
        if executives:
            executives_data = [
                {
                    "id": executive.id,
                    "manager_id": executive.manager_id,
                    "name": executive.name,
                    "email": executive.email,
                    "contact": executive.contact,
                    "profile": executive.profile,
                    "dob": executive.dob,
                    "age": executive.age,
                    "gender": executive.gender,
                    "role": executive.role,
                    "joined_date": executive.joined_date,
                    "status": executive.status,
                    "uuid": executive.uuid,
                    "employee_id": executive.emp_id,
                } for executive in executives
            ]

        return {
            "status": 200,
            "message": "Data retrieved successfully",
            "data": executives_data
        }

    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/executive-details/{executive_id}")
async def get_executive_details(
    executive_id: int,
    user_id: int,
    db: Session = Depends(get_db)
):
    try:
        executive = db.query(Executives).filter(
            and_(
                Executives.id == executive_id,
                Executives.manager_id == user_id
            )
        ).first()
        
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found")
        
        recent_visits = db.query(GymVisits).filter(
            GymVisits.user_id == executive_id
        ).order_by(desc(GymVisits.created_at)).limit(10).all()
        
        planned_visits = db.query(GymVisits).filter(
            and_(
                GymVisits.user_id == executive_id,
                GymVisits.status == 'scheduled'
            )
        ).order_by(GymVisits.assigned_date).all()
        
        visits_data = [
            {
                "id": visit.id,
                "gym_name": visit.gym_name,
                "contact_person": visit.contact_person,
                "visit_purpose": visit.visit_purpose,
                "final_status": visit.final_status,
                "completion_percentage": calculate_visit_completion(visit),
                "created_at": visit.created_at,
                "completed": visit.completed
            } for visit in recent_visits
        ]
        
        planned_data = [visit.to_dict() for visit in planned_visits]
        
        return {
            "status": 200,
            "message": "Executive details retrieved successfully",
            "data": {
                "executive": {
                    "id": executive.id,
                    "name": executive.name,
                    "email": executive.email,
                    "contact": executive.contact,
                    "profile": executive.profile,
                    "role": executive.role,
                    "status": executive.status,
                    "joined_date": executive.joined_date,
                    "employee_id":executive.employee_id
                },
                "recent_visits": visits_data,
                "planned_visits": planned_data
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting executive details: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


def calculate_visit_completion(visit):
    if visit.completed:
        return 100
    
    total_steps = 8
    current_step = visit.current_step or 0
    
    base_completion = (current_step / total_steps) * 100
    
    required_fields = [
        'gym_name', 'gym_address', 'contact_person', 'contact_phone', 'visit_purpose',
        'check_in_time', 'attendance_selfie', 'facility_photos', 'gym_size', 'total_member_count',
        'meeting_duration', 'pain_points', 'next_steps', 'visit_outcome', 'final_status'
    ]
    
    completed = 0
    for field in required_fields:
        field_value = getattr(visit, field, None)
        if field == 'facility_photos' and field_value and len(field_value) >= 3:
            completed += 1
        elif field_value:
            completed += 1
    
    field_completion = (completed / len(required_fields)) * 100
    return max(base_completion, field_completion)




@router.get("/executive-planner/{executive_id}")
async def get_executive_planner_details(
    executive_id: int,
    user_id: int,
    start_date: str = Query(...),
    end_date: str = Query(...),
    db: Session = Depends(get_db)
):
    try:
        executive = db.query(Executives).filter(
            and_(
                Executives.id == executive_id,
                Executives.manager_id == user_id
            )
        ).first()
        
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found")
        
        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        
        planned_visits = db.query(GymVisits).filter(
            and_(
                GymVisits.user_id == executive_id,
                GymVisits.assigned_date >= start_dt,
                GymVisits.assigned_date <= end_dt
            )
        ).order_by(GymVisits.assigned_date).all()
        
        visits_by_date = {}
        for visit in planned_visits:
            visit_date = visit.assigned_date.strftime('%Y-%m-%d')
            if visit_date not in visits_by_date:
                visits_by_date[visit_date] = []

            assignment = None
            if visit.gym_id:
                assignment = db.query(GymAssignments).filter(
                        GymAssignments.gym_id == visit.gym_id).first()
                    
                

            visits_by_date[visit_date].append({
                "id": visit.id,
                "gym_id": visit.gym_id,
                "assignment_id": assignment.id if assignment else None,
                "gymName": visit.gym_name,
                "location": visit.gym_address,
                "referal_id": visit.referal_id,
                "contactPerson": visit.contact_person,
                "contactNumber": visit.contact_phone,
                "visitPurpose": visit.visit_purpose,
                "visitType": visit.visit_type,
                "final_status": visit.final_status,
                "status": visit.status,
                "notes": visit.notes,
                "created_at": visit.created_at,
                "updated_at": visit.updated_at
            })

        print("dataaa",visits_by_date)
        
        return {
            "status": 200,
            "data": visits_by_date
        }
        
    except Exception as e:
        print(f"Error in executive planner details: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.post("/assign-gym-visit/{executive_id}")
async def assign_gym_visit_to_executive(
    executive_id: int,
    user_id: int,
    visit_data: dict,
    db: Session = Depends(get_db)
):
    try:
        executive = db.query(Executives).filter(
            and_(
                Executives.id == executive_id,
                Executives.manager_id == user_id
            )
        ).first()
        
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found")
        
        new_visit = GymVisits(
            user_id=executive_id,
            gym_name=visit_data.get('gym_name'),
            referal_id=visit_data.get('referal_id'),
            gym_address=visit_data.get('location'),
            contact_person=visit_data.get('contact_person'),
            contact_phone=visit_data.get('contact_number'),
            visit_purpose=visit_data.get('visit_purpose'),
            visit_type=visit_data.get('visit_type', 'sales_call'),
            assigned_date=datetime.fromisoformat(visit_data.get('visit_date')),
            assigned_on=datetime.now(),
            notes=visit_data.get('notes'),
            status='assigned',
            final_status='scheduled'
        )
        
        db.add(new_visit)
        db.commit()
        db.refresh(new_visit)
        
        return {
            "status": 200,
            "message": "Gym visit assigned successfully",
            "data": new_visit.to_dict()
        }
        
    except Exception as e:
        print(f"Error assigning gym visit: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/executive-visits/{executive_id}")
async def get_executive_gym_visits(
    executive_id: int,
    user_id: int,
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        executive = db.query(Executives).filter(
            and_(
                Executives.id == executive_id,
                Executives.manager_id == user_id
            )
        ).first()
        
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found")
        
        query = db.query(GymVisits).filter(GymVisits.user_id == executive_id)
        
        if status and status != 'all':
            query = query.filter(GymVisits.final_status == status)
        
        visits = query.order_by(desc(GymVisits.created_at)).all()
        
        visits_data = []
        for visit in visits:
            feedback = db.query(Feedback).filter(Feedback.visit_id == visit.id).first()
            
            visits_data.append({
                "id": visit.id,
                "gym_name": visit.gym_name,
                "gym_address": visit.gym_address,
                "referal_id":visit.referal_id,
                "contact_person": visit.contact_person,
                "contact_phone": visit.contact_phone,
                "visit_purpose": visit.visit_purpose,
                "final_status": visit.final_status,
                "interest_level": visit.interest_level,
                "overall_rating": visit.overall_rating,
                "meeting_duration": visit.meeting_duration,
                "people_met": visit.people_met,
                "pain_points": visit.pain_points,
                "competitors": visit.competitors,
                "current_tech": visit.current_tech,
                "key_benefits": visit.key_benefits,
                "visit_summary": visit.visit_summary,
                "conversion_notes": visit.conversion_notes,
                "rejection_reason": visit.rejection_reason,
                "next_follow_up_date": visit.next_follow_up_date,
                "decision_timeline": visit.decision_timeline,
                "exterior_photo": visit.exterior_photo,
                "gym_size": visit.gym_size,
                "total_member_count": visit.total_member_count,
                "check_in_time": visit.check_in_time,
                "completed": visit.completed,
                "current_step": visit.current_step,
                "feedback_submitted": feedback is not None,  
                "feedback": {
                    "id": feedback.id if feedback else None,
                    "category": feedback.category if feedback else None,
                    "rating": feedback.rating if feedback else None,
                    "created_at": feedback.created_at if feedback else None
                } if feedback else None,
                "created_at": visit.created_at,
                "updated_at": visit.updated_at
            })
        
        status_counts = {
            'all': len(visits_data),
            'pending': db.query(GymVisits).filter(
                and_(GymVisits.user_id == executive_id, GymVisits.final_status == 'pending')
            ).count(),
            'followup': db.query(GymVisits).filter(
                and_(GymVisits.user_id == executive_id, GymVisits.final_status == 'followup')
            ).count(),
            'converted': db.query(GymVisits).filter(
                and_(GymVisits.user_id == executive_id, GymVisits.final_status == 'converted')
            ).count(),
            'rejected': db.query(GymVisits).filter(
                and_(GymVisits.user_id == executive_id, GymVisits.final_status == 'rejected')
            ).count()
        }
        
        return {
            "status": 200,
            "data": {
                "visits": visits_data,
                "status_counts": status_counts
            }
        }
        
    except Exception as e:
        print(f"Error getting executive visits: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    


@router.get("/executive-stats/{executive_id}")
async def get_executive_detailed_stats(
    executive_id: int,
    user_id: int,
    db: Session = Depends(get_db)
):
    try:
        executive = db.query(Executives).filter(
            and_(
                Executives.id == executive_id,
                Executives.manager_id == user_id
            )
        ).first()
        
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found")
        
        total_visits = db.query(GymVisits).filter(GymVisits.user_id == executive_id).count()
        
        completed_visits = db.query(GymVisits).filter(
            and_(GymVisits.user_id == executive_id, GymVisits.completed == True)
        ).count()
        
        conversions = db.query(GymVisits).filter(
            and_(GymVisits.user_id == executive_id, GymVisits.final_status == 'converted')
        ).count()
        
        followups = db.query(GymVisits).filter(
            and_(GymVisits.user_id == executive_id, GymVisits.final_status == 'followup')
        ).count()
        
        rejections = db.query(GymVisits).filter(
            and_(GymVisits.user_id == executive_id, GymVisits.final_status == 'rejected')
        ).count()
        
        avg_interest = db.query(func.avg(GymVisits.interest_level)).filter(
            and_(GymVisits.user_id == executive_id, GymVisits.interest_level > 0)
        ).scalar() or 0
        
        avg_rating = db.query(func.avg(GymVisits.overall_rating)).filter(
            and_(GymVisits.user_id == executive_id, GymVisits.overall_rating > 0)
        ).scalar() or 0
        
        recent_visits = db.query(GymVisits).filter(
            GymVisits.user_id == executive_id
        ).order_by(desc(GymVisits.created_at)).limit(5).all()
        
        conversion_rate = (conversions / total_visits * 100) if total_visits > 0 else 0
        
        return {
            "status": 200,
            "data": {
                "total_visits": total_visits,
                "completed_visits": completed_visits,
                "conversions": conversions,
                "followups": followups,
                "rejections": rejections,
                "conversion_rate": round(conversion_rate, 1),
                "avg_interest_level": round(avg_interest, 1),
                "avg_overall_rating": round(avg_rating, 1),
                "recent_visits": [
                    {
                        "id": visit.id,
                        "gym_name": visit.gym_name,
                        "final_status": visit.final_status,
                        "created_at": visit.created_at
                    } for visit in recent_visits
                ]
            }
        }
        
    except Exception as e:
        print(f"Error getting executive stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

@router.get("/feedback-analytics")
async def get_manager_feedback_analytics(
    user_id: int,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        query = db.query(Feedback).join(Executives).filter(
            Executives.manager_id == user_id
        )
        
        if start_date:
            start_dt = datetime.fromisoformat(start_date)
            query = query.filter(Feedback.created_at >= start_dt)
        
        if end_date:
            end_dt = datetime.fromisoformat(end_date)
            query = query.filter(Feedback.created_at <= end_dt)
        
        feedback_list = query.all()
        
        total_feedback = len(feedback_list)
        
        if total_feedback == 0:
            return {
                "status": 200,
                "data": {
                    "total_feedback": 0,
                    "average_rating": 0,
                    "category_breakdown": {},
                    "rating_distribution": {},
                    "executive_performance": {},
                    "trends": {}
                }
            }
        
        average_rating = sum(f.rating for f in feedback_list) / total_feedback
        
        category_breakdown = {}
        for feedback in feedback_list:
            category_breakdown[feedback.category] = category_breakdown.get(feedback.category, 0) + 1
        
        rating_distribution = {}
        for i in range(1, 6):
            rating_distribution[str(i)] = len([f for f in feedback_list if f.rating == i])

        # === BATCH LOAD: Get all executives at once (N+1 FIX) ===
        exec_ids = list({f.executive_id for f in feedback_list if f.executive_id})
        exec_map = {}
        if exec_ids:
            executives = db.query(Executives).filter(Executives.id.in_(exec_ids)).all()
            exec_map = {e.id: e for e in executives}

        executive_performance = {}
        for feedback in feedback_list:
            exec_id = feedback.executive_id
            if exec_id not in executive_performance:
                # Get executive from pre-loaded map (was N+1 query)
                executive = exec_map.get(exec_id)
                executive_performance[exec_id] = {
                    "name": executive.name if executive else "Unknown",
                    "total_feedback": 0,
                    "average_rating": 0,
                    "ratings": []
                }

            executive_performance[exec_id]["total_feedback"] += 1
            executive_performance[exec_id]["ratings"].append(feedback.rating)
        
        for exec_id in executive_performance:
            ratings = executive_performance[exec_id]["ratings"]
            executive_performance[exec_id]["average_rating"] = sum(ratings) / len(ratings)
            del executive_performance[exec_id]["ratings"]  
        
        
        
        trends = {}
        for i in range(6):
            month_start = datetime.now().replace(day=1) - relativedelta(months=i)
            month_end = month_start + relativedelta(months=1) - timedelta(days=1)
            
            month_feedback = [f for f in feedback_list 
                            if month_start <= f.created_at <= month_end]
            
            month_key = month_start.strftime("%Y-%m")
            trends[month_key] = {
                "count": len(month_feedback),
                "average_rating": sum(f.rating for f in month_feedback) / len(month_feedback) if month_feedback else 0
            }
        
        return {
            "status": 200,
            "data": {
                "total_feedback": total_feedback,
                "average_rating": round(average_rating, 2),
                "category_breakdown": category_breakdown,
                "rating_distribution": rating_distribution,
                "executive_performance": executive_performance,
                "trends": trends
            }
        }
        
    except Exception as e:
        print(f"Error getting feedback analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")




@router.get("/executive-feedback-summary/{executive_id}")
async def get_executive_feedback_summary(
    executive_id: int,
    user_id: int,
    db: Session = Depends(get_db)
):
    try:
        executive = db.query(Executives).filter(
            and_(
                Executives.id == executive_id,
                Executives.manager_id == user_id
            )
        ).first()
        
        if not executive:
            raise HTTPException(status_code=404, detail="Executive not found")
        
        feedback_list = db.query(Feedback).filter(
            Feedback.executive_id == executive_id
        ).all()
        
        if not feedback_list:
            return {
                "status": 200,
                "data": {
                    "total_feedback": 0,
                    "average_rating": 0,
                    "latest_feedback": None,
                    "improvement_suggestions": []
                }
            }
        
        total_feedback = len(feedback_list)
        average_rating = sum(f.rating for f in feedback_list) / total_feedback
        
        latest_feedback = max(feedback_list, key=lambda x: x.created_at)
        
        improvement_suggestions = [
            f.improvement_areas for f in feedback_list 
            if f.improvement_areas and f.improvement_areas.strip()
        ][-3:] 

        return {
            "status": 200,
            "data": {
                "total_feedback": total_feedback,
                "average_rating": round(average_rating, 2),
                "latest_feedback": {
                    "rating": latest_feedback.rating,
                    "category": latest_feedback.category,
                    "comments": latest_feedback.comments,
                    "created_at": latest_feedback.created_at
                },
                "improvement_suggestions": improvement_suggestions
            }
        }
        
    except Exception as e:
        print(f"Error getting executive feedback summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

@router.get("/team-members/{manager_id}")
def get_manager_team_members(manager_id: int, db: Session = Depends(get_db)):
    try:
        manager = db.query(Managers).filter(Managers.id == manager_id).first()
        if not manager:
            raise HTTPException(status_code=404, detail="Manager not found")

        executives = db.query(Executives).filter(
            Executives.manager_id == manager_id,
            Executives.status == 'active'
        ).all()

        team_data = []
        for exec in executives:
            exec_info = {
                "id": exec.id,
                "name": exec.name,
                "email": exec.email,
                "contact": exec.contact,
                "employee_id": exec.emp_id,
                "joined_date": exec.joined_date.isoformat() if exec.joined_date else None,
                "status": exec.status
            }
            team_data.append(exec_info)

        return {
            "status": 200,
            "message": "Team members retrieved successfully",
            "data": team_data
        }

    except Exception as e:
        print(f"Error getting team members: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")