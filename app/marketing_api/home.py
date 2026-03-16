from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.models.marketingmodels import Executives, GymVisits, Managers, GymAssignments, GymDatabase, FollowupAttempts
from app.models.database import get_db
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from app.utils.redis_config import get_redis
from redis.asyncio import Redis
from sqlalchemy import func, desc, and_, or_


router = APIRouter(prefix="/marketing/home", tags=["Home"])

@router.get("/all")
async def get_home(user_id:int, role:str,db:Session= Depends(get_db), redis:Redis = Depends(get_redis)):
    try:
        if role=="BDE":
            user = db.query(Executives).filter(Executives.id == user_id).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            today = datetime.today().date()

            visits = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.check_in_time.isnot(None),
                func.date(GymVisits.check_in_time) == today
            ).all()

            todays_assignments = db.query(
                GymAssignments,
                GymDatabase,
                GymVisits
            ).join(
                GymDatabase, GymAssignments.gym_id == GymDatabase.id
            ).outerjoin(
                GymVisits, and_(
                    GymVisits.gym_id == GymAssignments.gym_id,
                    GymVisits.user_id == GymAssignments.executive_id,
                    func.date(GymVisits.assigned_date) == today
                )
            ).filter(
                GymAssignments.executive_id == user_id,
                func.date(GymAssignments.assigned_date) == today
            ).all()

            # Get visit_ids for this user
            user_visit_ids = [visit.id for visit in db.query(GymVisits.id).filter(
                GymVisits.user_id == user_id
            ).all()]

            # Get today's follow-ups from followup_attempts table
            todays_followups = db.query(
                FollowupAttempts,
                GymVisits
            ).join(
                GymVisits, FollowupAttempts.visit_id == GymVisits.id
            ).filter(
                FollowupAttempts.visit_id.in_(user_visit_ids),
                func.date(FollowupAttempts.next_followup_date) == today
            ).all()

            plans_data = []

            for assignment, gym, visit in todays_assignments:
                plans_data.append({
                    'assignment_id': assignment.id,
                    'visit_id': visit.id if visit else None,
                    'gym_id': gym.id,
                    'gym_name': gym.gym_name,
                    'location': gym.address,
                    'area': gym.area,
                    'city': gym.city,
                    'state': gym.state,
                    'pincode': gym.pincode,
                    'referal_id': gym.referal_id,
                    'contact_person': gym.contact_person,
                    'contact_number': gym.contact_phone,
                    'visit_purpose': visit.visit_purpose if visit else 'Initial gym visit and assessment',
                    'visit_type': visit.visit_type if visit else 'sales_call',
                    'assigned_date': assignment.assigned_date.isoformat() if assignment.assigned_date else None,
                    'status': visit.status if visit else 'assigned',
                    'final_status': visit.final_status if visit else 'pending',
                    'conversion_status': assignment.conversion_status,
                    'notes': visit.notes if visit else None,
                    'check_in_time': visit.check_in_time.isoformat() if visit and visit.check_in_time else None,
                    'completed': visit.completed if visit else False,
                    'created_at': assignment.created_at.isoformat() if assignment.created_at else None,
                    'updated_at': assignment.updated_at.isoformat() if assignment.updated_at else None
                })

         
            for followup_attempt, visit in todays_followups:
                plans_data.append({
                    'assignment_id': None,
                    'visit_id': visit.id,
                    'gym_id': visit.gym_id,
                    'gym_name': visit.gym_name,
                    'location': visit.gym_address,
                    'area': None,
                    'city': None,
                    'state': None,
                    'pincode': None,
                    'referal_id': visit.referal_id,
                    'contact_person': visit.contact_person,
                    'contact_number': visit.contact_phone,
                    'visit_purpose': visit.visit_purpose or 'Follow-up visit',
                    'visit_type': 'follow_up',
                    'assigned_date': followup_attempt.next_followup_date.isoformat() if followup_attempt.next_followup_date else None,
                    'status': visit.status,
                    'final_status': visit.final_status,
                    'conversion_status': 'followup',
                    'notes': followup_attempt.notes or visit.notes,
                    'check_in_time': visit.check_in_time.isoformat() if visit.check_in_time else None,
                    'completed': visit.completed,
                    'created_at': visit.created_at.isoformat() if visit.created_at else None,
                    'updated_at': visit.updated_at.isoformat() if visit.updated_at else None
                })

            target_for_today = db.query(GymAssignments).filter(
                GymAssignments.executive_id == user_id,
                func.date(GymAssignments.assigned_date) == today
            ).count()

            # Add today's follow-up count to target from followup_attempts
            followup_target_count = db.query(FollowupAttempts).filter(
                FollowupAttempts.visit_id.in_(user_visit_ids),
                func.date(FollowupAttempts.next_followup_date) == today
            ).count()

            target_for_today += followup_target_count

            # Add mapping_id to each item in plans_data
            for idx, item in enumerate(plans_data):
                item['mapping_id'] = idx

            total_assigned = db.query(GymVisits).filter(
                GymVisits.user_id == user_id
            ).count()

            pending_count = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.final_status == "pending"
            ).count()

            converted_count = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.final_status == "converted"
            ).count()

            followup_count = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.final_status == "followup"
            ).count()

            rejected_count = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.final_status == "rejected"
            ).count()

            converted_gyms = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.final_status == "converted"
            ).count()

            visited_gyms = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.check_in_time.isnot(None)
            ).count()

            follow_ups = db.query(GymVisits).filter(
                GymVisits.user_id == user_id,
                GymVisits.final_status == "followup"
            ).count()

            data = {
                "visited_gyms": len(visits),
                "target_gyms": target_for_today,
                "plans_data": plans_data,
                "total_assigned": total_assigned,
                "pending_count": pending_count,
                "converted_count": converted_count,
                "followup_count": followup_count,
                "rejected_count": rejected_count,
                "converted_gyms": converted_gyms,
                "total_visited_gyms": visited_gyms,
                "total_followups": follow_ups,
                "profile_data": {
                    "user_id": user.id,
                    "manager_id": user.manager_id,
                    "name": user.name,
                    "email": user.email,
                    "contact": user.contact,
                    "profile": user.profile,
                    "role": user.role
                }
            }

            return {
                'status': 200,
                "message": "Data retrieved successfully",
                "data": data
            }

        else:
            user = db.query(Managers).filter(Managers.id == user_id).first()
            if not user:
                raise HTTPException(status_code=404, detail="Manager not found")

            executives = db.query(Executives).filter(Executives.manager_id == user.id).all()
            executive_ids = [exec.id for exec in executives]

            today = datetime.today().date()

            # Include both team executive visits AND manager's self-assigned visits
            visits = db.query(GymVisits).filter(
                or_(
                    GymVisits.user_id.in_(executive_ids),  # Team visits
                    and_(
                        GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                        GymVisits.manager_id == user_id  # Manager's self-assigned visits
                    )
                ),
                GymVisits.check_in_time.isnot(None),
                func.date(GymVisits.check_in_time) == today
            ).all()

            # Get today's assignments including self-assigned gyms
            todays_assignments = db.query(
                GymAssignments,
                GymDatabase,
                Executives,
                GymVisits
            ).join(
                GymDatabase, GymAssignments.gym_id == GymDatabase.id
            ).outerjoin(  # Changed to OUTERJOIN to include self-assigned gyms (where executive_id is NULL)
                Executives, GymAssignments.executive_id == Executives.id
            ).outerjoin(
                GymVisits, and_(
                    GymVisits.gym_id == GymAssignments.gym_id,
                    or_(
                        GymVisits.user_id == GymAssignments.executive_id,  # Executive's visit
                        and_(
                            GymAssignments.executive_id.is_(None),  # Self-assigned gym
                            GymVisits.user_id.is_(None),  # Manager's visit (user_id is NULL)
                            GymVisits.manager_id == user_id  # Manager's ID
                        )
                    ),
                    func.date(GymVisits.assigned_date) == today
                )
            ).filter(
                GymAssignments.manager_id == user_id,
                func.date(GymAssignments.assigned_date) == today
            ).all()


            # Include both team visits AND manager's self-assigned visits
            manager_visit_ids = [visit.id for visit in db.query(GymVisits.id).filter(
                or_(
                    GymVisits.user_id.in_(executive_ids),  # Team visits
                    and_(
                        GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                        GymVisits.manager_id == user_id  # Manager's self-assigned visits
                    )
                )
            ).all()]

            # Get today's follow-ups from followup_attempts table (includes self-assigned)
            todays_followups = db.query(
                FollowupAttempts,
                GymVisits,
                Executives
            ).join(
                GymVisits, FollowupAttempts.visit_id == GymVisits.id
            ).outerjoin(  # Changed to OUTERJOIN to include manager's self-assigned follow-ups
                Executives, GymVisits.user_id == Executives.id
            ).filter(
                FollowupAttempts.visit_id.in_(manager_visit_ids),
                func.date(FollowupAttempts.next_followup_date) == today
            ).all()

            plans_data = []

            for assignment, gym, executive, visit in todays_assignments:
                # Handle both regular assignments and self-assigned gyms
                is_self_assigned = executive is None
                plans_data.append({
                    'assignment_id': assignment.id,
                    'visit_id': visit.id if visit else None,
                    'gym_id': gym.id,
                    'executive_id': executive.id if executive else None,
                    'executive_name': executive.name if executive else user.name,  # Use manager's name for self-assigned
                    'is_self_assigned': is_self_assigned,
                    'gym_name': gym.gym_name,
                    'location': gym.address,
                    'area': gym.area,
                    'city': gym.city,
                    'state': gym.state,
                    'pincode': gym.pincode,
                    'referal_id': gym.referal_id,
                    'contact_person': gym.contact_person,
                    'contact_number': gym.contact_phone,
                    'visit_purpose': visit.visit_purpose if visit else 'Initial gym visit and assessment',
                    'visit_type': visit.visit_type if visit else 'sales_call',
                    'assigned_date': assignment.assigned_date.isoformat() if assignment.assigned_date else None,
                    'status': visit.status if visit else 'assigned',
                    'final_status': visit.final_status if visit else 'pending',
                    'conversion_status': assignment.conversion_status,
                    'notes': visit.notes if visit else None,
                    'check_in_time': visit.check_in_time.isoformat() if visit and visit.check_in_time else None,
                    'completed': visit.completed if visit else False,
                    'created_at': assignment.created_at.isoformat() if assignment.created_at else None,
                    'updated_at': assignment.updated_at.isoformat() if assignment.updated_at else None
                })

            # Add today's follow-ups to plans_data
            for followup_attempt, visit, executive in todays_followups:
                # Handle both regular followups and self-assigned followups
                is_self_assigned = executive is None
                plans_data.append({
                    'assignment_id': None,
                    'visit_id': visit.id,
                    'gym_id': visit.gym_id,
                    'executive_id': executive.id if executive else None,
                    'executive_name': executive.name if executive else user.name,  # Use manager's name for self-assigned
                    'is_self_assigned': is_self_assigned,
                    'gym_name': visit.gym_name,
                    'location': visit.gym_address,
                    'area': None,
                    'city': None,
                    'state': None,
                    'pincode': None,
                    'referal_id': visit.referal_id,
                    'contact_person': visit.contact_person,
                    'contact_number': visit.contact_phone,
                    'visit_purpose': visit.visit_purpose or 'Follow-up visit',
                    'visit_type': 'follow_up',
                    'assigned_date': followup_attempt.next_followup_date.isoformat() if followup_attempt.next_followup_date else None,
                    'status': visit.status,
                    'final_status': visit.final_status,
                    'conversion_status': 'followup',
                    'notes': followup_attempt.notes or visit.notes,
                    'check_in_time': visit.check_in_time.isoformat() if visit.check_in_time else None,
                    'completed': visit.completed,
                    'created_at': visit.created_at.isoformat() if visit.created_at else None,
                    'updated_at': visit.updated_at.isoformat() if visit.updated_at else None
                })

            # Add mapping_id to each item in plans_data
            for idx, item in enumerate(plans_data):
                item['mapping_id'] = idx

            # Include manager's self-assigned visits in statistics
            converted_gyms = db.query(GymVisits).filter(
                or_(
                    GymVisits.user_id.in_(executive_ids),
                    and_(
                        GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                        GymVisits.manager_id == user_id  # Manager's self-assigned
                    )
                ),
                GymVisits.final_status == "converted"
            ).count()

            visited_gyms = db.query(GymVisits).filter(
                or_(
                    GymVisits.user_id.in_(executive_ids),
                    and_(
                        GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                        GymVisits.manager_id == user_id  # Manager's self-assigned
                    )
                ),
                GymVisits.check_in_time.isnot(None)
            ).count()

            follow_ups = db.query(GymVisits).filter(
                or_(
                    GymVisits.user_id.in_(executive_ids),
                    and_(
                        GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                        GymVisits.manager_id == user_id  # Manager's self-assigned
                    )
                ),
                GymVisits.final_status == "followup"
            ).count()

            target_for_today = db.query(GymAssignments).filter(
                GymAssignments.manager_id == user_id,
                func.date(GymAssignments.assigned_date) == today
            ).count()

            # Add today's follow-up count to target from followup_attempts
            followup_target_count = db.query(FollowupAttempts).filter(
                FollowupAttempts.visit_id.in_(manager_visit_ids),
                func.date(FollowupAttempts.next_followup_date) == today
            ).count()

            target_for_today += followup_target_count

            # Include manager's self-assigned visits in all counts
            total_assigned = db.query(GymVisits).filter(
                or_(
                    GymVisits.user_id.in_(executive_ids),
                    and_(
                        GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                        GymVisits.manager_id == user_id  # Manager's self-assigned
                    )
                )
            ).count()

            pending_count = db.query(GymVisits).filter(
                or_(
                    GymVisits.user_id.in_(executive_ids),
                    and_(
                        GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                        GymVisits.manager_id == user_id  # Manager's self-assigned
                    )
                ),
                GymVisits.final_status == "pending"
            ).count()

            converted_count = db.query(GymVisits).filter(
                or_(
                    GymVisits.user_id.in_(executive_ids),
                    and_(
                        GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                        GymVisits.manager_id == user_id  # Manager's self-assigned
                    )
                ),
                GymVisits.final_status == "converted"
            ).count()

            followup_count = db.query(GymVisits).filter(
                or_(
                    GymVisits.user_id.in_(executive_ids),
                    and_(
                        GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                        GymVisits.manager_id == user_id  # Manager's self-assigned
                    )
                ),
                GymVisits.final_status == "followup"
            ).count()

            rejected_count = db.query(GymVisits).filter(
                or_(
                    GymVisits.user_id.in_(executive_ids),
                    and_(
                        GymVisits.user_id.is_(None),  # Self-assigned (user_id is NULL)
                        GymVisits.manager_id == user_id  # Manager's self-assigned
                    )
                ),
                GymVisits.final_status == "rejected"
            ).count()

            total_bdes = db.query(Executives).filter(
                Executives.manager_id == user_id,
                Executives.status == "active"
            ).count()

            leaderboard = (
                db.query(
                    GymVisits.user_id,
                    func.count(GymVisits.id).label("converted_count")
                )
                .filter(
                    GymVisits.user_id.in_(executive_ids),
                    GymVisits.final_status == "converted"
                )
                .group_by(GymVisits.user_id)
                .order_by(desc("converted_count"))
                .limit(3)
                .all()
            )

            leaderboard_data = []
            for entry in leaderboard:
                executive = next((exec for exec in executives if exec.id == entry.user_id), None)
                if executive:
                    leaderboard_data.append({
                        "executive_id": executive.id,
                        "executive_name": executive.name,
                        "converted_gyms": entry.converted_count
                    })

            data = {
                "visited_gyms": len(visits),
                "target_gyms": target_for_today,
                "plans_data": plans_data,
                "total_assigned": total_assigned,
                "pending_count": pending_count,
                "converted_count": converted_count,
                "followup_count": followup_count,
                "rejected_count": rejected_count,
                "total_bde": total_bdes,
                "converted_gyms": converted_gyms,
                "total_visited_gyms": visited_gyms,
                "total_followups": follow_ups,
                "leaderboard_data": leaderboard_data,
                "profile_data": {
                    "user_id": user.id,
                    "manager_id": None,
                    "name": user.name,
                    "email": user.email,
                    "contact": user.contact,
                    "profile": user.profile,
                    "role": user.role
                }
            }

            return {
                'status': 200,
                "message": "Data retrieved successfully",
                "data": data
            }
        
    except Exception as e:
        print(str(e))
        raise HTTPException(status_code=500, detail=f'An error occured, {str(e)}')

@router.get("/followups")
async def get_followups(
    user_id: int, 
    role: str,
    status: Optional[str] = Query(None, description="Filter by status: pending, overdue, scheduled"),
    priority: Optional[str] = Query(None, description="Filter by priority: urgent, high, medium, low"),
    db: Session = Depends(get_db)
):
    try:
        if role == "BDE":
            user = db.query(Executives).filter(Executives.id == user_id).first()
            if not user:
                raise HTTPException(status_code=404, detail="Executive not found")
            
            followup_visits = db.query(GymVisits).filter(
                and_(
                    GymVisits.user_id == user_id,
                    GymVisits.final_status == 'followup',
                    GymVisits.next_follow_up_date.isnot(None)
                )
            ).order_by(GymVisits.next_follow_up_date.asc()).all()
            
        else:
            user = db.query(Managers).filter(Managers.id == user_id).first()
            if not user:
                raise HTTPException(status_code=404, detail="Manager not found")
            
            executives = db.query(Executives).filter(Executives.manager_id == user.id).all()
            executive_ids = [exec.id for exec in executives]
            
            followup_visits = db.query(GymVisits).filter(
                and_(
                    GymVisits.user_id.in_(executive_ids),
                    GymVisits.final_status == 'followup',
                    GymVisits.next_follow_up_date.isnot(None)
                )
            ).order_by(GymVisits.next_follow_up_date.asc()).all()
        
        followups_data = []
        today = datetime.now().date()
        
        for visit in followup_visits:
            visit_status = 'scheduled'
            priority = 'medium'
            
            if visit.next_follow_up_date:
                follow_up_date = visit.next_follow_up_date.date() if hasattr(visit.next_follow_up_date, 'date') else visit.next_follow_up_date
                days_diff = (follow_up_date - today).days
                
                if days_diff < 0:
                    visit_status = 'overdue'
                    priority = 'urgent'
                elif days_diff == 0:
                    visit_status = 'pending'
                    priority = 'high'
                elif days_diff <= 3:
                    visit_status = 'pending'
                    priority = 'high'
                elif days_diff <= 7:
                    visit_status = 'scheduled'
                    priority = 'medium'
                else:
                    visit_status = 'scheduled'
                    priority = 'low'
            
            executive_info = {}
            if role == "BDM":
                executive = next((exec for exec in executives if exec.id == visit.user_id), None)
                if executive:
                    executive_info = {
                        'executive_id': executive.id,
                        'executive_name': executive.name,
                        'executive_email': executive.email
                    }
            
            followup_data = {
                'id': visit.id,
                'gym_name': visit.gym_name,
                'gym_address': visit.gym_address,
                'referal_id':visit.referal_id,
                'contact_person': visit.contact_person,
                'contact_phone': visit.contact_phone,
                'visit_purpose': visit.visit_purpose,
                'visit_type': visit.visit_type,
                'original_visit_date': visit.assigned_date.isoformat() if visit.assigned_date else visit.start_date.isoformat() if visit.start_date else None,
                'last_contact_date': visit.updated_at.isoformat() if visit.updated_at else visit.created_at.isoformat() if visit.created_at else None,
                'next_follow_up_date': visit.next_follow_up_date.isoformat() if visit.next_follow_up_date else None,
                'status': visit_status,
                'priority': priority,
                'notes': visit.follow_up_notes or visit.notes or visit.visit_summary,
                'action_items': visit.action_items,
                'final_status': visit.final_status,
                'overall_rating': visit.overall_rating,
                'created_at': visit.created_at.isoformat() if visit.created_at else None,
                'updated_at': visit.updated_at.isoformat() if visit.updated_at else None,
                **executive_info
            }
            
            followups_data.append(followup_data)
        
        if status:
            followups_data = [f for f in followups_data if f['status'] == status]
        
        if priority:
            followups_data = [f for f in followups_data if f['priority'] == priority]
        
        total_followups = len(followups_data)
        pending_count = len([f for f in followups_data if f['status'] == 'pending'])
        overdue_count = len([f for f in followups_data if f['status'] == 'overdue'])
        scheduled_count = len([f for f in followups_data if f['status'] == 'scheduled'])
        urgent_count = len([f for f in followups_data if f['priority'] == 'urgent'])
        
        return {
            'status': 200,
            'message': 'Followups retrieved successfully',
            'data': {
                'followups': followups_data,
                'stats': {
                    'total': total_followups,
                    'pending': pending_count,
                    'overdue': overdue_count,
                    'scheduled': scheduled_count,
                    'urgent': urgent_count
                },
                'user_profile': {
                    'user_id': user.id,
                    'name': user.name,
                    'email': user.email,
                    'role': role
                }
            }
        }
        
    except Exception as e:
        print(f"Error getting followups: {str(e)}")
        raise HTTPException(status_code=500, detail=f'An error occurred: {str(e)}')