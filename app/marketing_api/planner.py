from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.models.marketingmodels import Executives, GymVisits, GymAssignments
from app.models.database import get_db
from typing import Optional, Dict
from datetime import datetime, date
from sqlalchemy import and_, extract, func, or_

router = APIRouter(prefix="/marketing/planner", tags=["Planner"])

def get_executive_profile(user_id: int, db: Session):
    user = db.query(Executives).filter(Executives.id == user_id).first()
    if user:
        return {
            'id': user.id,
            'name': user.name,
            'email': user.email,
            'contact': user.contact,
            'profile': user.profile,
            'role': 'BDE',  
            'user_type': 'executive',
            'joined_date': user.joined_date.isoformat() if user.joined_date else None,
            'status': user.status,
            'employee_id': user.emp_id
        }
    return None

def parse_date_safely(date_string: str) -> datetime:
    try:
        dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        return datetime.combine(dt.date(), datetime.min.time())
    except ValueError:
        try:
            dt = datetime.strptime(date_string, '%Y-%m-%d')
            return dt
        except ValueError:
            raise ValueError("Invalid date format. Use ISO format or YYYY-MM-DD.")

@router.get("/visits/{executive_id}")
def get_assigned_visits(
    executive_id: int,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        executive_profile = get_executive_profile(executive_id, db)
        if not executive_profile:
            raise HTTPException(status_code=404, detail="Executive not found")

        if not start_date or not end_date:
            raise HTTPException(status_code=400, detail="Start date and end date are required")

        start_dt = parse_date_safely(start_date)
        end_dt = parse_date_safely(end_date)
        end_dt = end_dt.replace(hour=23, minute=59, second=59)

        assigned_visits_query = db.query(GymVisits).filter(
            and_(
                GymVisits.user_id == executive_id,
                GymVisits.assigned_date >= start_dt,
                GymVisits.assigned_date <= end_dt
            )
        )

        followup_visits_query = db.query(GymVisits).filter(
            and_(
                GymVisits.user_id == executive_id,
                GymVisits.final_status == 'followup',
                GymVisits.next_follow_up_date.isnot(None),
                func.date(GymVisits.next_follow_up_date) >= start_dt.date(),
                func.date(GymVisits.next_follow_up_date) <= end_dt.date()
            )
        )

        assigned_visits = assigned_visits_query.all()
        followup_visits = followup_visits_query.all()

        visits_by_date = {}
        
        for visit in assigned_visits:
            visit_date_str = visit.assigned_date.strftime('%Y-%m-%d') if visit.assigned_date else visit.start_date.strftime('%Y-%m-%d')
            if visit_date_str not in visits_by_date:
                visits_by_date[visit_date_str] = []
            
            visits_by_date[visit_date_str].append({
                'id': visit.id,
                'gymName': visit.gym_name,
                'location': visit.gym_address,                
                'referal_id':visit.referal_id,
                'contactPerson': visit.contact_person,
                'contactNumber': visit.contact_phone,
                'visitPurpose': visit.visit_purpose,
                'visitType': visit.visit_type,
                'status': visit.status,
                'final_status': visit.final_status,
                'notes': visit.notes,
                'date': visit_date_str,
                'start_time': visit.check_in_time.strftime('%I:%M %p') if visit.check_in_time else None,
                'end_time': visit.check_out_time.strftime('%I:%M %p') if visit.check_out_time else None,
                'assigned_date': visit.assigned_date.strftime('%Y-%m-%d') if visit.assigned_date else None,
                'next_follow_up_date': visit.next_follow_up_date.strftime('%Y-%m-%d') if visit.next_follow_up_date else None,
                'is_followup': False,
                'original_visit_id': visit.id,
                'owner_profile': executive_profile
            })

        for visit in followup_visits:
            followup_date_str = visit.next_follow_up_date.strftime('%Y-%m-%d')
            if followup_date_str not in visits_by_date:
                visits_by_date[followup_date_str] = []
            
            existing_visit = next((v for v in visits_by_date[followup_date_str] 
                                 if v['original_visit_id'] == visit.id), None)
            
            if not existing_visit:
                visits_by_date[followup_date_str].append({
                    'id': f"followup_{visit.id}_{followup_date_str}",  
                    'gymName': visit.gym_name,
                    'location': visit.gym_address,
                    'referal_id':visit.referal_id,
                    'contactPerson': visit.contact_person,
                    'contactNumber': visit.contact_phone,
                    'visitPurpose': 'Follow-up Visit',
                    'visitType': 'follow_up',
                    'status': 'scheduled',
                    'final_status': 'followup',
                    'notes': visit.follow_up_notes or visit.notes,
                    'date': followup_date_str,
                    'start_time': None,
                    'end_time': None,
                    'assigned_date': visit.assigned_date.strftime('%Y-%m-%d') if visit.assigned_date else None,
                    'next_follow_up_date': followup_date_str,
                    'is_followup': True,
                    'original_visit_id': visit.id,
                    'original_visit_date': visit.assigned_date.strftime('%Y-%m-%d') if visit.assigned_date else visit.start_date.strftime('%Y-%m-%d'),
                    'owner_profile': executive_profile
                })
        
        return {
            "status": 200,
            "message": "Visits and followups retrieved successfully",
            "data": visits_by_date,
            "user_profile": executive_profile
        }
    
    except Exception as e:
        print(f"Error getting assigned visits: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/visits/{executive_id}/today")
def get_todays_visits(
    executive_id: int,
    db: Session = Depends(get_db)
):
    try:
        executive_profile = get_executive_profile(executive_id, db)
        if not executive_profile:
            raise HTTPException(status_code=404, detail="Executive not found")

        today = date.today()
        
        assigned_visits = db.query(GymVisits).filter(
            and_(
                GymVisits.user_id == executive_id,
                func.date(GymVisits.assigned_date) == today
            )
        ).all()

        followup_visits = db.query(GymVisits).filter(
            and_(
                GymVisits.user_id == executive_id,
                GymVisits.final_status == 'followup',
                GymVisits.next_follow_up_date.isnot(None),
                func.date(GymVisits.next_follow_up_date) == today
            )
        ).all()
        
        visits_data = []
        
        for visit in assigned_visits:
            visits_data.append({
                'id': visit.id,
                'gymName': visit.gym_name,
                'location': visit.gym_address,                
                'referal_id':visit.referal_id,
                'contactPerson': visit.contact_person,
                'contactNumber': visit.contact_phone,
                'visitPurpose': visit.visit_purpose,
                'visitType': visit.visit_type,
                'status': visit.status,
                'final_status': visit.final_status,
                'notes': visit.notes,
                'date': visit.assigned_date.strftime('%Y-%m-%d') if visit.assigned_date else visit.start_date.strftime('%Y-%m-%d'),
                'start_time': visit.check_in_time.strftime('%I:%M %p') if visit.check_in_time else None,
                'end_time': visit.check_out_time.strftime('%I:%M %p') if visit.check_out_time else None,
                'is_followup': False,
                'original_visit_id': visit.id,
                'owner_profile': executive_profile
            })

        for visit in followup_visits:
            existing_visit = next((v for v in visits_data 
                                 if v['original_visit_id'] == visit.id), None)
            
            if not existing_visit:
                visits_data.append({
                    'id': f"followup_{visit.id}_{today.strftime('%Y-%m-%d')}",
                    'gymName': visit.gym_name,
                    'referal_id':visit.referal_id,
                    'location': visit.gym_address,
                    'contactPerson': visit.contact_person,
                    'contactNumber': visit.contact_phone,
                    'visitPurpose': 'Follow-up Visit',
                    'visitType': 'follow_up',
                    'status': 'scheduled',
                    'final_status': 'followup',
                    'notes': visit.follow_up_notes or visit.notes,
                    'date': today.strftime('%Y-%m-%d'),
                    'start_time': None,
                    'end_time': None,
                    'is_followup': True,
                    'original_visit_id': visit.id,
                    'original_visit_date': visit.assigned_date.strftime('%Y-%m-%d') if visit.assigned_date else visit.start_date.strftime('%Y-%m-%d'),
                    'owner_profile': executive_profile
                })
        
        return {
            "status": 200,
            "message": "Today's visits and followups retrieved successfully",
            "data": visits_data,
            "user_profile": executive_profile
        }
    
    except Exception as e:
        print(f"Error getting today's visits: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/stats/{executive_id}")
def get_planner_stats(
    executive_id: int,
    month: Optional[int] = Query(None),
    year: Optional[int] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        executive_profile = get_executive_profile(executive_id, db)
        if not executive_profile:
            raise HTTPException(status_code=404, detail="Executive not found")

        if not month:
            month = datetime.now().month
        if not year:
            year = datetime.now().year

        # Get all gym assignments for this executive in the specified month/year
        gym_assignments = db.query(GymAssignments).filter(
            and_(
                GymAssignments.executive_id == executive_id,
                GymAssignments.status == 'assigned',
                extract('month', GymAssignments.assigned_date) == month,
                extract('year', GymAssignments.assigned_date) == year
            )
        ).all()

        # pending_visits: gyms that are assigned but conversion_status is 'pending' (not in followup, converted, rejected)
        pending_visits = len([
            ga for ga in gym_assignments
            if ga.conversion_status == 'pending'
        ])
        print(f"Pending Visits: {pending_visits}")

        # total_visits: gyms that are assigned and have any status like followup, converted, or rejected
        total_visits = len([
            ga for ga in gym_assignments
            if ga.conversion_status in ['followup', 'converted', 'rejected']
        ])

        # Get visits data for additional stats
        assigned_visits = db.query(GymVisits).filter(
            and_(
                GymVisits.user_id == executive_id,
                extract('month', GymVisits.assigned_date) == month,
                extract('year', GymVisits.assigned_date) == year
            )
        ).all()

        followup_visits = db.query(GymVisits).filter(
            and_(
                GymVisits.user_id == executive_id,
                GymVisits.final_status == 'followup',
                GymVisits.next_follow_up_date.isnot(None),
                extract('month', GymVisits.next_follow_up_date) == month,
                extract('year', GymVisits.next_follow_up_date) == year
            )
        ).all()

        # Count visits based on completion status
        completed_visits = len([v for v in assigned_visits if v.completed == True])

        total_followups = len(followup_visits)

        # Count by status field for additional breakdown
        scheduled_visits = len([v for v in assigned_visits if v.status == 'scheduled'])
        cancelled_visits = len([v for v in assigned_visits if v.status == 'cancelled'])
        assigned_visits_count = len([v for v in assigned_visits if v.status == 'assigned'])

        # Count by conversion_status from gym_assignments
        converted_visits = len([ga for ga in gym_assignments if ga.conversion_status == 'converted'])
        rejected_visits = len([ga for ga in gym_assignments if ga.conversion_status == 'rejected'])
        followup_count = len([ga for ga in gym_assignments if ga.conversion_status == 'followup'])

        visit_type_counts = {}
        for visit in assigned_visits:
            visit_type = visit.visit_type or 'sales_call'
            visit_type_counts[visit_type] = visit_type_counts.get(visit_type, 0) + 1

        if total_followups > 0:
            visit_type_counts['follow_up'] = visit_type_counts.get('follow_up', 0) + total_followups

        today = datetime.now().date()
        upcoming_assigned = len([
            v for v in assigned_visits
            if v.status == 'scheduled' and v.assigned_date and v.assigned_date.date() >= today
        ])
        upcoming_followups = len([
            v for v in followup_visits
            if v.next_follow_up_date and v.next_follow_up_date.date() >= today
        ])

        return {
            "status": 200,
            "message": "Planner statistics retrieved successfully",
            "data": {
                "total_visits": total_visits,
                "pending_visits": pending_visits,
                "completed_visits": completed_visits,
                "converted_visits": converted_visits,
                "rejected_visits": rejected_visits,
                "followup_visits": followup_count,
                "total_followups": total_followups,
                "scheduled_visits": scheduled_visits,
                "assigned_visits": assigned_visits_count,
                "cancelled_visits": cancelled_visits,
                "upcoming_visits": upcoming_assigned + upcoming_followups,
                "visit_type_counts": visit_type_counts,
                "month": month,
                "year": year
            },
            "user_profile": executive_profile
        }

    except Exception as e:
        print(f"Error getting planner stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@router.get("/visit/{visit_id}")
def get_visit_details(
    visit_id: str,
    executive_id: int = Query(..., description="Executive ID for authorization"),
    db: Session = Depends(get_db)
):
    try:
        executive_profile = get_executive_profile(executive_id, db)
        if not executive_profile:
            raise HTTPException(status_code=404, detail="Executive not found")

        if str(visit_id).startswith('followup_'):
            parts = str(visit_id).split('_')
            if len(parts) >= 2:
                original_visit_id = int(parts[1])
                visit = db.query(GymVisits).filter(
                    and_(
                        GymVisits.id == original_visit_id,
                        GymVisits.user_id == executive_id
                    )
                ).first()
                
                if not visit:
                    raise HTTPException(status_code=404, detail="Visit not found")
                
                visit_data = {
                    'id': visit_id,
                    'gymName': visit.gym_name,
                    'location': visit.gym_address,
                    'referal_id':visit.referal_id,
                    'contactPerson': visit.contact_person,
                    'contactNumber': visit.contact_phone,
                    'visitPurpose': 'Follow-up Visit',
                    'visitType': 'follow_up',
                    'status': 'scheduled',
                    'final_status': 'followup',
                    'notes': visit.follow_up_notes or visit.notes,
                    'date': visit.next_follow_up_date.strftime('%Y-%m-%d') if visit.next_follow_up_date else None,
                    'start_time': None,
                    'end_time': None,
                    'assigned_date': visit.assigned_date.strftime('%Y-%m-%d') if visit.assigned_date else None,
                    'next_follow_up_date': visit.next_follow_up_date.strftime('%Y-%m-%d') if visit.next_follow_up_date else None,
                    'is_followup': True,
                    'original_visit_id': visit.id,
                    'original_visit_date': visit.assigned_date.strftime('%Y-%m-%d') if visit.assigned_date else visit.start_date.strftime('%Y-%m-%d'),
                    'assigned_at': visit.assigned_date.isoformat() if visit.assigned_date else None,
                    'updated_at': visit.updated_at.isoformat() if visit.updated_at else None,
                    'owner_profile': executive_profile
                }
        else:
            visit = db.query(GymVisits).filter(
                and_(
                    GymVisits.id == visit_id,
                    GymVisits.user_id == executive_id
                )
            ).first()
            
            if not visit:
                raise HTTPException(status_code=404, detail="Visit not found")
            
            visit_data = {
                'id': visit.id,
                'gymName': visit.gym_name,
                'location': visit.gym_address,
                'referal_id':visit.referal_id,
                'contactPerson': visit.contact_person,
                'contactNumber': visit.contact_phone,
                'visitPurpose': visit.visit_purpose,
                'visitType': visit.visit_type,
                'status': visit.status,
                'final_status': visit.final_status,
                'notes': visit.notes,
                'date': visit.assigned_date.strftime('%Y-%m-%d') if visit.assigned_date else visit.start_date.strftime('%Y-%m-%d'),
                'start_time': visit.check_in_time.strftime('%I:%M %p') if visit.check_in_time else None,
                'end_time': visit.check_out_time.strftime('%I:%M %p') if visit.check_out_time else None,
                'assigned_date': visit.assigned_date.strftime('%Y-%m-%d') if visit.assigned_date else None,
                'next_follow_up_date': visit.next_follow_up_date.strftime('%Y-%m-%d') if visit.next_follow_up_date else None,
                'is_followup': False,
                'original_visit_id': visit.id,
                'assigned_at': visit.assigned_date.isoformat() if visit.assigned_date else None,
                'updated_at': visit.updated_at.isoformat() if visit.updated_at else None,
                'owner_profile': executive_profile
            }
        
        return {
            "status": 200,
            "message": "Visit details retrieved successfully",
            "data": visit_data,
            "user_profile": executive_profile
        }
    
    except Exception as e:
        print(f"Error getting visit details: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")