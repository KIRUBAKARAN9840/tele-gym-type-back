from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, case, and_, or_, desc
from datetime import datetime, timedelta
from typing import Optional, List
from decimal import Decimal
from app.models.database import get_db
from app.models.marketingmodels import Executives, Managers, GymVisits, Feedback, GymDatabase
import calendar

router = APIRouter(prefix="/marketing/leaderboard", tags=["Leaderboard"])

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
            'employee_id': user.emp_id,
            'manager_id': user.manager_id
        }
    return None

def get_manager_profile(user_id: int, db: Session):
    user = db.query(Managers).filter(Managers.id == user_id).first()
    if user:
        team_count = db.query(Executives).filter(
            Executives.manager_id == user.id,
            Executives.status == "active"
        ).count()
        
        return {
            'id': user.id,
            'name': user.name,
            'email': user.email,
            'contact': user.contact,
            'profile': user.profile,
            'role': 'BDM',  
            'user_type': 'manager',
            'joined_date': user.joined_date.isoformat() if user.joined_date else None,
            'status': user.status,
            'employee_id': user.emp_id,
            'team_size': team_count
        }
    return None

def get_user_profile_by_role(user_id: int, role: str, db: Session):
    if role == 'BDE':
        return get_executive_profile(user_id, db)
    elif role == 'BDM':
        return get_manager_profile(user_id, db)
    else:
        return None

def safe_float(value):
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    return float(value)

def safe_int(value):
    if value is None:
        return 0
    if isinstance(value, Decimal):
        return int(value)
    return int(value)

# --- MODIFIED FUNCTION ---
def calculate_conversion_score(total_visits, converted_visits):
    """
    Calculates the conversion rate (0-100) based only on total visits and converted visits.
    """
    total_visits = safe_int(total_visits)
    converted_visits = safe_int(converted_visits)
    
    if total_visits == 0:
        return 0.0
    
    conversion_rate = (float(converted_visits) / float(total_visits)) * 100.0
    
    return round(conversion_rate, 2)

# The original calculate_performance_score is removed as it's no longer used for BDE/BDM ranking

def get_date_filter(period: str, start_date: Optional[str], end_date: Optional[str]):
    if start_date and end_date:
        return datetime.strptime(start_date, "%Y-%m-%d"), datetime.strptime(end_date, "%Y-%m-%d")
    
    today = datetime.now()
    
    if period == "weekly":
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6)
    elif period == "monthly":
        start = today.replace(day=1)
        end = today.replace(day=calendar.monthrange(today.year, today.month)[1])
    elif period == "quarterly":
        quarter = (today.month - 1) // 3 + 1
        start = today.replace(month=(quarter - 1) * 3 + 1, day=1)
        end_month = quarter * 3
        end = today.replace(month=end_month, day=calendar.monthrange(today.year, end_month)[1])
    else:  
        return None, None
    
    # Ensure end date includes the entire day
    if end:
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
    return start, end

def calculate_executive_metrics(executive_id: int, date_start: datetime, date_end: datetime, db: Session,manager:Optional[bool]):
    try:

        if not manager:
        
            visits_query = db.query(GymVisits).filter(GymVisits.user_id == executive_id)

        else:
            visits_query = db.query(GymVisits).filter(GymVisits.manager_id == executive_id)


        if date_start and date_end:
            # Filter by assigned_date, and include visits where assigned_date is NULL but created_at is in range
            visits_query = visits_query.filter(
                or_(
                    and_(
                        GymVisits.assigned_date.isnot(None),
                        GymVisits.assigned_date >= date_start,
                        GymVisits.assigned_date <= date_end
                    ),
                    and_(
                        GymVisits.assigned_date.is_(None),
                        GymVisits.created_at >= date_start,
                        GymVisits.created_at <= date_end
                    )
                )
            )

        # Total visits = only completed visits (actually visited, not pending)
        # Conversion rate = converted / completed visits
        completed_visits = safe_int(visits_query.filter(GymVisits.completed == True).count())
        converted_visits = safe_int(visits_query.filter(GymVisits.final_status == 'converted').count())

        # Total visits for leaderboard = completed visits only
        total_visits = completed_visits
        conversion_rate = (float(converted_visits) / float(total_visits) * 100.0) if total_visits > 0 else 0.0

        # Keeping other metrics for detailed report data, but they won't affect ranking
        followup_visits = safe_int(visits_query.filter(GymVisits.final_status == 'followup').count())
        pending_visits = safe_int(visits_query.filter(GymVisits.final_status == 'pending').count())
        rejected_visits = safe_int(visits_query.filter(GymVisits.final_status == 'rejected').count())
        
        completion_rate = (float(completed_visits) / float(total_visits) * 100.0) if total_visits > 0 else 0.0
        follow_up_rate = (float(followup_visits + converted_visits) / float(total_visits) * 100.0) if total_visits > 0 else 0.0
        
        # Get feedback for this executive within the date range
        feedback_query = db.query(func.avg(Feedback.rating)).join(
            GymVisits, Feedback.visit_id == GymVisits.id
        ).filter(Feedback.executive_id == executive_id)

        if date_start and date_end:
            feedback_query = feedback_query.filter(
                or_(
                    and_(
                        GymVisits.assigned_date.isnot(None),
                        GymVisits.assigned_date >= date_start,
                        GymVisits.assigned_date <= date_end
                    ),
                    and_(
                        GymVisits.assigned_date.is_(None),
                        GymVisits.created_at >= date_start,
                        GymVisits.created_at <= date_end
                    )
                )
            )
        avg_rating = safe_float(feedback_query.scalar() or 0)


        self_assigned_query = db.query(GymVisits).join(
            GymDatabase, GymVisits.gym_id == GymDatabase.id
        ).filter(
            GymVisits.user_id == executive_id,
            GymDatabase.self_assigned == True
        )

        if date_start and date_end:
            self_assigned_query = self_assigned_query.filter(
                or_(
                    and_(
                        GymVisits.assigned_date.isnot(None),
                        GymVisits.assigned_date >= date_start,
                        GymVisits.assigned_date <= date_end
                    ),
                    and_(
                        GymVisits.assigned_date.is_(None),
                        GymVisits.created_at >= date_start,
                        GymVisits.created_at <= date_end
                    )
                )
            )

        self_assigned_total = safe_int(self_assigned_query.count())
        self_assigned_pending = safe_int(self_assigned_query.filter(GymVisits.final_status == 'pending').count())
        self_assigned_converted = safe_int(self_assigned_query.filter(GymVisits.final_status == 'converted').count())
        self_assigned_followup = safe_int(self_assigned_query.filter(GymVisits.final_status == 'followup').count())
        self_assigned_rejected = safe_int(self_assigned_query.filter(GymVisits.final_status == 'rejected').count())


        return {
            'total_visits': total_visits,
            'completed_visits': completed_visits,
            'converted_visits': converted_visits,
            'followup_visits': followup_visits,
            'pending_visits': pending_visits,
            'rejected_visits': rejected_visits,
            'completion_rate': round(completion_rate, 2),
            'conversion_rate': round(conversion_rate, 2),
            'follow_up_rate': round(follow_up_rate, 2),
            'avg_rating': round(avg_rating, 2),
            'self_assigned_total': self_assigned_total,
            'self_assigned_pending': self_assigned_pending,
            'self_assigned_converted': self_assigned_converted,
            'self_assigned_followup': self_assigned_followup,
            'self_assigned_rejected': self_assigned_rejected
        }
    except Exception as e:
        print(f"Error calculating metrics for executive {executive_id}: {str(e)}")
        return {
            'total_visits': 0,
            'completed_visits': 0,
            'converted_visits': 0,
            'followup_visits': 0,
            'pending_visits': 0,
            'rejected_visits': 0,
            'completion_rate': 0.0,
            'conversion_rate': 0.0,
            'follow_up_rate': 0.0,
            'avg_rating': 0.0,
            'self_assigned_total': 0,
            'self_assigned_pending': 0,
            'self_assigned_converted': 0,
            'self_assigned_followup': 0,
            'self_assigned_rejected': 0
        }

@router.get("/bde")
async def get_bde_leaderboard(
    user_id: int = Query(...),
    role: str = Query(...),
    period: str = Query(default="monthly"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    manager_id: Optional[int] = Query(None),
    db: Session = Depends(get_db)
):
    
    try:
        user_profile = get_user_profile_by_role(user_id, role, db)
        if not user_profile:
            raise HTTPException(status_code=404, detail="User not found")
        
        date_start, date_end = get_date_filter(period, start_date, end_date)
        
        executives_query = db.query(Executives)
        if manager_id:
            executives_query = executives_query.filter(Executives.manager_id == manager_id)
        
        executives = executives_query.filter(Executives.status == "active").all()

        # === BATCH LOAD: Get all managers at once (N+1 FIX) ===
        manager_ids = list({e.manager_id for e in executives if e.manager_id})
        manager_map = {}
        if manager_ids:
            managers = db.query(Managers).filter(Managers.id.in_(manager_ids)).all()
            manager_map = {m.id: m for m in managers}

        leaderboard_data = []

        for executive in executives:
            try:
                metrics = calculate_executive_metrics(executive.id, date_start, date_end, db,False)

                # Only include BDEs with at least one visit
                if metrics['total_visits'] == 0:
                    continue

                conversion_score = calculate_conversion_score(
                    metrics['total_visits'],
                    metrics['converted_visits']
                )

                # Get manager from pre-loaded map (was N+1 query)
                manager = manager_map.get(executive.manager_id)

                leaderboard_data.append({
                    'executive_id': executive.id,
                    'name': executive.name or 'Unknown',
                    'email': executive.email or '',
                    'profile': executive.profile or '',
                    'employee_id': executive.emp_id or '',
                    'manager_name': manager.name if manager else None,

                    # Core ranking metrics
                    'total_visits': metrics['total_visits'],
                    'converted_visits': metrics['converted_visits'],
                    'conversion_rate': metrics['conversion_rate'], # This is the primary score
                    'performance_score': conversion_score, # Alias for conversion_rate for general use

                    # Other metrics for report visibility (not ranking)
                    'completed_visits': metrics['completed_visits'],
                    'followup_visits': metrics['followup_visits'],
                    'pending_visits': metrics['pending_visits'],
                    'rejected_visits': metrics['rejected_visits'],
                    'completion_rate': metrics['completion_rate'],
                    'follow_up_rate': metrics['follow_up_rate'],
                    'avg_rating': metrics['avg_rating'],

                    # Self-assigned gym metrics
                    'self_assigned_total': metrics['self_assigned_total'],
                    'self_assigned_pending': metrics['self_assigned_pending'],
                    'self_assigned_converted': metrics['self_assigned_converted'],
                    'self_assigned_followup': metrics['self_assigned_followup'],
                    'self_assigned_rejected': metrics['self_assigned_rejected'],
                })
            except Exception as e:
                print(f"Error processing executive {executive.id}: {str(e)}")
                continue
        
        # --- MODIFIED RANKING LOGIC ---
        # 1. Highest Conversion Rate (Primary)
        # 2. Highest Converted Visits (Tie-breaker 1)
        # 3. Lowest Total Visits (Tie-breaker 2: Efficiency)
        # 4. Alphabetical Name (Tie-breaker 3)
        leaderboard_data.sort(key=lambda x: (
            x['converted_visits'], 
            x['conversion_rate'], 
            
            -x['total_visits'],  # Negative sign for ascending total_visits (less is better)
            x['name']
        ), reverse=True)
        
        for i, data in enumerate(leaderboard_data):
            data['rank'] = i + 1
        
        return {
            "status": 200,
            "message": "BDE leaderboard fetched successfully (Conversion-focused)",
            "data": {
                "leaderboard": leaderboard_data,
                "period": period,
                "date_range": {
                    "start_date": date_start.strftime("%Y-%m-%d") if date_start else None,
                    "end_date": date_end.strftime("%Y-%m-%d") if date_end else None
                },
                "total_bdes": len(leaderboard_data)
            },
            "user_profile": user_profile
        }
        
    except Exception as e:
        print(f"Error in BDE leaderboard: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching BDE leaderboard: {str(e)}")


@router.get("/bdm")
async def get_bdm_leaderboard(
    user_id: int = Query(...),
    role: str = Query(...),
    period: str = Query(default="monthly"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    
    try:
        user_profile = get_user_profile_by_role(user_id, role, db)
        if not user_profile:
            raise HTTPException(status_code=404, detail="User not found")
        date_start, date_end = get_date_filter(period, start_date, end_date)
        
        managers = db.query(Managers).filter(Managers.status == "active").all()
        print(f"Total active managers found: {len(managers)}")
        print(f"Manager names: {[m.name for m in managers]}")

        # === BATCH LOAD: Get all executives for all managers at once (N+1 FIX) ===
        manager_ids = [m.id for m in managers]
        all_executives = db.query(Executives).filter(
            Executives.manager_id.in_(manager_ids),
            Executives.status == "active"
        ).all()
        # Group executives by manager_id
        executives_by_manager = {}
        for e in all_executives:
            if e.manager_id not in executives_by_manager:
                executives_by_manager[e.manager_id] = []
            executives_by_manager[e.manager_id].append(e)

        leaderboard_data = []

        for manager in managers:
            print(f"\n=== Processing manager: {manager.name} (id={manager.id}) ===")
            try:
                # Get team members from pre-loaded map (was N+1 query)
                team_members = executives_by_manager.get(manager.id, [])
                print(f"Team members found: {len(team_members)} - {[e.name for e in team_members]}")

                
                
                team_stats = {
                    'total_visits': 0,
                    'converted_visits': 0,
                    'self_assigned_total': 0,
                    'self_assigned_pending': 0,
                    'self_assigned_converted': 0,
                    'self_assigned_followup': 0,
                    'self_assigned_rejected': 0,
                    # Other aggregated metrics are no longer needed for ranking
                }

                if team_members:

                    team_member_count = len(team_members)
                    for executive in team_members:
                        try:
                            metrics = calculate_executive_metrics(executive.id, date_start, date_end,db,False)

                            team_stats['total_visits'] += metrics['total_visits']
                            team_stats['converted_visits'] += metrics['converted_visits']
                            team_stats['self_assigned_total'] += metrics['self_assigned_total']
                            team_stats['self_assigned_pending'] += metrics['self_assigned_pending']
                            team_stats['self_assigned_converted'] += metrics['self_assigned_converted']
                            team_stats['self_assigned_followup'] += metrics['self_assigned_followup']
                            team_stats['self_assigned_rejected'] += metrics['self_assigned_rejected']
                        except Exception as e:
                            print(f"Error processing executive {executive.id} for manager {manager.id}: {str(e)}")
                            continue
                else:
                    team_member_count=0

                

                # Add manager's own self-assigned gyms
                try:
                    manager_metrics = calculate_executive_metrics(manager.id, date_start, date_end, db,True)
                    team_stats['total_visits'] += manager_metrics['total_visits']
                    team_stats['converted_visits'] += manager_metrics['converted_visits']
                    team_stats['self_assigned_total'] += manager_metrics['self_assigned_total']
                    team_stats['self_assigned_pending'] += manager_metrics['self_assigned_pending']
                    team_stats['self_assigned_converted'] += manager_metrics['self_assigned_converted']
                    team_stats['self_assigned_followup'] += manager_metrics['self_assigned_followup']
                    team_stats['self_assigned_rejected'] += manager_metrics['self_assigned_rejected']
                except Exception as e:
                    print(f"Error processing manager's self-assigned gyms for manager {manager.id}: {str(e)}")

                total_visits = team_stats['total_visits']
                print(f"Total visits for {manager.name}: {total_visits}, Converted: {team_stats['converted_visits']}")
                if total_visits == 0:
                    print(f"Skipping {manager.name} - zero total visits")
                    continue
                
                converted_visits = team_stats['converted_visits']
                
                conversion_rate = (float(converted_visits) / float(total_visits) * 100.0) if total_visits > 0 else 0.0
                conversion_score = calculate_conversion_score(total_visits, converted_visits)

                # Keeping a few non-ranking metrics for context
                completion_rate = (safe_float(team_stats.get('completed_visits', 0)) / float(total_visits) * 100.0) if total_visits > 0 else 0.0
                
                leaderboard_data.append({
                    'manager_id': manager.id,
                    'name': manager.name or 'Unknown',
                    'email': manager.email or '',
                    'profile': manager.profile or '',
                    'employee_id': manager.emp_id or '',
                    'team_size': team_member_count,

                    # Core ranking metrics
                    'total_visits': total_visits,
                    'converted_visits': converted_visits,
                    'conversion_rate': round(conversion_rate, 2),
                    'performance_score': conversion_score, # Alias for conversion_rate for general use

                    # Other metrics for report visibility (not ranking)
                    'completion_rate': round(completion_rate, 2),

                    # Self-assigned gym metrics (aggregated from team)
                    'self_assigned_total': team_stats['self_assigned_total'],
                    'self_assigned_pending': team_stats['self_assigned_pending'],
                    'self_assigned_converted': team_stats['self_assigned_converted'],
                    'self_assigned_followup': team_stats['self_assigned_followup'],
                    'self_assigned_rejected': team_stats['self_assigned_rejected'],
                })
                print(f"✅ Added {manager.name} to leaderboard")
            except Exception as e:
                print(f"❌ Error processing manager {manager.id} ({manager.name}): {str(e)}")
                import traceback
                traceback.print_exc()
                continue

        print(f"\n📊 Final leaderboard count: {len(leaderboard_data)} managers")
        print(f"Managers in leaderboard: {[m['name'] for m in leaderboard_data]}")

        # --- MODIFIED RANKING LOGIC (Same as BDE, applied to team stats) ---
        # 1. Highest Conversion Rate (Primary)
        # 2. Highest Converted Visits (Tie-breaker 1)
        # 3. Lowest Total Visits (Tie-breaker 2: Efficiency)
        # 4. Alphabetical Name (Tie-breaker 3)
        leaderboard_data.sort(key=lambda x: (
            x['converted_visits'],
            x['conversion_rate'], 
             
            -x['total_visits'],  # Negative sign for ascending total_visits (less is better)
            x['name']
        ), reverse=True)
        
        for i, data in enumerate(leaderboard_data):
            data['rank'] = i + 1
        
        return {
            "status": 200,
            "message": "BDM leaderboard fetched successfully (Conversion-focused)",
            "data": {
                "leaderboard": leaderboard_data,
                "period": period,
                "date_range": {
                    "start_date": date_start.strftime("%Y-%m-%d") if date_start else None,
                    "end_date": date_end.strftime("%Y-%m-%d") if end_date else None
                },
                "total_bdms": len(leaderboard_data)
            },
            "user_profile": user_profile
        }
        
    except Exception as e:
        print(f"Error in BDM leaderboard: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching BDM leaderboard: {str(e)}")

# Remaining endpoints (/team-bde and /stats) are kept as-is since the logic change only affects BDE/BDM ranking and their dependency.

@router.get("/team-bde")
async def get_team_bde_leaderboard(
    manager_id: int = Query(...),
    period: str = Query(default="monthly"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    
    try:
        user_profile = get_manager_profile(manager_id, db)
        if not user_profile:
            raise HTTPException(status_code=404, detail="Manager not found")
        
        # Fetch leaderboard scoped to the manager's team
        team_response = await get_bde_leaderboard(
            user_id=manager_id,
            role="BDM",
            period=period,
            start_date=start_date,
            end_date=end_date,
            manager_id=manager_id,
            db=db
        )

        # Fetch complete leaderboard across all BDEs irrespective of manager
        all_bde_response = await get_bde_leaderboard(
            user_id=manager_id,
            role="BDM",
            period=period,
            start_date=start_date,
            end_date=end_date,
            manager_id=None,
            db=db
        )
        
        # Preserve original team-scoped data while exposing all BDE results at the default keys
        team_leaderboard = team_response["data"].get("leaderboard", [])
        team_summary = team_response["data"].get("summary")

        team_response["data"]["team_leaderboard"] = team_leaderboard
        team_response["data"]["team_summary"] = team_summary

        team_response["data"]["leaderboard"] = all_bde_response["data"]["leaderboard"]
        team_response["data"]["summary"] = all_bde_response["data"].get("summary")
        team_response["user_profile"] = user_profile
        return team_response
        
    except Exception as e:
        print(f"Error in team BDE leaderboard: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching team BDE leaderboard: {str(e)}")

@router.get("/stats")
async def get_leaderboard_stats(
    user_id: int = Query(...),
    role: str = Query(...),
    period: str = Query(default="monthly"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    
    try:
        user_profile = get_user_profile_by_role(user_id, role, db)
        if not user_profile:
            raise HTTPException(status_code=404, detail="User not found")
        
        date_start, date_end = get_date_filter(period, start_date, end_date)
        
        total_bdes = safe_int(db.query(Executives).filter(Executives.status == "active").count())
        total_bdms = safe_int(db.query(Managers).filter(Managers.status == "active").count())
        
        visits_query = db.query(GymVisits)
        if date_start and date_end:
            visits_query = visits_query.filter(
                or_(
                    and_(
                        GymVisits.assigned_date.isnot(None),
                        GymVisits.assigned_date >= date_start,
                        GymVisits.assigned_date <= date_end
                    ),
                    and_(
                        GymVisits.assigned_date.is_(None),
                        GymVisits.created_at >= date_start,
                        GymVisits.created_at <= date_end
                    )
                )
            )
        
        total_visits = safe_int(visits_query.count())
        completed_visits = safe_int(visits_query.filter(GymVisits.completed == True).count())
        converted_visits = safe_int(visits_query.filter(GymVisits.final_status == 'converted').count())
        
        overall_completion_rate = (float(completed_visits) / float(total_visits) * 100.0) if total_visits > 0 else 0.0
        overall_conversion_rate = (float(converted_visits) / float(total_visits) * 100.0) if total_visits > 0 else 0.0
        
        return {
            "status": 200,
            "message": "Leaderboard stats fetched successfully",
            "data": {
                "summary": {
                    "total_bdes": total_bdes,
                    "total_bdms": total_bdms,
                    "total_visits": total_visits,
                    "completed_visits": completed_visits,
                    "converted_visits": converted_visits,
                    "overall_completion_rate": round(overall_completion_rate, 2),
                    "overall_conversion_rate": round(overall_conversion_rate, 2)
                },
                "period": period,
                "date_range": {
                    "start_date": date_start.strftime("%Y-%m-%d") if date_start else None,
                    "end_date": date_end.strftime("%Y-%m-%d") if date_end else None
                }
            },
            "user_profile": user_profile
        }
        
    except Exception as e:
        print(f"Error in leaderboard stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching leaderboard stats: {str(e)}")
