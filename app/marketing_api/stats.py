from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, case, or_, select
from app.models.database import get_db
from app.models.marketingmodels import GymVisits, Executives, Managers, GymDatabase, GymAssignments
from datetime import datetime, timedelta
from typing import Optional
from decimal import Decimal

router = APIRouter(prefix="/marketing/stats", tags=["stats"])

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

def get_date_range(start_date: Optional[str], end_date: Optional[str]):
    if start_date:
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD")
    
    if end_date:
        try:
            end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD")
    
    return start_date, end_date

def apply_date_filter(query, start_date, end_date):
    if start_date:
        query = query.filter(GymVisits.created_at >= start_date)
    if end_date:
        query = query.filter(GymVisits.created_at < end_date)
    return query

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

def get_status_counts(query):
    status_counts = query.with_entities(
        GymVisits.final_status,
        func.count(GymVisits.id).label('count')
    ).group_by(GymVisits.final_status).all()
    
    status_dict = {status: safe_int(count) for status, count in status_counts}
    return {
        'pending': status_dict.get('pending', 0),
        'followup': status_dict.get('followup', 0),
        'converted': status_dict.get('converted', 0),
        'rejected': status_dict.get('rejected', 0),
        'scheduled': status_dict.get('scheduled', 0)
    }

@router.get("/overview")
async def get_stats_overview(
    user_id: int,
    role: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    bde_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    try:
        user_profile = get_user_profile_by_role(user_id, role, db)
        if not user_profile:
            raise HTTPException(status_code=404, detail="User not found")
        
        start_date, end_date = get_date_range(start_date, end_date)
        
        query = db.query(GymVisits)

        if role == "BDM":
            # Include both team executive visits AND manager's self-assigned visits
            team_executives_query = select(Executives.id).where(
                Executives.manager_id == user_id
            )
            query = query.filter(
                or_(
                    GymVisits.user_id.in_(team_executives_query),
                    GymVisits.user_id == user_id  # Manager's self-assigned visits
                )
            )
        else:
            query = query.filter(GymVisits.user_id == user_id)
        
        query = apply_date_filter(query, start_date, end_date)
        
        if bde_id:
            query = query.filter(GymVisits.user_id == bde_id)
        
        total_visits = safe_int(query.count())
        
        status_counts = get_status_counts(query)
    
        converted = safe_int(status_counts.get('converted', 0))
        conversion_rate = (safe_float(converted) / safe_float(total_visits) * 100.0) if total_visits > 0 else 0.0
        
        thirty_days_ago = datetime.now() - timedelta(days=30)
        recent_query = apply_date_filter(query, thirty_days_ago, None)
        recent_visits = safe_int(recent_query.count())
        
        top_bde = None
        manager_own_stats = None

        if role == "BDM":
            # Get top BDE performance from team
            bde_performance = query.join(Executives).with_entities(
                Executives.name,
                Executives.id,
                func.count(GymVisits.id).label('total_visits'),
                func.sum(case(
                    (GymVisits.final_status == 'converted', 1),
                    else_=0
                )).label('conversions')
            ).group_by(Executives.id, Executives.name).order_by(
                func.sum(case(
                    (GymVisits.final_status == 'converted', 1),
                    else_=0
                )).desc()
            ).first()

            if bde_performance:
                top_bde = {
                    'name': bde_performance.name,
                    'id': bde_performance.id,
                    'total_visits': safe_int(bde_performance.total_visits),
                    'conversions': safe_int(bde_performance.conversions)
                }

            # Get manager's own self-assigned gym statistics
            manager_query = db.query(GymVisits).filter(GymVisits.user_id == user_id)
            manager_query = apply_date_filter(manager_query, start_date, end_date)

            manager_total_visits = safe_int(manager_query.count())

            if manager_total_visits > 0:
                manager_status_counts = get_status_counts(manager_query)
                manager_converted = safe_int(manager_status_counts.get('converted', 0))
                manager_conversion_rate = (safe_float(manager_converted) / safe_float(manager_total_visits) * 100.0) if manager_total_visits > 0 else 0.0

                manager_own_stats = {
                    'total_visits': manager_total_visits,
                    'status_breakdown': manager_status_counts,
                    'conversion_rate': round(manager_conversion_rate, 2),
                    'converted': manager_converted
                }

        return {
            "status": 200,
            "message": "Stats overview fetched successfully",
            "data": {
                "overview": {
                    "total_visits": total_visits,
                    "conversion_rate": round(conversion_rate, 2),
                    "recent_visits": recent_visits,
                    "status_breakdown": status_counts,
                    "top_performer": top_bde,
                    "manager_own_stats": manager_own_stats
                }
            },
            "user_profile": user_profile
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/bde")
async def get_bde_stats(
    user_id: int,
    role: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    bde_id: Optional[int] = None,
    status: Optional[str] = None,
    db: Session = Depends(get_db)
):
    try:
        start_date, end_date = get_date_range(start_date, end_date)
        
        base_query = db.query(GymVisits).join(Executives)
        
        if role == "BDM":
            base_query = base_query.filter(Executives.manager_id == user_id)
        else:
            base_query = base_query.filter(GymVisits.user_id == user_id)
        
        base_query = apply_date_filter(base_query, start_date, end_date)
        
        if bde_id:
            base_query = base_query.filter(GymVisits.user_id == bde_id)
        
        if status:
            base_query = base_query.filter(GymVisits.final_status == status)
        
        bde_stats = base_query.with_entities(
            Executives.id,
            Executives.name,
            Executives.email,
            Executives.emp_id,
            func.count(GymVisits.id).label('total_visits'),
            func.sum(case(
                (GymVisits.final_status == 'pending', 1),
                else_=0
            )).label('pending'),
            func.sum(case(
                (GymVisits.final_status == 'followup', 1),
                else_=0
            )).label('followup'),
            func.sum(case(
                (GymVisits.final_status == 'converted', 1),
                else_=0
            )).label('converted'),
            func.sum(case(
                (GymVisits.final_status == 'rejected', 1),
                else_=0
            )).label('rejected'),
            func.sum(case(
                (GymVisits.final_status == 'scheduled', 1),
                else_=0
            )).label('scheduled'),
            func.avg(GymVisits.overall_rating).label('avg_rating')
        ).group_by(
            Executives.id, Executives.name, Executives.email, Executives.emp_id
        ).order_by(func.count(GymVisits.id).desc()).all()
        
        bde_data = []
        for stat in bde_stats:
            total = safe_int(stat.total_visits or 0)
            converted = safe_int(stat.converted or 0)
            conversion_rate = (safe_float(converted) / safe_float(total) * 100.0) if total > 0 else 0.0
            avg_rating = safe_float(stat.avg_rating or 0)

            bde_data.append({
                "id": stat.id,
                "name": stat.name,
                "email": stat.email,
                "employee_id": stat.emp_id,
                "total_assigned": total,
                "pending_count": safe_int(stat.pending or 0),
                "followup_count": safe_int(stat.followup or 0),
                "converted_count": converted,
                "rejected_count": safe_int(stat.rejected or 0),
                "scheduled": safe_int(stat.scheduled or 0),
                "conversion_rate": round(conversion_rate, 2),
                "avg_rating": round(avg_rating, 2),
                "efficiency_score": round(conversion_rate + (avg_rating * 10.0), 2)
            })
        
        return {
            "status": 200,
            "data": {
                "bde_stats": bde_data,
                "total_bdes": len(bde_data)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/location")
async def get_location_stats(
    user_id: int,
    role: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    state: Optional[str] = None,
    city: Optional[str] = None,
    area: Optional[str] = None,
    pincode: Optional[str] = None,
    bde_id: Optional[int] = None,
    status: Optional[str] = None,
    db: Session = Depends(get_db)
):
    try:
        user_profile = get_user_profile_by_role(user_id, role, db)
        if not user_profile:
            raise HTTPException(status_code=404, detail="User not found")
            
        start_date, end_date = get_date_range(start_date, end_date)
        
        visits_query = db.query(GymVisits, GymDatabase).join(
            GymDatabase, GymVisits.gym_id == GymDatabase.id
        )
        
        if role == "BDM":
            team_executives_query = select(Executives.id).where(
                Executives.manager_id == user_id
            )
            visits_query = visits_query.filter(GymVisits.user_id.in_(team_executives_query))
        else:
            visits_query = visits_query.filter(GymVisits.user_id == user_id)
        
        visits_query = apply_date_filter(visits_query, start_date, end_date)
        
        if bde_id:
            visits_query = visits_query.filter(GymVisits.user_id == bde_id)
        
        if status:
            visits_query = visits_query.filter(GymVisits.final_status == status)
        
        if state:
            visits_query = visits_query.filter(GymDatabase.state == state)
        if city:
            visits_query = visits_query.filter(GymDatabase.city == city)
        if area:
            visits_query = visits_query.filter(GymDatabase.area == area)
        if pincode:
            visits_query = visits_query.filter(GymDatabase.pincode == pincode)
        
        all_visits_data = visits_query.all()
        
        location_groups = {}
        
        for visit, gym in all_visits_data:
            location_key = f"{gym.state or 'Unknown'}|{gym.city or 'Unknown'}|{gym.area or 'Unknown'}|{gym.pincode or 'Unknown'}"
            
            if location_key not in location_groups:
                location_groups[location_key] = {
                    'state': gym.state or 'Unknown',
                    'city': gym.city or 'Unknown',
                    'area': gym.area or 'Unknown',
                    'pincode': gym.pincode or 'Unknown',
                    'visits': [],
                    'unique_gyms': set()
                }
            
            location_groups[location_key]['visits'].append(visit)
            location_groups[location_key]['unique_gyms'].add(visit.gym_name)
        
        location_stats = []
        
        for location_key, location_data in location_groups.items():
            visits = location_data['visits']
            unique_gyms = location_data['unique_gyms']
            
            total_visits = len(visits)
            total_gyms = len(unique_gyms)

            pending_count = sum(1 for v in visits if v.final_status == 'pending')
            followup_count = sum(1 for v in visits if v.final_status == 'followup')
            converted_count = sum(1 for v in visits if v.final_status == 'converted')
            rejected_count = sum(1 for v in visits if v.final_status == 'rejected')
            scheduled_count = sum(1 for v in visits if v.final_status == 'scheduled')
            completed_visits = sum(1 for v in visits if v.completed)

            converted_gyms = len(set(v.gym_name for v in visits if v.final_status == 'converted'))
            followup_gyms = len(set(v.gym_name for v in visits if v.final_status == 'followup'))
            rejected_gyms = len(set(v.gym_name for v in visits if v.final_status == 'rejected'))
            pending_gyms = len(set(v.gym_name for v in visits if v.final_status == 'pending'))
            
            visit_conversion_rate = (safe_float(converted_count) / safe_float(total_visits) * 100.0) if total_visits > 0 else 0.0
            gym_conversion_rate = (safe_float(converted_gyms) / safe_float(total_gyms) * 100.0) if total_gyms > 0 else 0.0
            completion_rate = (safe_float(completed_visits) / safe_float(total_visits) * 100.0) if total_visits > 0 else 0.0
            
            avg_rating = safe_float(sum(v.overall_rating or 0 for v in visits)) / safe_float(total_visits) if total_visits > 0 else 0.0
            avg_members = safe_float(sum(v.total_member_count or 0 for v in visits)) / safe_float(total_visits) if total_visits > 0 else 0.0
            
            market_potential = min(safe_float(total_gyms) / 10.0, 10.0)  
            potential_score = (gym_conversion_rate * 0.7) + (market_potential * 0.3)
            
            location_name_parts = []
            if location_data['area'] and location_data['area'] != 'Unknown':
                location_name_parts.append(location_data['area'])
            if location_data['city'] and location_data['city'] != 'Unknown':
                location_name_parts.append(location_data['city'])
            if location_data['state'] and location_data['state'] != 'Unknown':
                location_name_parts.append(location_data['state'])
            if location_data['pincode'] and location_data['pincode'] != 'Unknown':
                location_name_parts.append(f"({location_data['pincode']})")
            
            location_name = ', '.join(location_name_parts) if location_name_parts else 'Unknown Location'
            
            location_stats.append({
                "location_id": location_key,
                "state": location_data['state'],
                "city": location_data['city'],
                "area": location_data['area'],
                "pincode": location_data['pincode'],
                "location_name": location_name,

                "total_assigned": total_visits,
                "completed_visits": completed_visits,
                "pending_count": pending_count,
                "followup_count": followup_count,
                "converted_count": converted_count,
                "rejected_count": rejected_count,
                "scheduled_count": scheduled_count,

                "total_gyms": total_gyms,
                "converted_gyms": converted_gyms,
                "followup_gyms": followup_gyms,
                "rejected_gyms": rejected_gyms,
                "pending_gyms": pending_gyms,
                "untouched_gyms": total_gyms - converted_gyms - rejected_gyms - followup_gyms - pending_gyms,

                "visit_conversion_rate": round(visit_conversion_rate, 2),
                "gym_conversion_rate": round(gym_conversion_rate, 2),
                "completion_rate": round(completion_rate, 2),
                "avg_rating": round(avg_rating, 2),
                "avg_members_per_gym": round(avg_members, 0),
                "potential_score": round(potential_score, 2),

                "market_penetration": round((safe_float(total_gyms) / 100.0) * 100.0, 2),
                "visits_per_gym": round(safe_float(total_visits) / safe_float(total_gyms), 2) if total_gyms > 0 else 0.0
            })
        
        location_stats.sort(key=lambda x: x['potential_score'], reverse=True)
        
        for i, location in enumerate(location_stats):
            location['rank'] = i + 1
        
        total_locations = len(location_stats)
        total_all_visits = sum(loc['total_assigned'] for loc in location_stats)
        total_all_gyms = sum(loc['total_gyms'] for loc in location_stats)
        total_converted_gyms = sum(loc['converted_gyms'] for loc in location_stats)
        
        overall_gym_conversion = (safe_float(total_converted_gyms) / safe_float(total_all_gyms) * 100.0) if total_all_gyms > 0 else 0.0
        
        top_locations = location_stats[:5] if len(location_stats) > 5 else location_stats
        
        return {
            "status": 200,
            "message": "Location stats fetched successfully",
            "data": {
                "location_stats": location_stats,
                "summary": {
                    "total_locations": total_locations,
                    "total_visits": total_all_visits,
                    "total_gyms": total_all_gyms,
                    "total_converted_gyms": total_converted_gyms,
                    "overall_gym_conversion_rate": round(overall_gym_conversion, 2),
                    "avg_visits_per_location": round(safe_float(total_all_visits) / safe_float(total_locations), 2) if total_locations > 0 else 0.0,
                    "avg_gyms_per_location": round(safe_float(total_all_gyms) / safe_float(total_locations), 2) if total_locations > 0 else 0.0
                },
                "top_locations": top_locations
            },
            "user_profile": user_profile
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/location-filters")
async def get_location_filters(
    user_id: int,
    role: str,
    db: Session = Depends(get_db)
):
    try:
        user_profile = get_user_profile_by_role(user_id, role, db)
        if not user_profile:
            raise HTTPException(status_code=404, detail="User not found")

        gym_query = db.query(GymDatabase).filter(GymDatabase.approval_status == 'approved')

        # if role == "BDM":
        #     team_exec_ids = db.query(Executives.id).filter(Executives.manager_id == user_id).subquery()
            
        #     assigned_gym_ids = db.query(GymAssignments.gym_id).filter(GymAssignments.executive_id.in_(team_exec_ids))
        #     gym_query = gym_query.filter(GymDatabase.id.in_(assigned_gym_ids))
        # elif role == "BDE":
        #     assigned_gym_ids = db.query(GymAssignments.gym_id).filter(GymAssignments.executive_id == user_id)
        #     gym_query = gym_query.filter(GymDatabase.id.in_(assigned_gym_ids))
        # else:
        #     pass

        gyms = gym_query.all()

        cities = set()
        states = set()
        areas = set()
        pincodes = set()

        for gym in gyms:
            if gym.city:
                cities.add(gym.city.strip())
            if gym.state:
                states.add(gym.state.strip())
            if gym.area:
                areas.add(gym.area.strip())
            if gym.pincode:
                pincodes.add(gym.pincode.strip())

        return {
            "status": 200,
            "message": "Location filters fetched successfully",
            "data": {
                "cities": sorted(list(cities)),
                "states": sorted(list(states)),
                "areas": sorted(list(areas)),
                "pincodes": sorted(list(pincodes)),
            },
            "user_profile": user_profile
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export")
async def export_stats(
    user_id: int,
    role: str,
    type: str,  
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    bde_id: Optional[int] = None,
    state: Optional[str] = None,
    city: Optional[str] = None,
    area: Optional[str] = None,
    pincode: Optional[str] = None,
    db: Session = Depends(get_db)
):
    try:
        user_profile = get_user_profile_by_role(user_id, role, db)
        if not user_profile:
            raise HTTPException(status_code=404, detail="User not found")
        
        if type == 'bde':
            response = await get_bde_stats(
                user_id=user_id,
                role=role,
                start_date=start_date,
                end_date=end_date,
                bde_id=bde_id,
                db=db
            )
        elif type == 'location':
            response = await get_location_stats(
                user_id=user_id,
                role=role,
                start_date=start_date,
                end_date=end_date,
                state=state,
                city=city,
                area=area,
                pincode=pincode,
                bde_id=bde_id,
                db=db
            )
        else:
            raise HTTPException(status_code=400, detail="Invalid export type. Use 'bde' or 'location'")
        
        response["export_info"] = {
            "exported_by": user_profile["name"],
            "export_type": type,
            "export_date": datetime.now().isoformat(),
            "filters_applied": {
                "start_date": start_date,
                "end_date": end_date,
                "bde_id": bde_id,
                "state": state,
                "city": city,
                "area": area,
                "pincode": pincode
            }
        }
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/location-gyms")
async def get_location_gyms(
    user_id: int,
    role: str,
    state: Optional[str] = None,
    city: Optional[str] = None,
    area: Optional[str] = None,
    pincode: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(get_db)
):
    try:
        user_profile = get_user_profile_by_role(user_id, role, db)
        if not user_profile:
            raise HTTPException(status_code=404, detail="User not found")
        
        start_date, end_date = get_date_range(start_date, end_date)
        
        gyms_query = db.query(
            GymDatabase.id,
            GymDatabase.gym_name,
            GymDatabase.area,
            GymDatabase.city,
            GymDatabase.state,
            GymDatabase.pincode,
            GymDatabase.contact_person,
            GymDatabase.contact_phone,
            GymDatabase.address,
            GymDatabase.operating_hours,
            GymAssignments.id.label('assignment_id'),
            GymAssignments.executive_id,
            GymAssignments.manager_id,
            GymAssignments.status.label('assignment_status'),
            GymAssignments.conversion_status.label('assignment_conversion_status'),
            GymAssignments.assigned_date,
            Executives.name.label('executive_name'),
            Executives.emp_id.label('executive_employee_id'),
            Managers.name.label('manager_name'),
            Managers.emp_id.label('manager_employee_id')
        ).outerjoin(
            GymAssignments, GymDatabase.id == GymAssignments.gym_id
        ).outerjoin(
            Executives, GymAssignments.executive_id == Executives.id
        ).outerjoin(
            Managers, GymAssignments.manager_id == Managers.id
        )
        
        if state:
            gyms_query = gyms_query.filter(GymDatabase.state == state)
        if city:
            gyms_query = gyms_query.filter(GymDatabase.city == city)
        if area:
            gyms_query = gyms_query.filter(GymDatabase.area == area)
        if pincode:
            gyms_query = gyms_query.filter(GymDatabase.pincode == pincode)
        
        if role == "BDM":
            gyms_query = gyms_query.filter(
                or_(
                    GymAssignments.manager_id == user_id,
                    GymAssignments.manager_id.is_(None)
                )
            )
        elif role == "BDE":
            gyms_query = gyms_query.filter(
                or_(
                    GymAssignments.executive_id == user_id,
                    GymAssignments.executive_id.is_(None)
                )
            )
        
        gyms_data = gyms_query.all()

        # === BATCH LOAD: Get all visits for all gyms at once (N+1 FIX) ===
        gym_ids = [g.id for g in gyms_data if g.id]
        all_visits_query = db.query(GymVisits).filter(GymVisits.gym_id.in_(gym_ids))
        all_visits_query = apply_date_filter(all_visits_query, start_date, end_date)

        # Apply role-based filter
        if role == "BDM":
            team_executive_ids = [e.id for e in db.query(Executives.id).filter(Executives.manager_id == user_id).all()]
            all_visits_query = all_visits_query.filter(GymVisits.user_id.in_(team_executive_ids))
        elif role == "BDE":
            all_visits_query = all_visits_query.filter(GymVisits.user_id == user_id)

        all_visits = all_visits_query.all()

        # Group visits by gym_id
        visits_by_gym = {}
        for visit in all_visits:
            if visit.gym_id not in visits_by_gym:
                visits_by_gym[visit.gym_id] = []
            visits_by_gym[visit.gym_id].append(visit)

        gym_visit_stats = {}

        for gym in gyms_data:
            if gym.id:
                # Get visits from pre-loaded map (was N+1 query)
                visits = visits_by_gym.get(gym.id, [])

                total_visits = len(visits)
                latest_visit = visits[-1] if visits else None

                status_counts = {}
                for visit in visits:
                    status = visit.final_status or 'pending'
                    status_counts[status] = status_counts.get(status, 0) + 1

                final_status = 'pending'
                if latest_visit:
                    final_status = latest_visit.final_status or 'pending'
                elif gym.assignment_conversion_status:
                    final_status = gym.assignment_conversion_status

                gym_visit_stats[gym.id] = {
                    'total_visits': total_visits,
                    'latest_visit': latest_visit,
                    'status_counts': status_counts,
                    'final_status': final_status
                }
        
        formatted_gyms = []
        for gym in gyms_data:
            visit_stats = gym_visit_stats.get(gym.id, {
                'total_visits': 0,
                'latest_visit': None,
                'status_counts': {},
                'final_status': 'pending'
            })
            
            location_parts = []
            if gym.area:
                location_parts.append(gym.area)
            if gym.city:
                location_parts.append(gym.city)
            if gym.state:
                location_parts.append(gym.state)
            if gym.pincode:
                location_parts.append(f"({gym.pincode})")
            
            location_string = ', '.join(location_parts)
            
            gym_data = {
                'id': gym.id,
                'gym_name': gym.gym_name,
                'area': gym.area,
                'city': gym.city,
                'state': gym.state,
                'pincode': gym.pincode,
                'contact_person': gym.contact_person,
                'contact_phone': gym.contact_phone,
                'address': gym.address,
                'location': location_string,
                'operating_hours': gym.operating_hours,
                
                'is_assigned': gym.assignment_id is not None,
                'assignment_id': gym.assignment_id,
                'assignment_status': gym.assignment_status,
                'assigned_date': gym.assigned_date.isoformat() if gym.assigned_date else None,
                
                'executive_id': gym.executive_id,
                'executive_name': gym.executive_name,
                'executive_employee_id': gym.executive_employee_id,
                'manager_id': gym.manager_id,
                'manager_name': gym.manager_name,
                'manager_employee_id': gym.manager_employee_id,
                
                'total_visits': visit_stats['total_visits'],
                'final_status': visit_stats['final_status'],
                'status_counts': visit_stats['status_counts'],
                'latest_visit_date': visit_stats['latest_visit'].created_at.isoformat() if visit_stats['latest_visit'] else None,
                
                'conversion_probability': 'high' if visit_stats['final_status'] == 'converted' else 'medium' if visit_stats['final_status'] == 'followup' else 'low'
            }
            
            formatted_gyms.append(gym_data)
        
        formatted_gyms.sort(key=lambda x: (not x['is_assigned'], x['gym_name']))
        
        location_summary = {
            'total_gyms': len(formatted_gyms),
            'assigned_gyms': len([g for g in formatted_gyms if g['is_assigned']]),
            'unassigned_gyms': len([g for g in formatted_gyms if not g['is_assigned']]),
            'converted_gyms': len([g for g in formatted_gyms if g['final_status'] == 'converted']),
            'followup_gyms': len([g for g in formatted_gyms if g['final_status'] == 'followup']),
            'pending_gyms': len([g for g in formatted_gyms if g['final_status'] == 'pending']),
            'rejected_gyms': len([g for g in formatted_gyms if g['final_status'] == 'rejected']),
            'total_visits': sum(g['total_visits'] for g in formatted_gyms)
        }
        
        location_identifier = {
            'state': state,
            'city': city,
            'area': area,
            'pincode': pincode
        }
        
        location_name_parts = []
        if area:
            location_name_parts.append(area)
        if city:
            location_name_parts.append(city)
        if state:
            location_name_parts.append(state)
        if pincode:
            location_name_parts.append(f"({pincode})")
        
        location_name = ', '.join(location_name_parts) if location_name_parts else 'Unknown Location'
        
        return {
            "status": 200,
            "message": "Location gyms fetched successfully",
            "data": {
                "location_info": {
                    "name": location_name,
                    "identifier": location_identifier,
                    "summary": location_summary
                },
                "gyms": formatted_gyms
            },
            "user_profile": user_profile
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))