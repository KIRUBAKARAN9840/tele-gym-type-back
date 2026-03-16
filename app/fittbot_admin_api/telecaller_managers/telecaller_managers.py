from fastapi import APIRouter, Depends, Query
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, desc, or_
from app.models.async_database import get_async_db
from app.models.telecaller_models import (
    Manager, Telecaller, GymAssignment, GymCallLogs, GymDatabase
)
from app.fittbot_admin_api.auth.authentication import get_current_admin_from_cookie
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/telecaller-managers", tags=["AdminTelecallerManagers"])


@router.get("/list")
async def get_telecaller_managers(
    db: AsyncSession = Depends(get_async_db)
):
    
    try:
        # Get all managers from telecaller schema
        stmt = select(
            Manager.id,
            Manager.name,
            Manager.mobile_number,
            Manager.status,
            Manager.verified,
            Manager.created_at
        ).order_by(Manager.created_at.desc())

        result = await db.execute(stmt)
        managers = result.all()

        manager_list = []
        for manager in managers:
            # Count telecallers under this manager
            telecaller_count_stmt = select(func.count(Telecaller.id)).where(
                Telecaller.manager_id == manager.id
            )
            telecaller_count_result = await db.execute(telecaller_count_stmt)
            team_count = telecaller_count_result.scalar() or 0

            manager_list.append({
                "id": manager.id,
                "name": manager.name,
                "mobile_number": manager.mobile_number,
                "status": manager.status,
                "verified": manager.verified,
                "created_at": manager.created_at.isoformat() if manager.created_at else None,
                "team_count": team_count
            })

     

        return {
            "success": True,
            "data": {
                "managers": manager_list,
                "total": len(manager_list)
            },
            "message": "Telecaller managers fetched successfully"
        }

    except Exception as e:
        logger.error(f"[TELECALLER-MANAGERS] Error: {str(e)}")
        raise Exception(f"Failed to fetch telecaller managers: {str(e)}")


@router.get("/{manager_id}/telecallers")
async def get_manager_telecallers(
    manager_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get all telecallers under a specific manager.
    """
    try:
        # Verify manager exists
        manager_stmt = select(Manager).where(Manager.id == manager_id)
        manager_result = await db.execute(manager_stmt)
        manager = manager_result.scalar_one_or_none()

        if not manager:
            return {
                "success": False,
                "message": "Manager not found"
            }

        # Get all telecallers under this manager
        telecaller_stmt = select(
            Telecaller.id,
            Telecaller.name,
            Telecaller.mobile_number,
            Telecaller.status,
            Telecaller.verified,
            Telecaller.created_at
        ).where(
            Telecaller.manager_id == manager_id
        ).order_by(Telecaller.created_at.desc())

        telecaller_result = await db.execute(telecaller_stmt)
        telecallers = telecaller_result.all()

        telecaller_list = []
        for telecaller in telecallers:
            telecaller_list.append({
                "id": telecaller.id,
                "name": telecaller.name,
                "mobile_number": telecaller.mobile_number,
                "status": telecaller.status,
                "verified": telecaller.verified,
                "created_at": telecaller.created_at.isoformat() if telecaller.created_at else None
            })

        
        return {
            "success": True,
            "data": {
                "manager": {
                    "id": manager.id,
                    "name": manager.name,
                    "mobile_number": manager.mobile_number
                },
                "telecallers": telecaller_list,
                "total": len(telecaller_list)
            },
            "message": "Telecallers fetched successfully"
        }

    except Exception as e:
        logger.error(f"[MANAGER-TELECALLERS] Error: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to fetch telecallers: {str(e)}"
        }


@router.get("/{manager_id}/telecallers/{telecaller_id}/details")
async def get_telecaller_details(
    manager_id: int,
    telecaller_id: int,
    status: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=100),
    search: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get telecaller details with gym assignments categorized by status.
    Status tabs: pending, follow_up, converted, rejected, no_response, out_of_service
    """
    try:
        # Verify telecaller exists
        telecaller_stmt = select(Telecaller).where(Telecaller.id == telecaller_id)
        telecaller_result = await db.execute(telecaller_stmt)
        telecaller = telecaller_result.scalar_one_or_none()

        if not telecaller:
            return {
                "success": False,
                "message": "Telecaller not found"
            }

        # Get all active gym assignments for this telecaller
        assignment_stmt = select(
            GymAssignment.gym_id,
            GymAssignment.assigned_at
        ).where(
            and_(
                GymAssignment.telecaller_id == telecaller_id,
                GymAssignment.status == "active"
            )
        )

        assignment_result = await db.execute(assignment_stmt)
        assignments = assignment_result.all()

        # Get gym IDs from assignments
        assigned_gym_ids = [assign.gym_id for assign in assignments]

        if not assigned_gym_ids:
            return {
                "success": True,
                "data": {
                    "telecaller": {
                        "id": telecaller.id,
                        "name": telecaller.name,
                        "mobile_number": telecaller.mobile_number
                    },
                    "gyms": [],
                    "total": 0
                },
                "message": "Telecaller details fetched successfully"
            }

    

        # First, find the latest call log ID for each gym
        latest_log_ids_subquery = (
            select(
                GymCallLogs.gym_id,
                func.max(GymCallLogs.created_at).label("max_created_at")
            )
            .where(
                and_(
                    GymCallLogs.gym_id.in_(assigned_gym_ids),
                    GymCallLogs.telecaller_id == telecaller_id
                )
            )
            .group_by(GymCallLogs.gym_id)
            .subquery()
        )


        latest_logs_stmt = (
            select(
                GymCallLogs.id.label("log_id"),
                GymCallLogs.gym_id,
                GymCallLogs.telecaller_id,
                GymCallLogs.assigned_telecaller_id,
                GymCallLogs.call_status,
                GymCallLogs.remarks,
                GymCallLogs.follow_up_date,
                GymCallLogs.created_at
            )
            .join(
                latest_log_ids_subquery,
                and_(
                    GymCallLogs.gym_id == latest_log_ids_subquery.c.gym_id,
                    GymCallLogs.created_at == latest_log_ids_subquery.c.max_created_at
                )
            )
        )

        latest_logs_result = await db.execute(latest_logs_stmt)
        latest_logs = latest_logs_result.all()

        # Categorize gyms based on their latest call status
        categorized_gyms = {
            "pending": [],
            "follow_up": [],
            "converted": [],
            "rejected": [],
            "no_response": [],
            "out_of_service": []
        }

        # Track which gyms have call logs
        gyms_with_logs = set()

        for log in latest_logs:
            gym_id = log[1]  # gym_id
            gyms_with_logs.add(gym_id)

            call_status = log[4]  # call_status is at index 4
            assigned_telecaller_id = log[3]  # assigned_telecaller_id

        
            if call_status == "delegated" and assigned_telecaller_id:
                if assigned_telecaller_id != telecaller_id:
                    # This was delegated TO someone else, skip it here
                    continue
             
            status_map = {
                "follow_up": "follow_up",
                "follow_up_required": "follow_up",
                "converted": "converted",
                "rejected": "rejected",
                "no_response": "no_response",
                "out_of_service": "out_of_service",
                "pending": "follow_up",
                "contacted": "follow_up",
                "interested": "follow_up",
                "not_interested": "follow_up",
                "delegated": "follow_up"  # Condition 9: Delegated goes to Follow Up
            }

            target_tab = status_map.get(call_status, "pending")
            if target_tab in categorized_gyms:
                categorized_gyms[target_tab].append({
                    "log_id": log[0],  # log_id is at index 0
                    "gym_id": gym_id,
                    "telecaller_id": log[2],  # telecaller_id
                    "call_status": call_status,
                    "remarks": log[5],  # remarks
                    "follow_up_date": log[6].isoformat() if log[6] else None,  # follow_up_date
                    "created_at": log[7].isoformat() if log[7] else None  # created_at
                })

        # Add gyms without any call logs to pending
        gyms_without_logs = set(assigned_gym_ids) - gyms_with_logs
        for gym_id in gyms_without_logs:
            categorized_gyms["pending"].append({
                "log_id": None,
                "gym_id": gym_id,
                "telecaller_id": telecaller_id,
                "call_status": "pending",
                "remarks": None,
                "follow_up_date": None,
                "created_at": None
            })

        # Get gym details for all gyms
        all_gym_ids = list(assigned_gym_ids)
        gym_details_stmt = select(
            GymDatabase.id,
            GymDatabase.gym_name,
            GymDatabase.contact_person,
            GymDatabase.contact_phone,
            GymDatabase.area,
            GymDatabase.city,
            GymDatabase.state,
            GymDatabase.address
        ).where(GymDatabase.id.in_(all_gym_ids))

        gym_details_result = await db.execute(gym_details_stmt)
        gym_details_list = gym_details_result.all()

        # Create a map of gym_id to gym details
        gym_details_map = {}
        for gym_detail in gym_details_list:
            gym_details_map[gym_detail.id] = {
                "id": gym_detail.id,
                "gym_name": gym_detail.gym_name,
                "contact_person": gym_detail.contact_person,
                "contact_phone": gym_detail.contact_phone,
                "area": gym_detail.area,
                "city": gym_detail.city,
                "state": gym_detail.state,
                "address": gym_detail.address
            }

        # Attach gym details to each categorized gym
        final_result = {
            "pending": [],
            "follow_up": [],
            "converted": [],
            "rejected": [],
            "no_response": [],
            "out_of_service": []
        }

        for tab, gyms in categorized_gyms.items():
            for gym_data in gyms:
                gym_info = gym_details_map.get(gym_data["gym_id"])
                # Only include gyms that exist in gym_database
                if gym_info:
                    final_result[tab].append({
                        **gym_data,
                        "gym_details": gym_info
                    })
                else:
                    logger.warning(f"[TELECALLER-DETAILS] Gym {gym_data['gym_id']} not found in gym_database")

        # Apply search filter if provided
        if search and search.strip():
            search_lower = search.strip().lower()
            for tab in final_result:
                filtered = []
                for gym_entry in final_result[tab]:
                    gym_details = gym_entry.get("gym_details", {})
                    gym_name = (gym_details.get("gym_name") or "").lower()
                    contact_person = (gym_details.get("contact_person") or "").lower()
                    contact_phone = gym_details.get("contact_phone") or ""
                    area = (gym_details.get("area") or "").lower()
                    city = (gym_details.get("city") or "").lower()

                    if (
                        search_lower in gym_name or
                        search_lower in contact_person or
                        search in contact_phone or
                        search_lower in area or
                        search_lower in city
                    ):
                        filtered.append(gym_entry)
                final_result[tab] = filtered


        if status and status in final_result:

            filtered_gyms = final_result[status]
        else:

            filtered_gyms = []
            for tab_gyms in final_result.values():
                filtered_gyms.extend(tab_gyms)

        # Apply pagination
        total_count = len(filtered_gyms)
        total_pages = (total_count + limit - 1) // limit
        offset = (page - 1) * limit

        paginated_gyms = filtered_gyms[offset:offset + limit]

        return {
            "success": True,
            "data": {
                "telecaller": {
                    "id": telecaller.id,
                    "name": telecaller.name,
                    "mobile_number": telecaller.mobile_number
                },
                "gyms": paginated_gyms,
                "total": total_count,
                "pagination": {
                    "page": page,
                    "limit": limit,
                    "total": total_count,
                    "totalPages": total_pages,
                    "hasNext": page < total_pages,
                    "hasPrev": page > 1
                },
                "counts": {
                    "pending": len(final_result["pending"]),
                    "follow_up": len(final_result["follow_up"]),
                    "converted": len(final_result["converted"]),
                    "rejected": len(final_result["rejected"]),
                    "no_response": len(final_result["no_response"]),
                    "out_of_service": len(final_result["out_of_service"])
                }
            },
            "message": "Telecaller details fetched successfully"
        }

    except Exception as e:
        logger.error(f"[TELECALLER-DETAILS] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Failed to fetch telecaller details: {str(e)}"
        }


@router.get("/gym-call-logs/{gym_id}")
async def get_gym_call_logs(
    gym_id: int,
    db: AsyncSession = Depends(get_async_db)
):
  
    try:
        # Fetch only the fields needed for display (plus id for React key)
        stmt = select(
            GymCallLogs.id,
            GymCallLogs.telecaller_id,
            GymCallLogs.call_status,
            GymCallLogs.remarks,
            GymCallLogs.follow_up_date,
            GymCallLogs.created_at,
            GymCallLogs.interest_level,
            GymCallLogs.total_members
        ).where(
            GymCallLogs.gym_id == gym_id
        ).order_by(GymCallLogs.created_at.desc())

        result = await db.execute(stmt)
        call_logs = result.all()

        # Get unique telecaller IDs for names
        telecaller_ids = list(set([log[1] for log in call_logs]))  # telecaller_id

        telecaller_map = {}
        if telecaller_ids:
            telecaller_stmt = select(
                Telecaller.id,
                Telecaller.name
            ).where(Telecaller.id.in_(telecaller_ids))
            telecaller_result = await db.execute(telecaller_stmt)
            for t in telecaller_result.all():
                telecaller_map[t.id] = t.name

        # Format response - only include displayed fields
        logs_list = []
        for log in call_logs:
            logs_list.append({
                "id": log[0],
                "telecaller_name": telecaller_map.get(log[1]) or "Unknown",
                "call_status": log[2],
                "remarks": log[3],
                "follow_up_date": log[4].isoformat() if log[4] else None,
                "created_at": log[5].isoformat() if log[5] else None,
                "interest_level": log[6],
                "total_members": log[7]
            })

        return {
            "success": True,
            "data": {
                "call_logs": logs_list,
                "total": len(logs_list)
            },
            "message": "Gym call logs fetched successfully"
        }

    except Exception as e:
        logger.error(f"[GYM-CALL-LOGS] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Failed to fetch gym call logs: {str(e)}"
        }
