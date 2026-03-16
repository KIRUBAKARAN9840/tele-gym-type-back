# # Backend Implementation for Gym Stats API
# from fastapi import APIRouter, Depends, HTTPException, Query
# from pydantic import BaseModel
# from sqlalchemy.orm import Session
# from sqlalchemy import func, or_, and_, desc, asc, case
# from typing import Optional, List
# from app.models.fittbot_models import Gym, GymOwner, Client, ClientFittbotAccess, TrainerProfile, FittbotPlans, GymPhoto
# from app.models.database import get_db
# import math

# router = APIRouter(prefix="/api/admin/gym-stats", tags=["AdminGymStats"])

# # Pydantic models for response
# class GymStatsResponse(BaseModel):
#     gym_id: int
#     gym_name: str
#     owner_name: str
#     contact_number: str
#     location: str
#     total_clients: int
#     active_clients: int
#     retention_rate: float
#     status: str
#     created_at: str
#     referal_id: Optional[str] = None
#     fittbot_verified: bool = False
    
#     class Config:
#         from_attributes = True

# class PaginatedGymsResponse(BaseModel):
#     gyms: List[GymStatsResponse]
#     total: int
#     page: int
#     limit: int
#     totalPages: int
#     hasNext: bool
#     hasPrev: bool
#     unverified_gyms_count: int

# class GymStatsSummary(BaseModel):
#     total_gyms: int
#     active_gyms: int
#     inactive_gyms: int
#     total_clients_across_all_gyms: int
#     average_retention_rate: float
#     unverified_gyms_count: int

# def apply_client_range_filter(query, client_count_column, range_value):
#     """Apply client count range filter to query"""
#     if range_value == "0-50":
#         return query.having(client_count_column >= 0, client_count_column <= 50)
#     elif range_value == "51-100":
#         return query.having(client_count_column >= 51, client_count_column <= 100)
#     elif range_value == "101-150":
#         return query.having(client_count_column >= 101, client_count_column <= 150)
#     elif range_value == "151-200":
#         return query.having(client_count_column >= 151, client_count_column <= 200)
#     elif range_value == ">200":
#         return query.having(client_count_column > 200)
#     return query

# def get_gym_stats_query(db: Session, verified_only=True):
#     """Base query for gym stats with client counts"""
#     query = db.query(
#         Gym.gym_id,
#         Gym.name.label('gym_name'),
#         Gym.location,
#         Gym.created_at,
#         Gym.referal_id,
#         Gym.fittbot_verified,
#         GymOwner.name.label('owner_name'),
#         GymOwner.contact_number,
#         # Count total clients (clients who have fittbot access)
#         func.count(Client.client_id).label('total_clients'),
#         # Count active clients based on ClientFittbotAccess.access_status
#         func.sum(case(
#             (ClientFittbotAccess.access_status == 'active', 1),
#             else_=0
#         )).label('active_clients'),
#         # Calculate retention rate using ClientFittbotAccess.access_status
#         case(
#             (func.count(Client.client_id) > 0,
#              func.round(
#                  (func.sum(case((ClientFittbotAccess.access_status == 'active', 1), else_=0)) * 100.0) / 
#                  func.count(Client.client_id), 2
#              )),
#             else_=0
#         ).label('retention_rate'),
#         # Determine gym status based on active clients (not just total clients)
#         case(
#             (func.sum(case((ClientFittbotAccess.access_status == 'active', 1), else_=0)) > 0, 'active'),
#             else_='inactive'
#         ).label('status')
#     ).outerjoin(
#         GymOwner, Gym.owner_id == GymOwner.owner_id
#     ).outerjoin(
#         Client, Gym.gym_id == Client.gym_id
#     ).outerjoin(
#         ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
#     )
    
#     if verified_only:
#         query = query.filter(Gym.fittbot_verified == True)
#     else:
#         query = query.filter(Gym.fittbot_verified == False)
    
#     return query.group_by(
#         Gym.gym_id, 
#         Gym.name, 
#         Gym.location, 
#         Gym.created_at,
#         Gym.referal_id,
#         Gym.fittbot_verified,
#         GymOwner.name, 
#         GymOwner.contact_number
#     )

# @router.get("/")
# async def get_gym_stats(
#     page: int = Query(1, ge=1, description="Page number"),
#     limit: int = Query(10, ge=1, le=100, description="Items per page"),
#     search: Optional[str] = Query(None, description="Search by gym name, owner, mobile, or location"),
#     status: Optional[str] = Query(None, description="Filter by gym status"),
#     total_clients_range: Optional[str] = Query(None, description="Filter by total clients range"),
#     active_clients_range: Optional[str] = Query(None, description="Filter by active clients range"),
#     sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order for created_at"),
#     db: Session = Depends(get_db)
# ):
#     try:
#         # Base query
#         query = get_gym_stats_query(db)
        
#         # Apply search filter
#         if search:
#             search_term = f"%{search.lower()}%"
#             query = query.having(
#                 or_(
#                     func.lower(Gym.name).like(search_term),
#                     func.lower(GymOwner.name).like(search_term),
#                     GymOwner.contact_number.like(search_term),
#                     func.lower(Gym.location).like(search_term)
#                 )
#             )
        
#         # Apply total clients range filter
#         if total_clients_range and total_clients_range != "all":
#             query = apply_client_range_filter(
#                 query, 
#                 func.count(Client.client_id), 
#                 total_clients_range
#             )
        
#         # Apply active clients range filter
#         if active_clients_range and active_clients_range != "all":
#             query = apply_client_range_filter(
#                 query, 
#                 func.sum(case((ClientFittbotAccess.access_status == 'active', 1), else_=0)), 
#                 active_clients_range
#             )
        
#         # Apply status filter - based on active clients, not total clients
#         if status and status != "all":
#             if status == "active":
#                 query = query.having(func.sum(case((ClientFittbotAccess.access_status == 'active', 1), else_=0)) > 0)
#             elif status == "inactive":
#                 query = query.having(func.sum(case((ClientFittbotAccess.access_status == 'active', 1), else_=0)) == 0)
        
#         # Apply sorting
#         if sort_order == "asc":
#             query = query.order_by(asc(Gym.created_at))
#         else:
#             query = query.order_by(desc(Gym.created_at))
        
#         # Get results as subquery for pagination (required for HAVING clauses)
#         subquery = query.subquery()
        
#         # Create new query from subquery for counting and pagination
#         main_query = db.query(subquery)
#         total_count = main_query.count()
        
#         # Apply pagination
#         offset = (page - 1) * limit
#         paginated_results = main_query.offset(offset).limit(limit).all()
        
#         # Get count of unverified gyms
#         unverified_count = db.query(Gym).filter(Gym.fittbot_verified == False).count()
        
#         # Convert to response format
#         gyms = []
#         for result in paginated_results:
#             gym_data = {
#                 "gym_id": result.gym_id,
#                 "gym_name": result.gym_name,
#                 "owner_name": result.owner_name or "N/A",
#                 "contact_number": result.contact_number or "N/A",
#                 "location": result.location or "N/A",
#                 "total_clients": result.total_clients or 0,
#                 "active_clients": result.active_clients or 0,
#                 "retention_rate": float(result.retention_rate or 0),
#                 "status": result.status,
#                 "created_at": result.created_at.isoformat() if result.created_at else None,
#                 "referal_id": result.referal_id,
#                 "fittbot_verified": result.fittbot_verified
#             }
#             gyms.append(gym_data)
        
#         # Calculate pagination info
#         total_pages = math.ceil(total_count / limit)
#         has_next = page < total_pages
#         has_prev = page > 1
        
#         return {
#             "success": True,
#             "data": {
#                 "gyms": gyms,
#                 "total": total_count,
#                 "page": page,
#                 "limit": limit,
#                 "totalPages": total_pages,
#                 "hasNext": has_next,
#                 "hasPrev": has_prev,
#                 "unverified_gyms_count": unverified_count
#             },
#             "message": "Gym statistics fetched successfully"
#         }
        
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error fetching gym statistics: {str(e)}")

# @router.get("/summary")
# async def get_gym_stats_summary(db: Session = Depends(get_db)):
#     """Get overall gym statistics summary"""
#     try:
#         # Basic gym counts
#         total_gyms = db.query(Gym).filter(Gym.fittbot_verified == True).count()
#         unverified_gyms_count = db.query(Gym).filter(Gym.fittbot_verified == False).count()
        
#         # Get gym stats for verified gyms
#         gym_stats = get_gym_stats_query(db, verified_only=True).all()
        
#         active_gyms = sum(1 for gym in gym_stats if (gym.active_clients or 0) > 0)
#         inactive_gyms = total_gyms - active_gyms
        
#         total_clients_across_all_gyms = sum(gym.total_clients for gym in gym_stats)
        
#         # Calculate average retention rate (only for gyms with active clients)
#         gyms_with_clients = [gym for gym in gym_stats if (gym.active_clients or 0) > 0]
#         average_retention_rate = (
#             sum(gym.retention_rate for gym in gyms_with_clients) / len(gyms_with_clients)
#             if gyms_with_clients else 0
#         )
        
#         return {
#             "success": True,
#             "data": {
#                 "total_gyms": total_gyms,
#                 "active_gyms": active_gyms,
#                 "inactive_gyms": inactive_gyms,
#                 "total_clients_across_all_gyms": total_clients_across_all_gyms,
#                 "average_retention_rate": round(average_retention_rate, 2),
#                 "unverified_gyms_count": unverified_gyms_count
#             },
#             "message": "Gym statistics summary fetched successfully"
#         }
        
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error fetching gym summary: {str(e)}")

# @router.get("/unverified")
# async def get_gym_clients(
#     gym_id: int,
#     page: int = Query(1, ge=1),
#     limit: int = Query(10, ge=1, le=100),
#     status: Optional[str] = Query(None, description="Filter by client access status"),
#     db: Session = Depends(get_db)
# ):
#     """Get clients for a specific gym"""
#     try:
#         gym = db.query(Gym).filter(Gym.gym_id == gym_id).first()
#         if not gym:
#             raise HTTPException(status_code=404, detail="Gym not found")
        
#         query = db.query(
#             Client.client_id,
#             Client.name,
#             Client.email,
#             Client.contact,
#             Client.created_at,
#             ClientFittbotAccess.access_status
#         ).filter(
#             Client.gym_id == gym_id
#         ).outerjoin(
#             ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
#         )
        
#         if status and status != "all":
#             query = query.filter(ClientFittbotAccess.access_status == status)
        
#         query = query.order_by(desc(Client.created_at))
        
#         total_count = query.count()
        
#         offset = (page - 1) * limit
#         clients = query.offset(offset).limit(limit).all()
        
#         clients_data = []
#         for client in clients:
#             client_data = {
#                 "client_id": client.client_id,
#                 "name": client.name,
#                 "email": client.email,
#                 "contact": client.contact,
#                 "access_status": client.access_status or "inactive",  
#                 "created_at": client.created_at.isoformat() if client.created_at else None
#             }
#             clients_data.append(client_data)
        
#         total_pages = math.ceil(total_count / limit)
        
#         return {
#             "success": True,
#             "data": {
#                 "gym_name": gym.name,
#                 "clients": clients_data,
#                 "total": total_count,
#                 "page": page,
#                 "limit": limit,
#                 "totalPages": total_pages,
#                 "hasNext": page < total_pages,
#                 "hasPrev": page > 1
#             },
#             "message": "Gym clients fetched successfully"
#         }
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error fetching gym clients: {str(e)}")

# @router.get("/unverified")
# async def get_unverified_gyms(
#     page: int = Query(1, ge=1, description="Page number"),
#     limit: int = Query(10, ge=1, le=100, description="Items per page"),
#     search: Optional[str] = Query(None, description="Search by gym name, owner, mobile, or location"),
#     status: Optional[str] = Query(None, description="Filter by gym status"),
#     total_clients_range: Optional[str] = Query(None, description="Filter by total clients range"),
#     active_clients_range: Optional[str] = Query(None, description="Filter by active clients range"),
#     sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order for created_at"),
#     db: Session = Depends(get_db)
# ):
#     """Get unverified gyms with search and pagination"""
#     try:
#         # Base query for unverified gyms
#         query = get_gym_stats_query(db, verified_only=False)
        
#         # Apply search filter
#         if search:
#             search_term = f"%{search.lower()}%"
#             query = query.having(
#                 or_(
#                     func.lower(Gym.name).like(search_term),
#                     func.lower(GymOwner.name).like(search_term),
#                     GymOwner.contact_number.like(search_term),
#                     func.lower(Gym.location).like(search_term)
#                 )
#             )
        
#         # Apply total clients range filter
#         if total_clients_range and total_clients_range != "all":
#             query = apply_client_range_filter(
#                 query, 
#                 func.count(Client.client_id), 
#                 total_clients_range
#             )
        
#         # Apply active clients range filter
#         if active_clients_range and active_clients_range != "all":
#             query = apply_client_range_filter(
#                 query, 
#                 func.sum(case((ClientFittbotAccess.access_status == 'active', 1), else_=0)), 
#                 active_clients_range
#             )
        
#         # Apply status filter - based on active clients, not total clients
#         if status and status != "all":
#             if status == "active":
#                 query = query.having(func.sum(case((ClientFittbotAccess.access_status == 'active', 1), else_=0)) > 0)
#             elif status == "inactive":
#                 query = query.having(func.sum(case((ClientFittbotAccess.access_status == 'active', 1), else_=0)) == 0)
        
#         # Apply sorting
#         if sort_order == "asc":
#             query = query.order_by(asc(Gym.created_at))
#         else:
#             query = query.order_by(desc(Gym.created_at))
        
#         # Get results as subquery for pagination (required for HAVING clauses)
#         subquery = query.subquery()
        
#         # Create new query from subquery for counting and pagination
#         main_query = db.query(subquery)
#         total_count = main_query.count()
        
#         # Apply pagination
#         offset = (page - 1) * limit
#         paginated_results = main_query.offset(offset).limit(limit).all()
        
#         # Convert to response format
#         gyms = []
#         for result in paginated_results:
#             gym_data = {
#                 "gym_id": result.gym_id,
#                 "gym_name": result.gym_name,
#                 "owner_name": result.owner_name or "N/A",
#                 "contact_number": result.contact_number or "N/A",
#                 "location": result.location or "N/A",
#                 "total_clients": result.total_clients or 0,
#                 "active_clients": result.active_clients or 0,
#                 "retention_rate": float(result.retention_rate or 0),
#                 "status": result.status,
#                 "created_at": result.created_at.isoformat() if result.created_at else None,
#                 "referal_id": result.referal_id,
#                 "fittbot_verified": result.fittbot_verified
#             }
#             gyms.append(gym_data)
        
#         # Calculate pagination info
#         total_pages = math.ceil(total_count / limit)
#         has_next = page < total_pages
#         has_prev = page > 1
        
#         return {
#             "success": True,
#             "data": {
#                 "gyms": gyms,
#                 "total": total_count,
#                 "page": page,
#                 "limit": limit,
#                 "totalPages": total_pages,
#                 "hasNext": has_next,
#                 "hasPrev": has_prev
#             },
#             "message": "Unverified gyms fetched successfully"
#         }
        
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error fetching unverified gyms: {str(e)}")

# @router.get("/{gym_id}/details")
# async def get_gym_details(gym_id: int, db: Session = Depends(get_db)):
#     """Get complete gym details including trainers, clients, plans, and photos"""
#     try:
#         # Get basic gym info
#         gym = db.query(
#             Gym.gym_id,
#             Gym.name,
#             Gym.location,
#             Gym.max_clients,
#             Gym.logo,
#             Gym.cover_pic,
#             Gym.subscription_start_date,
#             Gym.subscription_end_date,
#             Gym.created_at,
#             Gym.updated_at,
#             Gym.referal_id,
#             Gym.fittbot_verified,
#             GymOwner.name.label('owner_name'),
#             GymOwner.email.label('owner_email'),
#             GymOwner.contact_number.label('owner_contact'),
#             GymOwner.profile.label('owner_profile')
#         ).outerjoin(
#             GymOwner, Gym.owner_id == GymOwner.owner_id
#         ).filter(Gym.gym_id == gym_id).first()
        
#         if not gym:
#             raise HTTPException(status_code=404, detail="Gym not found")
        
#         # Get trainers
#         trainers = db.query(
#             TrainerProfile.profile_id,
#             TrainerProfile.trainer_id,
#             TrainerProfile.full_name,
#             TrainerProfile.email,
#             TrainerProfile.specializations,
#             TrainerProfile.experience,
#             TrainerProfile.certifications,
#             TrainerProfile.work_timings
#         ).filter(TrainerProfile.gym_id == gym_id).all()
        
#         # Get clients with their plans
#         clients = db.query(
#             Client.client_id,
#             Client.name,
#             Client.email,
#             Client.contact,
#             Client.profile,
#             Client.location,
#             Client.lifestyle,
#             Client.medical_issues,
#             Client.created_at,
#             ClientFittbotAccess.access_status,
#             FittbotPlans.plan_name,
#             FittbotPlans.duration,
#             FittbotPlans.image_url.label('plan_image_url')
#         ).filter(Client.gym_id == gym_id
#         ).outerjoin(
#             ClientFittbotAccess, Client.client_id == ClientFittbotAccess.client_id
#         ).outerjoin(
#             FittbotPlans, ClientFittbotAccess.plan_id == FittbotPlans.id
#         ).all()
        
#         # Get gym photos
#         photos = db.query(
#             GymPhoto.photo_id,
#             GymPhoto.area_type,
#             GymPhoto.image_url,
#             GymPhoto.file_name,
#             GymPhoto.file_size,
#             GymPhoto.created_at
#         ).filter(GymPhoto.gym_id == gym_id).all()
        
#         # Format response
#         gym_details = {
#             "gym_info": {
#                 "gym_id": gym.gym_id,
#                 "name": gym.name,
#                 "location": gym.location,
#                 "max_clients": gym.max_clients,
#                 "logo": gym.logo,
#                 "cover_pic": gym.cover_pic,
#                 "subscription_start_date": gym.subscription_start_date.isoformat() if gym.subscription_start_date else None,
#                 "subscription_end_date": gym.subscription_end_date.isoformat() if gym.subscription_end_date else None,
#                 "created_at": gym.created_at.isoformat() if gym.created_at else None,
#                 "updated_at": gym.updated_at.isoformat() if gym.updated_at else None,
#                 "referal_id": gym.referal_id,
#                 "fittbot_verified": gym.fittbot_verified,
#                 "owner_info": {
#                     "name": gym.owner_name,
#                     "email": gym.owner_email,
#                     "contact_number": gym.owner_contact,
#                     "profile": gym.owner_profile
#                 }
#             },
#             "trainers": [
#                 {
#                     "profile_id": trainer.profile_id,
#                     "trainer_id": trainer.trainer_id,
#                     "full_name": trainer.full_name,
#                     "email": trainer.email,
#                     "specializations": trainer.specializations,
#                     "experience": trainer.experience,
#                     "certifications": trainer.certifications,
#                     "work_timings": trainer.work_timings
#                 } for trainer in trainers
#             ],
#             "clients": [
#                 {
#                     "client_id": client.client_id,
#                     "name": client.name,
#                     "email": client.email,
#                     "contact": client.contact,
#                     "profile": client.profile,
#                     "location": client.location,
#                     "lifestyle": client.lifestyle,
#                     "medical_issues": client.medical_issues,
#                     "created_at": client.created_at.isoformat() if client.created_at else None,
#                     "access_status": client.access_status,
#                     "plan": {
#                         "plan_name": client.plan_name,
#                         "duration": client.duration,
#                         "image_url": client.plan_image_url
#                     } if client.plan_name else None
#                 } for client in clients
#             ],
#             "photos": [
#                 {
#                     "photo_id": photo.photo_id,
#                     "area_type": photo.area_type,
#                     "image_url": photo.image_url,
#                     "file_name": photo.file_name,
#                     "file_size": photo.file_size,
#                     "created_at": photo.created_at.isoformat() if photo.created_at else None
#                 } for photo in photos
#             ],
#             "stats": {
#                 "total_trainers": len(trainers),
#                 "total_clients": len(clients),
#                 "active_clients": len([c for c in clients if c.access_status == 'active']),
#                 "total_photos": len(photos)
#             }
#         }
        
#         return {
#             "success": True,
#             "data": gym_details,
#             "message": "Gym details fetched successfully"
#         }
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error fetching gym details: {str(e)}")



# class GymUpdateRequest(BaseModel):
#     name: Optional[str] = None
#     location: Optional[str] = None
#     max_clients: Optional[int] = None
#     referal_id: Optional[str] = None
#     fittbot_verified: Optional[bool] = None
#     owner_contact_number: Optional[str] = None

# @router.put("/{gym_id}")
# async def update_gym(
#     gym_id: int, 
#     update_data: GymUpdateRequest, 
#     db: Session = Depends(get_db)
# ):
#     """Update gym details including basic info and verification status"""
#     try:
#         # Check if gym exists
#         gym = db.query(Gym).filter(Gym.gym_id == gym_id).first()
#         if not gym:
#             raise HTTPException(status_code=404, detail="Gym not found")
        
#         # Update gym fields if provided
#         update_fields = {}
#         if update_data.name is not None:
#             update_fields[Gym.name] = update_data.name
#         if update_data.location is not None:
#             update_fields[Gym.location] = update_data.location
#         if update_data.max_clients is not None:
#             update_fields[Gym.max_clients] = update_data.max_clients
#         if update_data.referal_id is not None:
#             update_fields[Gym.referal_id] = update_data.referal_id
#         if update_data.fittbot_verified is not None:
#             update_fields[Gym.fittbot_verified] = update_data.fittbot_verified
        
#         # Update gym if there are fields to update
#         if update_fields:
#             db.query(Gym).filter(Gym.gym_id == gym_id).update(update_fields)
        
#         # Update owner contact number if provided
#         if update_data.owner_contact_number is not None and gym.owner_id:
#             db.query(GymOwner).filter(GymOwner.owner_id == gym.owner_id).update({
#                 GymOwner.contact_number: update_data.owner_contact_number
#             })
        
#         db.commit()
        
#         # Fetch updated gym details
#         updated_gym = db.query(
#             Gym.gym_id,
#             Gym.name,
#             Gym.location,
#             Gym.max_clients,
#             Gym.referal_id,
#             Gym.fittbot_verified,
#             Gym.updated_at,
#             GymOwner.name.label("owner_name"),
#             GymOwner.contact_number.label("owner_contact")
#         ).outerjoin(
#             GymOwner, Gym.owner_id == GymOwner.owner_id
#         ).filter(Gym.gym_id == gym_id).first()
        
#         return {
#             "success": True,
#             "data": {
#                 "gym_id": updated_gym.gym_id,
#                 "name": updated_gym.name,
#                 "location": updated_gym.location,
#                 "max_clients": updated_gym.max_clients,
#                 "referal_id": updated_gym.referal_id,
#                 "fittbot_verified": updated_gym.fittbot_verified,
#                 "updated_at": updated_gym.updated_at.isoformat() if updated_gym.updated_at else None,
#                 "owner_name": updated_gym.owner_name,
#                 "owner_contact": updated_gym.owner_contact
#             },
#             "message": "Gym details updated successfully"
#         }
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         db.rollback()
#         raise HTTPException(status_code=500, detail=f"Error updating gym: {str(e)}")
