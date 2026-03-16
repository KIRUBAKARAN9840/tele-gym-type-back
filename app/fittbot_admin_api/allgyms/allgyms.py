# Backend Implementation for All Gyms API
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, validator
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, and_, desc, asc, case, text
from typing import Optional, List, Dict, Any
import pandas as pd
import uuid
import io
import os
from datetime import datetime
import json
import math

from app.models.marketingmodels import GymDatabase, GymAssignments, Managers, Executives, GymVisits
from app.models.database import get_db
from app.models.fittbot_models import Gym, SessionSetting, FittbotPlans
from app.models.dailypass_models import DailyPassPricing

router = APIRouter(prefix="/api/admin/marketing", tags=["AdminMarketing"])

# Header mapping for professional headers to backend field names
HEADER_MAPPING = {
    'Gym Name *': 'gym_name',
    'Area/Locality *': 'area', 
    'City *': 'city',
    'State *': 'state',
    'Pincode *': 'pincode',
    'Contact Person': 'contact_person',
    'Contact Phone': 'contact_phone',
    'Full Address': 'address',
    'Operating Hours': 'operating_hours',
    'Additional Notes': 'submission_notes'
}

# Reverse mapping for error reporting
REVERSE_HEADER_MAPPING = {v: k for k, v in HEADER_MAPPING.items()}

# Pydantic models for response
class GymResponse(BaseModel):
    id: int
    gym_name: str
    area: str
    city: str
    state: str
    pincode: str
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    is_assigned: bool
    bdm_name: Optional[str] = None
    bde_name: Optional[str] = None
    conversion_status: Optional[str] = None
    assigned_date: Optional[str] = None
    created_at: str
    
    class Config:
        from_attributes = True

class PaginatedGymsResponse(BaseModel):
    gyms: List[GymResponse]
    total: int
    page: int
    limit: int
    totalPages: int
    hasNext: bool
    hasPrev: bool

class GymsSummary(BaseModel):
    total_gyms: int
    assigned_gyms: int
    unassigned_gyms: int
    converted_gyms: int
    pending_gyms: int
    rejected_gyms: int

class ManagerResponse(BaseModel):
    id: int
    name: str
    email: str
    role: str
    status: str
    
    class Config:
        from_attributes = True

class ExecutiveResponse(BaseModel):
    id: int
    name: str
    email: str
    manager_id: Optional[int] = None
    role: str
    status: str
    
    class Config:
        from_attributes = True

def get_gyms_query(db: Session):
    """Base query for gyms with assignments and team members"""
    return db.query(
        GymDatabase.id,
        GymDatabase.gym_name,
        GymDatabase.area,
        GymDatabase.city,
        GymDatabase.state,
        GymDatabase.pincode,
        GymDatabase.contact_person,
        GymDatabase.contact_phone,
        GymDatabase.created_at,
        # Assignment info
        case(
            (GymAssignments.status == 'assigned', True),
            else_=False
        ).label('is_assigned'),
        GymVisits.final_status,
        GymAssignments.assigned_date,
        # Manager (BDM) info
        Managers.name.label('bdm_name'),
        # Executive (BDE) info
        Executives.name.label('bde_name')
    ).outerjoin(
        GymAssignments, GymDatabase.id == GymAssignments.gym_id
    ).outerjoin(
        Managers, GymAssignments.manager_id == Managers.id
    ).outerjoin(
        Executives, GymAssignments.executive_id == Executives.id
    ).outerjoin(
        GymVisits, GymVisits.gym_id == GymDatabase.id
    )

@router.get("/gyms")
async def get_all_gyms(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by gym name, area, city, state, or pincode"),
    assignment: Optional[str] = Query(None, description="Filter by assignment status"),
    status: Optional[str] = Query(None, description="Filter by conversion status"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order for created_at"),
    db: Session = Depends(get_db)
):
    try:
        # Base query
        new_query = get_gyms_query(db)
        
        query = new_query.filter(GymDatabase.approval_status == "approved")

        if search:
            search_term = f"%{search.lower()}%"
            query = query.filter(
                or_(
                    func.lower(GymDatabase.gym_name).like(search_term),
                    func.lower(GymDatabase.area).like(search_term),
                    func.lower(GymDatabase.city).like(search_term),
                    func.lower(GymDatabase.state).like(search_term),
                    GymDatabase.pincode.like(search_term)
                )
            )
        
        # Apply assignment filter
        if assignment and assignment != "all":
            if assignment == "assigned":
                query = query.filter(GymAssignments.status == 'assigned')
            elif assignment == "not-assigned":
                query = query.filter(
                    or_(
                        GymAssignments.status.is_(None),
                        GymAssignments.status == 'not_assigned'
                    )
                )
        
        # Apply status filter
        if status and status != "all":
            if status == "na":
                # Show only unassigned gyms or gyms without conversion status
                query = query.filter(
                    or_(
                        GymAssignments.status.is_(None),
                        GymAssignments.status == 'not_assigned',
                        GymVisits.final_status.is_(None)
                    )
                )
            else:
                # Filter by specific conversion status for assigned gyms
                query = query.filter(
                    and_(
                        GymAssignments.status == 'assigned',
                        GymVisits.final_status == status
                    )
                )
        
        # Apply sorting
        if sort_order == "asc":
            query = query.order_by(asc(GymDatabase.created_at))
        else:
            query = query.order_by(desc(GymDatabase.created_at))
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * limit
        results = query.offset(offset).limit(limit).all()
        
        # Convert to response format
        gyms = []
        for result in results:
            # Check for plan types by referal_id
            referal_id = result.referal_id

            # Check if gym has session plans
            has_session_plans = db.query(SessionSetting).filter(
                SessionSetting.referal_id == referal_id
            ).first() is not None

            # Check if gym has membership plans (FittbotPlans)
            has_membership_plans = db.query(FittbotPlans).filter(
                FittbotPlans.referal_id == referal_id
            ).first() is not None

            # Check if gym has daily pass pricing
            has_daily_pass = db.query(DailyPassPricing).filter(
                DailyPassPricing.gym_id == referal_id
            ).first() is not None

            gym_data = {
                "id": result.id,
                "gym_name": result.gym_name or "N/A",
                "area": result.area or "N/A",
                "city": result.city or "N/A",
                "state": result.state or "N/A",
                "pincode": result.pincode or "N/A",
                "contact_person": result.contact_person,
                "contact_phone": result.contact_phone,
                "is_assigned": result.is_assigned or False,
                "bdm_name": result.bdm_name,
                "bde_name": result.bde_name,
                "conversion_status": result.final_status,
                "assigned_date": result.assigned_date.isoformat() if result.assigned_date else None,
                "created_at": result.created_at.isoformat() if result.created_at else None,
                "has_session_plans": has_session_plans,
                "has_membership_plans": has_membership_plans,
                "has_daily_pass": has_daily_pass,
            }
            gyms.append(gym_data)
        
        # Calculate pagination info
        total_pages = math.ceil(total_count / limit)
        has_next = page < total_pages
        has_prev = page > 1
        
        return {
            "success": True,
            "data": {
                "gyms": gyms,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            },
            "message": "Gyms fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching gyms: {str(e)}")

@router.get("/gyms/summary")
async def get_gyms_summary(db: Session = Depends(get_db)):
    """Get gyms statistics summary"""
    try:
        # Total gyms
        total_gyms = db.query(GymDatabase).count()
        
        # Assigned gyms
        assigned_gyms = db.query(GymAssignments).filter(
            GymAssignments.status == 'assigned'
        ).count()
        
        # Unassigned gyms
        unassigned_gyms = total_gyms - assigned_gyms
        
        # Converted gyms
        converted_gyms = db.query(GymAssignments).filter(
            and_(
                GymAssignments.status == 'assigned',
                GymAssignments.conversion_status == 'converted'
            )
        ).count()
        
        # Pending gyms
        pending_gyms = db.query(GymAssignments).filter(
            and_(
                GymAssignments.status == 'assigned',
                GymAssignments.conversion_status == 'pending'
            )
        ).count()
        
        # Rejected gyms
        rejected_gyms = db.query(GymAssignments).filter(
            and_(
                GymAssignments.status == 'assigned',
                GymAssignments.conversion_status == 'rejected'
            )
        ).count()
        
        return {
            "success": True,
            "data": {
                "total_gyms": total_gyms,
                "assigned_gyms": assigned_gyms,
                "unassigned_gyms": unassigned_gyms,
                "converted_gyms": converted_gyms,
                "pending_gyms": pending_gyms,
                "rejected_gyms": rejected_gyms
            },
            "message": "Gyms summary fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching gyms summary: {str(e)}")

@router.get("/managers")
async def get_managers(db: Session = Depends(get_db)):
    """Get all active managers (BDMs)"""
    try:
        managers = db.query(Managers).filter(
            Managers.status == 'active'
        ).all()
        
        managers_data = []
        for manager in managers:
            manager_data = {
                "id": manager.id,
                "name": manager.name,
                "email": manager.email,
                "role": manager.role,
                "status": manager.status
            }
            managers_data.append(manager_data)
        
        return {
            "success": True,
            "data": managers_data,
            "message": "Managers fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching managers: {str(e)}")

@router.get("/executives")
async def get_executives(
    manager_id: Optional[int] = Query(None, description="Filter by manager ID"),
    db: Session = Depends(get_db)
):
    """Get all active executives (BDEs), optionally filtered by manager"""
    try:
        query = db.query(Executives).filter(Executives.status == 'active')
        
        if manager_id:
            query = query.filter(Executives.manager_id == manager_id)
        
        executives = query.all()
        
        executives_data = []
        for executive in executives:
            executive_data = {
                "id": executive.id,
                "name": executive.name,
                "email": executive.email,
                "manager_id": executive.manager_id,
                "role": executive.role,
                "status": executive.status
            }
            executives_data.append(executive_data)
        
        return {
            "success": True,
            "data": executives_data,
            "message": "Executives fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching executives: {str(e)}")


@router.get("/gyms/sample-template")
async def download_sample_template():
    """Download sample Excel template for bulk upload"""
    try:
        # Create sample data with professional headers
        sample_data = [
            {
                'Gym Name *': 'Sample Fitness Center',
                'Area/Locality *': 'MG Road',
                'City *': 'Bangalore',
                'State *': 'Karnataka',
                'Pincode *': '560001',
                'Contact Person': 'John Doe',
                'Contact Phone': '9876543210',
                'Full Address': '123 MG Road, Bangalore',
                'Operating Hours': '[{"day":"everyday","startTime":"06:00","endTime":"22:00"}]',
                'Additional Notes': 'Sample gym for reference'
            },
            {
                'Gym Name *': 'Elite Gym & Spa',
                'Area/Locality *': 'Koramangala',
                'City *': 'Bangalore',
                'State *': 'Karnataka',
                'Pincode *': '560034',
                'Contact Person': 'Jane Smith',
                'Contact Phone': '9876543211',
                'Full Address': '456 Koramangala, Bangalore',
                'Operating Hours': '[{"day":"weekdays","startTime":"05:30","endTime":"23:00"},{"day":"weekends","startTime":"07:00","endTime":"21:00"}]',
                'Additional Notes': 'Premium fitness center'
            }
        ]
        
        # Create DataFrame
        df = pd.DataFrame(sample_data)
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Write sample data
            df.to_excel(writer, sheet_name='Sample Data', index=False)
            
            # Create instructions sheet with professional headers
            instructions_data = [
                ['Field Name', 'Required', 'Description', 'Example'],
                ['Gym Name *', 'Yes', 'Name of the gym', 'Gold\'s Gym'],
                ['Area/Locality *', 'Yes', 'Area/locality name', 'MG Road'],
                ['City *', 'Yes', 'City name', 'Bangalore'],
                ['State *', 'Yes', 'State name', 'Karnataka'],
                ['Pincode *', 'Yes', 'Pin code', '560001'],
                ['Contact Person', 'No', 'Contact person name', 'John Doe'],
                ['Contact Phone', 'No', 'Contact phone number', '9876543210'],
                ['Full Address', 'No', 'Complete address', '123 Main Street'],
                ['Operating Hours', 'No', 'Operating hours in JSON format', 'See sample data'],
                ['Additional Notes', 'No', 'Any relevant information', 'Additional details']
            ]
            
            instructions_df = pd.DataFrame(instructions_data[1:], columns=instructions_data[0])
            instructions_df.to_excel(writer, sheet_name='Instructions', index=False)
            
            # Add operating hours format sheet
            hours_format_data = [
                ['Format Type', 'JSON Example'],
                ['Everyday same hours', '[{"day":"everyday","startTime":"06:00","endTime":"22:00"}]'],
                ['Weekdays/Weekends different', '[{"day":"weekdays","startTime":"05:30","endTime":"23:00"},{"day":"weekends","startTime":"07:00","endTime":"21:00"}]'],
                ['Custom days', '[{"day":"custom","startTime":"06:00","endTime":"22:00","customDays":"Mon, Wed, Fri"}]']
            ]
            
            hours_df = pd.DataFrame(hours_format_data[1:], columns=hours_format_data[0])
            hours_df.to_excel(writer, sheet_name='Operating Hours Format', index=False)
        
        output.seek(0)
        excel_data = output.getvalue()
        
        # Return file using StreamingResponse with proper headers
        def iter_file():
            yield excel_data
            
        return StreamingResponse(
            iter_file(),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                "Content-Disposition": "attachment; filename=gym_upload_template.xlsx",
                "Content-Length": str(len(excel_data))
            }
        )
        
    except ImportError as e:
        raise HTTPException(
            status_code=500, 
            detail="Missing required dependencies. Please install pandas and openpyxl: pip install pandas openpyxl"
        )
    except Exception as e:
       
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")

@router.post("/gyms/bulk-upload")
async def bulk_upload_gyms(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """Bulk upload gyms from Excel file"""
    try:
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are allowed")
        
        # Read Excel file
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents), sheet_name='Sample Data')
        
        # Convert professional headers to backend field names
        df.rename(columns=HEADER_MAPPING, inplace=True)
        
        successful_uploads = 0
        failed_uploads = 0
        errors = []
        
        for index, row in df.iterrows():
            try:
                # Validate required fields
                required_fields = ['gym_name', 'area', 'city', 'state', 'pincode']
                missing_fields = []
                
                for field in required_fields:
                    if pd.isna(row[field]) or str(row[field]).strip() == '':
                        missing_fields.append(field)
                
                if missing_fields:
                    errors.append({
                        'row': index + 2,  # +2 because Excel rows start from 1 and we have header
                        'data': {REVERSE_HEADER_MAPPING.get(k, k): v for k, v in row.to_dict().items()},
                        'errors': [f"Missing required field: {REVERSE_HEADER_MAPPING.get(field, field)}" for field in missing_fields]
                    })
                    failed_uploads += 1
                    continue
                
                # Parse operating hours if provided
                operating_hours_json = None
                if not pd.isna(row.get('operating_hours')):
                    try:
                        operating_hours_data = json.loads(str(row['operating_hours']))
                        validated_hours = validate_operating_hours(operating_hours_data)
                        operating_hours_json = [hour.dict() for hour in validated_hours]
                    except Exception as e:
                        errors.append({
                            'row': index + 2,
                            'data': {REVERSE_HEADER_MAPPING.get(k, k): v for k, v in row.to_dict().items()},
                            'errors': [f"Invalid operating hours format: {str(e)}"]
                        })
                        failed_uploads += 1
                        continue
                
                # Check if gym already exists
                existing_gym = db.query(GymDatabase).filter(
                    and_(
                        func.lower(GymDatabase.gym_name) == func.lower(str(row['gym_name']).strip()),
                        func.lower(GymDatabase.area) == func.lower(str(row['area']).strip()),
                        func.lower(GymDatabase.city) == func.lower(str(row['city']).strip())
                    )
                ).first()
                
                if existing_gym:
                    errors.append({
                        'row': index + 2,
                        'data': {REVERSE_HEADER_MAPPING.get(k, k): v for k, v in row.to_dict().items()},
                        'errors': ['Gym already exists with same name, area, and city']
                    })
                    failed_uploads += 1
                    continue
                
                # Create new gym
                new_gym = GymDatabase(
                    gym_name=str(row['gym_name']).strip(),
                    area=str(row['area']).strip(),
                    city=str(row['city']).strip(),
                    state=str(row['state']).strip(),
                    pincode=str(row['pincode']).strip(),
                    contact_person=str(row['contact_person']).strip() if not pd.isna(row.get('contact_person')) else None,
                    contact_phone=str(row['contact_phone']).strip() if not pd.isna(row.get('contact_phone')) else None,
                    address=str(row['address']).strip() if not pd.isna(row.get('address')) else None,
                    operating_hours=operating_hours_json,
                    approval_status='approved',
                    submitter_type='manager',
                    submission_notes=str(row['submission_notes']).strip() if not pd.isna(row.get('submission_notes')) else None
                )
                
                db.add(new_gym)
                db.flush()
                
                # Generate referral ID
                new_gym.referal_id = generate_referral_id(new_gym.id)
                
                successful_uploads += 1
                
            except Exception as e:
                errors.append({
                    'row': index + 2,
                    'data': {REVERSE_HEADER_MAPPING.get(k, k): v for k, v in row.to_dict().items()},
                    'errors': [f"Unexpected error: {str(e)}"]
                })
                failed_uploads += 1
                continue
        
        if successful_uploads > 0:
            db.commit()
        else:
            db.rollback()
        
        return {
            "success": successful_uploads > 0,
            "message": f"Upload completed. {successful_uploads} successful, {failed_uploads} failed.",
            "total_rows": len(df),
            "successful_uploads": successful_uploads,
            "failed_uploads": failed_uploads,
            "errors": errors
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error processing bulk upload: {str(e)}")

@router.post("/gyms/download-errors")
async def download_errors(errors_data: Dict[str, Any]):
    """Convert validation errors to Excel and return for download"""
    try:
        errors = errors_data.get('errors', [])
        
        if not errors:
            raise HTTPException(status_code=400, detail="No errors to download")
        
        # Prepare error data for Excel
        error_rows = []
        for error in errors:
            row_data = error.get('data', {})
            row_data['row_number'] = error.get('row')
            row_data['errors'] = '; '.join(error.get('errors', []))
            error_rows.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(error_rows)
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Validation Errors', index=False)
        
        output.seek(0)
        
        return StreamingResponse(
            io.BytesIO(output.read()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": "attachment; filename=gym_upload_errors.xlsx"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating error file: {str(e)}")


@router.get("/gyms/pending-approvals/count")
async def get_pending_approvals_count(db: Session = Depends(get_db)):
    """Get count of gyms pending approval"""
    try:
        pending_count = db.query(GymDatabase).filter(
            GymDatabase.approval_status == 'pending'
        ).count()
        
        return {
            "success": True,
            "data": {
                "pending_count": pending_count
            },
            "message": "Pending approvals count fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching pending approvals count: {str(e)}")


@router.get("/gyms/pending-approvals")
async def get_pending_approvals(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by gym name, area, city, state, or pincode"),
    status: str = Query("all", description="Filter by approval status: pending, rejected, all"),
    sort_order: str = Query("desc", description="Sort order for created_at"),
    submitter_type: Optional[str] = Query(None, description="Filter by submitter type"),
    db: Session = Depends(get_db)
):
    """Get gyms pending approval with submitter details"""
    try:
        # Start with base query
        query = db.query(GymDatabase)
        
        # Filter by approval status first
        if status == "pending":
            query = query.filter(GymDatabase.approval_status == 'pending')
        elif status == "rejected":
            query = query.filter(GymDatabase.approval_status == 'rejected')
        elif status == "all":
            query = query.filter(GymDatabase.approval_status.in_(['pending', 'rejected']))
        else:
            # Default to pending and rejected if status is not recognized
            query = query.filter(GymDatabase.approval_status.in_(['pending', 'rejected']))
        
        # Apply search filter
        if search:
            search_term = f"%{search.lower()}%"
            query = query.filter(
                or_(
                    func.lower(GymDatabase.gym_name).like(search_term),
                    func.lower(GymDatabase.area).like(search_term),
                    func.lower(GymDatabase.city).like(search_term),
                    func.lower(GymDatabase.state).like(search_term),
                    GymDatabase.pincode.like(search_term)
                )
            )
        
        # Filter by submitter type
        if submitter_type and submitter_type != "all":
            query = query.filter(GymDatabase.submitter_type == submitter_type)
        
        # Apply sorting with validation
        if sort_order.lower() == "asc":
            query = query.order_by(asc(GymDatabase.created_at))
        else:
            # Default to desc for any other value
            query = query.order_by(desc(GymDatabase.created_at))
        
        # Get total count before pagination
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * limit
        gyms_results = query.offset(offset).limit(limit).all()
        
        # Now fetch submitter details for each gym
        gyms_data = []
        for gym in gyms_results:
            submitter_info = {}
            
            # Get submitter details based on type
            if gym.submitter_type == 'manager' and gym.submitted_by_manager:
                manager = db.query(Managers).filter(Managers.id == gym.submitted_by_manager).first()
                if manager:
                    submitter_info = {
                        "name": manager.name,
                        "email": manager.email,
                        "contact": manager.contact
                    }
            elif gym.submitter_type == 'executive' and gym.submitted_by_executive:
                executive = db.query(Executives).filter(Executives.id == gym.submitted_by_executive).first()
                if executive:
                    submitter_info = {
                        "name": executive.name,
                        "email": executive.email,
                        "contact": executive.contact
                    }
            
            gym_data = {
                "id": gym.id,
                "gym_name": gym.gym_name or "N/A",
                "area": gym.area or "N/A",
                "city": gym.city or "N/A",
                "state": gym.state or "N/A",
                "pincode": gym.pincode or "N/A",
                "contact_person": gym.contact_person,
                "contact_phone": gym.contact_phone,
                "address": gym.address,
                "approval_status": gym.approval_status,
                "submitter_type": gym.submitter_type,
                "submitter_info": submitter_info,
                "submission_notes": gym.submission_notes,
                "rejection_reason": gym.rejection_reason,
                "created_at": gym.created_at.isoformat() if gym.created_at else None
            }
            gyms_data.append(gym_data)
        
        # Calculate pagination info
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 0
        has_next = page < total_pages
        has_prev = page > 1
        
        return {
            "success": True,
            "data": {
                "gyms": gyms_data,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": has_next,
                "hasPrev": has_prev
            },
            "message": "Pending approvals fetched successfully"
        }
        
    except Exception as e:
   
        raise HTTPException(status_code=500, detail=f"Error fetching pending approvals: {str(e)}")



class ApprovalRequest(BaseModel):
    admin_notes: Optional[str] = None


class RejectionRequest(BaseModel):
    rejection_reason: str
    admin_notes: Optional[str] = None
    
    @validator('rejection_reason')
    def validate_rejection_reason(cls, v):
        if not v or not v.strip():
            raise ValueError('Rejection reason is required')
        return v.strip()

@router.patch("/gyms/{gym_id}/approve")
async def approve_gym(
    gym_id: int,
    approval_data: ApprovalRequest,
    db: Session = Depends(get_db)
):
    """Approve a gym submission"""
    try:
        gym = db.query(GymDatabase).filter(GymDatabase.id == gym_id).first()
        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")
        
        if gym.approval_status == 'approved':
            raise HTTPException(status_code=400, detail="Gym is already approved")
        
        # Update gym status
        gym.approval_status = 'approved'
        gym.approval_date = datetime.now()
        if approval_data.admin_notes:
            gym.admin_notes = approval_data.admin_notes
        gym.rejection_reason = None  # Clear any previous rejection reason
        gym.updated_at = datetime.now()
        
        # Generate referral ID if not exists
        if not gym.referal_id:
            gym.referal_id = generate_referral_id(gym.id)
        
        db.commit()
        
        return {
            "success": True,
            "message": "Gym approved successfully",
            "data": {
                "id": gym.id,
                "gym_name": gym.gym_name,
                "approval_status": gym.approval_status,
                "referal_id": gym.referal_id
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error approving gym: {str(e)}")

@router.patch("/gyms/{gym_id}/reject")
async def reject_gym(
    gym_id: int,
    rejection_data: RejectionRequest,
    db: Session = Depends(get_db)
):
    """Reject a gym submission"""
    try:
        gym = db.query(GymDatabase).filter(GymDatabase.id == gym_id).first()
        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")
        
        if gym.approval_status == 'approved':
            raise HTTPException(status_code=400, detail="Cannot reject an approved gym")
        
        # Update gym status
        gym.approval_status = 'rejected'
        gym.rejection_reason = rejection_data.rejection_reason
        if rejection_data.admin_notes:
            gym.admin_notes = rejection_data.admin_notes
        gym.updated_at = datetime.now()
        
        db.commit()
        
        return {
            "success": True,
            "message": "Gym rejected successfully",
            "data": {
                "id": gym.id,
                "gym_name": gym.gym_name,
                "approval_status": gym.approval_status,
                "rejection_reason": gym.rejection_reason
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error rejecting gym: {str(e)}")

@router.get("/gyms/pending-approvals/summary")
async def get_pending_approvals_summary(db: Session = Depends(get_db)):
    """Get summary statistics for pending approvals"""
    try:
        # Total pending approvals
        total_pending = db.query(GymDatabase).filter(
            GymDatabase.approval_status == 'pending'
        ).count()
        
        # Total rejected
        total_rejected = db.query(GymDatabase).filter(
            GymDatabase.approval_status == 'rejected'
        ).count()
        
        # Pending by submitter type
        pending_by_managers = db.query(GymDatabase).filter(
            and_(
                GymDatabase.approval_status == 'pending',
                GymDatabase.submitter_type == 'manager'
            )
        ).count()
        
        pending_by_executives = db.query(GymDatabase).filter(
            and_(
                GymDatabase.approval_status == 'pending',
                GymDatabase.submitter_type == 'executive'
            )
        ).count()
        
        return {
            "success": True,
            "data": {
                "total_pending": total_pending,
                "total_rejected": total_rejected,
                "pending_by_managers": pending_by_managers,
                "pending_by_executives": pending_by_executives
            },
            "message": "Pending approvals summary fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching pending approvals summary: {str(e)}")

# Helper function for referral ID generation (add this if not already present)
def generate_referral_id(gym_id: int) -> str:
    """Generate referral ID in format FIT{gym_id}{uuid4_short}"""
    import uuid
    return f"FIT{gym_id}{str(uuid.uuid4())[:4]}"


@router.patch("/gyms/{gym_id}/assignment")
async def update_gym_assignment(
    gym_id: int,
    assignment_data: dict,
    db: Session = Depends(get_db)
):
    """Update gym assignment"""
    try:
        # Check if gym exists
        gym = db.query(GymDatabase).filter(GymDatabase.id == gym_id).first()
        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")
        
        # Check if assignment already exists
        assignment = db.query(GymAssignments).filter(
            GymAssignments.gym_id == gym_id
        ).first()
        
        if assignment:
            # Update existing assignment
            assignment.manager_id = assignment_data.get("manager_id")
            assignment.executive_id = assignment_data.get("executive_id")
            assignment.status = assignment_data.get("status", "assigned")
            assignment.conversion_status = assignment_data.get("conversion_status", "pending")
            assignment.assigned_date = assignment_data.get("assigned_date")
            assignment.updated_at = func.now()
        else:
            # Create new assignment
            assignment = GymAssignments(
                gym_id=gym_id,
                manager_id=assignment_data.get("manager_id"),
                executive_id=assignment_data.get("executive_id"),
                status=assignment_data.get("status", "assigned"),
                conversion_status=assignment_data.get("conversion_status", "pending"),
                assigned_date=assignment_data.get("assigned_date")
            )
            db.add(assignment)
        
        db.commit()
        
        return {
            "success": True,
            "message": "Gym assignment updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating gym assignment: {str(e)}")


@router.get("/gyms/locations")
async def get_gym_locations(db: Session = Depends(get_db)):
    """Get unique gym locations for filters"""
    try:
        # Get unique cities and states
        cities = db.query(GymDatabase.city).filter(
            GymDatabase.city.isnot(None)
        ).distinct().all()
        
        states = db.query(GymDatabase.state).filter(
            GymDatabase.state.isnot(None)
        ).distinct().all()
        
        locations_data = {
            "cities": [city[0] for city in cities if city[0]],
            "states": [state[0] for state in states if state[0]]
        }
        
        return {
            "success": True,
            "data": locations_data,
            "message": "Gym locations fetched successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching gym locations: {str(e)}")



class OperatingHour(BaseModel):
    day: str  # everyday, weekdays, weekends, custom
    startTime: str  # HH:MM format
    endTime: str  # HH:MM format
    customDays: Optional[str] = None  # for custom days like "Mon, Wed, Fri"

class GymCreateRequest(BaseModel):
    gym_name: str
    area: str
    city: str
    state: str
    pincode: str
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    address: Optional[str] = None
    operating_hours: Optional[List[OperatingHour]] = []
    submission_notes: Optional[str] = None
    
    @validator('gym_name', 'area', 'city', 'state', 'pincode')
    def validate_required_fields(cls, v):
        if not v or not v.strip():
            raise ValueError('This field is required')
        return v.strip()

class GymUpdateRequest(BaseModel):
    gym_name: Optional[str] = None
    area: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    address: Optional[str] = None
    operating_hours: Optional[List[OperatingHour]] = []
    submission_notes: Optional[str] = None

class BulkUploadResponse(BaseModel):
    success: bool
    message: str
    total_rows: int
    successful_uploads: int
    failed_uploads: int
    errors: List[Dict[str, Any]] = []

def generate_referral_id(gym_id: int) -> str:
    """Generate referral ID in format FIT{gym_id}{uuid4_short}"""
    return f"FIT{gym_id}{str(uuid.uuid4())[:4]}"

def validate_operating_hours(hours_data: List[Dict]) -> List[OperatingHour]:
    """Validate and convert operating hours data"""
    validated_hours = []
    for hour in hours_data:
        try:
            validated_hour = OperatingHour(
                day=hour.get('day', 'everyday'),
                startTime=hour.get('startTime', ''),
                endTime=hour.get('endTime', ''),
                customDays=hour.get('customDays', None)
            )
            validated_hours.append(validated_hour)
        except Exception as e:
            raise ValueError(f"Invalid operating hours format: {str(e)}")
    return validated_hours

@router.post("/gyms")
async def create_gym(gym_data: GymCreateRequest, db: Session = Depends(get_db)):
    """Create a new gym"""
    try:
        # Convert operating hours to JSON format
        operating_hours_json = None
        if gym_data.operating_hours:
            operating_hours_json = [hour.dict() for hour in gym_data.operating_hours]
        
        # Create new gym
        new_gym = GymDatabase(
            gym_name=gym_data.gym_name,
            area=gym_data.area,
            city=gym_data.city,
            state=gym_data.state,
            pincode=gym_data.pincode,
            contact_person=gym_data.contact_person,
            contact_phone=gym_data.contact_phone,
            address=gym_data.address,
            operating_hours=operating_hours_json,
            approval_status='approved',  # Auto-approve admin created gyms
            submitter_type='manager',  # Assuming admin creates as manager
            submission_notes=gym_data.submission_notes
        )
        
        db.add(new_gym)
        db.flush()  # Flush to get the ID
        
        # Generate and set referral ID
        new_gym.referal_id = generate_referral_id(new_gym.id)
        
        db.commit()
        db.refresh(new_gym)
        
        return {
            "success": True,
            "message": "Gym created successfully",
            "data": {
                "id": new_gym.id,
                "gym_name": new_gym.gym_name,
                "referal_id": new_gym.referal_id
            }
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error creating gym: {str(e)}")

@router.put("/gyms/{gym_id}")
async def update_gym(gym_id: int, gym_data: GymUpdateRequest, db: Session = Depends(get_db)):
    """Update an existing gym"""
    try:
        gym = db.query(GymDatabase).filter(GymDatabase.id == gym_id).first()
        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")
        
        # Update fields that are provided
        update_data = gym_data.dict(exclude_unset=True)
        
        if 'operating_hours' in update_data and update_data['operating_hours']:
            # Convert operating hours to JSON format
            operating_hours_json = [hour for hour in update_data['operating_hours']]
            update_data['operating_hours'] = operating_hours_json
        
        for field, value in update_data.items():
            if hasattr(gym, field):
                setattr(gym, field, value)
        
        gym.updated_at = datetime.now()
        
        db.commit()
        db.refresh(gym)
        
        return {
            "success": True,
            "message": "Gym updated successfully",
            "data": {
                "id": gym.id,
                "gym_name": gym.gym_name
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Error updating gym: {str(e)}")

@router.patch("/gyms/{gym_id}/status")
async def update_gym_status(
    gym_id: int,
    status_data: dict,
    db: Session = Depends(get_db)
):
    """Update gym conversion status"""
    try:
        assignment = db.query(GymAssignments).filter(
            GymAssignments.gym_id == gym_id
        ).first()
        
        if not assignment:
            raise HTTPException(status_code=404, detail="Gym assignment not found")
        
        assignment.conversion_status = status_data.get("status")
        assignment.updated_at = func.now()
        
        db.commit()
        
        return {
            "success": True,
            "message": "Gym status updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error updating gym status: {str(e)}")

@router.delete("/gyms/{gym_id}")
async def delete_gym(gym_id: int, db: Session = Depends(get_db)):
    """Delete a gym (soft delete by setting status)"""
    try:
        gym = db.query(GymDatabase).filter(GymDatabase.id == gym_id).first()
        if not gym:
            raise HTTPException(status_code=404, detail="Gym not found")
        
        # Check if gym has any visits or assignments
        has_visits = db.query(GymVisits).filter(GymVisits.gym_id == gym_id).first()
        has_assignments = db.query(GymAssignments).filter(GymAssignments.gym_id == gym_id).first()
        
        if has_visits or has_assignments:
            raise HTTPException(
                status_code=400, 
                detail="Cannot delete gym that has visits or assignments. Please remove all related data first."
            )
        
        db.delete(gym)
        db.commit()
        
        return {
            "success": True,
            "message": "Gym deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting gym: {str(e)}")
    



@router.get("/gyms/{gym_id}")
async def get_gym_by_id(gym_id: int, db: Session = Depends(get_db)):
    """Get specific gym details by ID"""
    try:
        result = get_gyms_query(db).filter(GymDatabase.id == gym_id).first()
        
        if not result:
            raise HTTPException(status_code=404, detail="Gym not found")
        
        gym_data = {
            "id": result.id,
            "gym_name": result.gym_name or "N/A",
            "area": result.area or "N/A",
            "city": result.city or "N/A",
            "state": result.state or "N/A",
            "pincode": result.pincode or "N/A",
            "contact_person": result.contact_person,
            "contact_phone": result.contact_phone,
            "is_assigned": result.is_assigned or False,
            "bdm_name": result.bdm_name,
            "bde_name": result.bde_name,
            "conversion_status": result.conversion_status,
            "assigned_date": result.assigned_date.isoformat() if result.assigned_date else None,
            "created_at": result.created_at.isoformat() if result.created_at else None
        }
        
        return {
            "success": True,
            "data": gym_data,
            "message": "Gym details fetched successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching gym details: {str(e)}")
