from fastapi import APIRouter, Depends, Query
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, cast, String, or_, desc, union, union_all, literal, over
from app.models.async_database import get_async_db
from app.models.telecaller_models import Telecaller, UserConversion, ClientCallFeedback
from app.models.fittbot_models import Client, Gym

router = APIRouter(prefix="/api/admin/user-conversion", tags=["AdminUserConversion"])


@router.get("/telecallers")
async def get_telecallers_with_conversion_count(
    db: AsyncSession = Depends(get_async_db)
):
   
    try:
        # Get all telecallers
        telecaller_stmt = select(
            Telecaller.id,
            Telecaller.name,
            Telecaller.mobile_number,
            Telecaller.status,
            Telecaller.verified,
            Telecaller.created_at
        ).order_by(Telecaller.created_at.desc())

        telecaller_result = await db.execute(telecaller_stmt)
        telecallers = telecaller_result.all()

        telecaller_list = []
        for telecaller in telecallers:
            # Count distinct converted client_ids from both UserConversion and ClientCallFeedback
            # Use ROW_NUMBER to get only the latest entry per client
            uc_clients = select(
                UserConversion.id.label('conversion_id'),
                cast(UserConversion.client_id, String).label('client_id'),
                UserConversion.converted_at,
                literal('user_conversion').label('source')
            ).where(UserConversion.telecaller_id == telecaller.id)

            ccf_clients = select(
                ClientCallFeedback.id.label('conversion_id'),
                cast(ClientCallFeedback.client_id, String).label('client_id'),
                ClientCallFeedback.created_at.label('converted_at'),
                literal('call_feedback').label('source')
            ).where(
                ClientCallFeedback.executive_id == telecaller.id,
                ClientCallFeedback.status == 'converted'
            )

            combined = union_all(uc_clients, ccf_clients).subquery()

            # Use window function to rank by converted_at for each client
            ranked = select(
                combined.c.client_id,
                func.row_number().over(
                    partition_by=combined.c.client_id,
                    order_by=desc(combined.c.converted_at)
                ).label('rn')
            ).subquery()

            # Count only the latest (rn = 1) for each client
            count_stmt = select(func.count()).select_from(ranked).where(ranked.c.rn == 1)
            count_result = await db.execute(count_stmt)
            total_converted = count_result.scalar() or 0

            telecaller_list.append({
                "id": telecaller.id,
                "name": telecaller.name,
                "mobile_number": telecaller.mobile_number,
                "total_converted": total_converted
            })

        return {
            "success": True,
            "data": {
                "telecallers": telecaller_list,
                "total": len(telecaller_list)
            },
            "message": "Telecallers with conversion count fetched successfully"
        }

    except Exception as e:
        raise Exception(f"Failed to fetch telecallers with conversion count: {str(e)}")


@router.get("/telecallers/{telecaller_id}/converted-clients")
async def get_telecaller_converted_clients(
    telecaller_id: int,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=10, ge=1, le=100),
    search: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_async_db)
):
  
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

        # Get converted clients from both UserConversion and ClientCallFeedback
        # Source 1: UserConversion table
        uc_sub = select(
            UserConversion.id.label('conversion_id'),
            cast(UserConversion.client_id, String).label('client_id'),
            UserConversion.purchased_plan,
            UserConversion.converted_at,
            literal('user_conversion').label('source')
        ).where(
            UserConversion.telecaller_id == telecaller_id
        )

        # Source 2: ClientCallFeedback with status='converted'
        ccf_sub = select(
            ClientCallFeedback.id.label('conversion_id'),
            cast(ClientCallFeedback.client_id, String).label('client_id'),
            literal(None).label('purchased_plan'),
            ClientCallFeedback.created_at.label('converted_at'),
            literal('call_feedback').label('source')
        ).where(
            ClientCallFeedback.executive_id == telecaller_id,
            ClientCallFeedback.status == 'converted'
        )

        combined = union_all(uc_sub, ccf_sub).subquery()

        # Create a window function to rank records by converted_at for each client
        # This helps us get only the latest entry for each client_id
        from sqlalchemy import over, Integer
        ranked_stmt = select(
            combined.c.conversion_id,
            combined.c.client_id,
            combined.c.purchased_plan,
            combined.c.converted_at,
            combined.c.source,
            func.row_number().over(
                partition_by=combined.c.client_id,
                order_by=desc(combined.c.converted_at)
            ).label('rn')
        ).subquery()

        # Filter to keep only the latest record for each client (rn = 1)
        latest_conversions = select(
            ranked_stmt.c.conversion_id,
            ranked_stmt.c.client_id,
            ranked_stmt.c.purchased_plan,
            ranked_stmt.c.converted_at,
            ranked_stmt.c.source
        ).where(ranked_stmt.c.rn == 1).subquery()

        conversion_stmt = select(
            latest_conversions.c.conversion_id,
            latest_conversions.c.client_id,
            latest_conversions.c.purchased_plan,
            latest_conversions.c.converted_at,
            latest_conversions.c.source,
            Client.name.label('client_name'),
            Client.contact.label('client_contact'),
            Client.email.label('client_email'),
            Client.created_at.label('client_created_at'),
            Gym.name.label('gym_name')
        ).outerjoin(
            Client,
            latest_conversions.c.client_id == cast(Client.client_id, String)
        ).outerjoin(
            Gym,
            Client.gym_id == Gym.gym_id
        )

        # Apply search filter if provided
        if search and search.strip():
            search_term = f"%{search.lower()}%"
            conversion_stmt = conversion_stmt.where(
                or_(
                    func.lower(Client.name).like(search_term),
                    Client.contact.like(search_term),
                    latest_conversions.c.client_id.like(search_term)
                )
            )

        # Get total count before pagination
        count_subquery = conversion_stmt.subquery()
        count_stmt = select(func.count()).select_from(count_subquery)
        count_result = await db.execute(count_stmt)
        total_count = count_result.scalar() or 0

        # Apply pagination
        offset = (page - 1) * limit
        conversion_stmt = conversion_stmt.order_by(desc(latest_conversions.c.converted_at)).offset(offset).limit(limit)

        conversion_result = await db.execute(conversion_stmt)
        conversions = conversion_result.all()

        client_list = []
        for conversion in conversions:
            client_list.append({
                "conversion_id": conversion.conversion_id,
                "client_id": conversion.client_id,
                "name": conversion.client_name,
                "contact": conversion.client_contact,
                "email": conversion.client_email,
                "gym_name": conversion.gym_name,
                "purchased_plan": conversion.purchased_plan,
                "converted_at": conversion.converted_at.isoformat() if conversion.converted_at else None,
                "created_at": conversion.client_created_at.isoformat() if conversion.client_created_at else None,
                "source": conversion.source
            })

        total_pages = (total_count + limit - 1) // limit

        return {
            "success": True,
            "data": {
                "telecaller": {
                    "id": telecaller.id,
                    "name": telecaller.name,
                    "mobile_number": telecaller.mobile_number
                },
                "clients": client_list,
                "total": total_count,
                "page": page,
                "limit": limit,
                "totalPages": total_pages,
                "hasNext": page < total_pages,
                "hasPrev": page > 1
            },
            "message": "Converted clients fetched successfully"
        }

    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to fetch converted clients: {str(e)}"
        }
