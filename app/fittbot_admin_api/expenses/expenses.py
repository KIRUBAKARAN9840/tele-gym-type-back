# Expenses API - Optimized Async Implementation
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, date
from sqlalchemy import func, and_, or_, select, desc
from typing import Optional, List
from pydantic import BaseModel, Field
from decimal import Decimal

from app.models.async_database import get_async_db
from app.models.adminmodels import Expenses

router = APIRouter(prefix="/api/admin/expenses", tags=["AdminExpenses"])


# Pydantic Schemas
class ExpenseCreate(BaseModel):
    category: str = Field(..., description="operational or marketing")
    expense_type: str = Field(..., description="Type of expense")
    amount: float = Field(..., gt=0, description="Amount must be positive")
    expense_date: str = Field(..., description="Date in YYYY-MM-DD format")
    description: Optional[str] = Field(None, description="Optional description")


class ExpenseUpdate(BaseModel):
    category: Optional[str] = None
    expense_type: Optional[str] = None
    amount: Optional[float] = Field(None, gt=0)
    expense_date: Optional[str] = None
    description: Optional[str] = None


class ExpenseResponse(BaseModel):
    id: int
    category: str
    expense_type: str
    amount: float
    expense_date: str
    description: Optional[str]
    created_at: str
    updated_at: str


class ExpenseListResponse(BaseModel):
    success: bool
    data: List[ExpenseResponse]
    pagination: dict
    message: str


class ExpenseSummaryResponse(BaseModel):
    success: bool
    data: dict
    message: str


# Predefined expense types
OPERATIONAL_EXPENSE_TYPES = [
    "Rent / Lease",
    "Electricity",
    "Water",
    "Staff Salaries",
    "Maintenance & Repairs",
    "Cleaning & Housekeeping",
    "Internet & Utilities",
    "Software Subscriptions",
    "Equipment Servicing (AMC)",
    "Security Services",
    "Other"
]

MARKETING_EXPENSE_TYPES = [
    "Meta Platforms Ads",
    "Google Ads",
    "Influencer Marketing",
    "Offline Printing (Banners/Flyers)",
    "Event Sponsorship",
    "SEO Services",
    "Website Maintenance",
    "Video & Content Production",
    "Brand Photoshoot",
    "SMS & Email Marketing Campaigns",
    "Other"
]


@router.get("/types")
async def get_expense_types():
    """
    Get predefined expense types for both categories.
    """
    return {
        "success": True,
        "data": {
            "operational": OPERATIONAL_EXPENSE_TYPES,
            "marketing": MARKETING_EXPENSE_TYPES
        },
        "message": "Expense types fetched successfully"
    }


@router.get("/summary/overview")
async def get_expenses_summary(
    start_date: Optional[str] = Query(None, description="Filter by start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Filter by end date (YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="Page number for expense list"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page for expense list"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get expense overview with summary, types, and default operational expense list.
    This single API replaces the need to call /types and /expenses separately on initial load.
    Optimized with aggregated queries.
    NOTE: This route must be defined before /{expense_id} to avoid route conflicts
    """
    try:
        # Build base conditions
        conditions = []

        if start_date:
            try:
                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
                conditions.append(Expenses.expense_date >= start_date_obj)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start_date format")

        if end_date:
            try:
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
                conditions.append(Expenses.expense_date <= end_date_obj)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_date format")

        # Total expenses by category - Single aggregated query
        category_query = select(
            Expenses.category,
            func.coalesce(func.sum(Expenses.amount), 0).label("total")
        ).group_by(Expenses.category)

        if conditions:
            category_query = category_query.where(and_(*conditions))

        category_result = await db.execute(category_query)
        category_totals = {row.category: float(row.total) for row in category_result.all()}

        # Total expenses by type for operational - Single aggregated query
        operational_query = select(
            Expenses.expense_type,
            func.coalesce(func.sum(Expenses.amount), 0).label("total")
        ).where(Expenses.category == "operational")

        if conditions:
            operational_query = operational_query.where(and_(*conditions))

        operational_query = operational_query.group_by(Expenses.expense_type)
        operational_result = await db.execute(operational_query)
        operational_totals = {row.expense_type: float(row.total) for row in operational_result.all()}

        # Total expenses by type for marketing - Single aggregated query
        marketing_query = select(
            Expenses.expense_type,
            func.coalesce(func.sum(Expenses.amount), 0).label("total")
        ).where(Expenses.category == "marketing")

        if conditions:
            marketing_query = marketing_query.where(and_(*conditions))

        marketing_query = marketing_query.group_by(Expenses.expense_type)
        marketing_result = await db.execute(marketing_query)
        marketing_totals = {row.expense_type: float(row.total) for row in marketing_result.all()}

        # Grand total - Single query
        total_query = select(func.coalesce(func.sum(Expenses.amount), 0))
        if conditions:
            total_query = total_query.where(and_(*conditions))

        total_result = await db.execute(total_query)
        grand_total = float(total_result.scalar() or 0)

        # Count by category - Use separate count queries for better compatibility
        operational_count_query = select(func.count()).where(Expenses.category == "operational")
        marketing_count_query = select(func.count()).where(Expenses.category == "marketing")

        if conditions:
            operational_count_query = operational_count_query.where(and_(*conditions))
            marketing_count_query = marketing_count_query.where(and_(*conditions))

        operational_count_result = await db.execute(operational_count_query)
        marketing_count_result = await db.execute(marketing_count_query)
        operational_count = operational_count_result.scalar() or 0
        marketing_count = marketing_count_result.scalar() or 0

        # Fetch default operational expense list (paginated)
        list_query = select(Expenses).where(Expenses.category == "operational")

        # Apply date filters to the list query
        if conditions:
            list_query = list_query.where(and_(*conditions))

        # Get total count for pagination (operational only with date filters)
        list_count_query = select(func.count()).select_from(Expenses).where(Expenses.category == "operational")
        if conditions:
            list_count_query = list_count_query.where(and_(*conditions))

        list_count_result = await db.execute(list_count_query)
        list_total_count = list_count_result.scalar() or 0

        # Apply sorting (newest first)
        list_query = list_query.order_by(desc(Expenses.expense_date))

        # Apply pagination
        offset = (page - 1) * page_size
        list_query = list_query.offset(offset).limit(page_size)

        # Execute query
        list_result = await db.execute(list_query)
        expenses = list_result.scalars().all()

        # Format expense list response
        expense_data = []
        for expense in expenses:
            expense_data.append({
                "id": expense.id,
                "category": expense.category,
                "expense_type": expense.expense_type,
                "amount": expense.amount,
                "expense_date": expense.expense_date.isoformat(),
                "description": expense.description,
                "created_at": expense.created_at.isoformat() if expense.created_at else None,
                "updated_at": expense.updated_at.isoformat() if expense.updated_at else None
            })

        return {
            "success": True,
            "data": {
                # Summary data
                "grand_total": grand_total,
                "category_totals": {
                    "operational": category_totals.get("operational", 0),
                    "marketing": category_totals.get("marketing", 0)
                },
                "category_counts": {
                    "operational": int(operational_count),
                    "marketing": int(marketing_count)
                },
                "operational_breakdown": operational_totals,
                "marketing_breakdown": marketing_totals,

                # Expense types (predefined)
                "expense_types": {
                    "operational": OPERATIONAL_EXPENSE_TYPES,
                    "marketing": MARKETING_EXPENSE_TYPES
                },

                # Default operational expense list (paginated)
                "expenses": expense_data,
                "pagination": {
                    "total": list_total_count,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": (list_total_count + page_size - 1) // page_size
                },

                "filters": {
                    "start_date": start_date,
                    "end_date": end_date
                }
            },
            "message": "Expense summary fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"[EXPENSES] Error fetching summary: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("")
async def create_expense(
    expense: ExpenseCreate,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Create a new expense record.
    """
    try:
        # Validate category
        if expense.category not in ["operational", "marketing"]:
            raise HTTPException(status_code=400, detail="Invalid category. Must be 'operational' or 'marketing'")

        # Parse date
        try:
            expense_date_obj = datetime.strptime(expense.expense_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        # Create new expense
        new_expense = Expenses(
            category=expense.category,
            expense_type=expense.expense_type,
            amount=float(expense.amount),
            expense_date=expense_date_obj,
            description=expense.description
        )

        db.add(new_expense)
        await db.commit()
        await db.refresh(new_expense)

        return {
            "success": True,
            "data": {
                "id": new_expense.id,
                "category": new_expense.category,
                "expense_type": new_expense.expense_type,
                "amount": new_expense.amount,
                "expense_date": new_expense.expense_date.isoformat(),
                "description": new_expense.description,
                "created_at": new_expense.created_at.isoformat() if new_expense.created_at else None,
                "updated_at": new_expense.updated_at.isoformat() if new_expense.updated_at else None
            },
            "message": "Expense created successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        print(f"[EXPENSES] Error creating expense: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("")
async def get_expenses(
    category: Optional[str] = Query(None, description="Filter by category: operational or marketing"),
    expense_type: Optional[str] = Query(None, description="Filter by expense type"),
    start_date: Optional[str] = Query(None, description="Filter by start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Filter by end date (YYYY-MM-DD)"),
    search: Optional[str] = Query(None, description="Search in description and expense type"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page"),
    sort_by: str = Query("expense_date", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order: asc or desc"),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get expenses with filters, pagination, and sorting.
    All filters are applied at database level for optimal performance.
    """
    try:
        # Build base query
        query = select(Expenses)

        # Apply filters at database level
        conditions = []

        if category:
            if category not in ["operational", "marketing"]:
                raise HTTPException(status_code=400, detail="Invalid category")
            conditions.append(Expenses.category == category)

        if expense_type:
            conditions.append(Expenses.expense_type == expense_type)

        if start_date:
            try:
                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
                conditions.append(Expenses.expense_date >= start_date_obj)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD")

        if end_date:
            try:
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
                conditions.append(Expenses.expense_date <= end_date_obj)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD")

        if search:
            search_pattern = f"%{search}%"
            conditions.append(
                or_(
                    Expenses.expense_type.ilike(search_pattern),
                    Expenses.description.ilike(search_pattern)
                )
            )

        # Apply all conditions
        if conditions:
            query = query.where(and_(*conditions))

        # Get total count for pagination
        count_query = select(func.count()).select_from(Expenses)
        if conditions:
            count_query = count_query.where(and_(*conditions))

        count_result = await db.execute(count_query)
        total_count = count_result.scalar() or 0

        # Apply sorting
        sort_column = getattr(Expenses, sort_by, Expenses.expense_date)
        if sort_order == "desc":
            query = query.order_by(desc(sort_column))
        else:
            query = query.order_by(sort_column)

        # Apply pagination
        offset = (page - 1) * page_size
        query = query.offset(offset).limit(page_size)

        # Execute query
        result = await db.execute(query)
        expenses = result.scalars().all()

        # Format response
        expense_data = []
        for expense in expenses:
            expense_data.append({
                "id": expense.id,
                "category": expense.category,
                "expense_type": expense.expense_type,
                "amount": expense.amount,
                "expense_date": expense.expense_date.isoformat(),
                "description": expense.description,
                "created_at": expense.created_at.isoformat() if expense.created_at else None,
                "updated_at": expense.updated_at.isoformat() if expense.updated_at else None
            })

        return {
            "success": True,
            "data": expense_data,
            "pagination": {
                "total": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": (total_count + page_size - 1) // page_size
            },
            "message": "Expenses fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"[EXPENSES] Error fetching expenses: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{expense_id}")
async def get_expense(
    expense_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get a single expense by ID.
    """
    try:
        query = select(Expenses).where(Expenses.id == expense_id)
        result = await db.execute(query)
        expense = result.scalar_one_or_none()

        if not expense:
            raise HTTPException(status_code=404, detail="Expense not found")

        return {
            "success": True,
            "data": {
                "id": expense.id,
                "category": expense.category,
                "expense_type": expense.expense_type,
                "amount": expense.amount,
                "expense_date": expense.expense_date.isoformat(),
                "description": expense.description,
                "created_at": expense.created_at.isoformat() if expense.created_at else None,
                "updated_at": expense.updated_at.isoformat() if expense.updated_at else None
            },
            "message": "Expense fetched successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"[EXPENSES] Error fetching expense: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{expense_id}")
async def update_expense(
    expense_id: int,
    expense_update: ExpenseUpdate,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Update an existing expense.
    """
    try:
        # Get existing expense
        query = select(Expenses).where(Expenses.id == expense_id)
        result = await db.execute(query)
        expense = result.scalar_one_or_none()

        if not expense:
            raise HTTPException(status_code=404, detail="Expense not found")

        # Update fields
        update_data = expense_update.dict(exclude_unset=True)

        if "category" in update_data:
            if update_data["category"] not in ["operational", "marketing"]:
                raise HTTPException(status_code=400, detail="Invalid category")
            expense.category = update_data["category"]

        if "expense_type" in update_data:
            expense.expense_type = update_data["expense_type"]

        if "amount" in update_data:
            expense.amount = float(update_data["amount"])

        if "expense_date" in update_data:
            try:
                expense.expense_date = datetime.strptime(update_data["expense_date"], "%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

        if "description" in update_data:
            expense.description = update_data["description"]

        await db.commit()
        await db.refresh(expense)

        return {
            "success": True,
            "data": {
                "id": expense.id,
                "category": expense.category,
                "expense_type": expense.expense_type,
                "amount": expense.amount,
                "expense_date": expense.expense_date.isoformat(),
                "description": expense.description,
                "created_at": expense.created_at.isoformat() if expense.created_at else None,
                "updated_at": expense.updated_at.isoformat() if expense.updated_at else None
            },
            "message": "Expense updated successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        print(f"[EXPENSES] Error updating expense: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{expense_id}")
async def delete_expense(
    expense_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Delete an expense.
    """
    try:
        # Get existing expense
        query = select(Expenses).where(Expenses.id == expense_id)
        result = await db.execute(query)
        expense = result.scalar_one_or_none()

        if not expense:
            raise HTTPException(status_code=404, detail="Expense not found")

        await db.delete(expense)
        await db.commit()

        return {
            "success": True,
            "data": {"id": expense_id},
            "message": "Expense deleted successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        print(f"[EXPENSES] Error deleting expense: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
