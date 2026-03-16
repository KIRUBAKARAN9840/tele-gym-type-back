"""Commission calculation service"""

from typing import Optional, Tuple
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_

from .base_service import BaseService
from ..models import CommissionSchedule


class CommissionService(BaseService[CommissionSchedule]):
    """Service for commission calculation and management"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(CommissionSchedule, db_session)
    
    def compute_commission_for_unit(
        self, 
        gym_id: Optional[str], 
        sku: Optional[str], 
        base_amount_minor: int, 
        calculation_date: Optional[date] = None
    ) -> Tuple[int, float, int]:
        """
        Calculate commission for a unit based on hierarchical schedules.
        Priority: gym-specific > product-specific > global
        
        Args:
            gym_id: Gym ID for gym-specific rates
            sku: Product SKU for product-specific rates  
            base_amount_minor: Base amount in minor units (paise)
            calculation_date: Date for rate calculation (default: today)
        
        Returns:
            Tuple of (commission_amount_minor, applied_pct, applied_fixed_minor)
        """
        if calculation_date is None:
            calculation_date = date.today()
        
        # Try gym-specific schedule first
        schedule = None
        if gym_id:
            schedule = self._get_active_schedule("gym", gym_id, calculation_date)
        
        # Try product-specific schedule if no gym schedule
        if not schedule and sku:
            schedule = self._get_active_schedule("product", sku, calculation_date)
        
        # Fall back to global schedule
        if not schedule:
            schedule = self._get_active_schedule("global", None, calculation_date)
        
        if not schedule:
            # No commission schedule found, return zero commission
            return 0, 0.0, 0
        
        return schedule.calculate_commission(base_amount_minor)
    
    def _get_active_schedule(
        self, 
        scope: str, 
        scope_id: Optional[str], 
        calculation_date: date
    ) -> Optional[CommissionSchedule]:
        """Get active commission schedule for scope and date"""
        query = select(CommissionSchedule).where(
            and_(
                CommissionSchedule.scope == scope,
                CommissionSchedule.scope_id == scope_id if scope_id else CommissionSchedule.scope_id.is_(None),
                CommissionSchedule.effective_from <= calculation_date,
                or_(
                    CommissionSchedule.effective_to.is_(None),
                    CommissionSchedule.effective_to >= calculation_date
                )
            )
        ).order_by(CommissionSchedule.effective_from.desc())
        
        return self.db.execute(query).scalars().first()
    
    def create_schedule(
        self,
        scope: str,
        scope_id: Optional[str],
        commission_pct: float,
        commission_fixed_minor: int,
        effective_from: date,
        effective_to: Optional[date] = None
    ) -> CommissionSchedule:
        """Create a new commission schedule"""
        schedule = CommissionSchedule(
            id=f"cs_{scope}_{scope_id or 'global'}_{effective_from}",
            scope=scope,
            scope_id=scope_id,
            commission_pct=commission_pct,
            commission_fixed_minor=commission_fixed_minor,
            effective_from=effective_from,
            effective_to=effective_to
        )
        
        return self.save(schedule)
    
    def get_gym_schedule(self, gym_id: str, as_of: Optional[date] = None) -> Optional[CommissionSchedule]:
        """Get active commission schedule for a gym"""
        calculation_date = as_of or date.today()
        return self._get_active_schedule("gym", gym_id, calculation_date)
    
    def get_product_schedule(self, sku: str, as_of: Optional[date] = None) -> Optional[CommissionSchedule]:
        """Get active commission schedule for a product"""
        calculation_date = as_of or date.today()
        return self._get_active_schedule("product", sku, calculation_date)
    
    def get_global_schedule(self, as_of: Optional[date] = None) -> Optional[CommissionSchedule]:
        """Get active global commission schedule"""
        calculation_date = as_of or date.today()
        return self._get_active_schedule("global", None, calculation_date)
    
    def update_gym_commission(
        self,
        gym_id: str,
        commission_pct: float,
        commission_fixed_minor: int = 0,
        effective_from: Optional[date] = None
    ) -> CommissionSchedule:
        """Update commission schedule for a gym"""
        if effective_from is None:
            effective_from = date.today()
        
        # End current schedule if exists
        current_schedule = self.get_gym_schedule(gym_id, effective_from)
        if current_schedule and current_schedule.effective_to is None:
            current_schedule.effective_to = effective_from
            self.db.commit()
        
        # Create new schedule
        return self.create_schedule(
            scope="gym",
            scope_id=gym_id,
            commission_pct=commission_pct,
            commission_fixed_minor=commission_fixed_minor,
            effective_from=effective_from
        )
    
    def update_global_commission(
        self,
        commission_pct: float,
        commission_fixed_minor: int = 0,
        effective_from: Optional[date] = None
    ) -> CommissionSchedule:
        """Update global commission schedule"""
        if effective_from is None:
            effective_from = date.today()
        
        # End current global schedule if exists
        current_schedule = self.get_global_schedule(effective_from)
        if current_schedule and current_schedule.effective_to is None:
            current_schedule.effective_to = effective_from
            self.db.commit()
        
        # Create new global schedule
        return self.create_schedule(
            scope="global",
            scope_id=None,
            commission_pct=commission_pct,
            commission_fixed_minor=commission_fixed_minor,
            effective_from=effective_from
        )