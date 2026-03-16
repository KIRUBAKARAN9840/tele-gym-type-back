"""Entitlement service for managing customer entitlements"""

from typing import List, Optional
from datetime import datetime, timezone, date, timedelta
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import select

from .base_service import BaseService
from ..models import Entitlement, Order, OrderItem
from ..models.enums import StatusEnt, EntType, ItemType
from ..config.settings import get_payment_settings


class EntitlementService(BaseService[Entitlement]):
    """Service for entitlement management"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(Entitlement, db_session)
        self.settings = get_payment_settings()
    
    def create_entitlements_from_order(self, order: Order) -> List[Entitlement]:
        """Create entitlements from order items"""
        entitlements = []
        
        # Load order items if not already loaded
        order_items = self.db.execute(
            select(OrderItem).where(OrderItem.order_id == order.id)
        ).scalars().all()
        
        for item in order_items:
            item_entitlements = self._create_entitlements_for_item(item, order.customer_id)
            entitlements.extend(item_entitlements)
        
        # Save all entitlements
        self.db.add_all(entitlements)
        self.db.commit()
        
        for ent in entitlements:
            self.db.refresh(ent)
        
        return entitlements
    
    def _create_entitlements_for_item(
        self, 
        order_item: OrderItem, 
        customer_id: str
    ) -> List[Entitlement]:
        """Create entitlements for a specific order item"""
        entitlements = []
        now = datetime.now(timezone.utc)
        ist_now = now.astimezone(self.settings.ist_timezone)
        
        if order_item.item_type == ItemType.daily_pass:
            # Create multiple entitlements for daily passes
            scheduled_dates = self._get_scheduled_dates(order_item)
            
            for i in range(order_item.qty):
                scheduled_for = scheduled_dates[i] if i < len(scheduled_dates) else None
                
                entitlement = Entitlement(
                    id=f"ent_{order_item.id}_{i + 1}",
                    order_item_id=order_item.id,
                    customer_id=customer_id,
                    gym_id=order_item.gym_id,
                    trainer_id=None,
                    entitlement_type=EntType.visit,
                    scheduled_for=scheduled_for,
                    status=StatusEnt.pending,
                    created_at=now,
                    updated_at=now
                )
                entitlements.append(entitlement)
        
        elif order_item.item_type in (ItemType.direct_booking, ItemType.pt_session):
            # Single entitlement for bookings/sessions
            entitlement_type = EntType.session if order_item.item_type == ItemType.pt_session else EntType.visit
            scheduled_for = self._get_scheduled_date_from_metadata(order_item)
            
            entitlement = Entitlement(
                id=f"ent_{order_item.id}_1",
                order_item_id=order_item.id,
                customer_id=customer_id,
                gym_id=order_item.gym_id,
                trainer_id=order_item.trainer_id,
                entitlement_type=entitlement_type,
                scheduled_for=scheduled_for,
                status=StatusEnt.pending,
                created_at=now,
                updated_at=now
            )
            entitlements.append(entitlement)
        
        elif order_item.item_type == ItemType.app_subscription:
            # App subscription entitlement
            entitlement = Entitlement(
                id=f"ent_{order_item.id}_app",
                order_item_id=order_item.id,
                customer_id=customer_id,
                gym_id=None,
                trainer_id=None,
                entitlement_type=EntType.app,
                status=StatusEnt.pending,
                created_at=now,
                updated_at=now
            )
            entitlements.append(entitlement)
        
        return entitlements
    
    def _get_scheduled_dates(self, order_item: OrderItem) -> List[date]:
        """Get scheduled dates from order item metadata or generate defaults"""
        scheduled_dates = []
        
        meta = getattr(order_item, "item_metadata", None) or {}
        if (meta and isinstance(meta.get("scheduled_for"), list)):
            
            for date_str in meta["scheduled_for"]:
                try:
                    scheduled_dates.append(date.fromisoformat(date_str))
                except ValueError:
                    continue
        
        # Generate default dates if none provided
        ist_today = datetime.now(self.settings.ist_timezone).date()
        while len(scheduled_dates) < order_item.qty:
            scheduled_dates.append(ist_today + timedelta(days=len(scheduled_dates)))
        
        return scheduled_dates
    
    def _get_scheduled_date_from_metadata(self, order_item: OrderItem) -> Optional[date]:
        """Get scheduled date from order item metadata"""
        meta = getattr(order_item, "item_metadata", None) or {}
        if (meta and meta.get("scheduled_for")):
            
            try:
                return date.fromisoformat(meta["scheduled_for"])
            except ValueError:
                pass
        
        return None
    
    def get_customer_entitlements(
        self,
        customer_id: str,
        status: Optional[StatusEnt] = None,
        entitlement_type: Optional[EntType] = None
    ) -> List[Entitlement]:
        """Get entitlements for a customer"""
        query = select(Entitlement).where(Entitlement.customer_id == customer_id)
        
        if status:
            query = query.where(Entitlement.status == status)
        
        if entitlement_type:
            query = query.where(Entitlement.entitlement_type == entitlement_type)
        
        query = query.order_by(Entitlement.scheduled_for.asc().nullslast())
        
        return list(self.db.execute(query).scalars().all())
    
    def get_gym_entitlements(
        self,
        gym_id: str,
        scheduled_date: Optional[date] = None,
        status: Optional[StatusEnt] = None
    ) -> List[Entitlement]:
        """Get entitlements for a gym"""
        query = select(Entitlement).where(Entitlement.gym_id == gym_id)
        
        if scheduled_date:
            query = query.where(Entitlement.scheduled_for == scheduled_date)
        
        if status:
            query = query.where(Entitlement.status == status)
        
        query = query.order_by(Entitlement.scheduled_for.desc().nullslast())
        
        return list(self.db.execute(query).scalars().all())
    
    def mark_entitlement_used(self, entitlement_id: str) -> Optional[Entitlement]:
        """Mark an entitlement as used"""
        entitlement = self.get_by_id(entitlement_id)
        if not entitlement:
            return None
        
        if entitlement.status != StatusEnt.pending:
            raise ValueError(f"Entitlement is not pending (current status: {entitlement.status})")
        
        entitlement.status = StatusEnt.used
        entitlement.updated_at = datetime.now(timezone.utc)
        
        self.db.commit()
        self.db.refresh(entitlement)
        return entitlement
    
    def mark_entitlement_expired(self, entitlement_id: str) -> Optional[Entitlement]:
        """Mark an entitlement as expired"""
        entitlement = self.get_by_id(entitlement_id)
        if not entitlement:
            return None
        
        entitlement.status = StatusEnt.expired
        entitlement.updated_at = datetime.now(timezone.utc)
        
        self.db.commit()
        self.db.refresh(entitlement)
        return entitlement
    
    def get_expiring_entitlements(self, days_ahead: int = 7) -> List[Entitlement]:
        """Get entitlements that will expire in specified days"""
        cutoff_date = date.today() + timedelta(days=days_ahead)
        
        query = select(Entitlement).where(
            Entitlement.status == StatusEnt.pending,
            Entitlement.scheduled_for <= cutoff_date
        )
        
        return list(self.db.execute(query).scalars().all())
