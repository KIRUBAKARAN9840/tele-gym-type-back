"""Order service for order management"""

from typing import List, Optional
from datetime import datetime, timezone
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import select

from .base_service import BaseService
from ..models import Order, OrderItem, CatalogProduct
from ..models.enums import StatusOrder, ItemType
from ..schemas.orders import CreateOrderRequest, CreateOrderItem


class OrderService(BaseService[Order]):
    """Service for order management"""
    
    def __init__(self, db_session: Optional[Session] = None):
        super().__init__(Order, db_session)
    
    def create_order(self, request: CreateOrderRequest) -> Order:
        """Create a new order with items"""
        # Calculate gross amount and prepare order items
        gross_amount = 0
        order_items = []
        now = datetime.now(timezone.utc)
        
        for idx, item_request in enumerate(request.items):
            # Get unit price from catalog or use provided price
            unit_price = self._get_unit_price(item_request)
            total_price = unit_price * item_request.qty
            gross_amount += total_price
            
            order_item = OrderItem(
                id=f"oi_{request.order_id}_{idx + 1}",
                order_id=request.order_id,
                item_type=item_request.item_type,
                sku=item_request.sku,
                gym_id=item_request.gym_id,
                trainer_id=item_request.trainer_id,
                title=item_request.title or item_request.item_type,
                unit_price_minor=unit_price,
                qty=item_request.qty,
                metadata=item_request.metadata,
                created_at=now,
                updated_at=now
            )
            order_items.append(order_item)
        
        # Create order
        order = Order(
            id=request.order_id,
            customer_id=request.customer_id,
            currency=request.currency,
            provider=request.provider,
            gross_amount_minor=gross_amount,
            status=StatusOrder.pending,
            created_at=now,
            updated_at=now
        )
        
        # Save order and items
        self.db.add(order)
        self.db.add_all(order_items)
        self.db.commit()
        
        # Refresh with relationships
        self.db.refresh(order)
        return order
    
    def _get_unit_price(self, item_request: CreateOrderItem) -> int:
        """Get unit price for an order item"""
        if item_request.unit_price_minor is not None:
            return item_request.unit_price_minor
        
        if item_request.sku:
            catalog_product = self.db.get(CatalogProduct, item_request.sku)
            if catalog_product and catalog_product.active:
                return catalog_product.base_amount_minor
            else:
                raise ValueError(f"SKU {item_request.sku} not found or inactive")
        
        raise ValueError("Either unit_price_minor or sku must be provided")
    
    def get_order_with_items(self, order_id: str) -> Optional[Order]:
        """Get order with loaded items"""
        return self.db.execute(
            select(Order)
            .options(selectinload(Order.items))
            .where(Order.id == order_id)
        ).scalar_one_or_none()
    
    def get_orders_by_customer(
        self, 
        customer_id: str, 
        status: Optional[StatusOrder] = None,
        limit: Optional[int] = None
    ) -> List[Order]:
        """Get orders for a customer"""
        query = select(Order).where(Order.customer_id == customer_id)
        
        if status:
            query = query.where(Order.status == status)
        
        query = query.order_by(Order.created_at.desc())
        
        if limit:
            query = query.limit(limit)
        
        return list(self.db.execute(query).scalars().all())
    
    def update_order_status(self, order_id: str, status: StatusOrder) -> Optional[Order]:
        """Update order status"""
        order = self.get_by_id(order_id)
        if not order:
            return None
        
        order.status = status
        order.updated_at = datetime.now(timezone.utc)
        
        self.db.commit()
        self.db.refresh(order)
        return order
    
    def get_pending_orders(self, limit: Optional[int] = None) -> List[Order]:
        """Get all pending orders"""
        query = select(Order).where(Order.status == StatusOrder.pending)
        query = query.order_by(Order.created_at.asc())
        
        if limit:
            query = query.limit(limit)
        
        return list(self.db.execute(query).scalars().all())
    
    def cancel_order(self, order_id: str, reason: Optional[str] = None) -> Optional[Order]:
        """Cancel an order"""
        order = self.get_by_id(order_id)
        if not order:
            return None
        
        if order.status != StatusOrder.pending:
            raise ValueError(f"Cannot cancel order in {order.status} status")
        
        order.status = StatusOrder.canceled
        order.updated_at = datetime.now(timezone.utc)
        
        self.db.commit()
        self.db.refresh(order)
        return order