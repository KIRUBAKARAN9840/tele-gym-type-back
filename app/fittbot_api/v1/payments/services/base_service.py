"""Base service class for payment services"""

from typing import Optional, TypeVar, Generic, Type, List
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from ..config.database import get_db_session
from ..config.settings import get_payment_settings

ModelType = TypeVar("ModelType")


class BaseService(Generic[ModelType]):
    """Base service class with common database operations"""
    
    def __init__(self, model_class: Type[ModelType], db_session: Optional[Session] = None):
        self.model_class = model_class
        self.db = db_session
        self.settings = get_payment_settings()
        self._should_close_db = db_session is None
    
    def __enter__(self):
        if self.db is None:
            self.db = next(get_db_session())
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._should_close_db and self.db:
            self.db.close()
    
    def get_by_id(self, id: str) -> Optional[ModelType]:
        """Get model by ID"""
        return self.db.get(self.model_class, id)
    
    def get_all(self, limit: Optional[int] = None, offset: int = 0) -> List[ModelType]:
        """Get all models with optional pagination"""
        query = select(self.model_class).offset(offset)
        if limit:
            query = query.limit(limit)
        return list(self.db.execute(query).scalars().all())
    
    def create(self, **kwargs) -> ModelType:
        """Create a new model instance"""
        instance = self.model_class(**kwargs)
        self.db.add(instance)
        self.db.commit()
        self.db.refresh(instance)
        return instance
    
    def update(self, instance: ModelType, **kwargs) -> ModelType:
        """Update a model instance"""
        for key, value in kwargs.items():
            if hasattr(instance, key):
                setattr(instance, key, value)
        
        self.db.commit()
        self.db.refresh(instance)
        return instance
    
    def delete(self, instance: ModelType) -> None:
        """Delete a model instance"""
        self.db.delete(instance)
        self.db.commit()
    
    def save(self, instance: ModelType) -> ModelType:
        """Save a model instance"""
        self.db.add(instance)
        self.db.commit()
        self.db.refresh(instance)
        return instance
    
    def bulk_save(self, instances: List[ModelType]) -> List[ModelType]:
        """Save multiple instances"""
        self.db.add_all(instances)
        self.db.commit()
        for instance in instances:
            self.db.refresh(instance)
        return instances
    
    def count(self) -> int:
        """Get total count of models"""
        return self.db.execute(
            select(func.count(self.model_class.id))
        ).scalar_one()
    
    def exists(self, id: str) -> bool:
        """Check if model exists by ID"""
        return self.db.get(self.model_class, id) is not None