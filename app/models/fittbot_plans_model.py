from __future__ import annotations

from typing import Optional
from sqlalchemy import Column, String, Integer, BigInteger, Index
from sqlalchemy.orm import Session

from app.models.database import Base, get_db


class FittbotPlan(Base):
    __tablename__ = "fittbot_plans"

    id = Column(String(100), primary_key=True)
    plan_name = Column(String(255), nullable=True)
    price = Column(BigInteger, nullable=False)  # authoritative price in minor units
    duration=Column(Integer, nullable=False)  # duration in days
    
    image_url = Column(String(512), nullable=True)

def get_plan_by_id(db: Session, plan_id: str) -> Optional[FittbotPlan]:
    return db.query(FittbotPlan).filter(FittbotPlan.id == plan_id).first()


def get_plan_by_duration(db: Session, plan_id: str) -> Optional[FittbotPlan]:
    return db.query(FittbotPlan).filter(FittbotPlan.duration == plan_id).first()

