"""Premium Feature Gate System for Production App"""

from functools import wraps
from fastapi import HTTPException, Depends, Request
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime
from typing import Optional, Callable, Any

from ..config.database import get_db_session


class FeatureGate:
    """Premium feature access control system"""
    
    # Define which features require premium access
    PREMIUM_FEATURES = {
        "unlimited_workouts": {
            "name": "Unlimited Workouts",
            "description": "Access to all workout plans without limits",
            "free_limit": 3  # Free users get 3 workouts per month
        },
        "nutrition_plans": {
            "name": "Nutrition Plans", 
            "description": "Personalized nutrition and diet plans",
            "free_limit": 0  # Not available for free users
        },
        "expert_support": {
            "name": "Expert Support",
            "description": "24/7 chat support from fitness experts",
            "free_limit": 0
        },
        "advanced_analytics": {
            "name": "Advanced Analytics",
            "description": "Detailed progress tracking and insights", 
            "free_limit": 0
        },
        "priority_booking": {
            "name": "Priority Booking",
            "description": "Priority access to gym slots and trainers",
            "free_limit": 0
        },
        "exclusive_content": {
            "name": "Exclusive Content",
            "description": "Access to premium workouts and content",
            "free_limit": 0
        },
        "gym_access": {
            "name": "Gym Access",
            "description": "Book and access partner gyms",
            "free_limit": 2  # 2 free gym visits per month
        }
    }
    
    @staticmethod
    async def check_premium_access(client_id: str, db: Session) -> bool:
        """Check if user has active premium subscription"""
        try:
            result = db.execute(text("""
                SELECT COUNT(*) > 0 as has_premium
                FROM payments.subscriptions 
                WHERE customer_id = :client_id 
                AND status IN ('active', 'renewed')
                AND (active_until IS NULL OR active_until > NOW())
            """), {"client_id": client_id})
            
            return bool(result.scalar())
        except:
            return False
    
    @staticmethod
    async def check_feature_usage(client_id: str, feature_name: str, db: Session) -> dict:
        """Check user's usage of a specific feature"""
        try:
            # Check usage this month for free users
            result = db.execute(text("""
                SELECT COUNT(*) as usage_count
                FROM feature_usage_log 
                WHERE client_id = :client_id 
                AND feature_name = :feature_name
                AND created_at >= DATE_FORMAT(NOW(), '%Y-%m-01')
            """), {"client_id": client_id, "feature_name": feature_name})
            
            usage_count = result.scalar() or 0
            feature_config = FeatureGate.PREMIUM_FEATURES.get(feature_name, {})
            free_limit = feature_config.get("free_limit", 0)
            
            return {
                "usage_count": usage_count,
                "free_limit": free_limit,
                "limit_reached": usage_count >= free_limit
            }
        except:
            return {"usage_count": 0, "free_limit": 0, "limit_reached": False}
    
    @staticmethod
    async def log_feature_usage(client_id: str, feature_name: str, db: Session):
        """Log feature usage for tracking"""
        try:
            db.execute(text("""
                INSERT INTO feature_usage_log (client_id, feature_name, created_at)
                VALUES (:client_id, :feature_name, NOW())
            """), {"client_id": client_id, "feature_name": feature_name})
            db.commit()
        except:
            pass  # Don't fail API if logging fails


def requires_premium(feature_name: str, allow_free_limit: bool = True):
    """
    Decorator to protect premium features
    
    Args:
        feature_name: Name of the feature being protected
        allow_free_limit: If True, allows free users up to their usage limit
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract client_id and db from function arguments
            client_id = None
            db = None
            
            # Look for client_id in kwargs
            if 'client_id' in kwargs:
                client_id = kwargs['client_id']
            
            # Look for db session in kwargs
            for key, value in kwargs.items():
                if isinstance(value, Session):
                    db = value
                    break
            
            if not client_id or not db:
                raise HTTPException(
                    status_code=400,
                    detail="Client ID and database session required"
                )
            
            # Check if user has premium access
            has_premium = await FeatureGate.check_premium_access(client_id, db)
            
            if has_premium:
                # Premium user - full access
                await FeatureGate.log_feature_usage(client_id, feature_name, db)
                return await func(*args, **kwargs)
            
            # Free user - check limits
            if allow_free_limit:
                usage_info = await FeatureGate.check_feature_usage(client_id, feature_name, db)
                
                if not usage_info["limit_reached"]:
                    # Within free limit
                    await FeatureGate.log_feature_usage(client_id, feature_name, db)
                    return await func(*args, **kwargs)
                else:
                    # Free limit exceeded
                    feature_config = FeatureGate.PREMIUM_FEATURES.get(feature_name, {})
                    raise HTTPException(
                        status_code=402,
                        detail={
                            "error": "Premium subscription required",
                            "feature": feature_config.get("name", feature_name),
                            "description": feature_config.get("description", ""),
                            "usage_count": usage_info["usage_count"],
                            "free_limit": usage_info["free_limit"],
                            "upgrade_message": f"You've used your {usage_info['free_limit']} free {feature_name} this month. Upgrade to premium for unlimited access!"
                        }
                    )
            else:
                # Feature not available for free users
                feature_config = FeatureGate.PREMIUM_FEATURES.get(feature_name, {})
                raise HTTPException(
                    status_code=402,
                    detail={
                        "error": "Premium subscription required",
                        "feature": feature_config.get("name", feature_name),
                        "description": feature_config.get("description", ""),
                        "upgrade_message": f"Premium subscription required to access {feature_config.get('name', feature_name)}"
                    }
                )
        
        return wrapper
    return decorator


def premium_only(feature_name: str):
    """Decorator for premium-only features (no free access)"""
    return requires_premium(feature_name, allow_free_limit=False)


# Usage Examples for your existing APIs:

"""
# Add to your existing workout API
@app.get("/workouts/unlimited")
@requires_premium("unlimited_workouts")
async def get_unlimited_workouts(
    client_id: str,
    db: Session = Depends(get_db_session)
):
    # Your existing workout logic
    return get_workouts_for_user(client_id)

# Add to your nutrition API  
@app.get("/nutrition/plans")
@premium_only("nutrition_plans") 
async def get_nutrition_plans(
    client_id: str,
    db: Session = Depends(get_db_session)
):
    # Your existing nutrition logic
    return get_nutrition_data(client_id)

# Add to gym booking API
@app.post("/gym/book")
@requires_premium("gym_access")
async def book_gym_slot(
    client_id: str,
    gym_id: str,
    db: Session = Depends(get_db_session)
):
    # Your existing gym booking logic
    return book_slot(client_id, gym_id)
"""


# Create feature usage tracking table (run this migration)
CREATE_FEATURE_USAGE_TABLE = """
CREATE TABLE IF NOT EXISTS feature_usage_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id VARCHAR(100) NOT NULL,
    feature_name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_client_feature_month (client_id, feature_name, created_at),
    INDEX idx_created_at (created_at)
);
"""