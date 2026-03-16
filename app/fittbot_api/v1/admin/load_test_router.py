"""
Admin Load Test API
Trigger and monitor post creation load tests
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException, Header
from typing import Dict, Optional
import asyncio
import os

# Import the load test module
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))
from load_test_posts import run_load_test, test_results, CONFIG

router = APIRouter(prefix="/admin/load-test", tags=["admin-load-test"])

# Admin API key for security
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "fittbot-admin-secret-2025")

def verify_admin(x_admin_key: str = Header(...)):
    """Verify admin API key"""
    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid admin API key")

@router.post("/start")
async def start_load_test(
    background_tasks: BackgroundTasks,
    num_users: Optional[int] = 10,
    num_posts: Optional[int] = 3,
    x_admin_key: str = Header(...)
):
    """
    Start a load test
    
    Requires: X-Admin-Key header
    """
    verify_admin(x_admin_key)
    
    if test_results["status"] == "running":
        raise HTTPException(status_code=400, detail="Load test already running")
    
    CONFIG.NUM_USERS = num_users
    CONFIG.NUM_POSTS_PER_USER = num_posts
    
    test_results.update({
        "status": "starting",
        "posts_created": 0,
        "posts_completed": 0,
        "posts_stuck": 0,
        "posts_failed": 0,
        "errors": [],
    })
    
    background_tasks.add_task(run_load_test)
    
    return {"status": "started", "total_posts": num_users * num_posts}

@router.get("/status")
async def get_status(x_admin_key: str = Header(...)):
    """Get load test status"""
    verify_admin(x_admin_key)
    return test_results

