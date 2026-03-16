# Create Fittbot announcement post for a given gym_id

from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.async_database import get_async_db
from app.models.fittbot_models import Post, PostMedia
from app.utils.logging_utils import FittbotHTTPException
from app.utils.redis_config import get_redis

router = APIRouter(prefix="/owner/home", tags=["Gymowner"])


ANNOUNCEMENT_CONTENT = """🎉 FITTBOT REWARDS CHALLENGE starts on Jan 26, 2026!
Book Your Daily Pass, or Fitness Session through Fittbot.
Work out at the gym using your booking and earn 1 lucky draw entry per purchase.
More workouts = More Entries = Higher Chances to Win Exciting Prizes 🏆💪"""


MEDIA_FILES = [    
    {
        "file_name": "first.png",
        "file_type": "image",
        "file_path": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/feed_posters/1.png",
        "status":"completed"
    },
     {
        "file_name": "smartwatch.png",
        "file_type": "image",
        "file_path": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/feed_posters/2.png",
        "status":"completed"
    },
    {
        "file_name": "hoodie.png",
        "file_type": "image",
        "file_path": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/feed_posters/3.png",
        "status":"completed"
    },
    {
        "file_name": "sipper.png",
        "file_type": "image",
        "file_path": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/feed_posters/4.png",
        "status":"completed"
    },
    {
        "file_name": "sipper.png",
        "file_type": "image",
        "file_path": "https://fittbot-uploads.s3.ap-south-2.amazonaws.com/feed_posters/5.png",
        "status":"completed"
    },
]



class CreatePostRequest(BaseModel):
    gym_id: int


@router.post("/fittbot_announcement")
async def create_post_for_gym(
    request: CreatePostRequest,
    db: AsyncSession = Depends(get_async_db),
    redis: Redis = Depends(get_redis),
):

    try:
        gym_id = request.gym_id

        # Validate gym_id
        if not isinstance(gym_id, int) or gym_id <= 0:
            raise FittbotHTTPException(
                status_code=400,
                detail="Invalid gym_id",
                error_code="INVALID_GYM_ID",
                log_data={"gym_id": gym_id},
            )

        # Create new post
        new_post = Post(
            gym_id=gym_id,
            client_id=None,
            content=ANNOUNCEMENT_CONTENT,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            is_pinned=False,
            status="completed",
        )
        db.add(new_post)
        await db.flush()

        # Clear Redis cache for this gym's posts
        redis_key = f"gym:{gym_id}:posts"
        await redis.delete(redis_key)

        # Create post_media entries in batch
        new_media_list = [
            PostMedia(
                post_id=new_post.post_id,
                file_name=media["file_name"],
                file_type=media["file_type"],
                file_path=media["file_path"],
                created_at=datetime.now(),
                status="completed",
            )
            for media in MEDIA_FILES
        ]
        db.add_all(new_media_list)
        await db.flush()

        new_media_ids = [m.media_id for m in new_media_list]

        await db.commit()

        return {
            "status": 200,
            "message": "Post created successfully",
            "data": {
                "post_id": new_post.post_id,
                "gym_id": gym_id,
                "media_ids": new_media_ids,
            },
        }

    except FittbotHTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to create post",
            error_code="POST_CREATE_ERROR",
            log_data={"gym_id": request.gym_id, "error": repr(e)},
        )
