# app/tasks/image_scanner_tasks.py
"""
Celery tasks for AI food image scanning
Production-ready with error handling and progress updates
"""
import json
import base64
import logging
import io
import re
from typing import List, Dict, Any, Tuple, Optional
from celery import current_task
from app.celery_app import celery_app
from app.utils.openai_pool import get_openai_client
from app.utils.redis_config import get_redis_sync
from app.models.database import get_db_sync
from PIL import Image

# Production logging configuration
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


# ─── CUSTOM EXCEPTIONS ────────────────────────────────────────
class ImageProcessingError(Exception):
    """Custom exception for image processing failures"""
    pass


class CompressionError(Exception):
    """Custom exception for compression failures"""
    pass


class ValidationError(Exception):
    """Custom exception for image validation failures"""
    pass


# ─── IMAGE COMPRESSION CONSTANTS ────────────────────────────────
TARGET_SIZE = 150 * 1024  # 150KB target
MAX_DIMENSION = 512  # Max dimension
COMPRESSION_QUALITY = 55  # Aggressive compression for speed
SUPPORTED_FORMATS = {"image/jpeg", "image/jpg", "image/png", "image/avif", "image/webp", "application/octet-stream"}
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".avif", ".webp"}

# Disable PIL's DecompressionBombWarning since we handle size limits ourselves
Image.MAX_IMAGE_PIXELS = None


def publish_progress(task_id: str, data: dict):
    """Publish progress to Redis pub/sub for SSE streaming"""
    try:
        redis_client = get_redis_sync()
        redis_client.publish(
            f"task:{task_id}",
            json.dumps(data)
        )
        logger.debug(f"Task {task_id}: Published progress - {data.get('status')} ({data.get('progress', 0)}%)")
    except Exception as e:
        logger.error(f"Task {task_id}: Failed to publish progress - {e}")


# ─── IMAGE COMPRESSION FUNCTIONS (MOVED FROM FASTAPI) ─────────────
def _get_image_format(content_type: str, filename: str = "") -> str:
    """Determine image format from content type and filename"""
    if content_type and content_type != "application/octet-stream":
        if "jpeg" in content_type or "jpg" in content_type:
            return "JPEG"
        elif "png" in content_type:
            return "PNG"
        elif "avif" in content_type:
            return "AVIF"
        elif "webp" in content_type:
            return "WEBP"

    if filename:
        ext = filename.lower().split('.')[-1]
        if ext in ['jpg', 'jpeg']:
            return "JPEG"
        elif ext == 'png':
            return "PNG"
        elif ext == 'avif':
            return "AVIF"
        elif ext == 'webp':
            return "WEBP"

    return "JPEG"


def _is_supported_image(content_type: str, filename: str = "") -> bool:
    """Check if image format is supported"""
    if content_type in {"image/jpeg", "image/jpg", "image/png", "image/avif", "image/webp"}:
        return True
    if content_type == "application/octet-stream" and filename:
        ext = f".{filename.lower().split('.')[-1]}" if '.' in filename else ""
        return ext in SUPPORTED_EXTENSIONS
    return False


def _compress_image_in_celery(raw: bytes, original_format: str = "JPEG") -> Tuple[bytes, str]:
    """
    Optimized image compression for Celery workers
    Ultra-fast compression optimized for AI model compatibility, not visual quality
    """
    try:
        # If image is already small enough, return as-is
        if len(raw) <= TARGET_SIZE:
            return raw, f"image/{original_format.lower()}"

        with Image.open(io.BytesIO(raw)) as im:
            # Resize if needed using NEAREST for speed (3-4x faster than BILINEAR)
            if max(im.width, im.height) > MAX_DIMENSION:
                if im.width > im.height:
                    new_size = (MAX_DIMENSION, int(im.height * MAX_DIMENSION / im.width))
                else:
                    new_size = (int(im.width * MAX_DIMENSION / im.height), MAX_DIMENSION)

                # Use NEAREST filter for maximum speed
                im.thumbnail(new_size, Image.NEAREST)

            # Convert to RGB if needed
            if im.mode not in ("RGB", "L"):
                if im.mode == "RGBA":
                    background = Image.new("RGB", im.size, (255, 255, 255))
                    background.paste(im, mask=im.split()[3] if im.mode == "RGBA" else None)
                    im = background
                else:
                    im = im.convert("RGB")
            elif im.mode == "L":
                # Keep grayscale, JPEG supports it
                pass

            # Compress with aggressive settings for speed
            buf = io.BytesIO()
            im.save(
                buf,
                format="JPEG",
                quality=COMPRESSION_QUALITY,
                optimize=False,  # Skip optimization pass for speed
                progressive=False,  # Disable progressive encoding
                subsampling=2  # 4:2:0 chroma subsampling (fastest, smallest)
            )

            compressed = buf.getvalue()
            return compressed, "image/jpeg"

    except Exception as e:
        logger.error(f"Image compression failed: {e}")
        raise CompressionError(f"Failed to compress image: {str(e)}")


def _validate_and_compress_raw_image(raw_bytes: bytes, content_type: str, filename: str = "") -> Tuple[bytes, str]:
    """
    Validate image format and compress for AI processing
    Returns compressed bytes and content type
    """
    # Validate format
    if not _is_supported_image(content_type, filename):
        supported_formats = "JPEG, JPG, PNG, AVIF, WebP"
        raise ValidationError(f"Unsupported file format: {content_type}. Supported formats: {supported_formats}")

    # Get original format
    original_format = _get_image_format(content_type, filename)

    # Compress image
    compressed, content_type = _compress_image_in_celery(raw_bytes, original_format)

    return compressed, content_type


@celery_app.task(bind=True, name="app.tasks.image_scanner_tasks.analyze_food_image_v2")
def analyze_food_image_v2(
    self,
    user_id: int,
    raw_image_data_list: List[str],  # List of dicts with raw image data and metadata
    food_scan: bool = True
):
    """
    Enhanced task with full image processing pipeline in Celery
    Handles compression, validation, and AI analysis entirely in Celery
    """
    task_id = self.request.id

    try:
        # Progress: Starting processing
        publish_progress(task_id, {
            "status": "progress",
            "progress": 5,
            "message": f"Processing {len(raw_image_data_list)} image(s)..."
        })

        logger.info(f"Task {task_id}: Starting full image processing for user {user_id} ({len(raw_image_data_list)} images)")

        # Step 1: Validate and compress all images
        compressed_images = []
        for idx, image_data in enumerate(raw_image_data_list):
            try:
                # Extract raw bytes and metadata
                raw_bytes = base64.b64decode(image_data.get("raw_data", ""))
                content_type = image_data.get("content_type", "image/jpeg")
                filename = image_data.get("filename", "")

                # Validate and compress
                compressed, content_type = _validate_and_compress_raw_image(raw_bytes, content_type, filename)
                compressed_b64 = base64.b64encode(compressed).decode('utf-8')
                compressed_images.append((compressed_b64, content_type))

                # Progress update for compression
                compression_progress = 5 + (20 * (idx + 1) / len(raw_image_data_list))
                publish_progress(task_id, {
                    "status": "progress",
                    "progress": compression_progress,
                    "message": f"Processed {idx + 1}/{len(raw_image_data_list)} images..."
                })

                logger.debug(f"Task {task_id}: Image {idx+1} compressed - {len(raw_bytes)} -> {len(compressed)} bytes")

            except (ValidationError, CompressionError) as e:
                logger.error(f"Task {task_id}: Image {idx+1} processing failed: {e}")
                raise ImageProcessingError(f"Failed to process image {idx+1}: {str(e)}")

        # Step 2: AI Analysis (25% - 70%)
        publish_progress(task_id, {
            "status": "progress",
            "progress": 25,
            "message": "Analyzing food items..."
        })

        # Import AI functions
        from app.fittbot_api.v1.client.client_api.food_scanner_AI.ai_food_scanner import (
            _ask,
            _normalise
        )

        # Run async functions in sync context
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        all_items = []
        all_insights = []

        for idx, (compressed_b64, content_type) in enumerate(compressed_images):
            try:
                # Decode compressed image
                image_bytes = base64.b64decode(compressed_b64)

                # Analyze this image
                result = loop.run_until_complete(
                    _ask(image_bytes, content_type, brief=food_scan)
                )

                # Collect items from this image
                items_from_image = result.get("items", [])
                all_items.extend(items_from_image)

                # Collect insights from this image
                insights_from_image = result.get("insights", [])
                all_insights.extend(insights_from_image)

                # Progress update for AI analysis
                analysis_progress = 25 + (45 * (idx + 1) / len(compressed_images))
                publish_progress(task_id, {
                    "status": "progress",
                    "progress": analysis_progress,
                    "message": f"Analyzed {idx + 1}/{len(compressed_images)} images..."
                })

                logger.debug(f"Task {task_id}: Image {idx+1} analyzed - {len(items_from_image)} items found")

            except Exception as e:
                logger.error(f"Task {task_id}: AI analysis failed for image {idx+1}: {e}")
                # Continue with other images rather than failing completely
                continue

        loop.close()

        if not all_items:
            logger.warning(f"Task {task_id}: No food items identified in any images")
            all_items = []

        logger.info(f"Task {task_id}: Identified {len(all_items)} total food items from {len(compressed_images)} images")

        # Step 3: Normalize and process items (70% - 90%)
        publish_progress(task_id, {
            "status": "progress",
            "progress": 70,
            "message": "Processing food items..."
        })

        enriched_items = _normalise(all_items)
        logger.debug(f"Task {task_id}: Normalized {len(enriched_items)} items")

        # Step 4: Finalize results (90% - 100%)
        publish_progress(task_id, {
            "status": "progress",
            "progress": 90,
            "message": "Finalizing results..."
        })

        # Extract food labels and calculate totals (same as legacy)
        food_labels = [item.get("label", "Unknown") for item in enriched_items]

        total_calories = sum(item.get("calories", 0) or 0 for item in enriched_items)
        total_protein_g = sum(item.get("protein_g", 0) or 0 for item in enriched_items)
        total_carbs_g = sum(item.get("carbs_g", 0) or 0 for item in enriched_items)
        total_fat_g = sum(item.get("fat_g", 0) or 0 for item in enriched_items)
        total_fibre_g = sum(item.get("fibre_g", 0) or 0 for item in enriched_items)
        total_sugar_g = sum(item.get("sugar_g", 0) or 0 for item in enriched_items)

        # Calculate micronutrients
        micro_nutrients = {
            "calcium_mg": round(sum(item.get("calcium_mg", 0) or 0 for item in enriched_items), 2),
            "magnesium_mg": round(sum(item.get("magnesium_mg", 0) or 0 for item in enriched_items), 2),
            "sodium_mg": round(sum(item.get("sodium_mg", 0) or 0 for item in enriched_items), 2),
            "potassium_mg": round(sum(item.get("potassium_mg", 0) or 0 for item in enriched_items), 2),
            "iron_mg": round(sum(item.get("iron_mg", 0) or 0 for item in enriched_items), 2),
            "iodine_mcg": round(sum(item.get("iodine_mcg", 0) or 0 for item in enriched_items), 2),
        }

        # Match exact legacy format
        result = {
            "status": "completed",
            "progress": 100,
            "result": {
                "success": True,
                "items": sorted(food_labels),  # Sorted list of food name strings
                "totals": {
                    "calories": int(round(total_calories)),
                    "protein_g": int(round(total_protein_g)),
                    "carbs_g": int(round(total_carbs_g)),
                    "fat_g": int(round(total_fat_g)),
                    "fibre_g": int(round(total_fibre_g)),
                    "sugar_g": int(round(total_sugar_g)),
                },
                "micro_nutrients": micro_nutrients,
                "insights": all_insights[:2] if all_insights else [],
                "message": f"✅ Identified {len(enriched_items)} food items"
            }
        }

        # Publish completion
        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Full image processing completed for user {user_id} - {len(enriched_items)} items identified")
        return result["result"]

    except Exception as e:
        logger.error(f"Task {task_id}: Full image processing failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Publish error
        error_result = {
            "status": "error",
            "progress": 0,
            "result": {
                "success": False,
                "error": str(e),
                "message": f"Failed to process images: {str(e)}"
            }
        }
        publish_progress(task_id, error_result)

        raise self.retry(exc=e, countdown=60, max_retries=3)


@celery_app.task(bind=True, name="app.tasks.image_scanner_tasks.analyze_food_image")
def analyze_food_image(
    self,
    user_id: int,
    image_data_list: List[str],  # Base64 encoded images
    food_scan: bool = True
):
    
    
    task_id = self.request.id

    try:
        # Progress: Starting
        publish_progress(task_id, {
            "status": "progress",
            "progress": 10,
            "message": f"Analyzing {len(image_data_list)} image(s)..."
        })

        # Import core food scanner functions
        from app.fittbot_api.v1.client.client_api.food_scanner_AI.ai_food_scanner import (
            _ask,
            _normalise,
            _get_image_format
        )

        logger.info(f"Task {task_id}: Starting image analysis for user {user_id} ({len(image_data_list)} images)")

        # Step 1: Analyze each image
        publish_progress(task_id, {
            "status": "progress",
            "progress": 30,
            "message": "Identifying food items..."
        })

        # Run async function in sync context
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        all_items = []
        all_insights = []
        for idx, b64_image in enumerate(image_data_list):
            # Decode base64 back to bytes
            image_bytes = base64.b64decode(b64_image)
            content_type = "image/jpeg"  # Default, compressed images are JPEG

            # Analyze this image
            result = loop.run_until_complete(
                _ask(image_bytes, content_type, brief=food_scan)
            )

            # Collect items from this image
            items_from_image = result.get("items", [])
            all_items.extend(items_from_image)

            # Collect insights from this image
            insights_from_image = result.get("insights", [])
            all_insights.extend(insights_from_image)

            logger.debug(f"Task {task_id}: Image {idx+1}/{len(image_data_list)} analyzed - {len(items_from_image)} items found")

        if not all_items:
            logger.warning(f"Task {task_id}: No food items identified in any images")
            all_items = []  # Empty list, not an error

        logger.info(f"Task {task_id}: Identified {len(all_items)} total food items from {len(image_data_list)} images")

        # Step 2: Normalize and process items
        publish_progress(task_id, {
            "status": "progress",
            "progress": 70,
            "message": "Processing food items..."
        })

        enriched_items = _normalise(all_items)
        logger.debug(f"Task {task_id}: Normalized {len(enriched_items)} items")

        # Step 4: Calculate totals and prepare result (MATCH LEGACY FORMAT EXACTLY)
        publish_progress(task_id, {
            "status": "progress",
            "progress": 90,
            "message": "Finalizing results..."
        })

        # Extract just food labels (strings) for items list
        food_labels = []
        for item in enriched_items:
            label = item.get("label", "Unknown")
            food_labels.append(label)

        # Calculate totals with ALL 6 keys as integers (matching legacy format)
        total_calories = sum(item.get("calories", 0) or 0 for item in enriched_items)
        total_protein_g = sum(item.get("protein_g", 0) or 0 for item in enriched_items)
        total_carbs_g = sum(item.get("carbs_g", 0) or 0 for item in enriched_items)
        total_fat_g = sum(item.get("fat_g", 0) or 0 for item in enriched_items)
        total_fibre_g = sum(item.get("fibre_g", 0) or 0 for item in enriched_items)
        total_sugar_g = sum(item.get("sugar_g", 0) or 0 for item in enriched_items)

        # Calculate micronutrients (matching legacy format)
        micro_nutrients = {
            "calcium_mg": round(sum(item.get("calcium_mg", 0) or 0 for item in enriched_items), 2),
            "magnesium_mg": round(sum(item.get("magnesium_mg", 0) or 0 for item in enriched_items), 2),
            "sodium_mg": round(sum(item.get("sodium_mg", 0) or 0 for item in enriched_items), 2),
            "potassium_mg": round(sum(item.get("potassium_mg", 0) or 0 for item in enriched_items), 2),
            "iron_mg": round(sum(item.get("iron_mg", 0) or 0 for item in enriched_items), 2),
            "iodine_mcg": round(sum(item.get("iodine_mcg", 0) or 0 for item in enriched_items), 2),
        }

        loop.close()

        # Match EXACT format from legacy code (lines 1046-1066)
        result = {
            "status": "completed",
            "progress": 100,
            "result": {
                "success": True,
                "items": sorted(food_labels),  # Sorted list of food name STRINGS only
                "totals": {
                    "calories": int(round(total_calories)),
                    "protein_g": int(round(total_protein_g)),
                    "carbs_g": int(round(total_carbs_g)),
                    "fat_g": int(round(total_fat_g)),
                    "fibre_g": int(round(total_fibre_g)),
                    "sugar_g": int(round(total_sugar_g)),
                },
                "micro_nutrients": micro_nutrients,
                "insights": all_insights[:2] if all_insights else [],
                "message": f"✅ Identified {len(enriched_items)} food items"
            }
        }

        # Publish completion
        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Image analysis completed successfully for user {user_id} - {len(enriched_items)} items identified")
        return result["result"]

    except Exception as e:
        logger.error(f"Task {task_id}: Image analysis failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Publish error
        error_result = {
            "status": "error",
            "progress": 0,
            "result": {
                "success": False,
                "error": str(e),
                "message": f"Failed to analyze images: {str(e)}"
            }
        }
        publish_progress(task_id, error_result)

        raise  # Re-raise for Celery retry
