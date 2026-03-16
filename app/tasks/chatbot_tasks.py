# app/tasks/chatbot_tasks.py
"""
Celery tasks for AI chatbot (general chat and voice)
Production-ready with error handling and progress updates

Uses SYNC OpenAI client for gevent compatibility.
"""
import json
import logging
from celery import current_task
from app.celery_app import celery_app
from app.utils.openai_sync import get_sync_openai_client, sync_openai_call
from app.utils.redis_config import get_redis_sync
from app.models.database import get_db_sync

# Production logging configuration
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Import Groq transcription
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.asr import (
    get_groq_api_key,
    GROQ_BASE_URL,
    GROQ_ASR_MODEL
)


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


@celery_app.task(bind=True, name="app.tasks.chatbot_tasks.process_chat_message")
def process_chat_message(
    self,
    user_id: int,
    messages: list,
    model: str = "gpt-4o-mini",
    temperature: float = 0.7
):
    """
    Process chat message with OpenAI - ONLY makes the API call

    Uses SYNC client for gevent compatibility - yields during I/O wait.

    Args:
        user_id: Client ID
        messages: Complete messages array (already built by FastAPI)
        model: OpenAI model to use
        temperature: Temperature for response

    Returns:
        dict: Chat response
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Processing your message..."
        })

        logger.info(f"Task {task_id}: Starting chat processing for user {user_id} (model={model}, temp={temperature})")

        # Get SYNC client - gevent-friendly
        oai_client = get_sync_openai_client()
        logger.debug(f"Task {task_id}: Sync OpenAI client acquired from pool")

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Generating response..."
        })

        # SYNC call - gevent will yield during I/O wait
        response = sync_openai_call(
            oai_client,
            model=model,
            messages=messages,
            temperature=temperature
        )

        content = (response.choices[0].message.content or "").strip()

        result = {
            "status": "completed",
            "progress": 100,
            "result": {
                "type": "chat_response",
                "message": content
            }
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Chat completed successfully for user {user_id} (response_length={len(content)} chars)")
        return result["result"]

    except Exception as e:
        logger.error(f"Task {task_id}: Chat processing failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        error_result = {
            "status": "error",
            "progress": 0,
            "result": {
                "type": "error",
                "message": f"Failed to process chat: {str(e)}"
            }
        }
        publish_progress(task_id, error_result)

        raise


@celery_app.task(bind=True, name="app.tasks.chatbot_tasks.process_voice_chat")
def process_voice_chat(
    self,
    user_id: int,
    audio_bytes: bytes,
    conversation_history: list = None
):
    """
    Process voice chat message (transcribe + chat)

    Uses SYNC clients for gevent compatibility.

    Args:
        user_id: Client ID
        audio_bytes: Audio file bytes
        conversation_history: Previous messages

    Returns:
        dict: Transcription + chat response
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 10,
            "message": "Transcribing audio..."
        })

        logger.info(f"Task {task_id}: Starting voice chat processing for user {user_id} (audio_size={len(audio_bytes)} bytes)")

        # Step 1: Transcribe with Groq (already sync - httpx.Client)
        import httpx
        import io

        audio_file = io.BytesIO(audio_bytes)

        with httpx.Client(timeout=60.0) as client:
            files = {"file": ("audio.wav", audio_file, "audio/wav")}
            data = {"model": GROQ_ASR_MODEL}
            headers = {"Authorization": f"Bearer {get_groq_api_key()}"}
            url = f"{GROQ_BASE_URL}/openai/v1/audio/transcriptions"

            response = client.post(url, data=data, files=files, headers=headers)
            response.raise_for_status()

            result = response.json()
            transcript = (result.get("text") or "").strip()

        if not transcript:
            raise ValueError("Empty transcript from Groq")

        logger.info(f"Task {task_id}: Audio transcribed successfully - '{transcript[:100]}{'...' if len(transcript) > 100 else ''}'")

        publish_progress(task_id, {
            "status": "progress",
            "progress": 40,
            "message": "Generating response..."
        })

        # Step 2: Process with chatbot using SYNC client
        from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.llm_helpers import (
            GENERAL_SYSTEM
        )

        oai_client = get_sync_openai_client()

        messages = [{"role": "system", "content": GENERAL_SYSTEM}]

        if conversation_history:
            messages.extend(conversation_history[-10:])

        messages.append({"role": "user", "content": transcript})

        # SYNC call - gevent yields during I/O
        response = sync_openai_call(
            oai_client,
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.7
        )

        content = (response.choices[0].message.content or "").strip()

        result = {
            "status": "completed",
            "progress": 100,
            "result": {
                "type": "voice_chat_response",
                "transcript": transcript,
                "message": content
            }
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Voice chat completed successfully for user {user_id} (transcript='{transcript[:50]}...', response_length={len(content)} chars)")
        return result["result"]

    except Exception as e:
        logger.error(f"Task {task_id}: Voice chat failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        error_result = {
            "status": "error",
            "progress": 0,
            "result": {
                "type": "error",
                "message": f"Failed to process voice: {str(e)}"
            }
        }
        publish_progress(task_id, error_result)

        raise
