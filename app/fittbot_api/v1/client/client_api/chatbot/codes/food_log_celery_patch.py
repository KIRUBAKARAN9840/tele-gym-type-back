# # app/fittbot_api/v1/client/client_api/chatbot/codes/food_log_celery_patch.py
# """
# NEW voice_stream endpoint that uses Celery queue
# Replace the current /voice/stream endpoint with this code

# NO APP CHANGES NEEDED - Same SSE interface!
# """
# import json
# import asyncio
# from fastapi import APIRouter, UploadFile, File, Query, Depends
# from fastapi.responses import StreamingResponse
# from sqlalchemy.orm import Session
# from redis.asyncio import Redis

# from app.models.database import get_db
# from app.models.deps import get_http
# from app.utils.redis_config import get_redis

# # Import Celery task
# from app.tasks.voice_tasks import process_voice_message


# def sse_json(data: dict) -> str:
#     """Format data as SSE JSON event"""
#     return f"data: {json.dumps(data)}\n\n"


# @router.post("/voice/stream")
# async def voice_stream_sse(
#     user_id: int,
#     audio: UploadFile = File(...),
#     meal: str = Query(None),
#     redis: Redis = Depends(get_redis),
#     db: Session = Depends(get_db),
# ):
#     """
#     Process voice message using Celery queue
#     NO APP CHANGES - Same SSE streaming interface!

#     Flow:
#     1. App sends voice file
#     2. Queue to Celery (returns immediately)
#     3. Keep SSE connection open
#     4. Stream progress updates from worker
#     5. Return final result
#     6. App sees same SSE stream as before!
#     """

#     async def _stream_with_celery():
#         try:
#             # Read audio file
#             audio_bytes = await audio.read()

#             if not audio_bytes:
#                 yield sse_json({"type": "error", "message": "Empty audio file"})
#                 yield "event: done\ndata: [DONE]\n\n"
#                 return

#             # Immediate response - connection established
#             yield sse_json({
#                 "type": "status",
#                 "message": "🎤 Received voice message, processing..."
#             })

#             # Queue job to Celery worker
#             task = process_voice_message.delay(
#                 user_id=user_id,
#                 audio_bytes=audio_bytes,
#                 meal=meal
#             )

#             task_id = task.id
#             print(f"[Voice Stream] Queued task {task_id} for user {user_id}")

#             # Subscribe to Redis pub/sub for real-time updates
#             pubsub = redis.pubsub()
#             await pubsub.subscribe(f"task:{task_id}")

#             # Stream updates from Celery worker
#             timeout_seconds = 120  # 2 minutes timeout
#             start_time = asyncio.get_event_loop().time()

#             try:
#                 async for message in pubsub.listen():
#                     # Check timeout
#                     if asyncio.get_event_loop().time() - start_time > timeout_seconds:
#                         yield sse_json({
#                             "type": "error",
#                             "message": "Processing timeout. Please try again."
#                         })
#                         break

#                     if message['type'] == 'message':
#                         data = json.loads(message['data'])
#                         status = data.get('status')

#                         if status == 'progress':
#                             # Progress update from worker
#                             yield sse_json({
#                                 "type": "progress",
#                                 "message": data.get('message', 'Processing...'),
#                                 "progress": data.get('progress', 0)
#                             })

#                         elif status == 'completed':
#                             # Final result - same format as before!
#                             result = data.get('result', {})
#                             yield sse_json(result)
#                             yield "event: done\ndata: [DONE]\n\n"
#                             break

#                         elif status == 'error':
#                             # Error from worker
#                             yield sse_json({
#                                 "type": "error",
#                                 "message": data.get('message', 'Processing failed')
#                             })
#                             yield "event: done\ndata: [DONE]\n\n"
#                             break

#             finally:
#                 # Cleanup
#                 await pubsub.unsubscribe(f"task:{task_id}")
#                 await pubsub.close()

#         except Exception as e:
#             print(f"[Voice Stream] Error: {e}")
#             import traceback
#             traceback.print_exc()

#             yield sse_json({
#                 "type": "error",
#                 "message": f"Failed to process voice: {str(e)}"
#             })
#             yield "event: done\ndata: [DONE]\n\n"

#     return StreamingResponse(
#         _stream_with_celery(),
#         media_type="text/event-stream",
#         headers={
#             "Cache-Control": "no-cache",
#             "X-Accel-Buffering": "no",
#             "Connection": "keep-alive"
#         }
#     )


# # ========================================
# # INSTRUCTIONS TO REPLACE IN food_log.py:
# # ========================================
# # 1. Add this import at top of food_log.py:
# #    from app.tasks.voice_tasks import process_voice_message
# #
# # 2. Replace the ENTIRE voice_stream_sse function (lines 128-155) with the function above
# #
# # 3. Keep everything else the same!
# #
# # That's it! No other changes needed!
