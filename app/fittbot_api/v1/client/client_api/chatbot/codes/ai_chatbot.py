# routes/ai_chatbot.py - General Chatbot (Intent Detection and Specialized Bots Removed)
from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Depends
from fastapi.responses import StreamingResponse
from fastapi_limiter.depends import RateLimiter
from pydantic import BaseModel
import pytz, os, hashlib, orjson, json, re
from datetime import datetime
import io
from sqlalchemy.orm import Session
from app.models.database import get_db

from app.models.deps import get_http, get_oai, get_mem
from app.utils.async_openai import async_openai_call
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.kb_store import KB
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.llm_helpers import (
    PlainTextStreamFilter, oai_chat_stream, GENERAL_SYSTEM, TOP_K,
    build_messages, heuristic_confidence, OPENAI_MODEL,
    sse_json, sse_escape, gpt_small_route, is_yes, is_no, is_fitness_related,
    has_action_verb, is_fittbot_meta_query, is_plan_request, STYLE_PLAN, STYLE_CHAT_FORMAT, pretty_plan
)

from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.report_analysis import (
    is_analysis_intent, is_followup_question, set_mode, get_mode,
    set_analysis_artifacts, get_analysis_artifacts, build_analysis_dataset_dict,
    build_summary_hints, run_analysis_generator, STYLE_INSIGHT_REPORT,
)
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.asr import transcribe_audio

router = APIRouter(prefix="/chatbot", tags=["chatbot"])

APP_ENV = os.getenv("APP_ENV", "prod")
TZNAME = os.getenv("TZ", "Asia/Kolkata")
IST = pytz.timezone(TZNAME)

class KBUpsertIn(BaseModel):
    source: str
    text: str

class KBSearchIn(BaseModel):
    query: str
    k: int = 4

# Simplified chatbot - all helper functions for specialized bots removed

@router.get("/healthz")
async def healthz():
    return {"ok": True, "env": APP_ENV, "tz": TZNAME, "kb_chunks": len(KB.texts)}

class RichTextStreamFilter:
    def __init__(self): self.buf = ""
    def feed(self, ch: str) -> str:
        if not ch: return ""
        ch = ch.replace("\r\n", "\n").replace("\r", "\n")
        self.buf += ch
        out, self.buf = self.buf, ""
        return out
    def flush(self) -> str:
        out, self.buf = self.buf, ""
        return out

def pretty_plan(markdown: str) -> str:
    if not markdown:
        return ""

    txt = markdown.replace("\r\n", "\n").replace("\r", "\n")
    txt = re.sub(r'^\s*#{1,6}\s*', '', txt, flags=re.M)
    txt = re.sub(r'\*\*(.*?)\*\*', r'\1', txt)
    txt = re.sub(r'\*(.*?)\*', r'\1', txt)
    txt = re.sub(r'^\s*(\d+)\.\s*', r'\1) ', txt, flags=re.M)
    txt = re.sub(r'^\s*[-•]\s*', '• ', txt, flags=re.M)
    txt = re.sub(r':(?!\s)', ': ', txt)
    txt = re.sub(r',(?!\s)', ', ', txt)
    txt = re.sub(r'([A-Za-z])([-–—])([A-Za-z])', r'\1 \2 \3', txt)
    txt = re.sub(r'\n{3,}', '\n\n', txt)
    txt = "\n".join(line.rstrip() for line in txt.split("\n"))
    return txt.strip()

@router.get("/chat/stream_test")
async def chat_stream(
    user_id: int,
    text: str = Query(...),
    mem = Depends(get_mem),
    oai = Depends(get_oai),
    db: Session = Depends(get_db),
):
    if not user_id or not text.strip():
        raise HTTPException(400, "user_id and text required")

    text = text.strip()
    tlower = text.lower().strip()
    
    pend = (await mem.get_pending(user_id)) or {}
    mode = await get_mode(mem, user_id)

    # Simplified general chatbot - no intent detection or specialized routing

    # Continue with existing analysis and normal chat logic
    if mode == "analysis" and not is_plan_request(tlower):
        if is_followup_question(text):
            dataset, summary = await get_analysis_artifacts(mem, user_id)
            if dataset:
                await mem.add(user_id, "user", text.strip())
                msgs = [
                    {"role":"system","content": GENERAL_SYSTEM},
                    {"role":"system","content": STYLE_CHAT_FORMAT},
                    {"role":"system","content":
                        "ANALYSIS_MODE=ON. Use this context without re-querying DB.\n"
                        f"DATASET:\n{orjson.dumps(dataset).decode()}\n\n"
                        f"PRIOR_SUMMARY:\n{summary or ''}\n"
                        "Answer follow-up concisely with numbers where helpful."
                    },
                    {"role":"user","content": text.strip()},
                ]
                # Queue OpenAI call to Celery (non-blocking)
                from app.tasks.chatbot_tasks import process_chat_message
                import asyncio
                from celery.result import AsyncResult

                # Pass complete messages array (includes analysis context)
                task = process_chat_message.delay(
                    user_id=user_id,
                    messages=msgs,
                    model=OPENAI_MODEL,
                    temperature=0
                )

                # Wait for result (async polling)
                max_wait = 120
                poll_interval = 0.5
                elapsed = 0

                while elapsed < max_wait:
                    celery_task = AsyncResult(task.id)
                    if celery_task.ready():
                        if celery_task.successful():
                            result = celery_task.result
                            content = result.get("message", "")
                            break
                        else:
                            raise HTTPException(500, f"Chat task failed: {celery_task.info}")
                    await asyncio.sleep(poll_interval)
                    elapsed += poll_interval
                else:
                    raise HTTPException(504, "Chat request timed out")

                content = content.strip()
                content = re.sub(r'\bfit\s*bot\b|\bfit+bot\b|\bfitbot\b', 'Fittbot', content, flags=re.I)
                pretty = pretty_plan(content)

                async def _one_shot_followup():
                    yield sse_escape(pretty)
                    await mem.add(user_id, "assistant", pretty)
                    yield "event: done\ndata: [DONE]\n\n"
                return StreamingResponse(_one_shot_followup(), media_type="text/event-stream",
                                        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

    if is_analysis_intent(tlower) and not pend:
        await mem.set_pending(user_id, {"state":"awaiting_analysis_confirm"})
        async def _ask_confirm():
            yield sse_json({"type":"analysis","status":"confirm",
                            "prompt":"Sure—let me analyse your diet and workout data. Shall we start?"})
            yield "event: ping\ndata: {}\n\n"
            yield "event: done\ndata: [DONE]\n\n"
        return StreamingResponse(_ask_confirm(), media_type="text/event-stream",
                                headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

    # Regular chat processing with friendly responses
    # print(f"DEBUG: Processing as regular fitness chat")
    
    
    
    # More precise greeting detection - only trigger for actual greetings, not fitness questions
    simple_greetings = ['hi', 'hello', 'hey', 'good morning', 'good afternoon', 'good evening']
    personal_questions = ['how are you', 'how do you do', 'whats up', 'what\'s up', 'your name', 'who are you', 'what are you', 'introduce yourself', 'tell me about yourself']
    
    # Check if the ENTIRE message is just a greeting (not part of a longer question)
    text_clean = text.lower().strip()
    is_simple_greeting = text_clean in simple_greetings
    is_personal_question = any(personal in text_clean for personal in personal_questions) and len(text.split()) <= 5
    
    is_greeting_or_personal = is_simple_greeting or is_personal_question
    
    # Only give the generic redirect for clear non-fitness topics AND not greetings
    non_fitness_keywords = [
        'weather', 'politics', 'election', 'movies', 'films', 'music', 'songs', 'concert',
        'sports team', 'football team', 'basketball', 'news today', 'stock market', 'crypto',
        'programming', 'coding', 'javascript', 'python', 'technology', 'computer',
        'travel destination', 'vacation', 'restaurant review', 'recipe for'
    ]
    
    # Only redirect if it's clearly about non-fitness topics AND not a greeting
    is_clearly_non_fitness = any(keyword in text.lower() for keyword in non_fitness_keywords) and not is_greeting_or_personal and 'fitness' not in text.lower() and 'health' not in text.lower()
    
    if is_clearly_non_fitness:
        async def _friendly_redirect():
            friendly_msg = "I focus on fitness, health, and wellness topics to give you the best guidance possible! Whether you need workout routines, nutrition advice, meal planning, or health tips, I'm here to help. What aspect of your fitness journey can I assist with today?"
            yield sse_escape(friendly_msg)
            await mem.add(user_id, "user", text.strip())
            await mem.add(user_id, "assistant", friendly_msg)
            yield "event: done\ndata: [DONE]\n\n"
        return StreamingResponse(_friendly_redirect(), media_type="text/event-stream",
                                 headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})
    
    elif is_greeting_or_personal:
        async def _greeting_response():
            if text_clean in ['hi', 'hello', 'hey', 'good morning', 'good afternoon', 'good evening']:
                friendly_msg = "Hello! I'm Kyra, your friendly fitness assistant. I'm here to help you with workouts, nutrition, diet planning, and all things fitness. What would you like to work on today?"
            elif any(phrase in text_clean for phrase in ['how are you', 'how do you do']):
                friendly_msg = "I'm doing great, thanks for asking! I'm excited to help you with your fitness journey. Whether you need workout plans, diet advice, or nutrition guidance, I'm here for you. What can I help you with?"
            elif any(phrase in text_clean for phrase in ['your name', 'who are you', 'what are you', 'introduce yourself']):
                friendly_msg = "I'm Kyra, your dedicated fitness companion! I specialize in helping people achieve their health and fitness goals through personalized workout plans, nutrition guidance, and wellness tips. How can I support your fitness journey?"
            else:
                friendly_msg = "Hi there! I'm Kyra, and I'm passionate about helping you with fitness, nutrition, and wellness. Whether you want to build muscle, lose weight, plan meals, or just get healthier, I'm here to guide you. What fitness goal are you working towards?"
            
            yield sse_escape(friendly_msg)
            await mem.add(user_id, "user", text.strip())
            await mem.add(user_id, "assistant", friendly_msg)
            yield "event: done\ndata: [DONE]\n\n"
        return StreamingResponse(_greeting_response(), media_type="text/event-stream",
                                 headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

    # For all other queries (fitness, nutrition, supplements, etc.), proceed with normal AI chat
    try:
        is_meta = is_fittbot_meta_query(text)
        is_plan = is_plan_request(text)

        await mem.add(user_id, "user", text.strip())

        if is_plan:
            msgs, _ = await build_messages(user_id, text.strip(), use_context=True, oai=oai, mem=mem,
                                           context_only=False, k=TOP_K)
            msgs.insert(1, {"role": "system", "content": STYLE_PLAN})
            temperature = 0
        else:
            msgs, _ = await build_messages(user_id, text.strip(), use_context=True, oai=oai, mem=mem,
                                           context_only=is_meta, k=8 if is_meta else TOP_K)
            msgs.insert(1, {"role": "system", "content": STYLE_CHAT_FORMAT})
            temperature = 0

        # Queue OpenAI call to Celery (non-blocking)
        from app.tasks.chatbot_tasks import process_chat_message
        import asyncio
        from celery.result import AsyncResult

        # Pass complete messages array (preserves all context, styling, KB data)
        task = process_chat_message.delay(
            user_id=user_id,
            messages=msgs,
            model=OPENAI_MODEL,
            temperature=temperature
        )

        # Wait for result (async polling, doesn't block event loop)
        max_wait = 120
        poll_interval = 0.5
        elapsed = 0

        while elapsed < max_wait:
            celery_task = AsyncResult(task.id)
            if celery_task.ready():
                if celery_task.successful():
                    result = celery_task.result
                    content = result.get("message", "")
                    break
                else:
                    raise HTTPException(500, f"Chat task failed: {celery_task.info}")
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        else:
            raise HTTPException(504, "Chat request timed out")

        content = content.strip()

        # Fix brand name confusion - Kyra is AI, Fittbot is app
        content = re.sub(r'\bfit\s*bot\b|\bfit+bot\b', 'Fittbot', content, flags=re.I)
        content = re.sub(r'\bfitbot\b', 'Fittbot', content, flags=re.I)

        # Don't replace Kyra with Fittbot when talking about the AI
        # But do replace wrong usage like "About Kyra" when user asks about Fittbot
        if 'fittbot' in text.lower() and 'about kyra' in content.lower():
            content = re.sub(r'\bAbout Kyra\b', 'About Fittbot', content, flags=re.I)
            content = re.sub(r'\bKyra is a comprehensive fitness app\b', 'Fittbot is a comprehensive fitness app', content, flags=re.I)
            content = re.sub(r'\bKyra is perfect for\b', 'Fittbot is perfect for', content, flags=re.I)

        content = re.sub(r'Would you like to log more foods.*?\?.*?🍏?', '', content, flags=re.I | re.DOTALL)
        content = re.sub(r'Let me know.*?log.*?for you.*?🍏?', '', content, flags=re.I | re.DOTALL)
        content = re.sub(r'Do you want.*?log.*?\?', '', content, flags=re.I)

        pretty = pretty_plan(content)
        async def _one_shot():
            try:
                yield sse_escape(pretty)
                await mem.add(user_id, "assistant", pretty)
                yield "event: done\ndata: [DONE]\n\n"
            except Exception as stream_error:
                print(f"Error in general chat stream: {stream_error}")
                yield sse_escape("I'm having trouble completing this response. Please try again!")
                yield "event: done\ndata: [DONE]\n\n"
        return StreamingResponse(_one_shot(), media_type="text/event-stream",
                                 headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})
    except Exception as e:
        print(f"Error in general chat processing: {e}")
        import traceback
        print(f"General chat error traceback: {traceback.format_exc()}")

        # Provide fallback response and clear any pending state
        async def _general_chat_error():
            error_msg = "I'm having trouble processing your request right now. Please try again, and I'll do my best to help you with your fitness journey!"
            try:
                await mem.add(user_id, "user", text.strip())
                await mem.add(user_id, "assistant", error_msg)
            except:
                pass  # Don't let memory errors cascade
            yield sse_escape(error_msg)
            yield "event: done\ndata: [DONE]\n\n"

        return StreamingResponse(_general_chat_error(), media_type="text/event-stream",
                                 headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

# Keep all existing endpoints
@router.post("/kb/upsert")
async def kb_upsert(inp: KBUpsertIn):
    return {"added_chunks": KB.upsert(inp.source, inp.text)}

@router.post("/kb/search")
async def kb_search(inp: KBSearchIn):
    return {"hits": KB.search(inp.query, k=inp.k)}

@router.post("/kb/upsert_file")
async def kb_upsert_file(
    src: str = Depends(lambda: "upload"),
    file: UploadFile = File(...),
):
    data = await file.read()
    if file.filename.endswith(".pdf"):
        from pypdf import PdfReader
        text = "\n".join(p.extract_text() or "" for p in PdfReader(io.BytesIO(data)).pages)
    elif file.filename.endswith((".docx", ".doc")):
        from docx import Document
        text = "\n".join(p.text for p in Document(io.BytesIO(data)).paragraphs)
    else:
        text = text.decode("utf-8", "ignore")
    return {"added_chunks": KB.upsert(src or file.filename, text)}

@router.post("/voice/transcribe")
async def voice_transcribe(
    user_id: int,
    audio: UploadFile = File(...),
    http = Depends(get_http),
    oai = Depends(get_oai),
):
    """Transcribe audio with Groq and translate to English - uses Celery queue"""
    # Queue transcription + translation to Celery worker (non-blocking)
    from app.tasks.voice_tasks import transcribe_and_translate
    from celery.result import AsyncResult
    import asyncio

    # Read audio bytes
    audio_bytes = await audio.read()

    # Queue to Celery with "general" context
    task = transcribe_and_translate.delay(
        user_id=user_id,
        audio_bytes=audio_bytes,
        context="general"
    )

    # Wait for result (non-blocking for FastAPI)
    max_wait = 60  # 1 minute timeout
    poll_interval = 0.3
    elapsed = 0

    while elapsed < max_wait:
        celery_task = AsyncResult(task.id)

        if celery_task.ready():
            if celery_task.successful():
                result = celery_task.result

                # Return in same format as before (no business logic change)
                return {
                    "transcript": result.get("english", ""),
                    "lang": result.get("lang", "unknown"),
                    "english": result.get("english", ""),
                }
            else:
                # Task failed
                raise HTTPException(500, f"Transcription failed: {str(celery_task.info)}")

        # Wait before next poll
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    # Timeout
    raise HTTPException(504, "Transcription timed out")

@router.post("/voice/stream_test")
async def voice_stream_sse(
    user_id: int,
    audio: UploadFile = File(...),
    mem = Depends(get_mem),
    oai = Depends(get_oai),
    http = Depends(get_http),
):
    """Voice chat stream - uses Celery for transcription"""
    # Queue transcription to Celery (non-blocking)
    from app.tasks.voice_tasks import transcribe_and_translate
    from celery.result import AsyncResult
    import asyncio

    # Read audio bytes
    audio_bytes = await audio.read()

    # Queue transcription to Celery
    transcribe_task = transcribe_and_translate.delay(
        user_id=user_id,
        audio_bytes=audio_bytes,
        context="general"
    )

    # Wait for transcription result
    max_wait = 60
    poll_interval = 0.3
    elapsed = 0

    transcript = None
    while elapsed < max_wait:
        celery_task = AsyncResult(transcribe_task.id)
        if celery_task.ready():
            if celery_task.successful():
                result = celery_task.result
                transcript = result.get("english", "")
                break
            else:
                raise HTTPException(500, f"Transcription failed: {str(celery_task.info)}")
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    if not transcript:
        raise HTTPException(400, "empty transcript")

    await mem.add(user_id, "user", transcript)
    msgs, _ = await build_messages(user_id, transcript, use_context=True, oai=oai, mem=mem)

    # Queue voice chat to Celery (non-blocking)
    from app.tasks.chatbot_tasks import process_chat_message
    import asyncio
    from celery.result import AsyncResult

    # Pass complete messages array (preserves all context)
    task = process_chat_message.delay(
        user_id=user_id,
        messages=msgs,
        model=OPENAI_MODEL,
        temperature=0
    )

    async def token_iter():
        # Wait for Celery result
        max_wait = 120
        poll_interval = 0.5
        elapsed = 0

        while elapsed < max_wait:
            celery_task = AsyncResult(task.id)
            if celery_task.ready():
                if celery_task.successful():
                    result = celery_task.result
                    content = result.get("message", "")
                    # Yield the complete response
                    yield sse_escape(content)
                    # Save to memory
                    await mem.add(user_id, "assistant", content.strip())
                    yield "event: done\ndata: [DONE]\n\n"
                    return
                else:
                    error_msg = "I'm having trouble processing your voice message. Please try again!"
                    yield sse_escape(error_msg)
                    yield "event: done\ndata: [DONE]\n\n"
                    return
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        # Timeout
        yield sse_escape("Request timed out. Please try again!")
        yield "event: done\ndata: [DONE]\n\n"

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no"
    }
    return StreamingResponse(token_iter(), media_type="text/event-stream; charset=utf-8", headers=headers)

class Userid(BaseModel):
    user_id: int

@router.post("/delete_chat")
async def chat_close(
    req: Userid,
    mem = Depends(get_mem),
):
    print(f"Deleting chat history for user {req.user_id}")
    history_key = f"chat:{req.user_id}:history"
    pending_key = f"chat:{req.user_id}:pending"
    deleted = await mem.r.delete(history_key, pending_key)
    return {"status": 200}

@router.delete("/kb/clear")
async def kb_clear():
    """Clear all KB content completely"""
    initial_count = len(KB.texts)
    KB.texts.clear()
    return {
        "status": "cleared", 
        "cleared_chunks": initial_count,
        "remaining_chunks": len(KB.texts)
    }

@router.get("/kb/status")
async def kb_status():
    """Check current KB status"""
    return {
        "total_chunks": len(KB.texts),
        "kb_empty": len(KB.texts) == 0
    }