# app/tasks/workout_tasks.py
"""
Celery tasks for workout template generation and analysis
Production-ready with error handling and progress updates
"""
import json
import asyncio
import logging
from celery import current_task
from app.celery_app import celery_app
from app.utils.openai_pool import get_openai_client
from app.utils.redis_config import get_redis_sync
from app.models.database import get_db_sync

# Production logging configuration
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
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


@celery_app.task(bind=True, name="app.tasks.workout_tasks.generate_workout_template")
def generate_workout_template(
    self,
    user_id: int,
    audio_bytes: bytes = None,
    text_prompt: str = None,
    user_profile: dict = None
):
    """
    Generate workout template from voice or text

    Args:
        user_id: Client ID
        audio_bytes: Audio file bytes (optional)
        text_prompt: Text prompt (optional)
        user_profile: User profile data (goals, experience, etc.)

    Returns:
        dict: Generated workout template
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 10,
            "message": "Processing your request..."
        })

        logger.info(f"Task {task_id}: Generating workout template for user {user_id}")

        # Step 1: Transcribe audio if provided
        transcript = None
        if audio_bytes:
            publish_progress(task_id, {
                "status": "progress",
                "progress": 20,
                "message": "Transcribing audio..."
            })

            # Use Groq for transcription
            from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.asr import (
                get_groq_api_key, GROQ_BASE_URL, GROQ_ASR_MODEL
            )

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

            logger.debug(f"Task {task_id}: Voice transcript received - '{transcript[:50]}...'")

        # Use text prompt or transcript
        prompt = transcript or text_prompt
        if not prompt:
            raise ValueError("No prompt provided (neither audio nor text)")

        # Step 2: Generate workout template with OpenAI
        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Generating workout template..."
        })

        oai_client = get_openai_client()

        # Import workout generation logic
        from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.workout_llm_helper import (
            llm_generate_template_from_profile
        )

        # Run async function in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Generate template (this is a placeholder - you'll need to adapt based on your actual function)
        # For now, just make a simple OpenAI call
        from app.utils.async_openai import async_openai_call

        messages = [
            {"role": "system", "content": "You are a professional fitness trainer. Generate a detailed workout template based on the user's request."},
            {"role": "user", "content": f"User request: {prompt}\n\nUser profile: {json.dumps(user_profile or {})}\n\nGenerate a structured workout plan."}
        ]

        response = loop.run_until_complete(
            async_openai_call(
                oai_client,
                model="gpt-4o-mini",
                messages=messages,
                stream=False,
                temperature=0.7
            )
        )

        content = (response.choices[0].message.content or "").strip()

        loop.close()

        publish_progress(task_id, {
            "status": "progress",
            "progress": 90,
            "message": "Finalizing template..."
        })

        result = {
            "status": "completed",
            "progress": 100,
            "result": {
                "type": "workout_template",
                "template": content,
                "prompt": prompt
            }
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Workout template generated successfully for user {user_id}")
        return result["result"]

    except Exception as e:
        logger.error(f"Task {task_id}: Task failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\\n{traceback.format_exc()}")

        error_result = {
            "status": "error",
            "progress": 0,
            "result": {
                "type": "error",
                "message": f"Failed to generate workout template: {str(e)}"
            }
        }
        publish_progress(task_id, error_result)

        raise


@celery_app.task(bind=True, name="app.tasks.workout_tasks.analyze_workout_intent")
def analyze_workout_intent(
    self,
    user_id: int,
    user_input: str,
    conversation_context: dict = None
):
    """
    Analyze user intent for workout template chatbot using Celery queue

    Args:
        user_id: Client ID
        user_input: User's message text
        conversation_context: Current conversation state and context

    Returns:
        dict: Intent analysis result
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Analyzing intent..."
        })

        logger.info(f"Task {task_id}: Analyzing workout intent for user {user_id}")

        context = conversation_context or {}

        system_prompt = """You are an AI assistant helping users create workout templates. Analyze the user's input and determine their intent.

Available intents:
- "create": User wants to create a new workout template
- "show": User wants to see their existing template
- "edit": User wants to modify their template
- "save": User wants to save their template
- "yes": User is agreeing/confirming something
- "no": User is disagreeing/declining something
- "specify_days": User is specifying number of workout days
- "specify_names": User is providing day names/titles
- "ask_question": User is asking a question
- "unclear": Intent is unclear

Additional information to extract:
- days_count: If user mentions number of days (1-7)
- day_names: If user provides specific day names
- muscle_groups: Any muscle groups mentioned
- positive_sentiment: true/false if response seems positive
- negative_sentiment: true/false if response seems negative
- exercise_requests: Any specific exercises mentioned

Handle typos, variations, and natural language. Be flexible and understanding.

Respond in JSON format with: intent, confidence (0-1), days_count, day_names (array), muscle_groups (array), positive_sentiment, negative_sentiment, exercise_requests (array), reasoning"""

        user_prompt = f"""User input: "{user_input}"

Context:
Current conversation state: {context.get('state', 'unknown')}
Has existing template: {bool(context.get('template'))}
Profile info: {context.get('profile', {})}

Analyze this input and determine what the user wants to do."""

        oai_client = get_openai_client()

        # Run async OpenAI call
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        from app.utils.async_openai import async_openai_call

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Processing with AI..."
        })

        response = loop.run_until_complete(
            async_openai_call(
                oai_client,
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                stream=False,
                temperature=0.1
            )
        )

        content = (response.choices[0].message.content or "").strip()
        loop.close()

        # Parse JSON response
        try:
            parsed_result = json.loads(content)
        except json.JSONDecodeError:
            logger.warning(f"Task {task_id}: Failed to parse AI response as JSON: {content[:100]}")
            parsed_result = {}

        # Ensure all expected fields are present with defaults
        final_result = {
            "intent": parsed_result.get("intent", "unclear"),
            "confidence": float(parsed_result.get("confidence", 0.0)),
            "days_count": parsed_result.get("days_count"),
            "day_names": parsed_result.get("day_names", []),
            "muscle_groups": parsed_result.get("muscle_groups", []),
            "positive_sentiment": parsed_result.get("positive_sentiment", False),
            "negative_sentiment": parsed_result.get("negative_sentiment", False),
            "exercise_requests": parsed_result.get("exercise_requests", []),
            "reasoning": parsed_result.get("reasoning", "")
        }

        # Fallback: If AI fails to detect intent, use keyword matching
        if final_result["intent"] == "unclear" or final_result["confidence"] < 0.3:
            user_lower = user_input.lower().strip()
            if any(word in user_lower for word in ["save", "save it", "save template"]):
                final_result["intent"] = "save"
                final_result["confidence"] = 0.9
                final_result["positive_sentiment"] = True
            elif any(word in user_lower for word in ["yes", "yeah", "yep", "ok", "okay"]):
                final_result["intent"] = "yes"
                final_result["confidence"] = 0.8
                final_result["positive_sentiment"] = True
            elif any(word in user_lower for word in ["no", "nope", "nah"]):
                final_result["intent"] = "no"
                final_result["confidence"] = 0.8
                final_result["negative_sentiment"] = True

        result = {
            "status": "completed",
            "progress": 100,
            "result": final_result
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Intent analysis completed for user {user_id} - intent={final_result['intent']}, confidence={final_result['confidence']}")
        return final_result

    except Exception as e:
        logger.error(f"Task {task_id}: Intent analysis failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Return fallback result instead of raising
        fallback_result = {
            "intent": "unclear",
            "confidence": 0.0,
            "days_count": None,
            "day_names": [],
            "muscle_groups": [],
            "positive_sentiment": False,
            "negative_sentiment": False,
            "exercise_requests": [],
            "reasoning": f"Failed to analyze: {e}, used keyword fallback"
        }

        error_result = {
            "status": "completed",  # Still return result so chatbot can continue
            "progress": 100,
            "result": fallback_result
        }
        publish_progress(task_id, error_result)

        return fallback_result


@celery_app.task(bind=True, name="app.tasks.workout_tasks.determine_workout_flow")
def determine_workout_flow(
    self,
    user_id: int,
    user_input: str,
    current_state: str,
    context: dict = None
):
    """
    Determine conversation flow for workout template chatbot using Celery queue

    Args:
        user_id: Client ID
        user_input: User's message text
        current_state: Current conversation state
        context: Conversation context

    Returns:
        dict: Flow decision with next_state
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Determining flow..."
        })

        logger.info(f"Task {task_id}: Determining workout flow for user {user_id}, state={current_state}")

        context = context or {}

        system_prompt = """You are managing a workout template creation conversation. Based on the user input and current context, determine what should happen next.

Available states:
- "FETCH_PROFILE": Get user's fitness profile and show it to them
- "PROFILE_CONFIRMATION": Show profile and ask for confirmation
- "ASK_DAYS": Ask how many workout days per week
- "ASK_NAMES": Ask for day names/titles
- "DRAFT_GENERATION": Create the workout template
- "SHOW_TEMPLATE": Display the current template
- "EDIT_TEMPLATE": User wants to edit the template
- "SAVE_TEMPLATE": Save the current template
- "DONE": Conversation complete

Current state: {current_state}

Rules:
1. If user says number (1-7), they're specifying days count -> ASK_NAMES
2. If user provides names/themes, they want day names -> DRAFT_GENERATION
3. If user says "show", "display", "view" -> SHOW_TEMPLATE
4. If user says "edit", "change", "modify" -> EDIT_TEMPLATE
5. If user says "save", "done", "finish" -> SAVE_TEMPLATE
6. If user says "yes", "ok" for confirmation -> Move to next logical state
7. If user says "no", "cancel" -> Stay or go back

Respond with JSON: {{"next_state": "STATE_NAME", "reasoning": "why"}}"""

        user_prompt = f"""Current state: {current_state}
User input: "{user_input}"
Has template: {bool(context.get('template'))}
Profile: {context.get('profile', {})}

What should be the next state?"""

        oai_client = get_openai_client()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        from app.utils.async_openai import async_openai_call

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Processing flow decision..."
        })

        response = loop.run_until_complete(
            async_openai_call(
                oai_client,
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                stream=False,
                temperature=0.1
            )
        )

        content = (response.choices[0].message.content or "").strip()
        loop.close()

        # Parse JSON response
        try:
            parsed_result = json.loads(content)
        except json.JSONDecodeError:
            logger.warning(f"Task {task_id}: Failed to parse flow response as JSON: {content[:100]}")
            parsed_result = {"next_state": current_state, "reasoning": "Parse error, staying in current state"}

        final_result = {
            "next_state": parsed_result.get("next_state", current_state),
            "reasoning": parsed_result.get("reasoning", "")
        }

        result = {
            "status": "completed",
            "progress": 100,
            "result": final_result
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Flow decision completed for user {user_id} - next_state={final_result['next_state']}")
        return final_result

    except Exception as e:
        logger.error(f"Task {task_id}: Flow decision failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Return current state as fallback
        fallback_result = {
            "next_state": current_state,
            "reasoning": f"Error: {e}, staying in current state"
        }

        error_result = {
            "status": "completed",
            "progress": 100,
            "result": fallback_result
        }
        publish_progress(task_id, error_result)

        return fallback_result


@celery_app.task(bind=True, name="app.tasks.workout_tasks.generate_day_names")
def generate_day_names(
    self,
    user_id: int,
    user_request: str,
    days_count: int
):
    """
    Generate creative day names for workout template using Celery queue

    Args:
        user_id: Client ID
        user_request: User's naming request/theme
        days_count: Number of days to name

    Returns:
        list: Generated day names
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Generating day names..."
        })

        logger.info(f"Task {task_id}: Generating {days_count} day names for user {user_id}")

        system_prompt = f"""You are a creative fitness coach. Generate exactly {days_count} unique, motivating names for workout days based on the user's request.

Rules:
1. Generate exactly {days_count} names
2. Names should be single words only (no spaces)
3. Names should be appropriate for workout days
4. Make them fun and motivating
5. Follow the user's theme/request as closely as possible

Return ONLY a JSON array of strings, nothing else.

Examples:
- User: "animal names" → ["Lion", "Tiger", "Bear", "Wolf", "Eagle"]
- User: "king names" → ["Arthur", "Alexander", "Napoleon", "Caesar", "Viking"]
- User: "superhero names" → ["Thor", "Hulk", "Superman", "Captain", "Storm"]
- User: "first 2 days Lion and Tiger" → ["Lion", "Tiger", "Day3", "Day4", "Day5"]"""

        oai_client = get_openai_client()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        from app.utils.async_openai import async_openai_call

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Creating creative names..."
        })

        response = loop.run_until_complete(
            async_openai_call(
                oai_client,
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"User request: '{user_request}' for {days_count} workout days"}
                ],
                stream=False,
                temperature=0.7
            )
        )

        content = (response.choices[0].message.content or "").strip()
        loop.close()

        # Parse JSON array
        try:
            day_names = json.loads(content)
            if isinstance(day_names, list) and len(day_names) == days_count:
                day_names = [str(name).title() for name in day_names]
            else:
                day_names = [f"Day{i+1}" for i in range(days_count)]
        except json.JSONDecodeError:
            logger.warning(f"Task {task_id}: Failed to parse day names response: {content[:100]}")
            day_names = [f"Day{i+1}" for i in range(days_count)]

        result = {
            "status": "completed",
            "progress": 100,
            "result": day_names
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Day names generated for user {user_id}: {day_names}")
        return day_names

    except Exception as e:
        logger.error(f"Task {task_id}: Day name generation failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Return default names as fallback
        fallback_names = [f"Day{i+1}" for i in range(days_count)]

        result = {
            "status": "completed",
            "progress": 100,
            "result": fallback_names
        }
        publish_progress(task_id, result)

        return fallback_names


@celery_app.task(bind=True, name="app.tasks.workout_tasks.detect_edit_intent_type")
def detect_edit_intent_type(
    self,
    user_id: int,
    user_input: str
):
    """
    Detect the type of edit intent from user input using Celery queue

    Args:
        user_id: Client ID
        user_input: User's edit request

    Returns:
        str: Intent type (BULK_RENAME, INDIVIDUAL_RENAME, EXERCISE_CHANGE)
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Detecting edit intent..."
        })

        logger.info(f"Task {task_id}: Detecting edit intent for user {user_id}")

        prompt = f"""Analyze this workout template edit request and respond with EXACTLY one of these:
- BULK_RENAME (if user wants to rename ALL days with a theme like "animal names", "superhero names")
- INDIVIDUAL_RENAME (if user wants to rename a SPECIFIC day like "rename day 1 to X")
- EXERCISE_CHANGE (if user wants to add, remove, or modify exercises)

User request: "{user_input}"

Examples:
- "Change day 1 name as spiderman" → INDIVIDUAL_RENAME
- "Give all days animal names" → BULK_RENAME
- "Add more chest exercises" → EXERCISE_CHANGE
- "Rename day 2 to batman" → INDIVIDUAL_RENAME
- "Remove squats" → EXERCISE_CHANGE

Response:"""

        oai_client = get_openai_client()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        from app.utils.async_openai import async_openai_call

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Analyzing edit type..."
        })

        response = loop.run_until_complete(
            async_openai_call(
                oai_client,
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                stream=False,
                temperature=0.1
            )
        )

        content = (response.choices[0].message.content or "").strip().upper()
        loop.close()

        # Validate response is one of expected values
        if content not in ["BULK_RENAME", "INDIVIDUAL_RENAME", "EXERCISE_CHANGE"]:
            content = "EXERCISE_CHANGE"  # Default fallback

        result = {
            "status": "completed",
            "progress": 100,
            "result": content
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Edit intent detected for user {user_id}: {content}")
        return content

    except Exception as e:
        logger.error(f"Task {task_id}: Edit intent detection failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Return default fallback
        result = {
            "status": "completed",
            "progress": 100,
            "result": "EXERCISE_CHANGE"
        }
        publish_progress(task_id, result)

        return "EXERCISE_CHANGE"


@celery_app.task(bind=True, name="app.tasks.workout_tasks.generate_contextual_response")
def generate_contextual_response(
    self,
    user_id: int,
    conversation_state: str,
    user_input: str,
    context: dict = None
):
    """
    Generate natural, contextual responses for workout conversations using Celery queue

    Args:
        user_id: Client ID
        conversation_state: Current conversation state
        user_input: User's message
        context: Conversation context

    Returns:
        str: Generated response message
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Generating response..."
        })

        logger.info(f"Task {task_id}: Generating contextual response for user {user_id}")

        context = context or {}

        system_prompt = """You are a friendly, encouraging fitness assistant helping users create workout templates.

Generate natural, conversational responses that:
1. Acknowledge what the user said
2. Provide clear guidance on next steps
3. Stay encouraging and positive
4. Handle typos and unclear input gracefully
5. Ask clarifying questions when needed

Keep responses concise but warm. Use emojis sparingly and appropriately."""

        context_info = {
            "state": conversation_state,
            "has_profile": bool(context.get("profile")),
            "has_template": bool(context.get("template")),
            "user_info": context.get("profile", {})
        }

        user_prompt = f"""Current conversation state: {conversation_state}
User just said: "{user_input}"
Context: {json.dumps(context_info)}

Generate an appropriate response for this situation."""

        oai_client = get_openai_client()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        from app.utils.async_openai import async_openai_call

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Creating response..."
        })

        response = loop.run_until_complete(
            async_openai_call(
                oai_client,
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                stream=False,
                temperature=0.7
            )
        )

        content = (response.choices[0].message.content or "").strip()
        loop.close()

        result = {
            "status": "completed",
            "progress": 100,
            "result": content
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Contextual response generated for user {user_id}")
        return content

    except Exception as e:
        logger.error(f"Task {task_id}: Contextual response failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Return fallback message
        fallback = "I'm here to help! What would you like to do with your workout template?"

        result = {
            "status": "completed",
            "progress": 100,
            "result": fallback
        }
        publish_progress(task_id, result)

        return fallback


@celery_app.task(bind=True, name="app.tasks.workout_tasks.analyze_workout_log")
def analyze_workout_log(
    self,
    user_id: int,
    text_query: str,
    workout_data: list = None
):
    """
    Analyze workout logs with OpenAI

    Args:
        user_id: Client ID
        text_query: User's question about their workouts
        workout_data: Recent workout data for analysis

    Returns:
        dict: Analysis and recommendations
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Analyzing your workout data..."
        })

        logger.info(f"Task {task_id}: Analyzing workout log for user {user_id}")

        oai_client = get_openai_client()

        # Build analysis prompt
        workout_context = ""
        if workout_data:
            workout_context = f"\n\nRecent Workout Data:\n{json.dumps(workout_data, indent=2)}"

        messages = [
            {
                "role": "system",
                "content": "You are a professional fitness coach analyzing workout performance. Provide insights, identify patterns, and suggest improvements."
            },
            {
                "role": "user",
                "content": f"User question: {text_query}{workout_context}"
            }
        ]

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Generating insights..."
        })

        # Run async OpenAI call
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        from app.utils.async_openai import async_openai_call

        response = loop.run_until_complete(
            async_openai_call(
                oai_client,
                model="gpt-4o-mini",
                messages=messages,
                stream=False,
                temperature=0.7
            )
        )

        content = (response.choices[0].message.content or "").strip()

        loop.close()

        result = {
            "status": "completed",
            "progress": 100,
            "result": {
                "type": "workout_analysis",
                "analysis": content,
                "query": text_query
            }
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Workout analysis completed successfully for user {user_id}")
        return result["result"]

    except Exception as e:
        logger.error(f"Task {task_id}: Task failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\\n{traceback.format_exc()}")

        error_result = {
            "status": "error",
            "progress": 0,
            "result": {
                "type": "error",
                "message": f"Failed to analyze workout: {str(e)}"
            }
        }
        publish_progress(task_id, error_result)

        raise


# ===== Workout Log Exercise Extraction Tasks =====

@celery_app.task(bind=True, name="app.tasks.workout_tasks.extract_exercises")
def extract_exercises(
    self,
    user_id: int,
    text: str
):
    """
    Extract exercise names from user input text

    Args:
        user_id: Client ID
        text: User's workout text input

    Returns:
        list: List of exercise names
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Extracting exercises..."
        })

        logger.info(f"Task {task_id}: Extracting exercises for user {user_id}")

        prompt = f"""
        Extract exercise names from this text: "{text}"

        RULES:
        1. Extract ANY exercise or physical activity mentioned
        2. Handle common misspellings and variations
        3. Normalize exercise names to standard form
        4. Be very permissive - if it sounds like exercise, include it

        EXAMPLES:
        - "pushup" → "Push Up"
        - "dumbell bench pres" → "Dumbbell Bench Press"
        - "bicep curls" → "Bicep Curl"
        - "squats and lunges" → ["Squat", "Lunge"]

        Return JSON:
        {{
            "exercises": ["Exercise Name 1", "Exercise Name 2"]
        }}
        """

        oai_client = get_openai_client()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        from app.utils.async_openai import async_openai_call

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Processing with AI..."
        })

        response = loop.run_until_complete(
            async_openai_call(
                oai_client,
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an exercise recognition expert. Extract and normalize exercise names."},
                    {"role": "user", "content": prompt}
                ],
                stream=False,
                temperature=0.1
            )
        )

        content = (response.choices[0].message.content or "").strip()
        loop.close()

        # Clean JSON formatting
        import re
        content = re.sub(r"^```json\s*", "", content)
        content = re.sub(r"\s*```$", "", content)

        try:
            parsed = json.loads(content)
            exercises = parsed.get("exercises", [])
            exercises = [ex.strip() for ex in exercises if ex.strip()]
        except json.JSONDecodeError:
            logger.warning(f"Task {task_id}: Failed to parse exercises response: {content[:100]}")
            exercises = []

        result = {
            "status": "completed",
            "progress": 100,
            "result": exercises
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Extracted {len(exercises)} exercises for user {user_id}")
        return exercises

    except Exception as e:
        logger.error(f"Task {task_id}: Exercise extraction failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        result = {
            "status": "completed",
            "progress": 100,
            "result": []
        }
        publish_progress(task_id, result)

        return []


@celery_app.task(bind=True, name="app.tasks.workout_tasks.extract_exercises_with_details")
def extract_exercises_with_details(
    self,
    user_id: int,
    text: str
):
    """
    Extract exercises with sets/reps/duration details from user input

    Args:
        user_id: Client ID
        text: User's workout text input

    Returns:
        list: List of dicts with exercise details
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Extracting exercise details..."
        })

        logger.info(f"Task {task_id}: Extracting exercise details for user {user_id}")

        prompt = f"""
        Parse workout information from this text: "{text}"

        CRITICAL PARSING RULES:
        - Intensity words (low, light, moderate, medium, high, hard, intense, heavy) belong to the EXERCISE they modify
        - Duration belongs to the exercise it comes after
        - NEVER create separate exercises from intensity or duration words
        - Each exercise object should represent ONE actual physical exercise
        - Format variations: "3x10", "3*10", "3 sets of 10" all mean 3 sets, 10 reps

        Extract ALL exercises mentioned with their details including sets, reps, duration, AND intensity.

        EXAMPLES:
        - "I did pushups and pullups" → [{{"exercise": "Push Up", "has_sets_reps": false, "intensity": null, "has_duration": false, "duration_minutes": null}}, {{"exercise": "Pull Up", "has_sets_reps": false, "intensity": null, "has_duration": false, "duration_minutes": null}}]
        - "I did 3 sets of pushups" → [{{"exercise": "Push Up", "has_sets_reps": true, "sets": 3, "reps": null, "intensity": null, "has_duration": false, "duration_minutes": null}}]
        - "I did 3x10 pushups" → [{{"exercise": "Push Up", "has_sets_reps": true, "sets": 3, "reps": 10, "intensity": null, "has_duration": false, "duration_minutes": null}}]
        - "I did 3*10 pushups" → [{{"exercise": "Push Up", "has_sets_reps": true, "sets": 3, "reps": 10, "intensity": null, "has_duration": false, "duration_minutes": null}}]
        - "I did 3x10 heavy pushups" → [{{"exercise": "Push Up", "has_sets_reps": true, "sets": 3, "reps": 10, "intensity": "high", "has_duration": false, "duration_minutes": null}}]
        - "I did 3*10 low intensity pushups for 3 minutes" → [{{"exercise": "Push Up", "has_sets_reps": true, "sets": 3, "reps": 10, "intensity": "low", "has_duration": true, "duration_minutes": 3}}]
        - "pushup 3*10 low intensity 3min" → [{{"exercise": "Push Up", "has_sets_reps": true, "sets": 3, "reps": 10, "intensity": "low", "has_duration": true, "duration_minutes": 3}}]
        - "I did 3x10 pushups and 4x8 moderate pullups" → [{{"exercise": "Push Up", "has_sets_reps": true, "sets": 3, "reps": 10, "intensity": null, "has_duration": false, "duration_minutes": null}}, {{"exercise": "Pull Up", "has_sets_reps": true, "sets": 4, "reps": 8, "intensity": "moderate", "has_duration": false, "duration_minutes": null}}]
        - "Light jog for 30 minutes" → [{{"exercise": "Running", "has_duration": true, "duration_minutes": 30, "intensity": "low", "has_sets_reps": false, "sets": null, "reps": null}}]
        - "30 minutes of intense running" → [{{"exercise": "Running", "has_duration": true, "duration_minutes": 30, "intensity": "high", "has_sets_reps": false, "sets": null, "reps": null}}]

        Return JSON array:
        [
            {{
                "exercise": "Exercise Name",
                "has_sets_reps": true/false,
                "sets": number or null,
                "reps": number or null,
                "has_duration": true/false,
                "duration_minutes": number or null,
                "intensity": "low/moderate/high" or null
            }}
        ]

        INTENSITY KEYWORDS:
        - LOW INTENSITY: low, light, easy, gentle, slow
        - MODERATE INTENSITY: moderate, medium, normal, steady, medium
        - HIGH INTENSITY: high, hard, intense, heavy, vigorous, tough

        RULES:
        - MOST IMPORTANT: Intensity and duration MODIFY exercises, they are NOT separate exercises
        - If sets or reps are mentioned for an exercise, set has_sets_reps to true
        - If only sets mentioned, include sets but set reps to null
        - If only reps mentioned, set sets to null
        - For cardio exercises with duration, set has_duration to true
        - Extract intensity words mentioned with exercises (heavy pushups, light jog, intense running)
        - Handle both "3x10" and "3*10" formats for sets and reps
        - Parse durations mentioned as "3min", "3 min", "3 minutes", "3 min" as duration_minutes: 3
        - When ALL details are provided (sets, reps, intensity, duration), set all relevant fields to true
        - For strength exercises with both sets/reps AND duration, include both sets_reps_data and duration_minutes
        - NEVER return "Low Intensity" or "High Intensity" as an exercise name - these are modifiers
        - If no sets/reps/duration mentioned, set has_sets_reps and has_duration to false
        - If no intensity mentioned, set intensity to null
        """

        oai_client = get_openai_client()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        from app.utils.async_openai import async_openai_call

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Processing workout details..."
        })

        response = loop.run_until_complete(
            async_openai_call(
                oai_client,
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an exercise recognition expert. Extract exercises and their sets/reps/duration details."},
                    {"role": "user", "content": prompt}
                ],
                stream=False,
                temperature=0.1
            )
        )

        content = (response.choices[0].message.content or "").strip()
        loop.close()

        # Clean JSON formatting
        import re
        content = re.sub(r"^```json\s*", "", content)
        content = re.sub(r"\s*```$", "", content)

        try:
            parsed = json.loads(content)
            if not isinstance(parsed, list):
                parsed = [parsed]
        except json.JSONDecodeError:
            logger.warning(f"Task {task_id}: Failed to parse exercise details: {content[:100]}")
            parsed = []

        result = {
            "status": "completed",
            "progress": 100,
            "result": parsed
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Extracted details for {len(parsed)} exercises for user {user_id}")
        return parsed

    except Exception as e:
        logger.error(f"Task {task_id}: Exercise details extraction failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        result = {
            "status": "completed",
            "progress": 100,
            "result": []
        }
        publish_progress(task_id, result)

        return []


@celery_app.task(bind=True, name="app.tasks.workout_tasks.parse_sets_reps")
def parse_sets_reps(
    self,
    user_id: int,
    text: str,
    exercise_name: str = ""
):
    """
    Parse sets and reps from user input text

    Args:
        user_id: Client ID
        text: User's sets/reps text input
        exercise_name: Name of the exercise (for context)

    Returns:
        dict: Parsed sets/reps data
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Parsing sets and reps..."
        })

        logger.info(f"Task {task_id}: Parsing sets/reps for user {user_id}")

        prompt = f"""
        Parse sets and reps from this text: "{text}"
        Exercise: {exercise_name}

        Handle these formats:
        - "3 sets 10 reps" → {{"sets": 3, "reps": 10, "format": "uniform"}}
        - "30 in first, 40 in 2nd set" → {{"sets": 2, "reps": [30, 40], "format": "variable"}}
        - "3x12" → {{"sets": 3, "reps": 12, "format": "uniform"}}
        - "15, 12, 10" → {{"sets": 3, "reps": [15, 12, 10], "format": "variable"}}

        Return JSON: {{"sets": number, "reps": number_or_array, "format": "uniform"|"variable"}}
        """

        oai_client = get_openai_client()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        from app.utils.async_openai import async_openai_call

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Processing..."
        })

        response = loop.run_until_complete(
            async_openai_call(
                oai_client,
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "Extract workout sets/reps data. Return valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                stream=False,
                temperature=0.1
            )
        )

        content = (response.choices[0].message.content or "").strip()
        loop.close()

        # Clean JSON formatting
        import re
        content = re.sub(r"^```json\s*", "", content)
        content = re.sub(r"\s*```$", "", content)

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            logger.warning(f"Task {task_id}: Failed to parse sets/reps: {content[:100]}")
            parsed = {"sets": None, "reps": None, "format": "unknown"}

        result = {
            "status": "completed",
            "progress": 100,
            "result": parsed
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Parsed sets/reps for user {user_id}: {parsed}")
        return parsed

    except Exception as e:
        logger.error(f"Task {task_id}: Sets/reps parsing failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        fallback = {"sets": None, "reps": None, "format": "unknown"}

        result = {
            "status": "completed",
            "progress": 100,
            "result": fallback
        }
        publish_progress(task_id, result)

        return fallback
