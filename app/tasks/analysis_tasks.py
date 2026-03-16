# app/tasks/analysis_tasks.py
"""
Celery tasks for AI fitness analysis reports
Production-ready with error handling and progress updates

Uses SYNC OpenAI client for gevent compatibility.
"""
import json
import re
import logging
import orjson
from typing import Dict, Optional
from datetime import datetime, timedelta, date
from app.celery_app import celery_app
from app.utils.openai_sync import get_sync_openai_client, sync_openai_call
from app.utils.redis_config import get_redis_sync

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


@celery_app.task(bind=True, name="app.tasks.analysis_tasks.generate_analysis_report")
def generate_analysis_report(
    self,
    user_id: int,
    dataset: dict,
    hints: dict,
    user_request: str = ""
):
    """
    Generate fitness analysis report using AI

    Args:
        user_id: Client ID
        dataset: Analysis dataset with macros, workouts, etc.
        hints: Summary hints for personalization

    Returns:
        dict: Generated report content
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Analyzing your fitness data..."
        })

        logger.info(f"Task {task_id}: Generating analysis report for user {user_id}")

        # Import the style constants
        from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.report_analysis import (
            pretty_plan_report
        )
        from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.llm_helpers import (
            GENERAL_SYSTEM, STYLE_CHAT_FORMAT, OPENAI_MODEL
        )

        # Dynamic format generation based on user request
        def generate_dynamic_style(user_request: str, dataset: dict) -> str:
            """Generate flexible report format based on user's specific request"""

            # Analyze the user's request to understand format preferences
            request_lower = user_request.lower()

            # Default format if no specific preferences detected
            base_style = """You are KyraAI, a caring fitness coach. Generate a helpful fitness report based on the user's request and the provided data.

Tone: warm, encouraging, and professional
Language: Simple, clear, and mobile-friendly
Visuals: Use emojis appropriately to make the report engaging and easy to scan

Key Guidelines:
- Only analyze and report on data that actually exists
- If certain data is missing, acknowledge it gracefully
- Adapt the report structure to match what the user is asking for
- Be concise but thorough
- Make insights actionable and personalized"""

            # Detect specific format preferences
            if any(word in request_lower for word in ['summary', 'brief', 'quick', 'overview']):
                return base_style + """

Format: Executive Summary
- Keep it concise (2-3 sections max)
- Focus on the most important insights
- Use bullet points for key takeaways
- Highlight any areas needing attention"""

            elif any(word in request_lower for word in ['detailed', 'comprehensive', 'thorough', 'complete']):
                return base_style + """

Format: Comprehensive Analysis
- Include all available data sections
- Provide detailed breakdowns and insights
- Add specific recommendations for each area
- Include trends and patterns where data allows"""

            elif any(word in request_lower for word in ['nutrition', 'diet', 'food', 'macros']):
                return base_style + """

Format: Nutrition Focus
- Prioritize nutrition data and insights
- Detailed macro analysis
- Meal pattern observations
- Specific nutrition recommendations"""

            elif any(word in request_lower for word in ['workout', 'exercise', 'training', 'fitness']):
                return base_style + """

Format: Workout Focus
- Prioritize workout and training data
- Exercise performance analysis
- Training volume and intensity insights
- Fitness progress assessment"""

            elif any(word in request_lower for word in ['weight', 'loss', 'gain', 'progress']):
                return base_style + """

Format: Progress Analysis
- Focus on weight and body composition changes
- Progress toward goals
- Trend analysis
- Timeline projections"""

            else:
                # Use original fixed format by default
                from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.report_analysis import STYLE_INSIGHT_REPORT
                return STYLE_INSIGHT_REPORT

        # Generate dynamic style based on user request
        dynamic_style = generate_dynamic_style(user_request, dataset)

        # Add data completeness info to system prompt
        data_context = (
            f"\nDATE RANGE: {dataset.get('timeframe')}\n"
            f"USER REQUEST: {user_request}\n"
            f"Data Completeness: {orjson.dumps(dataset.get('data_completeness')).decode()}\n"
            f"IMPORTANT: Only analyze and report on data that exists. "
            f"If certain data is missing (e.g., no workout data), skip that section gracefully or mention it's not available. "
            f"Do not make up or fabricate insights for missing data.\n"
            f"CRITICAL WEIGHT DATA: For weight snapshot section, ALWAYS use current_weight and target_weight from the main dataset level. "
            f"NEVER use avg_weight from weekly data as the current weight - avg_weight is for trend analysis only."
        )

        msgs = [
            {"role": "system", "content": GENERAL_SYSTEM},
            {"role": "system", "content": STYLE_CHAT_FORMAT},
            {"role": "system", "content": dynamic_style},
            {"role": "system", "content": data_context},
            {"role": "system", "content":
                "HINTS_FOR_OVERALL_SUMMARY="
                + orjson.dumps(hints).decode()
                + "\nInterpret remaining_kg (negative = kg to lose, positive = kg to gain). "
                  "Use these hints to make the report precise and personalized."
            },
            {"role": "user", "content":
                f"Based on this request: '{user_request}'\n\n"
                "Please analyze this dataset and generate a report that directly addresses what the user is asking for:\n\n"
                f"{orjson.dumps(dataset).decode()}"
            },
        ]

        oai_client = get_sync_openai_client()

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Creating your personalized report..."
        })

        response = sync_openai_call(
            oai_client,
            model=OPENAI_MODEL,
            messages=msgs,
            temperature=0
        )

        content = (response.choices[0].message.content or "").strip()

        # Clean up the content
        content = re.sub(r'\bfit\s*bot\b|\bfit+bot\b|\bfitbot\b', 'Fittbot', content, flags=re.I)
        pretty = pretty_plan_report(content)

        result = {
            "status": "completed",
            "progress": 100,
            "result": {
                "report": pretty,
                "raw_content": content
            }
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Analysis report completed for user {user_id}")
        return result["result"]

    except Exception as e:
        logger.error(f"Task {task_id}: Analysis report failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Return fallback
        fallback = {
            "report": "I'm having trouble generating your report right now. Please try again in a moment.",
            "raw_content": "",
            "error": str(e)
        }

        result = {
            "status": "completed",
            "progress": 100,
            "result": fallback
        }
        publish_progress(task_id, result)

        return fallback


@celery_app.task(bind=True, name="app.tasks.analysis_tasks.extract_date_range")
def extract_date_range(
    self,
    user_id: int,
    text: str
):
    """
    Extract date range from user's natural language text using AI

    Args:
        user_id: Client ID
        text: User's text containing date information

    Returns:
        dict: Extracted dates or None values
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Understanding date range..."
        })

        logger.info(f"Task {task_id}: Extracting date range for user {user_id}")

        if not text or not text.strip():
            return {"start_date": None, "end_date": None}

        today = date.today()

        extraction_prompt = f"""Today's date is {today.strftime('%Y-%m-%d')}.

Analyze the following user request and extract the date range they want for their fitness report.

User request: "{text}"

If the user specifies a date range (like "last 7 days", "from Jan 1 to Jan 31", "this month", "last month", etc.),
respond with ONLY a JSON object in this exact format:
{{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}}

If NO date range is mentioned, respond with ONLY:
{{"start_date": null, "end_date": null}}

Examples:
- "analyze last 7 days" -> {{"start_date": "{(today - timedelta(days=6)).strftime('%Y-%m-%d')}", "end_date": "{today.strftime('%Y-%m-%d')}"}}
- "analyze last 3 days" -> {{"start_date": "{(today - timedelta(days=2)).strftime('%Y-%m-%d')}", "end_date": "{today.strftime('%Y-%m-%d')}"}}
- "analyze last 2 days" -> {{"start_date": "{(today - timedelta(days=1)).strftime('%Y-%m-%d')}", "end_date": "{today.strftime('%Y-%m-%d')}"}}
- "report from 2024-01-01 to 2024-01-31" -> {{"start_date": "2024-01-01", "end_date": "2024-01-31"}}
- "this week" -> {{"start_date": "{(today - timedelta(days=today.weekday())).strftime('%Y-%m-%d')}", "end_date": "{today.strftime('%Y-%m-%d')}"}}
- "last month" -> calculate last month's first and last day
- "analyze my progress" -> {{"start_date": null, "end_date": null}}

Respond with ONLY the JSON, nothing else."""

        oai_client = get_sync_openai_client()

        from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.llm_helpers import OPENAI_MODEL

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Processing date information..."
        })

        response = sync_openai_call(
            oai_client,
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": extraction_prompt}],
            temperature=0
        )

        content = (response.choices[0].message.content or "").strip()

        # Parse JSON response
        try:
            parsed = orjson.loads(content)
            extracted = {
                "start_date": parsed.get("start_date"),
                "end_date": parsed.get("end_date")
            }
        except Exception:
            logger.warning(f"Task {task_id}: Failed to parse date extraction response: {content[:100]}")
            extracted = {"start_date": None, "end_date": None}

        result = {
            "status": "completed",
            "progress": 100,
            "result": extracted
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Date extraction completed for user {user_id}: {extracted}")
        return extracted

    except Exception as e:
        logger.error(f"Task {task_id}: Date extraction failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Return empty result
        fallback = {"start_date": None, "end_date": None}

        result = {
            "status": "completed",
            "progress": 100,
            "result": fallback
        }
        publish_progress(task_id, result)

        return fallback


@celery_app.task(bind=True, name="app.tasks.analysis_tasks.generate_followup_response")
def generate_followup_response(
    self,
    user_id: int,
    user_text: str,
    summary: str,
    dataset: dict = None,
    is_followup: bool = False
):
    """
    Generate follow-up response to user's question about analysis

    Args:
        user_id: Client ID
        user_text: User's follow-up question
        summary: Previous analysis summary
        dataset: Original dataset for context
        is_followup: Whether this is a follow-up question needing full context

    Returns:
        str: Generated response
    """
    task_id = self.request.id

    try:
        publish_progress(task_id, {
            "status": "progress",
            "progress": 20,
            "message": "Processing your question..."
        })

        logger.info(f"Task {task_id}: Generating followup response for user {user_id}")

        from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.llm_helpers import (
            GENERAL_SYSTEM, STYLE_CHAT_FORMAT, OPENAI_MODEL
        )
        from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.report_analysis import (
            STYLE_INSIGHT_REPORT
        )

        msgs = [
            {"role": "system", "content": GENERAL_SYSTEM},
            {"role": "system", "content": STYLE_CHAT_FORMAT},
            {"role": "system", "content": STYLE_INSIGHT_REPORT},
            {"role": "assistant", "content": f"Here's your analysis:\n\n{summary}"},
            {"role": "user", "content": user_text},
        ]

        # Add dataset context for deeper questions
        if is_followup and dataset:
            msgs.insert(3, {
                "role": "system",
                "content": f"User's fitness data (for reference):\n{orjson.dumps(dataset).decode()}"
            })

        oai_client = get_sync_openai_client()

        publish_progress(task_id, {
            "status": "progress",
            "progress": 50,
            "message": "Generating response..."
        })

        response = sync_openai_call(
            oai_client,
            model=OPENAI_MODEL,
            messages=msgs,
            temperature=0.3
        )

        content = (response.choices[0].message.content or "").strip()

        result = {
            "status": "completed",
            "progress": 100,
            "result": content
        }

        publish_progress(task_id, result)

        logger.info(f"Task {task_id}: Followup response completed for user {user_id}")
        return content

    except Exception as e:
        logger.error(f"Task {task_id}: Followup response failed for user {user_id} - {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Task {task_id}: Traceback:\n{traceback.format_exc()}")

        # Return fallback
        fallback = "I'm having trouble answering that right now. Could you try rephrasing your question?"

        result = {
            "status": "completed",
            "progress": 100,
            "result": fallback
        }
        publish_progress(task_id, result)

        return fallback
