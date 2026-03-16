"""AI and chatbot routes: food scanner, chatbot, workout templates, food logging."""

from fastapi import APIRouter

from app.fittbot_api.v1.client.client_api.food_scanner_AI.ai_food_scanner import router as ai_food_scanner_router
from app.fittbot_api.v1.client.client_api.chatbot.codes.ai_chatbot import router as ai_chatbot_router
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.report_analysis import router as analysis_chatbot_router
from app.fittbot_api.v1.client.client_api.chatbot.codes.workout_template_chatbot import router as workout_template_chatbot_router
from app.fittbot_api.v1.client.client_api.chatbot.chatbot_services.workout_structured import router as workout_structured_router
from app.fittbot_api.v1.client.client_api.chatbot.codes.food_log import router as food_log_router
from app.fittbot_api.v1.client.client_api.chatbot.codes.food_template import router as food_template_router
from app.fittbot_api.v1.client.client_api.chatbot.codes.workout_log import router as workout_automation_router

router = APIRouter()

router.include_router(ai_food_scanner_router)
router.include_router(ai_chatbot_router)
router.include_router(analysis_chatbot_router)
router.include_router(workout_template_chatbot_router)
router.include_router(workout_structured_router)
router.include_router(food_log_router)
router.include_router(food_template_router)
router.include_router(workout_automation_router)
