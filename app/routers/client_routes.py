"""Client-facing API routes: profile, workout, diet, sessions, sidebar, and more."""

from fastapi import APIRouter

# ── Home & General ──────────────────────────────────────────────────
from app.fittbot_api.v1.client.client import router as client_router
from app.fittbot_api.v1.client.client_api.home.reminders import router as reminders_router
from app.fittbot_api.v1.client.client_api.home.reward_interest import router as reward_interest_router
from app.fittbot_api.v1.client.client_api.reward_program.reward_program import router as reward_program_router
from app.fittbot_api.v1.client.client_api.home.free_trial import router as free_trial_router
from app.fittbot_api.v1.client.client_api.home.gym_buddy import router as gym_buddy_router
from app.fittbot_api.v1.client.client_api.home.general_analysis import router as analysis_router
from app.fittbot_api.v1.client.client_api.general_modal.modal import router as general_modal_router
from app.fittbot_api.v1.client.client_api.home.my_rewards import router as my_rewards_router
from app.fittbot_api.v1.client.client_api.home.client_xp import router as client_xp_router
from app.fittbot_api.v1.client.client_api.home.my_gym import router as my_gym_router
from app.fittbot_api.v1.client.client_api.home.my_progress import router as my_progress_router
from app.fittbot_api.v1.client.client_api.home.gym_studios import router as gym_studios_router
from app.fittbot_api.v1.client.client_api.home.calculate_macros import router as macros_router
from app.fittbot_api.v1.client.client_api.home.water import router as water_router
from app.fittbot_api.v1.client.client_api.home.leaderboard import router as leaderboard_router
from app.fittbot_api.v1.client.client_api.home.check_client_target import router as check_target_router
from app.fittbot_api.v1.client.client_api.home.get_unpaid_home import router as unpaid_home_router
from app.fittbot_api.v1.client.client_api.home.update_expo_token import router as expo_token_router

# ── Feed ────────────────────────────────────────────────────────────
from app.fittbot_api.v1.client.client_api.feed.feed import router as feed_router
from app.fittbot_api.v1.client.client_api.feed.report_user import router as report_router

# ── Sidebar ─────────────────────────────────────────────────────────
from app.fittbot_api.v1.client.client_api.side_bar.gym_subscription import router as gym_subscription_router
from app.fittbot_api.v1.client.client_api.side_bar.feedback import router as feedback_router
from app.fittbot_api.v1.client.client_api.side_bar.support_token import router as support_token_router
from app.fittbot_api.v1.client.client_api.side_bar.profile_pic import router as profile_pic_router
from app.fittbot_api.v1.client.client_api.side_bar.plans import router as plans_router
from app.fittbot_api.v1.client.client_api.side_bar.purchase_history import router as purchase_history_router
from app.fittbot_api.v1.client.client_api.side_bar.manage_fittbot_subscriptions import router as manage_subscriptions_router
from app.fittbot_api.v1.client.client_api.side_bar.referral_code import router as referral_code_router
from app.fittbot_api.v1.client.client_api.side_bar.ratings import router as ratings_router
from app.fittbot_api.v1.client.client_api.side_bar.gym_membership import router as gym_membership_router

# ── Workout ─────────────────────────────────────────────────────────
from app.fittbot_api.v1.client.client_api.workout.attendance import router as attendance_router
from app.fittbot_api.v1.client.client_api.workout.personal_template import router as personal_template_router
from app.fittbot_api.v1.client.client_api.workout.workout_analysis import router as workout_analysis_router
from app.fittbot_api.v1.client.client_api.workout.actual_workout import router as actual_workout_router
from app.fittbot_api.v1.client.client_api.workout.fittbot_workout import router as fittbot_workout_router
from app.fittbot_api.v1.client.client_api.workout.home_workout import router as home_workout_router
from app.fittbot_api.v1.client.client_api.workout.default_workout_template import router as default_workout_template_router
from app.fittbot_api.v1.client.client_api.workout.equipment import router as equipment_router
from app.fittbot_api.v1.client.client_api.workout.scan_qr import router as qr_router

# ── Diet ────────────────────────────────────────────────────────────
from app.fittbot_api.v1.client.client_api.diet.default_template import router as default_template_router
from app.fittbot_api.v1.client.client_api.diet.actual_diet import router as actual_diet_router
from app.fittbot_api.v1.client.client_api.diet.crud_diet import router as crud_diet_router
from app.fittbot_api.v1.client.client_api.diet.diet_analysis import router as diet_analysis_router
from app.fittbot_api.v1.client.client_api.diet.search_food import router as food_search_router
from app.fittbot_api.v1.client.client_api.diet.personal_template import router as personal_diet_template_router
from app.fittbot_api.v1.client.client_api.diet.report import router as diet_report_router

# ── Sessions ────────────────────────────────────────────────────────
from app.fittbot_api.v1.client.client_api.sessions.sessions import router as client_sessions_router
from app.fittbot_api.v1.client.client_api.sessions.get_sessions import router as client_get_sessions_router

# ── Daily Pass ──────────────────────────────────────────────────────
from app.fittbot_api.v1.client.client_api.dailypass.get_dailypass import router as get_dailypass_router
from app.fittbot_api.v1.client.client_api.dailypass.dailypass_qr import router as dailypass_qr_router

# ── Food, Nutrition & XP ───────────────────────────────────────────
from app.fittbot_api.v1.client.client_api.foods_db.foods import router as food_router
from app.fittbot_api.v1.client.client_api.nutrition.nutrition import router as nutrition_router
from app.fittbot_api.v1.client.client_api.xp.get_xp import router as all_xp_router

# ── AI Consent & Reports ───────────────────────────────────────────
from app.fittbot_api.v1.client.client_api.ai_consent.ai_consent import router as ai_consent_router
from app.fittbot_api.v1.client.client_api.ai_consent.ai_reports import router as ai_reports_router
from app.fittbot_api.v1.client.client_api.ai_consent.step_consent import router as step_consent_router

# ── Registration & Auth ────────────────────────────────────────────
from app.fittbot_api.v1.client.client_api.registeration.registration import router as client_registration_router
from app.fittbot_api.v1.client.client_api.registeration.login import router as client_login_router
from app.fittbot_api.v1.client.client_api.registeration.usertype import router as client_usertype_router

# ── Misc Client ─────────────────────────────────────────────────────
from app.fittbot_api.v1.client.client_api.redirect.redirect import router as redirect_router
from app.fittbot_api.v1.client.client_api.redirect.whatsapp_test import router as whatsapp_test_router
from app.fittbot_api.v1.client.client_api.offer_eligibility.routes import router as offer_eligibility_router
from app.fittbot_api.v1.client.client_api.client_scanning import router as client_scanning_router
from app.fittbot_api.v1.client.client_api.offline_requests.request_to_join import router as offline_requests_router
from app.fittbot_api.v1.client.client_api.delete_account.delete_requests import router as delete_requests_router
from app.fittbot_api.v1.client.client_api.app_open.app_open import router as app_open_router
from app.fittbot_api.v1.client.client_api.smartwatch.smartwatch import router as smartwatch_router
from app.fittbot_api.v1.client.client_api.operating_hour.operatinghours import router as operating_hours_router
from app.utils.set_gym_id import router as set_gym_id_router

# ── Collector ───────────────────────────────────────────────────────
router = APIRouter()

# Registration order preserved from original main.py
router.include_router(reminders_router)
router.include_router(reward_interest_router)
router.include_router(reward_program_router)
router.include_router(free_trial_router)
router.include_router(get_dailypass_router)
router.include_router(dailypass_qr_router)
router.include_router(actual_diet_router)
router.include_router(gym_buddy_router)
router.include_router(actual_workout_router)
router.include_router(default_template_router)
router.include_router(client_router)
router.include_router(analysis_router)
router.include_router(general_modal_router)
router.include_router(redirect_router)
router.include_router(whatsapp_test_router)
router.include_router(client_registration_router)
router.include_router(client_login_router)
router.include_router(client_usertype_router)
router.include_router(diet_analysis_router)
router.include_router(workout_analysis_router)
router.include_router(personal_template_router)
router.include_router(fittbot_workout_router)
router.include_router(home_workout_router)
router.include_router(report_router)
router.include_router(feed_router)
router.include_router(my_progress_router)
router.include_router(my_rewards_router)
router.include_router(client_xp_router)
router.include_router(my_gym_router)
router.include_router(gym_subscription_router)
router.include_router(feedback_router)
router.include_router(support_token_router)
router.include_router(profile_pic_router)
router.include_router(plans_router)
router.include_router(purchase_history_router)
router.include_router(manage_subscriptions_router)
router.include_router(referral_code_router)
router.include_router(ratings_router)
router.include_router(attendance_router)
router.include_router(default_workout_template_router)
router.include_router(equipment_router)
router.include_router(crud_diet_router)
router.include_router(food_router)
router.include_router(macros_router)
router.include_router(water_router)
router.include_router(leaderboard_router)
router.include_router(food_search_router)
router.include_router(check_target_router)
router.include_router(gym_membership_router)
router.include_router(unpaid_home_router)
router.include_router(expo_token_router)
router.include_router(all_xp_router)
router.include_router(personal_diet_template_router)
router.include_router(diet_report_router)
router.include_router(client_sessions_router)
router.include_router(client_get_sessions_router)
router.include_router(set_gym_id_router)
router.include_router(gym_studios_router)
router.include_router(offer_eligibility_router)
router.include_router(client_scanning_router)
router.include_router(qr_router)
router.include_router(offline_requests_router)
router.include_router(nutrition_router)
router.include_router(ai_consent_router)
router.include_router(ai_reports_router)
router.include_router(step_consent_router)
router.include_router(delete_requests_router)
router.include_router(app_open_router)
router.include_router(smartwatch_router)
router.include_router(operating_hours_router)
