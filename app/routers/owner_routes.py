"""Owner/gym-management API routes: home, add-bar, sidebar, members, onboarding."""

from fastapi import APIRouter

# ── Home ────────────────────────────────────────────────────────────
from app.fittbot_api.v1.owner.owner import router as owner_router
from app.fittbot_api.v1.owner.owner_api.home.all import router as all_owner_router
from app.fittbot_api.v1.owner.owner_api.home.ledger import router as ledger_router
from app.fittbot_api.v1.owner.owner_api.home.newbies import router as newbies_router
from app.fittbot_api.v1.owner.owner_api.home.sessions import router as sessions_router
from app.fittbot_api.v1.owner.owner_api.home.set_sessions import router as set_sessions_router
from app.fittbot_api.v1.owner.owner_api.home.view_bookings import router as view_bookings_router
from app.fittbot_api.v1.owner.owner_api.home.create_post import router as create_post_router
from app.fittbot_api.v1.owner.owner_api.home.get_old_data import router as get_old_data_router

# ── Add Bar ─────────────────────────────────────────────────────────
from app.fittbot_api.v1.owner.owner_api.add_bar.diet_templates import router as diet_templates_router
from app.fittbot_api.v1.owner.owner_api.add_bar.workout_templates import router as workout_templates_router
from app.fittbot_api.v1.owner.owner_api.add_bar.brouchure import router as brouchure_router
from app.fittbot_api.v1.owner.owner_api.add_bar.gym_photos import router as gym_photos_router
from app.fittbot_api.v1.owner.owner_api.add_bar.gym_onboarding_pics import router as gym_onboarding_pics_router
from app.fittbot_api.v1.owner.owner_api.add_bar.biometric_interest import router as biometric_interest_router
from app.fittbot_api.v1.owner.owner_api.add_bar.onboarding_esign import router as onboarding_esign_router
from app.fittbot_api.v1.owner.owner_api.add_bar.add_rewards import router as add_rewards_router
from app.fittbot_api.v1.owner.owner_api.add_bar.no_cost_emi import router as no_cost_emi_router
from app.fittbot_api.v1.owner.owner_api.add_bar.add_client import router as add_client_qr_router
from app.fittbot_api.v1.owner.owner_api.add_bar.prizes import router as prizes_router
from app.fittbot_api.v1.owner.owner_api.add_bar.trainers import router as trainers_router
from app.fittbot_api.v1.owner.owner_api.add_bar.view_request import router as view_request_router
from app.fittbot_api.v1.owner.owner_api.add_bar.plans_batches import router as plan_batch_router
from app.fittbot_api.v1.owner.owner_api.add_bar.upcoming_plans import router as upcoming_plans_router

# ── Sidebar ─────────────────────────────────────────────────────────
from app.fittbot_api.v1.owner.owner_api.side_bar.gym_profile import router as gym_profile_router
from app.fittbot_api.v1.owner.owner_api.side_bar.support_token_owner import router as support_token_owner_router

# ── Members, Rewards, Feed ──────────────────────────────────────────
from app.fittbot_api.v1.owner.owner_api.scan_session import router as scan_session_router
from app.fittbot_api.v1.owner.owner_api.members.members import router as members_router
from app.fittbot_api.v1.owner.owner_api.expo_token.owner_expo_token import router as owner_expo_token_router
from app.fittbot_api.v1.owner.owner_api.rewards.rewards import router as rewards_router
from app.fittbot_api.v1.owner.owner_api.owner_feed.announcements_offers import router as announcements_offers_router
from app.fittbot_api.v1.owner.owner_api.owner_feed.offers import router as offers_router

# ── Profile & Gyms ──────────────────────────────────────────────────
from app.fittbot_api.v1.owner.owner_api.profile.add_new_gyms import router as add_new_gyms_router
from app.fittbot_api.v1.owner.owner_api.owner_profile.gym_details import router as owner_profile_router
from app.fittbot_api.v1.trainer_attendance.trainer_attendance_router import router as trainer_attendance_router

# ── Daily Pass & Membership ─────────────────────────────────────────
from app.fittbot_api.v1.owner.owner_api.daily_pass_gym_membership.get_status import router as membership_revenue_router
from app.fittbot_api.v1.owner.owner_api.daily_pass_gym_membership.pricing import router as membership_pricing_router
from app.fittbot_api.v1.owner.owner_api.daily_pass_gym_membership.schedule import router as membership_schedule_router

# ── Referral, Offline Users, Royalty ────────────────────────────────
from app.fittbot_api.v1.owner.owner_api.royalty.royalty import router as royalty_router
from app.fittbot_api.v1.owner.owner_api.referral.get_referral import router as get_referral_router
from app.fittbot_api.v1.owner.owner_api.offline_users.add_offline_users import router as offline_users_router
from app.fittbot_api.v1.owner.owner_api.general_modal.modal import router as owner_general_modal_router

# ── Owner Login & Registration (OTP-first flow) ───────────────────
from app.fittbot_api.v1.owner.owner_api.registeration.login import router as owner_login_router
from app.fittbot_api.v1.owner.owner_api.registeration.register import router as owner_register_router

# ── Digital Onboarding ──────────────────────────────────────────────
from app.fittbot_api.v1.owner.owner_api.digital_onboarding.registration import router as new_registration_router
from app.fittbot_api.v1.owner.owner_api.digital_onboarding.document_steps import router as document_steps_router
from app.fittbot_api.v1.owner.owner_api.digital_onboarding.gym_agreement import router as gym_agreement_router
from app.fittbot_api.v1.owner.owner_api.digital_onboarding.agreement_acceptance import router as agreement_acceptance_router

# ── Manual & Import Registration ────────────────────────────────────
from app.fittbot_api.v1.owner.owner_api.manual_registration.manual_client import router as manual_client_router
from app.fittbot_api.v1.owner.owner_api.import_registration.import_client import router as import_client_router
from app.fittbot_api.v1.owner.owner_api.offerprice.owneroffer import router as offerprice_router

# ── Delete Account ──────────────────────────────────────────────────
from app.fittbot_api.v1.owner.owner_api.delete_account.delete_requests import router as owner_delete_requests_router

# ── Collector ───────────────────────────────────────────────────────
router = APIRouter()

# Registration order preserved from original main.py
router.include_router(owner_delete_requests_router)
router.include_router(owner_router)
router.include_router(all_owner_router)
router.include_router(ledger_router)
router.include_router(newbies_router)
router.include_router(sessions_router)
router.include_router(set_sessions_router)
router.include_router(view_bookings_router)
router.include_router(create_post_router)
router.include_router(get_old_data_router)
router.include_router(diet_templates_router)
router.include_router(workout_templates_router)
router.include_router(brouchure_router)
router.include_router(gym_photos_router)
router.include_router(gym_onboarding_pics_router)
router.include_router(biometric_interest_router)
router.include_router(onboarding_esign_router)
router.include_router(no_cost_emi_router)
router.include_router(add_rewards_router)
router.include_router(prizes_router)
router.include_router(trainers_router)
router.include_router(view_request_router)
router.include_router(gym_profile_router)
router.include_router(support_token_owner_router)
router.include_router(scan_session_router)
router.include_router(members_router)
router.include_router(owner_expo_token_router)
router.include_router(rewards_router)
router.include_router(announcements_offers_router)
router.include_router(offers_router)
router.include_router(add_new_gyms_router)
router.include_router(owner_profile_router)
router.include_router(trainer_attendance_router)
router.include_router(add_client_qr_router)
router.include_router(plan_batch_router)
router.include_router(upcoming_plans_router)
router.include_router(royalty_router)
router.include_router(membership_revenue_router)
router.include_router(membership_schedule_router)
router.include_router(membership_pricing_router)
router.include_router(get_referral_router)
router.include_router(offline_users_router)
router.include_router(owner_general_modal_router)
router.include_router(owner_login_router)
router.include_router(owner_register_router)
router.include_router(new_registration_router)
router.include_router(document_steps_router)
router.include_router(gym_agreement_router)
router.include_router(agreement_acceptance_router)
router.include_router(manual_client_router)
router.include_router(import_client_router)
router.include_router(offerprice_router)
