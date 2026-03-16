# subscriber.py
# ──────────────────────────────────
# Runtime: Python 3.12 | 128 MB | 60 s
# Trigger: SQS → Lambda Event Source Mapping on BROADCAST_QUEUE_URL
# No extra environment vars needed

import json, logging
from typing import List, Dict, Any
from exponent_server_sdk import PushClient, PushMessage, PushServerError

push = PushClient()
log  = logging.getLogger()
log.setLevel(logging.INFO)

# ─── TEMPLATE DEFINITION ───────────
TEMPLATES: Dict[str, Dict[str, str]] = {
    "breakfast":         {"title":"🌞 Good Morning, {name}!","body":"Kick-start your metabolism with a protein-packed breakfast. 🥣💪","channel":"diet_channel"},
    "mid_morning_snack": {"title":"Snack O’Clock 🍏","body":"Grab a fruit or handful of nuts to keep energy steady.","channel":"diet_channel"},
    "lunch":             {"title":"🍱 Lunch Time","body":"Balance your plate: ½ veggies, ¼ protein, ¼ carbs.","channel":"diet_channel"},
    "dinner":            {"title":"🍽️ Dinner","body":"Light & early helps recovery. Have you planned your meal?","channel":"diet_channel"},
    "water":             {"title":"💧 Hydration Check","body":"Sip some water now—your body will thank you!","channel":"default"},
    "workout":           {"title":"🔥 Workout Time","body":"Let’s smash today’s session. Start with a quick warm-up!","channel":"workout_channel"},
    "stretch_break":     {"title":"🧘‍♂️ Stretch Break","body":"Stand up, roll your shoulders, 30-sec hamstring stretch—go!","channel":"workout_channel"},
    "step_goal":         {"title":"🚶 Hit Your Steps","body":"You’re {steps_left} steps away from today’s goal—take a walk!","channel":"workout_channel"},
    "breathing":         {"title":"🌬️ Breathing","body":"Close your eyes, inhale 4s, exhale 6s ×5 sets.","channel":"mind_channel"},
    "gratitude":         {"title":"🙏 Gratitude","body":"Think of 1 thing you’re grateful for today.","channel":"mind_channel"},
    "sleep_prep":        {"title":"🌙 Wind-Down","body":"Dim lights & avoid screens 30 min before bed.","channel":"mind_channel"},
    "posture":           {"title":"📏 Posture","body":"Straighten your back, relax shoulders.","channel":"default"},
}

MAX_BATCH = 100


def lambda_handler(event: Dict[str, Any], _ctx):
    # SQS wraps messages under Records[]
    records = event.get("Records") or []
    for rec in records:
        try:
            payload = json.loads(rec["body"])
            tpl_key = payload["template"]
        except Exception as e:
            log.error("Invalid SQS record: %s", e)
            continue

        tpl = TEMPLATES.get(tpl_key)
        if not tpl:
            log.error("Unknown template '%s'", tpl_key)
            continue

        # payload['recipients'] is a list of {token, name}
        recs = payload.get("recipients", [])
        if not isinstance(recs, list) or not recs:
            log.warning("No recipients for template '%s'", tpl_key)
            continue

        # Chunk into MAX_BATCH and send
        for i in range(0, len(recs), MAX_BATCH):
            _send_batch(tpl_key, tpl, recs[i : i + MAX_BATCH])


def _send_batch(tpl_key: str, tpl: Dict[str, str], batch: List[Dict[str, str]]):
    messages = []
    for r in batch:
        token = r.get("token")
        name  = r.get("name", "there")
        if not token:
            continue
        messages.append(
            PushMessage(
                to=token,
                title=tpl["title"].format(name=name),
                body=tpl["body"].format(name=name),
                sound="default",
                priority="high",
                channel_id=tpl["channel"],
                data={"template": tpl_key},
                display_in_foreground=True,
            )
        )

    if not messages:
        log.warning("Skipping empty batch for '%s'", tpl_key)
        return

    try:
        resp = push.publish_multiple(messages)
        ok  = sum(1 for r in resp if r.status == "ok")
        err = len(resp) - ok
        log.info("Template '%s' → Sent %d ok, %d errors", tpl_key, ok, err)
    except PushServerError as exc:
        log.error("Expo push failed for '%s': %s", tpl_key, exc)
        raise   # let SQS/Lambda retry
