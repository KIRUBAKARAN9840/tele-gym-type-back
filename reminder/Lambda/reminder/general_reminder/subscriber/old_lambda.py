# subscriber.py
# ────────────────────────────────────────────────────────────
# Runtime: Python 3.12 | 128 MB | 60 s
# Trigger: SQS → Lambda (Event Source Mapping on BROADCAST_QUEUE_URL)
# No extra environment vars needed

import json, logging, random
from typing import List, Dict, Any
from exponent_server_sdk import PushClient, PushMessage, PushServerError

push = PushClient()
log  = logging.getLogger()
log.setLevel(logging.INFO)

# ─── TEMPLATE DEFINITIONS ───────────────────────────────────
# One key per logical reminder.  Each key holds 3-4 variants.
# TEMPLATES: Dict[str, List[Dict[str, str]]] = {
#     "breakfast": [
#         {"title": "🌞 Good morning, {name}!",
#          "body":  "Kick-start your metabolism with a protein-packed breakfast 🥣💪",
#          "channel": "default"},
#         {"title": "🥞 Rise & dine, {name}",
#          "body":  "Fuel your gains – 30 g protein + slow carbs.",
#          "channel": "default"},
#         {"title": "🍳 Breakfast check-in,{name}",
#          "body":  "Don’t skip the most important meal of the day!",
#          "channel": "default"},
#         {"title": "🌅 {name}, time to eat!",
#          "body":  "Quality breakfast keeps cravings away all morning.",
#          "channel": "default"},
#     ],
#     "mid_morning_snack": [
#         {"title": "🍏 Snack O’Clock,{name}",
#          "body":  "Grab a fruit or nuts to keep energy steady.",
#          "channel": "default"},
#         {"title": "🥜 Quick bite, {name}?",
#          "body":  "A handful of almonds beats a sugary bar every time.",
#          "channel": "default"},
#         {"title": "🚀 Mini-refuel,{name}",
#          "body":  "Protein yoghurt or banana – pick one and power on!",
#          "channel": "default"},
#     ],
#     "lunch": [
#         {"title": "🍱 Lunch time,{name}",
#          "body":  "½ veggies, ¼ protein, ¼ carbs – plate it right.",
#          "channel": "default"},
#         {"title": "🥗 Balanced lunch alert,{name}",
#          "body":  "Add colour: greens + lean protein boost recovery.",
#          "channel": "default"},
#         {"title": "🍛 Lunchtime, {name}",
#          "body":  "Slow down and chew – mindful eating aids digestion.",
#          "channel": "default"},
#     ],
#     "dinner": [
#         {"title": "🍽️ Dinner call,{name}",
#          "body":  "Light & early helps recovery – what’s on your plate?",
#          "channel": "default"},
#         {"title": "🌙 Wind-down meal,{name}",
#          "body":  "Protein + veggies, skip heavy carbs for better sleep.",
#          "channel": "default"},
#         {"title": "🥦 Evening fuel,{name}",
#          "body":  "Remember to hydrate and keep portions moderate.",
#          "channel": "default"},
#     ],
#     "water": [
#         {"title": "💧 Hydration check,{name}",
#          "body":  "Sip some water now—your body will thank you!",
#          "channel": "default"},
#         {"title": "🚰 Drink break,{name}",
#          "body":  "A glass of water boosts focus in minutes.",
#          "channel": "default"},
#         {"title": "🌊 H2O time,{name}",
#          "body":  "Stay ahead of thirst – take a few sips.",
#          "channel": "default"},
#     ],

#     "stretch_break": [
#         {"title": "{name},🧘‍♂️ Stretch break",
#          "body":  "Stand up, roll shoulders, 30-sec hamstring stretch—go!",
#          "channel": "default"},
#         {"title": "🙆 Mobility minute,{name}",
#          "body":  "Neck circles + chest opener = instant refresh.",
#          "channel": "default"},
#         {"title": "🦵 Leg stretch time,{name}",
#          "body":  "Desk posture fix: quad stretch & ankle rolls.",
#          "channel": "default"},
#          {"title": "📏 Posture check,{name}",
#          "body":  "Straighten your back, relax shoulders.",
#          "channel": "default"},
#         {"title": "🪑 Sit tall,{name}",
#          "body":  "Ear-hip-ankle line = painless spine.",
#          "channel": "default"},
#         {"title": "🔔 Back alert,{name}",
#          "body":  "Roll shoulder blades down & away from ears.",
#          "channel": "default"},
#     ],
#     "session_nudge": [
#         {"title":"⏱  Still working out, {name}?",
#          "body":"Hope you’re crushing it! Don’t forget to punch-out when done 💪",
#          "channel":"workout_channel"}
#     ],
#   "punchout_intimation": [
#   {
#     "title": "🏁 Session complete, {name}!",
#     "body": "You’ve been going strong for over 2 hours—let us handle the punch-out so your progress stays on track 💪✅",
#     "channel": "workout_channel"
#   }
# ],
# "birthday": [
#         {"title": "🎂 Happy Birthday, {name}!",
#          "body":  "Team FittBot wishes you an amazing day & year ahead. Keep smashing your goals! 🎉",
#          "channel": "default"},
#         {"title": "🥳 {name}, it’s your special day!",
#          "body":  "Celebrate big, recover well, and let us fuel your fitness journey. Happy Birthday!",
#          "channel": "default"},
#         {"title": "🎉 Cheers to you, {name}",
#          "body":  "Another lap around the sun—stay strong and keep lifting. Happy Birthday from FittBot!",
#          "channel": "default"},
#     ],

# }

TEMPLATES: Dict[str, List[Dict[str, str]]] = {
    "birthday": [
        {
            "title": "🎂 Happy Birthday, {name}!",
            "body": "The Whole FittBot Crew Wishes You a Year Full of Strength, Health & PR-Breaking Workouts. Enjoy Your Day! 🥳",
            "channel": "default"
        }
    ],
    "breakfast": [
        {
            "title": "🌞 Good Morning, {name}!",
            "body": "Kick-Start Your Metabolism with a Protein-Packed Breakfast 🥣💪",
            "channel": "default"
        },
        {
            "title": "🥞 Rise & Dine, {name}",
            "body": "Fuel Your Gains – 30 g Protein + Slow Carbs.",
            "channel": "default"
        },
        {
            "title": "🍳 Breakfast Check-In, {name}",
            "body": "Don’t Skip the Most Important Meal of the Day!",
            "channel": "default"
        },
        {
            "title": "🌅 {name}, Time to Eat!",
            "body": "Quality Breakfast Keeps Cravings Away All Morning.",
            "channel": "default"
        },
    ],
    "mid_morning_snack": [
        {
            "title": "🍏 Snack O’Clock, {name}",
            "body": "Grab a Fruit or Nuts to Keep Energy Steady.",
            "channel": "default"
        },
        {
            "title": "🥜 Quick Bite, {name}?",
            "body": "A Handful of Almonds Beats a Sugary Bar Every Time.",
            "channel": "default"
        },
        {
            "title": "🚀 Mini-Refuel, {name}",
            "body": "Protein Yoghurt or Banana – Pick One and Power On!",
            "channel": "default"
        },
    ],
    "lunch": [
        {
            "title": "🍱 Lunch Time, {name}",
            "body": "½ Veggies, ¼ Protein, ¼ Carbs – Plate It Right.",
            "channel": "default"
        },
        {
            "title": "🥗 Balanced Lunch Alert, {name}",
            "body": "Add Colour: Greens + Lean Protein Boost Recovery.",
            "channel": "default"
        },
        {
            "title": "🍛 Lunchtime, {name}",
            "body": "Slow Down and Chew – Mindful Eating Aids Digestion.",
            "channel": "default"
        },
    ],
    "dinner": [
        {
            "title": "🍽️ Dinner Call, {name}",
            "body": "Light & Early Helps Recovery – What’s on Your Plate?",
            "channel": "default"
        },
        {
            "title": "🌙 Wind-Down Meal, {name}",
            "body": "Protein + Veggies, Skip Heavy Carbs for Better Sleep.",
            "channel": "default"
        },
        {
            "title": "🥦 Evening Fuel, {name}",
            "body": "Remember to Hydrate and Keep Portions Moderate.",
            "channel": "default"
        },
    ],
    "water": [
        {
            "title": "💧 Hydration Check, {name}",
            "body": "Sip Some Water Now—Your Body Will Thank You!",
            "channel": "default"
        },
        {
            "title": "🚰 Drink Break, {name}",
            "body": "A Glass of Water Boosts Focus in Minutes.",
            "channel": "default"
        },
        {
            "title": "🌊 H2O Time, {name}",
            "body": "Stay Ahead of Thirst – Take a Few Sips.",
            "channel": "default"
        },
    ],
    "stretch_break": [
        {
            "title": "{name}, 🧘‍♂️ Stretch Break",
            "body": "Stand Up, Roll Shoulders, 30-Sec Hamstring Stretch—Go!",
            "channel": "default"
        },
        {
            "title": "🙆 Mobility Minute, {name}",
            "body": "Neck Circles + Chest Opener = Instant Refresh.",
            "channel": "default"
        },
        {
            "title": "🦵 Leg Stretch Time, {name}",
            "body": "Desk Posture Fix: Quad Stretch & Ankle Rolls.",
            "channel": "default"
        },
        {
            "title": "📏 Posture Check, {name}",
            "body": "Straighten Your Back, Relax Shoulders.",
            "channel": "default"
        },
        {
            "title": "🪑 Sit Tall, {name}",
            "body": "Ear-Hip-Ankle Line = Painless Spine.",
            "channel": "default"
        },
        {
            "title": "🔔 Back Alert, {name}",
            "body": "Roll Shoulder Blades Down & Away from Ears.",
            "channel": "default"
        },
    ],
    "session_nudge": [
        {
            "title": "⏱ Still Working Out, {name}?",
            "body": "Hope You’re Crushing It! Don’t Forget to Punch-Out When Done 💪",
            "channel": "workout_channel"
        }
    ],
    "punchout_intimation": [
        {
            "title": "🏁 Session Complete, {name}!",
            "body": "You’ve Been Going Strong for Over 2 Hours—Let Us Handle the Punch-Out So Your Progress Stays on Track 💪✅",
            "channel": "workout_channel"
        }
    ]
}


MAX_BATCH = 100

# ─── LAMBDA HANDLER ─────────────────────────────────────────
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

        variants = TEMPLATES.get(tpl_key)
        if not variants:
            log.error("Unknown template '%s'", tpl_key)
            continue

        tpl = random.choice(variants)  # ← pick 1 of the 3-4 variants

        # payload['recipients'] is a list of {token, name}
        recs = payload.get("recipients", [])
        if not isinstance(recs, list) or not recs:
            log.warning("No recipients for template '%s'", tpl_key)
            continue

        # Chunk into MAX_BATCH and send
        for i in range(0, len(recs), MAX_BATCH):
            _send_batch(tpl_key, tpl, recs[i : i + MAX_BATCH])

# ─── INTERNAL: SEND ONE BATCH ───────────────────────────────
def _send_batch(tpl_key: str, tpl: Dict[str, str], batch: List[Dict[str, str]]):
    messages = []
    
    for r in batch:
        token = r.get("token")
        name  = r.get("name", "there")
        if not token:
            continue
        messages.append(
            PushMessage(
                to        = token,
                title     = tpl["title"].format(name=name),
                body      = tpl["body"].format(name=name, steps_left=r.get("steps_left", "")),
                sound     = "default",
                priority  = "high",
                channel_id= tpl["channel"],
                data      = {"template": tpl_key},
                display_in_foreground=True,
            )
        )

    if not messages:
        log.warning("Skipping empty batch for '%s'", tpl_key)
        return

    try:
        resp = push.publish_multiple(messages)
        ok   = sum(1 for r in resp if r.status == "ok")
        err  = len(resp) - ok
        log.info("Template '%s' → Sent %d ok, %d errors", tpl_key, ok, err)
    except PushServerError as exc:
        log.error("Expo push failed for '%s': %s", tpl_key, exc)
        raise  # let SQS/Lambda retry
