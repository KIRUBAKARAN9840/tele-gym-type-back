from __future__ import annotations
import orjson, json
from typing import Dict, Any, Tuple, List

DAYS6 = ["monday","tuesday","wednesday","thursday","friday","saturday"]
DAYS  = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]

# ────────────────────────── INTENT ──────────────────────────
_TRIGGER_WORDS = {
    "workout template","training template","create template","make template",
    "build plan","create plan","workout plan","training plan","routine","program",
    "upper lower","push pull legs","ppl","full body","muscle group","split"
}
def is_workout_template_intent(t: str) -> bool:
    tt = (t or "").lower()
    return any(k in tt for k in _TRIGGER_WORDS)

# ────────────────────── RENDER (Markdown) ───────────────────
def render_markdown_from_template(tpl: Dict[str,Any]) -> str:
    """Render Mon–Sat, with weekday in every H2 heading."""
    name = tpl.get("name") or "Workout Template (Mon–Sat)"
    goal = (tpl.get("goal") or "").replace("_"," ").title()
    days  = tpl.get("days") or {}
    notes = tpl.get("notes") or []

    out = [f"# {name}"]
    if goal:
        out += [f"**Goal:** {goal}", ""]

    for d in DAYS6:
        if d in days:
            day = days[d] or {}
            split_title = (day.get("title") or "").strip()
            heading = f"{d.title()}" + (f" — {split_title}" if split_title else "")
            out.append(f"## {heading}")

            mgs = day.get("muscle_groups") or []
            if mgs:
                out.append(f"**Muscle Focus:** {', '.join(mgs)}")

            for ex in day.get("exercises") or []:
                nm   = ex.get("name") or "Exercise"
                sets = ex.get("sets")
                reps = ex.get("reps")
                note = ex.get("note")
                line = f"- {nm}"
                if sets is not None and reps is not None:
                    line += f" — {sets}×{reps}"
                elif sets is not None:
                    line += f" — {sets} sets"
                if note:
                    line += f" ({note})"
                out.append(line)
            out.append("")

    if notes:
        out.append("**Notes**")
        for n in notes: out.append(f"- {n}")
        out.append("")

    return "\n".join(out).strip()

# ─────────────────────── Utilities ──────────────────────────
def _safe_json(text: str, fallback: Dict[str,Any]) -> Dict[str,Any]:
    try:
        return orjson.loads(text)
    except Exception:
        try:
            return json.loads(text)
        except Exception:
            return fallback

def _template_skeleton_mon_sat() -> Dict[str,Any]:
    return {
        "name": "Template (Mon–Sat)",
        "goal": "muscle_gain",
        "days": {d: {"title": d.title(), "muscle_groups": [], "exercises": []} for d in DAYS6},
        "notes": [],
    }

# ───────────────── LLM: generate from profile ───────────────
GEN_SYSTEM = (
    "You are a certified strength & conditioning coach. "
    "Output ONLY strict JSON with this schema:\n"
    "{\n"
    '  "template": {\n'
    '    "name": string,\n'
    '    "goal": "muscle_gain" | "fat_loss" | "strength" | "performance",\n'
    '    "days": {\n'
    '      "monday":    {"title": string, "muscle_groups": string[], "exercises":[{"name":string,"sets":int|null,"reps":string|int|null,"note":string|null}]},\n'
    '      "tuesday":   {...},\n'
    '      "wednesday": {...},\n'
    '      "thursday":  {...},\n'
    '      "friday":    {...},\n'
    '      "saturday":  {...}\n'
    "    },\n"
    '    "notes": string[]\n'
    "  },\n"
    '  "rationale": string\n'
    "}\n"
    "- ALWAYS produce Monday–Saturday only (no Sunday).\n"
    "- Choose a sensible split across the week (Upper/Lower, Push/Pull/Legs, or body-part).\n"
    "- Evidence-based volumes for the goal & experience. No markdown, ONLY JSON."
)

def llm_generate_template_from_profile(oai, model: str, profile: Dict[str,Any]) -> Tuple[Dict[str,Any], str]:
    goal = (profile.get("client_goal") or profile.get("goal") or "muscle gain")
    experience = (profile.get("experience") or "beginner")
    cw = profile.get("current_weight")
    tw = profile.get("target_weight")
    delta_txt = profile.get("weight_delta_text") or ""

    user_prompt = (
        "Build a weekly workout template for this client profile:\n"
        f"- Goal: {goal}\n"
        f"- Days per week: 6 (Mon–Sat only)\n"
        f"- Experience: {experience}\n"
        f"- Current Weight: {cw}\n"
        f"- Target Weight:  {tw}\n"
        f"- Weight Goal: {delta_txt}\n"
        "Use day keys in English: monday..saturday.\n"
        "Pick a split like Upper/Lower or muscle group split so each day has a clear focus."
    )

    try:
        resp = oai.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": GEN_SYSTEM},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
        )
        obj = _safe_json(resp.choices[0].message.content or "{}", {
            "template": _template_skeleton_mon_sat(),
            "rationale": ""
        })

        tpl = obj.get("template") or _template_skeleton_mon_sat()
        rat = obj.get("rationale") or ""

        if isinstance(tpl.get("days"), dict):
            tpl["days"] = {k: v for k, v in tpl["days"].items() if k in DAYS6}
            for d in DAYS6:
                tpl["days"].setdefault(d, {"title": d.title(), "muscle_groups": [], "exercises": []})

        tpl.setdefault("name", "Workout Template (Mon–Sat)")
        return tpl, rat

    except Exception:
        return _template_skeleton_mon_sat(), "Fallback skeleton due to generation error."

# ───────────────────── LLM: edit template ───────────────────
EDIT_SYSTEM = (
    "You are modifying an existing workout template. "
    "Return ONLY strict JSON with schema: {\"template\": <updated>, \"summary\": string}. "
    "Keep unspecified days/exercises unchanged. Respect the instruction precisely. "
    "No markdown; ONLY JSON. Always keep Monday–Saturday day keys."
)

def llm_edit_template(oai, model: str, template: Dict[str,Any], instruction: str, profile_hint: Dict[str,Any]) -> Tuple[Dict[str,Any], str]:
    msgs = [
        {"role":"system","content":EDIT_SYSTEM},
        {"role":"user","content":(
            "Current template JSON:\n"
            + orjson.dumps(template).decode()
            + "\n\nClient hints (goal/experience/weights):\n"
            + orjson.dumps(profile_hint).decode()
            + "\n\nInstruction:\n"
            + (instruction or "").strip()
        )},
    ]
    try:
        resp = oai.chat.completions.create(
            model=model,
            messages=msgs,
            response_format={"type":"json_object"},
            temperature=0,
        )
        obj = _safe_json(resp.choices[0].message.content or "{}", {"template": template, "summary":"No change"})
        updated = obj.get("template") or template

        if isinstance(updated.get("days"), dict):
            updated["days"] = {k: v for k, v in updated["days"].items() if k in DAYS6}
            for d in DAYS6:
                updated["days"].setdefault(d, {"title": d.title(), "muscle_groups": [], "exercises": []})
        updated.setdefault("name", template.get("name") or "Workout Template (Mon–Sat)")

        summary = obj.get("summary") or "Updated."
        return updated, summary
    except Exception:
        return template, "Could not apply change (LLM error); kept previous version."

# ───────────────────── LLM: explain rationale ───────────────
def explain_template_with_llm(oai, model: str, profile: Dict[str,Any], template: Dict[str,Any]) -> str:
    sys = "Explain briefly (2–4 sentences) the training logic. Plain English. No markdown."
    usr = "Client profile:\n" + orjson.dumps(profile).decode() + "\n\nTemplate (Mon–Sat only):\n" + orjson.dumps(template).decode()
    try:
        resp = oai.chat.completions.create(
            model=model,
            messages=[{"role":"system","content":sys},{"role":"user","content":usr}],
            temperature=0.2,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        return "Compound-first approach with weekly distribution tailored to your goal, experience, and Mon–Sat frequency."
