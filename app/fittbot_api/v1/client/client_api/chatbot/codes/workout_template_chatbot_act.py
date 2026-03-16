# from __future__ import annotations
# import os, orjson, uuid, re, secrets
# from typing import Dict, Any, Optional, Iterable
# from fastapi import APIRouter, HTTPException, Depends, Query
# from fastapi.responses import StreamingResponse
# from fastapi_limiter.depends import RateLimiter
# from sqlalchemy.orm import Session

# from app.models.deps import get_mem, get_oai
# from app.models.database import get_db

# from app.fittbot_api.v1.client_api.chatbot.chatbot_services.llm_helpers import (
#     sse_json, OPENAI_MODEL, is_yes as _is_yes_base, is_no as _is_no_base
# )
# from app.fittbot_api.v1.client_api.chatbot.chatbot_services.workout_llm_helper import (
#     is_workout_template_intent,
#     render_markdown_from_template,
#     llm_generate_template_from_profile,
#     llm_edit_template,
#     explain_template_with_llm,
#     DAYS6,
#     build_id_only_structure,   # NEW
# )

# from app.models.fittbot_models import Client, WeightJourney, WorkoutTemplate

# router = APIRouter(prefix="/workout_template", tags=["workout_template"])

# # ───────────────────── utilities ─────────────────────
# def _evt(payload: Dict[str, Any]) -> str:
#     """Wrap as SSE line. Always set unique ids and avoid 'prompt' so the UI uses 'ask'."""
#     payload = {"msg_id": str(uuid.uuid4()), "id": str(uuid.uuid4()), "prompt": "", **payload}
#     return sse_json(payload)

# def _pick(lines: Iterable[str]) -> str:
#     lines = list(lines)
#     if not lines: return ""
#     i = secrets.randbelow(len(lines))
#     return lines[i]

# # More tolerant yes/no detectors for this route only
# _YES_WORDS = {"yes","yeah","yep","sure","ok","okay","please","do it","go ahead","proceed","affirmative","yup"}
# _NO_WORDS  = {"no","nope","nah","not now","later","skip","don’t save","do not save","cancel","hold","not yet","stop"}

# def _is_yes(txt: str) -> bool:
#     t = (txt or "").strip().lower()
#     if _is_yes_base(t): return True
#     return any(w in t for w in _YES_WORDS)

# def _is_no(txt: str) -> bool:
#     t = (txt or "").strip().lower()
#     if _is_no_base(t): return True
#     return any(w in t for w in _NO_WORDS)

# _EDIT_HINT_WORDS = ("change","edit","modify","swap","replace","tweak","adjust","add","remove","increase","decrease","reduce","more","less","heavier","lighter")
# _DAY_WORDS = tuple(DAYS6)

# def _looks_like_edit_instruction(txt: str) -> bool:
#     t = (txt or "").lower()
#     return any(w in t for w in _EDIT_HINT_WORDS) or any(d in t for d in _DAY_WORDS)

# # ───────────────────── Profile from DB ──────────────────────
# def _fetch_profile(db: Session, client_id: int) -> Dict[str, Any]:
#     w = (
#         db.query(WeightJourney)
#         .where(WeightJourney.client_id == client_id)
#         .order_by(WeightJourney.id.desc())
#         .first()
#     )
#     current_weight = float(w.actual_weight) if w and w.actual_weight is not None else None
#     target_weight  = float(w.target_weight) if w and w.target_weight is not None else None

#     weight_delta_text = None
#     if current_weight is not None and target_weight is not None:
#         diff = round(target_weight - current_weight, 1)
#         if diff > 0:
#             weight_delta_text = f"Gain {abs(diff)} kg (from {current_weight} → {target_weight})"
#         elif diff < 0:
#             weight_delta_text = f"Lose {abs(diff)} kg (from {current_weight} → {target_weight})"
#         else:
#             weight_delta_text = f"Maintain {current_weight} kg"

#     c = db.query(Client).where(Client.client_id == client_id).first()
#     goal = (getattr(c, "goals", None) or getattr(c, "goal", None) or "muscle gain") if c else "muscle gain"
#     # experience = (getattr(c, "experience", None) or "beginner") if c else "beginner"

#     return {
#         "current_weight": current_weight,
#         "target_weight": target_weight,
#         "weight_delta_text": weight_delta_text,
#         "client_goal": goal,
#         "days_per_week": 6,  # Mon–Sat
#         "experience": experience,
#     }

# # ───────────────────── Storage helpers ──────────────────────
# async def _store_template(mem, db: Session, client_id: int, template: dict, name: str) -> bool:
#     try:
#         id_only = build_id_only_structure(template)  # NEW
#         await mem.r.set(
#             f"workout_template:{client_id}",
#             orjson.dumps({"name": name, "template": template, "template_ids": id_only})
#         )
#     except Exception:
#         pass

#     ok = True
#     return ok

# async def _get_saved_template(mem, db: Session, client_id: int) -> Optional[Dict[str, Any]]:
#     try:
#         raw = await mem.r.get(f"workout_template:{client_id}")
#         if raw:
#             obj = orjson.loads(raw)
#             # backfill ids-only if an older record
#             if "template" in obj and "template_ids" not in obj:
#                 obj["template_ids"] = build_id_only_structure(obj["template"])
#             return obj
#     except Exception:
#         pass

#     try:
#         rec = (
#             db.query(WorkoutTemplate)
#             .where(WorkoutTemplate.client_id == client_id)
#             .order_by(WorkoutTemplate.id.desc())
#             .first()
#         )
#         if rec and getattr(rec, "json", None):
#             tpl = orjson.loads(rec.json)
#             return {"name": rec.name, "template": tpl, "template_ids": build_id_only_structure(tpl)}
#     except Exception:
#         pass
#     return None

# # ───────────────────── Prompt variants ──────────────────────
# ASK_EDIT_Q = (
#     "Do you want to change anything? (yes/no)",
#     "Want to tweak the plan? (yes/no)",
#     "Shall we make any edits? (yes/no)",
#     "Happy with this or edit something? (yes/no)",
#     "Do you need any adjustments? (yes/no)",
#     "Any changes you’d like? (yes/no)",
#     "Should I modify anything? (yes/no)",
#     "Keep as is or change something? (yes/no)",
#     "Do you prefer any edits? (yes/no)",
#     "Shall I revise parts of it? (yes/no)",
#     "Want to swap any exercises? (yes/no)",
#     "Change the split or keep it? (yes/no)",
#     "Edit sets/reps anywhere? (yes/no)",
#     "Need different muscle focus on a day? (yes/no)",
#     "Adjust volume/intensity? (yes/no)",
#     "Shall we personalize it further? (yes/no)",
#     "Anything to fine-tune? (yes/no)",
#     "Do you want another pass? (yes/no)",
#     "Should we refactor a day? (yes/no)",
#     "Ready to edit or save? (yes/no)",
#     "Do you want me to alter something? (yes/no)",
# )

# ASK_EDIT_FREE = (
#     "Tell me the change (e.g., “On Wednesday replace lunges with leg press 4×10”).",
#     "What should I edit? You can say “Make Monday Upper, Tuesday Lower”.",
#     "Shoot your tweak—day + exercise + sets/reps if needed.",
#     "What would you like to adjust (split, muscles, exercises, volume)?",
#     "Give me the instruction, I’ll update the template.",
#     "Type your change in natural language; I’ll understand.",
#     "Which day/exercise should we modify?",
#     "Describe the edit and I’ll apply it.",
#     "What needs to be different?",
#     "Tell me exactly what to change.",
# )

# CONFIRM_SAVE = (
#     "Save this template? (yes/no)",
#     "Shall I save it now? (yes/no)",
#     "Do you want me to store this? (yes/no)",
#     "Keep this as your workout template? (yes/no)",
#     "Should I save these changes? (yes/no)",
#     "Save to your profile? (yes/no)",
#     "Do you want to finalize and save? (yes/no)",
#     "Lock this in? (yes/no)",
#     "Store this version? (yes/no)",
#     "Save it, or keep editing? (yes/no)",
#     "Is this the final version to save? (yes/no)",
#     "Shall I commit the template? (yes/no)",
#     "Do you want to persist this plan? (yes/no)",
#     "Ready to save? (yes/no)",
#     "Make it official and save? (yes/no)",
#     "Should I archive this as your plan? (yes/no)",
#     "Confirm save? (yes/no)",
#     "Save current draft? (yes/no)",
#     "Write this to your account? (yes/no)",
#     "Store and finish? (yes/no)",
# )

# CONFIRM_START = (
#     "Shall I create your Mon–Sat workout template? (yes/no)",
#     "Do you want me to build a Mon–Sat plan now? (yes/no)",
#     "Start generating your Mon–Sat template? (yes/no)",
#     "Shall I begin the Mon–Sat template? (yes/no)",
#     "Create your weekly plan (Mon–Sat) now? (yes/no)",
# )

# LOADER_LINES = (
#     "Preparing template for you…",
#     "Building your Mon–Sat plan…",
#     "Assembling exercises and splits…",
#     "Drafting your weekly routine…",
#     "Optimizing volume across the week…",
# )

# # ───────────────────────── SSE Route ────────────────────────
# @router.get("/workout_stream")
# # @router.get("/workout_stream", dependencies=[Depends(RateLimiter(times=30, seconds=60))])
# async def workout_template_stream(
#     user_id: int,
#     text: str = Query(...),
#     mem = Depends(get_mem),
#     oai  = Depends(get_oai),
#     db: Session = Depends(get_db),
# ):
#     if not user_id or not text.strip():
#         raise HTTPException(400, "user_id and text required")

#     tlower = text.lower().strip()
#     pend = (await mem.get_pending(user_id)) or {}

#     # ───────────── SHOW saved template ─────────────
#     if any(k in tlower for k in ("show", "view", "see", "display", "my template")) and any(
#         k in tlower for k in ("workout", "training", "template", "plan", "routine")
#     ):
#         saved = await _get_saved_template(mem, db, user_id)
#         if saved:
#             tpl = saved.get("template") or {}
#             md  = render_markdown_from_template(tpl)
#             tpl_ids = saved.get("template_ids") or build_id_only_structure(tpl)
#             async def _show_saved():
#                 yield _evt({"type":"workout_template","status":"fetched",
#                             "template_markdown": md, "template_json": tpl,
#                             "template_ids": tpl_ids})
#                 yield "event: done\ndata: [DONE]\n\n"
#             return StreamingResponse(_show_saved(), media_type="text/event-stream",
#                                      headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})
#         async def _no_saved():
#             yield _evt({"type":"workout_template","status":"hint",
#                         "ask":"No saved workout template yet. Say “create workout template” to build one."})
#             yield "event: done\ndata: [DONE]\n\n"
#         return StreamingResponse(_no_saved(), media_type="text/event-stream",
#                                  headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

#     # ───────────── confirm start ─────────────
#     if pend.get("state") == "awaiting_wt_confirm":
#         prof = pend.get("wt_profile") or {}

#         if _is_yes(text):
#             async def _run():
#                 yield _evt({"type": "workout_template", "status": "start", "loader": _pick(LOADER_LINES)})

#                 tpl, why = llm_generate_template_from_profile(oai, OPENAI_MODEL, prof, db)
#                 md = render_markdown_from_template(tpl)
#                 tpl_ids = build_id_only_structure(tpl)

#                 await mem.set_pending(user_id, {
#                     "state": "awaiting_wt_edit_decision",
#                     "wt_profile": prof,
#                     "wt_template": tpl
#                 })

#                 yield _evt({"type": "workout_template", "status": "draft",
#                             "template_markdown": md, "template_json": tpl,
#                             "template_ids": tpl_ids, "why": why or ""})

#                 yield _evt({"type": "workout_template", "status": "ask_edit_q", "ask": _pick(ASK_EDIT_Q)})
#                 yield "event: done\ndata: [DONE]\n\n"
#             return StreamingResponse(_run(), media_type="text/event-stream",
#                                      headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

#         if _is_no(text):
#             await mem.set_pending(user_id, {"state": "wt_editing", "wt_profile": prof, "wt_template": None})
#             async def _ask_edit_prefs():
#                 yield _evt({"type": "workout_template", "status": "ask_edit", "ask": _pick(ASK_EDIT_FREE)})
#                 yield "event: done\ndata: [DONE]\n\n"
#             return StreamingResponse(_ask_edit_prefs(), media_type="text/event-stream",
#                                      headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

#         async def _clar():
#             yield _evt({"type": "workout_template", "status": "confirm_start", "ask": _pick(CONFIRM_START)})
#             yield "event: done\ndata: [DONE]\n\n"
#         return StreamingResponse(_clar(), media_type="text/event-stream",
#                                  headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

#     # ───────────── user decides to edit after draft ─────────────
#     if pend.get("state") == "awaiting_wt_edit_decision":
#         prof = pend.get("wt_profile") or {}
#         tpl  = pend.get("wt_template") or {}

#         if _is_yes(text):
#             await mem.set_pending(user_id, {"state": "wt_editing", "wt_profile": prof, "wt_template": tpl})
#             async def _ask_what_to_edit():
#                 yield _evt({"type": "workout_template", "status": "ask_edit", "ask": _pick(ASK_EDIT_FREE)})
#                 yield "event: done\ndata: [DONE]\n\n"
#             return StreamingResponse(_ask_what_to_edit(), media_type="text/event-stream",
#                                      headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

#         if _is_no(text):
#             await mem.set_pending(user_id, {"state": "awaiting_wt_store_confirm", "wt_profile": prof, "wt_template": tpl})
#             async def _ask_save():
#                 yield _evt({"type": "workout_template", "status": "confirm_store", "ask": _pick(CONFIRM_SAVE)})
#                 yield "event: done\ndata: [DONE]\n\n"
#             return StreamingResponse(_ask_save(), media_type="text/event-stream",
#                                      headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

#         async def _clar_edit_decision():
#             yield _evt({"type":"workout_template","status":"ask_edit_q","ask":_pick(ASK_EDIT_Q)})
#             yield "event: done\ndata: [DONE]\n\n"
#         return StreamingResponse(_clar_edit_decision(), media_type="text/event-stream",
#                                  headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

#     # ───────────── edit mode ─────────────
#     if pend.get("state") == "wt_editing":
#         prof = pend.get("wt_profile") or {}
#         tpl  = pend.get("wt_template")
#         if not tpl:
#             tpl, _ = llm_generate_template_from_profile(oai, OPENAI_MODEL, prof, db)

#         new_tpl, summary = llm_edit_template(oai, OPENAI_MODEL, tpl, text, prof, db)
#         md = render_markdown_from_template(new_tpl)
#         tpl_ids = build_id_only_structure(new_tpl)

#         await mem.set_pending(user_id, {"state": "awaiting_wt_store_confirm", "wt_profile": prof, "wt_template": new_tpl})

#         async def _edited():
#             yield _evt({"type": "workout_template", "status": "edit_applied",
#                         "template_markdown": md, "template_json": new_tpl,
#                         "template_ids": tpl_ids, "summary": summary or "Applied your change."})
#             yield _evt({"type": "workout_template", "status": "confirm_store", "ask": _pick(CONFIRM_SAVE)})
#             yield "event: done\ndata: [DONE]\n\n"
#         return StreamingResponse(_edited(), media_type="text/event-stream",
#                                  headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

#     # ───────────── store confirm ─────────────
#     if pend.get("state") == "awaiting_wt_store_confirm":
#         prof = pend.get("wt_profile") or {}
#         tpl  = pend.get("wt_template") or {}

#         if _is_no(text) and _looks_like_edit_instruction(text):
#             new_tpl, summary = llm_edit_template(oai, OPENAI_MODEL, tpl, text, prof, db)
#             md = render_markdown_from_template(new_tpl)
#             tpl_ids = build_id_only_structure(new_tpl)
#             await mem.set_pending(user_id, {"state": "awaiting_wt_store_confirm", "wt_profile": prof, "wt_template": new_tpl})
#             async def _applied_from_no():
#                 yield _evt({"type":"workout_template","status":"edit_applied",
#                             "template_markdown": md, "template_json": new_tpl,
#                             "template_ids": tpl_ids, "summary": summary or "Applied your change."})
#                 yield _evt({"type":"workout_template","status":"confirm_store","ask": _pick(CONFIRM_SAVE)})
#                 yield "event: done\ndata: [DONE]\n\n"
#             return StreamingResponse(_applied_from_no(), media_type="text/event-stream",
#                                      headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

#         if _is_yes(text):
#             name = tpl.get("name") or "Workout Template (Mon–Sat)"
#             await _store_template(mem, db, user_id, tpl, name)
#             print("template is", name)

#             try:
#                 import httpx
#                 # if your app is same process, you can call the function directly instead of HTTP.
#                 STRUCT_URL = "http://localhost:8000/workout_template/structurize_and_save"
#                 async with httpx.AsyncClient(timeout=20) as client:
#                     await client.post(STRUCT_URL, json={"client_id": user_id, "template": tpl})
#             except Exception as e:
#                 # non-blocking; log if you want
#                 print("structurize_and_save failed:", e)

#             await mem.set_pending(user_id, None)
#             async def _done():
#                 yield _evt({"type": "workout_template", "status": "stored", "template_name": name, "info": "Saved."})
#                 yield "event: done\ndata: [DONE]\n\n"
#             return StreamingResponse(_done(), media_type="text/event-stream",
#                                      headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

#         if _is_no(text):
#             await mem.set_pending(user_id, {"state":"wt_editing","wt_profile": prof, "wt_template": tpl})
#             async def _ask_more():
#                 yield _evt({"type": "workout_template", "status": "ask_edit", "ask": _pick(ASK_EDIT_FREE)})
#                 yield "event: done\ndata: [DONE]\n\n"
#             return StreamingResponse(_ask_more(), media_type="text/event-stream",
#                                      headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

#         async def _clar_store():
#             yield _evt({"type": "workout_template", "status": "confirm_store", "ask": _pick(CONFIRM_SAVE)})
#             yield "event: done\ndata: [DONE]\n\n"
#         return StreamingResponse(_clar_store(), media_type="text/event-stream",
#                                  headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

#     # ───────────── fresh: create flow ──────────
#     if is_workout_template_intent(tlower) or "template" in tlower or "workout" in tlower or "routine" in tlower:
#         prof = _fetch_profile(db, user_id) or {}
#         goal_txt = (prof.get("client_goal") or "muscle gain")
#         cw, tw, dt = prof.get("current_weight"), prof.get("target_weight"), prof.get("weight_delta_text")
#         exp = prof.get("experience") or "beginner"

#         line = f"I pulled your profile — Goal: {goal_txt}; Days/Week: Mon–Sat; Experience: {exp}."
#         if cw is not None and tw is not None and dt:
#             line += f" Current: {cw} kg → Target: {tw} kg ({dt})."

#         await mem.set_pending(user_id, {"state":"awaiting_wt_confirm","wt_profile":{
#             "goal": goal_txt, "days_per_week": 6, "experience": exp,
#             "current_weight": cw, "target_weight": tw, "weight_delta_text": dt
#         }})

#         async def _confirm_profile():
#             yield _evt({"type": "workout_template", "status": "confirm_start", "ask": line + " " + _pick(CONFIRM_START)})
#             yield "event: done\ndata: [DONE]\n\n"
#         return StreamingResponse(_confirm_profile(), media_type="text/event-stream",
#                                  headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

#     # ───────────── user typed edit command without state ─────────────
#     if _looks_like_edit_instruction(tlower):
#         saved = await _get_saved_template(mem, db, user_id)
#         prof  = _fetch_profile(db, user_id) or {}
#         if saved:
#             await mem.set_pending(user_id, {"state":"wt_editing","wt_profile": prof, "wt_template": saved.get("template")})
#             async def _go_edit():
#                 yield _evt({"type":"workout_template","status":"ask_edit","ask":_pick(ASK_EDIT_FREE)})
#                 yield "event: done\ndata: [DONE]\n\n"
#             return StreamingResponse(_go_edit(), media_type="text/event-stream",
#                                      headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

#         await mem.set_pending(user_id, {"state":"awaiting_wt_confirm","wt_profile": prof})
#         async def _need_tpl_first():
#             yield _evt({"type":"workout_template","status":"confirm_start","ask":"Let’s create your template first. " + _pick(CONFIRM_START)})
#             yield "event: done\ndata: [DONE]\n\n"
#         return StreamingResponse(_need_tpl_first(), media_type="text/event-stream",
#                                  headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

#     # default hint
#     async def _hint():
#         yield _evt({"type":"workout_template","status":"hint",
#                     "ask":"Say “create workout template” to start, “show my workout template” to view, or type a change like “On Tuesday replace rows with pull-ups 3×8”."
#                    })
#         yield "event: done\ndata: [DONE]\n\n"
#     return StreamingResponse(_hint(), media_type="text/event-stream",
#                              headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})
