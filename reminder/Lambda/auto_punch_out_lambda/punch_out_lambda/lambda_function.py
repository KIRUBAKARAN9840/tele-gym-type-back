

import os
import json
import logging
import httpx

log = logging.getLogger()
log.setLevel(logging.INFO)

OUT_PUNCH_URL = "https://app.fittbot.com/check_attendance/out_punch"
TIMEOUT       = float(os.environ.get("OUT_PUNCH_TIMEOUT", "3"))

def lambda_handler(event, _context):
    total, success, failed = 0, 0, 0

    with httpx.Client(timeout=TIMEOUT) as client:
        for rec in event.get("Records", []):
            # parse the SQS record body
            try:
                payload = json.loads(rec["body"])
                items   = payload.get("list", [])
            except json.JSONDecodeError as e:
                log.error("Invalid JSON in record: %s", e)
                continue

            # dedupe the list so we only punch each (gym_id, client_id) once
            seen = set()
            for it in items:
                gym_id    = it.get("gym_id")
                client_id = it.get("client_id")
                if gym_id is None or client_id is None:
                    log.error("Missing gym_id or client_id: %s", it)
                    failed += 1
                    continue

                key = (gym_id, client_id)
                if key in seen:
                    continue
                seen.add(key)

                total += 1
                try:
                    resp = client.post(
                        OUT_PUNCH_URL,
                        json={"gym_id": gym_id, "client_id": client_id},
                    )
                    # treat 200 OK _and_ 400 “already punched out” as success
                    if resp.status_code == 200 or resp.status_code == 400:
                        success += 1
                    else:
                        failed += 1
                        log.error(
                            "Punch-out failed %s → %d %s",
                            it, resp.status_code, resp.text[:200]
                        )
                except Exception as e:
                    failed += 1
                    log.error("HTTP error punching out %s: %s", it, e)

    log.info("Auto-punch summary: total=%d success=%d failed=%d", total, success, failed)

    # only raise (and trigger retry) if you had real failures
    if failed > 0:
        raise RuntimeError(f"{failed}/{total} punch-out calls failed")

    return {"total": total, "success": success, "failed": failed}




# import os
# import json
# import logging
# import httpx

# log = logging.getLogger()
# log.setLevel(logging.INFO)


# OUT_PUNCH_URL = "https://app.fittbot.com/check_attendance/out_punch"
# TIMEOUT       = float(os.environ.get("OUT_PUNCH_TIMEOUT", "3"))

# def lambda_handler(event, _context):

#     total, success, failed = 0, 0, 0

#     with httpx.Client(timeout=TIMEOUT) as client:
#         for rec in event.get("Records", []):
#             try:
#                 payload = json.loads(rec["body"])
#                 items   = payload.get("list", [])
#             except json.JSONDecodeError as e:
#                 log.error("Invalid JSON in record: %s", e)
#                 continue

#             for it in items:
#                 total += 1
#                 gym_id    = it.get("gym_id")
#                 client_id = it.get("client_id")
#                 if gym_id is None or client_id is None:
#                     log.error("Missing gym_id or client_id: %s", it)
#                     failed += 1
#                     continue

#                 try:
#                     resp = client.post(
#                         OUT_PUNCH_URL,
#                         json={"gym_id": gym_id, "client_id": client_id},
#                     )
#                     if resp.status_code == 200:
#                         success += 1
#                     else:
#                         failed += 1
#                         log.error(
#                             "Punch-out failed %s → %d %s",
#                             it, resp.status_code, resp.text[:200]
#                         )
#                 except Exception as e:
#                     failed += 1
#                     log.error("HTTP error punching out %s: %s", it, e)

#     log.info("Auto-punch summary: total=%d success=%d failed=%d", total, success, failed)

#     if failed > 0:
#         raise RuntimeError(f"{failed}/{total} punch-out calls failed")

#     return {"total": total, "success": success, "failed": failed}



