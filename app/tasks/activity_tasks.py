

import json
import logging
from datetime import datetime, timedelta
from typing import Optional

from app.celery_app import celery_app
from app.models.async_database import create_celery_async_sessionmaker
from app.utils.celery_asyncio import get_worker_loop
from app.utils.redis_config import get_redis_sync

logger = logging.getLogger("tasks.activity")

# Thresholds
ABANDONED_CHECKOUT_MINUTES = 30  
REPEATED_VIEW_THRESHOLD = 3
EVENTS_BATCH_SIZE = 100
MAX_EVENTS_PER_CYCLE = 1000  


QUEUE_DEPTH_WARNING = 500
QUEUE_DEPTH_CRITICAL = 2000


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 1: Process events from Redis queue → MySQL
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(
    name="activity.process_events",
    bind=True,
    max_retries=3,
    default_retry_delay=10,
)
def process_events(self):

    try:
        redis = get_redis_sync()
        total_processed = 0
        total_summaries = 0
        total_bookings = 0

        # Adaptive draining: keep processing batches until queue is empty or safety cap hit
        while total_processed < MAX_EVENTS_PER_CYCLE:
            events = []

            # Pop up to EVENTS_BATCH_SIZE events per batch
            for _ in range(EVENTS_BATCH_SIZE):
                raw = redis.lpop("activity:events:queue")
                if raw is None:
                    break
                try:
                    events.append(json.loads(raw))
                except (json.JSONDecodeError, TypeError):
                    logger.warning("[PROCESS_EVENTS] Skipping malformed activity event")

            if not events:
                break  # queue is empty

            logger.info(f"[PROCESS_EVENTS] Popped {len(events)} events (total so far: {total_processed})")
            for ev in events:
                logger.debug(f"[PROCESS_EVENTS] event: client={ev.get('client_id')} type={ev.get('event_type')} gym={ev.get('gym_id')} product={ev.get('product_type')} cmd={ev.get('command_id')}")

            loop = get_worker_loop()
            result = loop.run_until_complete(_persist_events(events))

            total_processed += result.get("processed", 0)
            total_summaries += result.get("summaries_updated", 0)
            total_bookings += result.get("booking_confirmations_sent", 0)

        # Log remaining queue depth after draining
        if total_processed >= MAX_EVENTS_PER_CYCLE:
            remaining = redis.llen("activity:events:queue")
            if remaining > 0:
                logger.warning(f"[PROCESS_EVENTS] Hit safety cap ({MAX_EVENTS_PER_CYCLE}), still {remaining} events in queue")

        logger.info(f"[PROCESS_EVENTS] Done: processed={total_processed} summaries={total_summaries} bookings={total_bookings}")
        return {
            "processed": total_processed,
            "summaries_updated": total_summaries,
            "booking_confirmations_sent": total_bookings,
        }

    except Exception as exc:
        logger.error(f"[PROCESS_EVENTS] ERROR: {repr(exc)}")
        raise


async def _persist_events(events: list) -> dict:
    """Bulk-insert events and upsert summaries. Trigger booking WhatsApp for completed checkouts."""
    from sqlalchemy import select, func
    from app.models.client_activity_models import (
        ClientActivityEvent,
        ClientActivitySummary,
    )

    # Collect checkout_completed events for booking confirmation WhatsApp
    completed_checkouts = [ev for ev in events if ev["event_type"] == "checkout_completed"]
    if completed_checkouts:
        logger.info(f"[PERSIST] Found {len(completed_checkouts)} checkout_completed events, will send booking WhatsApp")

    SessionLocal = create_celery_async_sessionmaker()
    async with SessionLocal() as db:
        # Bulk insert raw events
        event_objects = []
        for ev in events:
            event_objects.append(ClientActivityEvent(
                client_id=ev["client_id"],
                event_type=ev["event_type"],
                gym_id=ev.get("gym_id"),
                product_type=ev.get("product_type"),
                product_details=ev.get("product_details"),
                source=ev.get("source"),
                command_id=ev.get("command_id"),
                created_at=datetime.fromisoformat(ev["created_at"]),
            ))
        db.add_all(event_objects)
        logger.info(f"[PERSIST] Inserted {len(event_objects)} events into DB")

        # Upsert summaries for each unique client-gym pair
        summary_updates = {}
        for ev in events:
            gym_id = ev.get("gym_id")
            if gym_id is None:
                continue
            key = (ev["client_id"], gym_id)
            if key not in summary_updates:
                summary_updates[key] = {
                    "views": 0, "checkouts": 0, "purchases": 0,
                    "products": set(),
                }
            et = ev["event_type"]
            if et in ("gym_viewed", "dailypass_viewed", "session_viewed", "membership_viewed"):
                summary_updates[key]["views"] += 1
                product_map = {
                    "dailypass_viewed": "dailypass",
                    "session_viewed": "session",
                    "membership_viewed": "membership",
                }
                if et in product_map:
                    summary_updates[key]["products"].add(product_map[et])
            elif et == "checkout_initiated":
                summary_updates[key]["checkouts"] += 1
                if ev.get("product_type"):
                    summary_updates[key]["products"].add(ev["product_type"])
            elif et == "checkout_completed":
                summary_updates[key]["purchases"] += 1

        now = datetime.now()
        for (client_id, gym_id), delta in summary_updates.items():
            stmt = select(ClientActivitySummary).where(
                ClientActivitySummary.client_id == client_id,
                ClientActivitySummary.gym_id == gym_id,
            )
            result = await db.execute(stmt)
            summary = result.scalars().first()

            if summary is None:
                summary = ClientActivitySummary(
                    client_id=client_id,
                    gym_id=gym_id,
                    total_views=0,
                    checkout_attempts=0,
                    purchases=0,
                    interested_products=[],
                )
                db.add(summary)
                logger.debug(f"[PERSIST] New summary for client={client_id} gym={gym_id}")

            summary.total_views += delta["views"]
            summary.checkout_attempts += delta["checkouts"]
            summary.purchases += delta["purchases"]
            if delta["views"] > 0:
                summary.last_viewed_at = now
            if delta["checkouts"] > 0:
                summary.last_checkout_at = now
            if delta["purchases"] > 0:
                summary.last_purchase_at = now

            # Merge interested products
            existing = set(summary.interested_products or [])
            existing.update(delta["products"])
            summary.interested_products = list(existing)

            # Recalculate lead score and status
            summary.lead_score = (
                summary.total_views * 1
                + summary.checkout_attempts * 5
                - summary.purchases * 10
            )
            if summary.purchases > 0:
                summary.lead_status = "converted"
            elif summary.checkout_attempts > 0:
                summary.lead_status = "hot"
            elif summary.total_views >= REPEATED_VIEW_THRESHOLD:
                summary.lead_status = "warm"
            else:
                summary.lead_status = "cold"

            logger.debug(f"[PERSIST] Summary client={client_id} gym={gym_id}: views={summary.total_views} checkouts={summary.checkout_attempts} purchases={summary.purchases} status={summary.lead_status}")

        await db.commit()
        logger.info(f"[PERSIST] Committed {len(summary_updates)} summary upserts")

    # Send booking confirmation WhatsApp for completed checkouts
    bookings_sent = 0
    if completed_checkouts:
        bookings_sent = await _send_booking_whatsapp(completed_checkouts)

    return {
        "processed": len(events),
        "summaries_updated": len(summary_updates),
        "booking_confirmations_sent": bookings_sent,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Booking confirmation WhatsApp (triggered by checkout_completed events)
# ═══════════════════════════════════════════════════════════════════════════════

async def _send_booking_whatsapp(completed_events: list) -> int:
    """
    Send WhatsApp booking confirmation for completed checkouts.
    Looks up the matching checkout_initiated event (same command_id) for gym_id and product_details.
    """
    from sqlalchemy import select
    from app.models.fittbot_models import Client, Gym
    from app.models.client_activity_models import ClientActivityEvent, ClientWhatsAppLog
    from app.utils.whatsapp.whatsapp_client import (
        get_whatsapp_client,
        WHATSAPP_TEMPLATE_BOOKED_DAILYPASS,
        WHATSAPP_TEMPLATE_BOOKED_SESSION,
        WHATSAPP_TEMPLATE_BOOKED_MEMBERSHIP,
        WHATSAPP_TEMPLATE_BOOKED_SUBSCRIPTION,
    )

    logger.info(f"[BOOKING_WA] Processing {len(completed_events)} completed checkouts")
    messages_sent = 0
    wa_client = get_whatsapp_client()

    SessionLocal = create_celery_async_sessionmaker()
    async with SessionLocal() as db:
        for ev in completed_events:
            client_id = ev["client_id"]
            command_id = ev.get("command_id")
            product_type = ev.get("product_type")

            logger.info(f"[BOOKING_WA] client={client_id} product={product_type} command={command_id}")

            if not product_type:
                logger.warning(f"[BOOKING_WA] SKIP client={client_id}: missing product_type")
                continue

            # Look up the most recent checkout_initiated for this client + product_type
            # (command_id differs between checkout and verify, so we match by client + product instead)
            initiated_result = await db.execute(
                select(ClientActivityEvent).where(
                    ClientActivityEvent.client_id == client_id,
                    ClientActivityEvent.event_type == "checkout_initiated",
                    ClientActivityEvent.product_type == product_type,
                ).order_by(ClientActivityEvent.created_at.desc()).limit(1)
            )
            initiated = initiated_result.scalars().first()

            if initiated:
                gym_id = initiated.gym_id
                product_details = initiated.product_details or {}
                logger.info(f"[BOOKING_WA] Found checkout_initiated: gym_id={gym_id} product_details={product_details}")
            else:
                gym_id = None
                product_details = {}
                logger.warning(f"[BOOKING_WA] No checkout_initiated found for client={client_id} product={product_type}, gym_id will be None")

            # Get client info
            client_result = await db.execute(
                select(Client).where(Client.client_id == client_id)
            )
            client = client_result.scalars().first()
            if not client:
                logger.warning(f"[BOOKING_WA] SKIP client={client_id}: client not found in DB")
                continue
            if not client.contact:
                logger.warning(f"[BOOKING_WA] SKIP client={client_id}: no contact number")
                continue

            client_name = client.name or "there"

            # Get gym name
            gym_name = "your gym"
            if gym_id:
                gym_result = await db.execute(
                    select(Gym).where(Gym.gym_id == gym_id)
                )
                gym = gym_result.scalars().first()
                if gym:
                    gym_name = gym.name or "your gym"

            logger.info(f"[BOOKING_WA] Sending: client={client_id} name={client_name} phone={client.contact} product={product_type} gym={gym_name}")

            try:
                if product_type == "dailypass":
                    days = str(product_details.get("days", 1))
                    logger.info(f"[BOOKING_WA] send_booked_dailypass to={client.contact} vars=[{client_name}, Daily Pass, {gym_name}, {days}]")
                    response = await wa_client.send_booked_dailypass(
                        to=client.contact,
                        client_name=client_name,
                        pass_name="Daily Pass",
                        gym_name=gym_name,
                        days=days,
                    )
                    template_id = WHATSAPP_TEMPLATE_BOOKED_DAILYPASS
                    variables = [client_name, "Daily Pass", gym_name, days]

                elif product_type == "session":
                    session_count = str(product_details.get("sessions_count", 1))
                    logger.info(f"[BOOKING_WA] send_booked_session to={client.contact} vars=[{client_name}, {session_count}, {gym_name}]")
                    response = await wa_client.send_booked_session(
                        to=client.contact,
                        client_name=client_name,
                        session_count=session_count,
                        gym_name=gym_name,
                    )
                    template_id = WHATSAPP_TEMPLATE_BOOKED_SESSION
                    variables = [client_name, session_count, gym_name]

                elif product_type == "membership":
                    logger.info(f"[BOOKING_WA] send_booked_membership to={client.contact} vars=[{client_name}, {gym_name}]")
                    response = await wa_client.send_booked_membership(
                        to=client.contact,
                        client_name=client_name,
                        gym_name=gym_name,
                    )
                    template_id = WHATSAPP_TEMPLATE_BOOKED_MEMBERSHIP
                    variables = [client_name, gym_name]

                elif product_type == "subscription":
                    plan_name = product_details.get("plan_sku", "Fymble")
                    logger.info(f"[BOOKING_WA] send_booked_subscription to={client.contact} vars=[{client_name}, {plan_name}]")
                    response = await wa_client.send_booked_subscription(
                        to=client.contact,
                        client_name=client_name,
                        plan_name=plan_name,
                    )
                    template_id = WHATSAPP_TEMPLATE_BOOKED_SUBSCRIPTION
                    variables = [client_name, plan_name]

                else:
                    logger.warning(f"[BOOKING_WA] SKIP client={client_id}: unknown product_type={product_type}")
                    continue

                logger.info(f"[BOOKING_WA] API response: success={response.success} status={response.status_code} guid={response.guid} error={response.error}")

                # Log the WhatsApp message
                log_entry = ClientWhatsAppLog(
                    client_id=client_id,
                    trigger_type="booking_confirmation",
                    template_name=template_id,
                    variables=variables,
                    gym_id=gym_id,
                    whatsapp_guid=response.guid if response.success else None,
                    status="sent" if response.success else "failed",
                    sent_by="system",
                )
                db.add(log_entry)

                if response.success:
                    messages_sent += 1
                    logger.info(f"[BOOKING_WA] SUCCESS client={client_id} guid={response.guid}")
                else:
                    logger.warning(f"[BOOKING_WA] FAILED client={client_id} error={response.error}")

            except Exception as e:
                logger.error(f"[BOOKING_WA] EXCEPTION client={client_id}: {repr(e)}")

        await db.commit()

    logger.info(f"[BOOKING_WA] Done: {messages_sent}/{len(completed_events)} messages sent")
    return messages_sent


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 2: Detect abandoned checkouts → send WhatsApp
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(
    name="activity.check_abandoned_checkouts",
    bind=True,
    max_retries=2,
    default_retry_delay=30,
)
def check_abandoned_checkouts(self):

    try:
        redis = get_redis_sync()
        cutoff = datetime.now() - timedelta(minutes=ABANDONED_CHECKOUT_MINUTES)
        abandoned = []

        # Use index set instead of scan_iter for O(active_clients) instead of O(total_keys)
        active_clients = redis.smembers("activity:checkout:active_clients")
        logger.info(f"[ABANDONED] Checking {len(active_clients)} active clients for checkouts older than {ABANDONED_CHECKOUT_MINUTES}min (cutoff={cutoff.isoformat()})")

        for client_id_raw in active_clients:
            client_id_str = client_id_raw if isinstance(client_id_raw, str) else client_id_raw.decode()
            try:
                client_id = int(client_id_str)
            except (ValueError, TypeError):
                redis.srem("activity:checkout:active_clients", client_id_raw)
                continue

            key = f"activity:checkout:{client_id}"

            # If the checkout hash expired, clean up the index
            if not redis.exists(key):
                redis.srem("activity:checkout:active_clients", client_id_str)
                logger.debug(f"[ABANDONED] Cleaned up expired index for client={client_id}")
                continue

            checkouts = redis.hgetall(key)
            logger.debug(f"[ABANDONED] client={client_id} has {len(checkouts)} pending checkouts")

            for command_id, data_raw in checkouts.items():
                if isinstance(command_id, bytes):
                    command_id = command_id.decode()
                if isinstance(data_raw, bytes):
                    data_raw = data_raw.decode()

                try:
                    data = json.loads(data_raw)
                except (json.JSONDecodeError, TypeError):
                    logger.debug(f"[ABANDONED] Skipping malformed data for client={client_id} cmd={command_id}")
                    continue

                initiated_at = datetime.fromisoformat(data.get("initiated_at", ""))
                if initiated_at < cutoff:
                    logger.info(f"[ABANDONED] FOUND: client={client_id} product={data.get('product_type')} gym={data.get('gym_id')} initiated_at={initiated_at.isoformat()} cmd={command_id}")
                    abandoned.append({
                        "client_id": client_id,
                        "gym_id": data.get("gym_id"),
                        "product_type": data.get("product_type"),
                        "command_id": command_id,
                    })
                    # Remove from tracking after processing
                    redis.hdel(key, command_id)
                else:
                    logger.debug(f"[ABANDONED] NOT YET: client={client_id} initiated_at={initiated_at.isoformat()} (still within {ABANDONED_CHECKOUT_MINUTES}min window)")

            # If all checkouts for this client were processed, remove from index
            if not redis.exists(key) or redis.hlen(key) == 0:
                redis.srem("activity:checkout:active_clients", client_id_str)

        logger.info(f"[ABANDONED] Checked {len(active_clients)} active clients, found {len(abandoned)} abandoned checkouts")

        if not abandoned:
            return {"abandoned_found": 0, "messages_sent": 0}

        loop = get_worker_loop()
        result = loop.run_until_complete(_send_abandoned_whatsapp(abandoned))

        logger.info(f"[ABANDONED] Result: {result}")
        return result

    except Exception as exc:
        logger.error(f"[ABANDONED] ERROR: {repr(exc)}")
        raise


async def _send_abandoned_whatsapp(abandoned: list) -> dict:
    """Send WhatsApp messages for abandoned checkouts using pre-built methods."""
    from sqlalchemy import select, func
    from app.models.fittbot_models import Client, Gym
    from app.models.client_activity_models import ClientActivityEvent, ClientWhatsAppLog
    from app.utils.whatsapp.whatsapp_client import get_whatsapp_client, WHATSAPP_TEMPLATE_ABANDONED
    from app.utils.redis_config import get_redis

    redis = await get_redis()
    messages_sent = 0
    clients_with_recent_purchase = set()

    logger.info(f"[ABANDONED_WA] Processing {len(abandoned)} abandoned checkouts")

    SessionLocal = create_celery_async_sessionmaker()
    async with SessionLocal() as db:
        for item in abandoned:
            client_id = item["client_id"]
            gym_id = item.get("gym_id")
            product = item.get("product_type", "dailypass")

            logger.info(f"[ABANDONED_WA] Checking client={client_id} product={product} gym={gym_id}")

            # Rate limit: 1 WhatsApp per user per day
            rate_key = f"activity:wa_sent:{client_id}"
            already_sent = await redis.get(rate_key)
            if already_sent:
                logger.info(f"[ABANDONED_WA] SKIP client={client_id}: rate limited (already sent today)")
                continue

            # Skip if this client made ANY purchase (any gym, any product) in the last 48h
            if client_id in clients_with_recent_purchase:
                logger.info(f"[ABANDONED_WA] SKIP client={client_id}: recently purchased (cached)")
                continue
            recent_cutoff = datetime.now() - timedelta(hours=48)
            purchase_check = await db.execute(
                select(func.count()).select_from(ClientActivityEvent).where(
                    ClientActivityEvent.client_id == client_id,
                    ClientActivityEvent.event_type == "checkout_completed",
                    ClientActivityEvent.created_at >= recent_cutoff,
                )
            )
            purchase_count = purchase_check.scalar()
            if purchase_count > 0:
                clients_with_recent_purchase.add(client_id)
                logger.info(f"[ABANDONED_WA] SKIP client={client_id}: has {purchase_count} purchase(s) in last 48h")
                continue

            # Get client phone number
            client_result = await db.execute(
                select(Client).where(Client.client_id == client_id)
            )
            client = client_result.scalars().first()
            if not client:
                logger.warning(f"[ABANDONED_WA] SKIP client={client_id}: not found in DB")
                continue
            if not client.contact:
                logger.warning(f"[ABANDONED_WA] SKIP client={client_id}: no contact number")
                continue

            # Get gym name for template
            gym_name = "your gym"
            if gym_id:
                gym_result = await db.execute(
                    select(Gym).where(Gym.gym_id == gym_id)
                )
                gym = gym_result.scalars().first()
                if gym:
                    gym_name = gym.name or "your gym"

            client_name = client.name or "there"

            logger.info(f"[ABANDONED_WA] Sending: client={client_id} name={client_name} phone={client.contact} product={product} gym={gym_name}")

            # Send WhatsApp using product-specific pre-built method
            try:
                wa_client = get_whatsapp_client()

                if product == "subscription":
                    response = await wa_client.send_abandoned_subscription(
                        to=client.contact,
                        client_name=client_name,
                        plan_name=gym_name,
                    )
                else:
                    send_method = {
                        "dailypass": wa_client.send_abandoned_dailypass,
                        "membership": wa_client.send_abandoned_membership,
                        "session": wa_client.send_abandoned_session,
                    }.get(product, wa_client.send_abandoned_dailypass)

                    response = await send_method(
                        to=client.contact,
                        client_name=client_name,
                        gym_name=gym_name,
                    )

                logger.info(f"[ABANDONED_WA] API response: success={response.success} status={response.status_code} guid={response.guid} error={response.error}")

                # Log the message
                product_label = {"dailypass": "Daily Pass", "membership": "Membership", "session": "Session", "subscription": "Subscription"}.get(product, "Daily Pass")
                log_entry = ClientWhatsAppLog(
                    client_id=client_id,
                    trigger_type="abandoned_checkout",
                    template_name=WHATSAPP_TEMPLATE_ABANDONED,
                    variables=[client_name, product_label, gym_name],
                    gym_id=gym_id,
                    whatsapp_guid=response.guid if response.success else None,
                    status="sent" if response.success else "failed",
                    sent_by="system",
                )
                db.add(log_entry)

                if response.success:
                    messages_sent += 1
                    await redis.set(rate_key, "1", ex=86400)
                    logger.info(f"[ABANDONED_WA] SUCCESS client={client_id} guid={response.guid}")
                else:
                    logger.warning(f"[ABANDONED_WA] FAILED client={client_id} error={response.error}")

            except Exception as e:
                logger.error(f"[ABANDONED_WA] EXCEPTION client={client_id}: {repr(e)}")

        await db.commit()

    logger.info(f"[ABANDONED_WA] Done: {messages_sent}/{len(abandoned)} messages sent")
    return {"abandoned_found": len(abandoned), "messages_sent": messages_sent}


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 3: Detect repeated browsing → send WhatsApp
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(
    name="activity.check_repeated_browsing",
    bind=True,
    max_retries=2,
    default_retry_delay=30,
)
def check_repeated_browsing(self):
    """
    Scan Redis for clients who viewed the same gym 3+ times without checkout.
    Send a WhatsApp nudge. Runs every hour via Celery Beat.
    """
    try:
        redis = get_redis_sync()
        warm_leads = []

        # Use index set instead of scan_iter for O(active_clients) instead of O(total_keys)
        active_clients = redis.smembers("activity:views:active_clients")
        logger.info(f"[BROWSING] Checking {len(active_clients)} active clients for {REPEATED_VIEW_THRESHOLD}+ views")

        for client_id_raw in active_clients:
            client_id_str = client_id_raw if isinstance(client_id_raw, str) else client_id_raw.decode()
            try:
                client_id = int(client_id_str)
            except (ValueError, TypeError):
                redis.srem("activity:views:active_clients", client_id_raw)
                continue

            key = f"activity:views:{client_id}"

            # If the views hash expired, clean up the index
            if not redis.exists(key):
                redis.srem("activity:views:active_clients", client_id_str)
                logger.debug(f"[BROWSING] Cleaned up expired index for client={client_id}")
                continue

            views = redis.hgetall(key)
            for gym_id_raw, count_raw in views.items():
                if isinstance(gym_id_raw, bytes):
                    gym_id_raw = gym_id_raw.decode()
                if isinstance(count_raw, bytes):
                    count_raw = count_raw.decode()

                try:
                    count = int(count_raw)
                    gym_id = int(gym_id_raw)
                except (ValueError, TypeError):
                    continue

                if count >= REPEATED_VIEW_THRESHOLD:
                    logger.info(f"[BROWSING] WARM LEAD: client={client_id} gym={gym_id} views={count}")
                    warm_leads.append({
                        "client_id": client_id,
                        "gym_id": gym_id,
                        "view_count": count,
                    })
                    redis.hdel(key, gym_id_raw)

            # If all view entries for this client were processed, remove from index
            if not redis.exists(key) or redis.hlen(key) == 0:
                redis.srem("activity:views:active_clients", client_id_str)

        logger.info(f"[BROWSING] Checked {len(active_clients)} active clients, found {len(warm_leads)} warm leads")

        if not warm_leads:
            return {"warm_leads_found": 0, "messages_sent": 0}

        loop = get_worker_loop()
        result = loop.run_until_complete(_send_browsing_whatsapp(warm_leads))

        logger.info(f"[BROWSING] Result: {result}")
        return result

    except Exception as exc:
        logger.error(f"[BROWSING] ERROR: {repr(exc)}")
        raise


async def _send_browsing_whatsapp(warm_leads: list) -> dict:
    """Send WhatsApp nudge for repeated browsing using pre-built method."""
    from sqlalchemy import select, func
    from app.models.fittbot_models import Client, Gym
    from app.models.client_activity_models import ClientActivityEvent, ClientActivitySummary, ClientWhatsAppLog
    from app.utils.whatsapp.whatsapp_client import get_whatsapp_client, WHATSAPP_TEMPLATE_BROWSING
    from app.utils.redis_config import get_redis

    redis = await get_redis()
    messages_sent = 0
    clients_with_recent_purchase = set()

    logger.info(f"[BROWSING_WA] Processing {len(warm_leads)} warm leads")

    SessionLocal = create_celery_async_sessionmaker()
    async with SessionLocal() as db:
        for item in warm_leads:
            client_id = item["client_id"]
            gym_id = item["gym_id"]

            logger.info(f"[BROWSING_WA] Checking client={client_id} gym={gym_id} views={item.get('view_count')}")

            # Rate limit: 1 WhatsApp per user per day
            rate_key = f"activity:wa_sent:{client_id}"
            already_sent = await redis.get(rate_key)
            if already_sent:
                logger.info(f"[BROWSING_WA] SKIP client={client_id}: rate limited (already sent today)")
                continue

            # Skip if this client made ANY purchase in the last 48h
            if client_id in clients_with_recent_purchase:
                logger.info(f"[BROWSING_WA] SKIP client={client_id}: recently purchased (cached)")
                continue
            recent_cutoff = datetime.now() - timedelta(hours=48)
            purchase_check = await db.execute(
                select(func.count()).select_from(ClientActivityEvent).where(
                    ClientActivityEvent.client_id == client_id,
                    ClientActivityEvent.event_type == "checkout_completed",
                    ClientActivityEvent.created_at >= recent_cutoff,
                )
            )
            purchase_count = purchase_check.scalar()
            if purchase_count > 0:
                clients_with_recent_purchase.add(client_id)
                logger.info(f"[BROWSING_WA] SKIP client={client_id}: has {purchase_count} purchase(s) in last 48h")
                continue

            # Get client info
            client_result = await db.execute(
                select(Client).where(Client.client_id == client_id)
            )
            client = client_result.scalars().first()
            if not client:
                logger.warning(f"[BROWSING_WA] SKIP client={client_id}: not found in DB")
                continue
            if not client.contact:
                logger.warning(f"[BROWSING_WA] SKIP client={client_id}: no contact number")
                continue

            # Get gym name
            gym_result = await db.execute(
                select(Gym).where(Gym.gym_id == gym_id)
            )
            gym = gym_result.scalars().first()
            gym_name = gym.name if gym else "your gym"

            client_name = client.name or "there"
            lowest_price = "49"

            logger.info(f"[BROWSING_WA] Sending: client={client_id} name={client_name} phone={client.contact} gym={gym_name}")

            try:
                wa_client = get_whatsapp_client()
                response = await wa_client.send_browsing_followup(
                    to=client.contact,
                    client_name=client_name,
                    gym_name=gym_name,
                    lowest_price=lowest_price,
                )

                logger.info(f"[BROWSING_WA] API response: success={response.success} status={response.status_code} guid={response.guid} error={response.error}")

                log_entry = ClientWhatsAppLog(
                    client_id=client_id,
                    trigger_type="repeated_browsing",
                    template_name=WHATSAPP_TEMPLATE_BROWSING,
                    variables=[client_name, gym_name, lowest_price],
                    gym_id=gym_id,
                    whatsapp_guid=response.guid if response.success else None,
                    status="sent" if response.success else "failed",
                    sent_by="system",
                )
                db.add(log_entry)

                if response.success:
                    messages_sent += 1
                    await redis.set(rate_key, "1", ex=86400)
                    logger.info(f"[BROWSING_WA] SUCCESS client={client_id} guid={response.guid}")
                else:
                    logger.warning(f"[BROWSING_WA] FAILED client={client_id} error={response.error}")

            except Exception as e:
                logger.error(f"[BROWSING_WA] EXCEPTION client={client_id}: {repr(e)}")

        await db.commit()

    logger.info(f"[BROWSING_WA] Done: {messages_sent}/{len(warm_leads)} messages sent")
    return {"warm_leads_found": len(warm_leads), "messages_sent": messages_sent}


# ═══════════════════════════════════════════════════════════════════════════════
# TASK 4: Monitor queue depth for early warning
# ═══════════════════════════════════════════════════════════════════════════════

@celery_app.task(name="activity.monitor_queue_health")
def monitor_queue_health():
    """Check event queue depth and log warnings if backing up."""
    try:
        redis = get_redis_sync()
        depth = redis.llen("activity:events:queue")
        active_checkouts = redis.scard("activity:checkout:active_clients")
        active_views = redis.scard("activity:views:active_clients")

        if depth >= QUEUE_DEPTH_CRITICAL:
            logger.critical(f"[MONITOR] Event queue CRITICAL: {depth} events backed up!")
        elif depth >= QUEUE_DEPTH_WARNING:
            logger.warning(f"[MONITOR] Event queue WARNING: {depth} events pending")
        else:
            logger.info(f"[MONITOR] Event queue OK: depth={depth} active_checkouts={active_checkouts} active_views={active_views}")

        return {
            "queue_depth": depth,
            "active_checkout_clients": active_checkouts,
            "active_view_clients": active_views,
            "status": "critical" if depth >= QUEUE_DEPTH_CRITICAL else "warning" if depth >= QUEUE_DEPTH_WARNING else "ok",
        }

    except Exception as exc:
        logger.error(f"[MONITOR] ERROR: {repr(exc)}")
        raise
