"""Utilities to determine client access tiers based on subscriptions and memberships."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from sqlalchemy import desc, select
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.models.fittbot_models import Client, FittbotGymMembership


Tier = Literal["premium", "premium_gym", "freemium_gym", "freemium"]


def get_client_tier(db: Session, client_id: int) -> Tier:

    current_time = datetime.now()
    today = date.today()

    # Auto-expire subscriptions that have passed their active_until date but still show as active
    expired_subscriptions = (
        db.query(Subscription)
        .filter(
            Subscription.customer_id == str(client_id),
            Subscription.active_until < current_time,
            Subscription.status == "active"
        )
        .all()
    )
    for sub in expired_subscriptions:
        sub.status = "expired"
    if expired_subscriptions:
        db.commit()

    has_subscription = False
    has_gym_membership = False
    tier = "freemium"

    subscription = (
        db.query(Subscription)
        .filter(
            Subscription.customer_id == str(client_id),
            Subscription.active_until > current_time
        )
        .order_by(Subscription.active_until.desc())
        .first()
    )

    if subscription:
        has_subscription = True
        tier = "premium"

    gym_membership = (
        db.query(FittbotGymMembership)
        .filter(
            FittbotGymMembership.client_id == str(client_id),
            FittbotGymMembership.status == "active",
            FittbotGymMembership.expires_at > today
        )
        .order_by(desc(FittbotGymMembership.id))
        .first()
    )

    if gym_membership:
        has_gym_membership = True

    if has_subscription and has_gym_membership:
        tier = "premium_gym"

    elif has_subscription:
        tier = "premium"

    else:
        client = (
            db.query(Client)
            .filter(Client.client_id == client_id)
            .first()
        )

        if client and client.gym_id and has_gym_membership:
            tier = "freemium_gym"

    return tier


async def get_client_tier_async(db: AsyncSession, client_id: int) -> Tier:

    current_time = datetime.now()
    today = date.today()

    # Auto-expire subscriptions that have passed their active_until date but still show as active
    expired_subscriptions_result = await db.execute(
        select(Subscription)
        .filter(
            Subscription.customer_id == str(client_id),
            Subscription.active_until < current_time,
            Subscription.status == "active"
        )
    )
    expired_subscriptions = expired_subscriptions_result.scalars().all()

    for sub in expired_subscriptions:
        sub.status = "expired"
    if expired_subscriptions:
        await db.commit()

    has_subscription = False
    has_gym_membership = False
    tier = "freemium"

    subscription_result = await db.execute(
        select(Subscription)
        .filter(
            Subscription.customer_id == str(client_id),
            Subscription.active_until > current_time
        )
        .order_by(Subscription.active_until.desc())
    )
    subscription = subscription_result.scalars().first()

    if subscription:
        has_subscription = True
        tier = "premium"

    gym_membership_result = await db.execute(
        select(FittbotGymMembership)
        .filter(
            FittbotGymMembership.client_id == str(client_id),
            FittbotGymMembership.status == "active",
            FittbotGymMembership.expires_at > today
        )
        .order_by(desc(FittbotGymMembership.id))
    )
    gym_membership = gym_membership_result.scalars().first()

    if gym_membership:
        has_gym_membership = True

    if has_subscription and has_gym_membership:
        tier = "premium_gym"

    elif has_subscription:
        tier = "premium"

    else:
        client_result = await db.execute(
            select(Client)
            .filter(Client.client_id == client_id)
        )
        client = client_result.scalars().first()

        if client and client.gym_id and has_gym_membership:
            tier = "freemium_gym"

    return tier
