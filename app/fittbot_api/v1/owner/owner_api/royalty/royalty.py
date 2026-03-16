from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional, Tuple

from fastapi import APIRouter, Depends, Query
from sqlalchemy import collate
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.fittbot_models import Client, Royalty, RoyaltyStatus
from app.utils.logging_utils import FittbotHTTPException
from app.fittbot_api.v1.client.client_api.side_bar.manage_fittbot_subscriptions import (
    get_plan_name_from_product_id,
)
from app.fittbot_api.v1.payments.models.subscriptions import Subscription
from app.fittbot_api.v1.payments.models.catalog import CatalogProduct


router = APIRouter(prefix="/royalty", tags=["Gymowner"])


def _parse_month(month_value: str) -> int:
    month_clean = month_value.strip()
    if not month_clean:
        raise FittbotHTTPException(
            status_code=400,
            detail="Month cannot be empty",
            error_code="EMPTY_MONTH",
        )

    if month_clean.isdigit():
        month_number = int(month_clean)
        if 1 <= month_number <= 12:
            return month_number
        raise FittbotHTTPException(
            status_code=400,
            detail="Month number must be between 1 and 12",
            error_code="INVALID_MONTH_RANGE",
            log_data={"month": month_value},
        )

    for fmt in ("%B", "%b"):
        try:
            parsed = datetime.strptime(month_clean.title(), fmt)
            return parsed.month
        except ValueError:
            continue

    raise FittbotHTTPException(
        status_code=400,
        detail="Invalid month format. Use full/short month name or numeric value.",
        error_code="INVALID_MONTH_FORMAT",
        log_data={"month": month_value},
    )


def _resolve_month(month: Optional[str], year: Optional[int]) -> Tuple[date, date]:
    today = date.today()

    target_month = _parse_month(month) if month else today.month
    target_year = year if year else today.year

    if target_year < 1900 or target_year > 9999:
        raise FittbotHTTPException(
            status_code=400,
            detail="Year must be between 1900 and 9999",
            error_code="INVALID_YEAR_RANGE",
            log_data={"year": year},
        )

    month_start = date(target_year, target_month, 1)
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1, day=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1, day=1)

    return month_start, next_month


@router.get("/summary")
async def get_monthly_royalty_summary(
    gym_id: int = Query(..., description="Gym identifier"),
    month: Optional[str] = Query(None, description="Month name or number (e.g., 'November' or '11')"),
    year: Optional[int] = Query(None, description="Year in YYYY format"),
    db: Session = Depends(get_db),
):
    try:
        month_start, next_month = _resolve_month(month, year)
        month_key = month_start.strftime("%Y-%m")

        royalty_rows: List[Tuple[Royalty, Client, Subscription, Optional[CatalogProduct]]] = (
            db.query(Royalty, Client, Subscription, CatalogProduct)
            .join(Client, Royalty.client_id == Client.client_id)
            .join(
                Subscription,
                collate(Royalty.subscription_id, "utf8mb4_unicode_ci")
                == collate(Subscription.id, "utf8mb4_unicode_ci"),
            )
            .outerjoin(
                CatalogProduct,
                collate(Subscription.product_id, "utf8mb4_unicode_ci")
                == collate(CatalogProduct.sku, "utf8mb4_unicode_ci"),
            )
            .filter(
                Royalty.gym_id == gym_id,
                Royalty.date >= month_start,
                Royalty.date < next_month,
            )
            .all()
        )

        royalty_entries = []
        total_plan_amount = 0.0

        for royalty, client, subscription, catalog in royalty_rows:
            product_id = subscription.product_id if subscription else None
            plan_name = get_plan_name_from_product_id(product_id) if product_id else "Unknown Plan"
            plan_amount = (
                (catalog.base_amount_minor / 100.0) if catalog and catalog.base_amount_minor else 0.0
            )

            royalty_entries.append(
                {
                    "royalty_id": royalty.id,
                    "client_id": client.client_id,
                    "client_name": client.name,
                    "profile_pic": client.profile,
                    "subscription_id": royalty.subscription_id,
                    "plan_name": plan_name,
                    "plan_amount": round(plan_amount, 2),
                    "recorded_date": royalty.date.isoformat(),
                }
            )

            total_plan_amount += plan_amount

        royalty_status_row = (
            db.query(RoyaltyStatus)
            .filter(RoyaltyStatus.gym_id == gym_id, RoyaltyStatus.month == month_key)
            .first()
        )

        royalty_status = (
            royalty_status_row.payment_status if royalty_status_row else "not_initiated"
        )
        total_clients = len(royalty_entries)
        royalty_share = round(total_plan_amount * 0.20)

        data={
                "month": month_key,
                "gym_id": gym_id,
                "entries": royalty_entries,
                "total_clients": total_clients,
                "total_plan_amount": round(total_plan_amount, 2),
                "fittbot_share": royalty_share,
                "royalty_status": royalty_status,
            }
        

        print("dataaaa is",data)

        return {
            "status": 200,
            "message": "Royalty summary fetched successfully",
            "data": {
                "month": month_key,
                "gym_id": gym_id,
                "entries": royalty_entries,
                "total_clients": total_clients,
                "total_plan_amount": round(total_plan_amount, 2),
                "royalty_share": royalty_share,
                "royalty_status": royalty_status,
            },
        }
    except FittbotHTTPException:
        raise
    except Exception as exc:
        raise FittbotHTTPException(
            status_code=500,
            detail="Failed to fetch royalty summary",
            error_code="ROYALTY_SUMMARY_FETCH_FAILED",
            log_data={"gym_id": gym_id, "month": month, "error": repr(exc)},
        ) from exc
