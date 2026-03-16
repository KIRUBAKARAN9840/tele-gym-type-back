"""
Centralized platform pricing configuration.

ALL markup/commission percentages are controlled from here.
The value is read from the env var PLATFORM_MARKUP_PERCENT (default: 10).

Usage:
    from app.config.pricing import get_markup_multiplier, get_markup_percent

    price_with_markup = round(base_price * get_markup_multiplier())   # e.g. 1.10
    markup_pct = get_markup_percent()                                  # e.g. 10
"""

from app.config.settings import settings


def get_markup_percent() -> int:
    """Return the platform markup percentage (e.g. 10 for 10%)."""
    return settings.platform_markup_percent


def get_markup_multiplier() -> float:
    """Return the multiplier to apply to base prices (e.g. 1.10 for 10%)."""
    return 1 + (settings.platform_markup_percent / 100)
