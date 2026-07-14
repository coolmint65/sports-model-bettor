"""Geography helpers shared across sport frameworks.

Centralizes the "is this league in a far-east timezone" check so
basketball, hockey, and any future sport framework can reuse one
canonical set of countries without re-defining it.
"""
from __future__ import annotations


# Countries whose local game-night straddles ET midnight (UTC+8 to +13).
# 7 PM local in Beijing/Tokyo/Seoul/Sydney/Auckland = 3-7 AM ET next day,
# so games their fans consider "tonight" stamp as tomorrow ET in our DB.
FAR_EAST_COUNTRIES = frozenset({
    "China", "Japan", "South Korea", "Australia", "New Zealand",
    "Indonesia", "Philippines", "Vietnam", "Thailand", "Malaysia",
})


def is_far_east_country(country: str | None) -> bool:
    """True when the country sits in the Asia/Oceania night-owl zone."""
    return (country or "") in FAR_EAST_COUNTRIES
