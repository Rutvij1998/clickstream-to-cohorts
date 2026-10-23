"""Stable event id shared by the fixture generator and the Spark job.

The id is the hex SHA-256 of these fields joined with a pipe, in this order:
event time as `yyyy-MM-dd HH:mm:ss` (UTC, no timezone suffix), event type,
product id, category id, category code or empty, brand or empty, price with
two decimal places, user id, user session.
"""

from __future__ import annotations

import hashlib


def event_id_payload(
    event_time: str,
    event_type: str,
    product_id: str,
    category_id: str,
    category_code: str | None,
    brand: str | None,
    price: float,
    user_id: str,
    user_session: str,
) -> str:
    parts = [
        event_time,
        event_type,
        str(product_id),
        str(category_id),
        category_code or "",
        brand or "",
        f"{float(price):.2f}",
        str(user_id),
        user_session,
    ]
    return "|".join(parts)


def event_id_sha256(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
