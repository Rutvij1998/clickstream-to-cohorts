select
    event_date,
    category_l1,
    view_sessions,
    cart_sessions,
    cart_without_view_sessions
from {{ ref('mart_funnel_daily_by_category') }}
where
    cart_sessions > view_sessions
    and cart_without_view_sessions < cart_sessions - view_sessions
