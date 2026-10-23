select
    sessions.session_id as order_id,
    sessions.session_id,
    sessions.user_id,
    sessions.session_start_at as order_at,
    sessions.purchase_events,
    sessions.revenue,
    sessions.session_start_date as order_date
from {{ ref('int_sessions') }} as sessions
where sessions.is_purchase = 1
