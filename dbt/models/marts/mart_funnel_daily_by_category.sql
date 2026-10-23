with session_category as (
    select
        events.event_date,
        events.category_l1,
        events.session_id,
        max(case when events.event_type = 'view' then 1 else 0 end) as has_view,
        max(case when events.event_type = 'cart' then 1 else 0 end) as has_cart,
        max(case when events.event_type = 'purchase' then 1 else 0 end) as has_purchase
    from {{ ref('fct_events') }} as events
    group by events.event_date, events.category_l1, events.session_id
)

select
    session_category.event_date,
    session_category.category_l1,
    sum(session_category.has_view) as view_sessions,
    sum(session_category.has_cart) as cart_sessions,
    sum(session_category.has_purchase) as purchase_sessions,
    sum(
        case
            when session_category.has_cart = 1 and session_category.has_view = 0 then 1
            else 0
        end
    ) as cart_without_view_sessions,
    sum(
        case
            when session_category.has_purchase = 1 and session_category.has_cart = 0 then 1
            else 0
        end
    ) as purchase_without_cart_sessions,
    sum(session_category.has_cart) * 1.0
    / nullif(sum(session_category.has_view), 0) as view_to_cart_rate,
    sum(session_category.has_purchase) * 1.0
    / nullif(sum(session_category.has_cart), 0) as cart_to_purchase_rate,
    sum(session_category.has_purchase) * 1.0
    / nullif(sum(session_category.has_view), 0) as session_cvr
from session_category
group by session_category.event_date, session_category.category_l1
