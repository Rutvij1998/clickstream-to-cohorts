with slices as (
    select
        events.event_date,
        events.category_l1,
        events.price_band,
        events.session_id,
        max(case when events.event_type = 'cart' then 1 else 0 end) as has_cart,
        max(case when events.event_type = 'purchase' then 1 else 0 end) as has_purchase
    from {{ ref('fct_events') }} as events
    group by events.event_date, events.category_l1, events.price_band, events.session_id
)

select
    slices.event_date,
    slices.category_l1,
    slices.price_band,
    sum(slices.has_cart) as cart_sessions,
    sum(
        case when slices.has_cart = 1 and slices.has_purchase = 0 then 1 else 0 end
    ) as abandoned_sessions,
    sum(case when slices.has_cart = 1 and slices.has_purchase = 0 then 1 else 0 end) * 1.0
    / nullif(sum(slices.has_cart), 0) as cart_abandonment_rate
from slices
group by slices.event_date, slices.category_l1, slices.price_band
having sum(slices.has_cart) > 0
