with orders as (
    select sum(revenue) as order_revenue
    from {{ ref('fct_orders') }}
),

purchases as (
    select sum(price) as event_revenue
    from {{ ref('fct_events') }}
    where event_type = 'purchase'
)

select
    orders.order_revenue,
    purchases.event_revenue
from orders
cross join purchases
where
    orders.order_revenue is null
    or purchases.event_revenue is null
    or abs(orders.order_revenue - purchases.event_revenue) > 0.01
