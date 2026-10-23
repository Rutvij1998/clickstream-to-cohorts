with orders as (
    select
        int_orders.order_id,
        int_orders.session_id,
        int_orders.user_id,
        int_orders.order_at,
        int_orders.purchase_events,
        int_orders.revenue,
        int_orders.order_date
    from {{ ref('int_orders') }} as int_orders
)

select
    orders.order_id,
    orders.session_id,
    orders.user_id,
    orders.order_at,
    orders.purchase_events,
    orders.revenue,
    orders.order_date
from orders
