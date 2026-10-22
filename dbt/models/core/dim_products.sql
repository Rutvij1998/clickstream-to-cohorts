select
    events.product_id,
    events.category_id,
    events.category_l1,
    events.category_l2,
    events.category_l3,
    events.brand_name,
    events.price as latest_price
from {{ ref('fct_events') }} as events
qualify row_number() over (
    partition by events.product_id
    order by events.event_time desc, events.event_id desc
) = 1
