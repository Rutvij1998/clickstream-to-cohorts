{{ config(materialized="view") }}

select
    events.event_id,
    events.event_time,
    events.event_type,
    events.user_id,
    events.user_session as session_id,
    events.product_id,
    events.category_id,
    events.category_code,
    case
        when events.category_code is null then 'Unknown'
        else split_part(events.category_code, '.', 1)
    end as category_l1,
    nullif(split_part(events.category_code, '.', 2), '') as category_l2,
    nullif(split_part(events.category_code, '.', 3), '') as category_l3,
    coalesce(events.brand, 'Unknown') as brand_name,
    events.price,
    case
        when events.price < 10 then 'under_10'
        when events.price < 50 then '10_to_50'
        when events.price < 200 then '50_to_200'
        when events.price < 500 then '200_to_500'
        else '500_plus'
    end as price_band,
    events.event_date
from {{ ref('stg_rees46__events') }} as events
