{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date'
) }}

select
    events.event_id,
    events.event_time,
    events.event_type,
    events.user_id,
    events.session_id,
    events.product_id,
    events.category_id,
    events.category_code,
    events.category_l1,
    events.category_l2,
    events.category_l3,
    events.brand_name,
    events.price,
    events.price_band,
    events.event_date
from {{ ref('int_events_enriched') }} as events
