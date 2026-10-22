{% if target.type == 'athena' %}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    table_type='hive',
    format='parquet',
    partitioned_by=['session_start_date']
) }}
{% else %}
    {{ config(materialized='table') }}
{% endif %}

with sessions as (
    select
        int_sessions.session_id,
        int_sessions.user_id,
        int_sessions.session_start_at,
        int_sessions.session_end_at,
        int_sessions.event_count,
        int_sessions.view_events,
        int_sessions.cart_events,
        int_sessions.remove_from_cart_events,
        int_sessions.purchase_events,
        int_sessions.is_purchase,
        int_sessions.revenue,
        int_sessions.session_start_date
    from {{ ref('int_sessions') }} as int_sessions
)

select
    sessions.session_id,
    sessions.user_id,
    sessions.session_start_at,
    sessions.session_end_at,
    sessions.event_count,
    sessions.view_events,
    sessions.cart_events,
    sessions.remove_from_cart_events,
    sessions.purchase_events,
    sessions.is_purchase,
    sessions.revenue,
    sessions.session_start_date
from sessions
{% if target.type == 'athena' and var('event_date', none) is not none %}
inner join (
    select distinct sessions.session_start_date
    from sessions
    inner join {{ ref('fct_events') }} as events
        on sessions.session_id = events.session_id
    where events.event_date = cast('{{ var("event_date") }}' as date)
) as touched
    on sessions.session_start_date = touched.session_start_date
{% endif %}
