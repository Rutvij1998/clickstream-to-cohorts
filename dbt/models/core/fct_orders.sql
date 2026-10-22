{% if target.type == 'athena' %}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    table_type='hive',
    format='parquet',
    partitioned_by=['order_date']
) }}
{% else %}
    {{ config(materialized='table') }}
{% endif %}

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
{% if target.type == 'athena' and var('event_date', none) is not none %}
inner join (
    select distinct orders.order_date
    from orders
    inner join {{ ref('fct_events') }} as events
        on orders.session_id = events.session_id
    where events.event_date = cast('{{ var("event_date") }}' as date)
) as touched
    on orders.order_date = touched.order_date
{% endif %}
