with raw_events as (
    {% if var('silver_source', 'seed') == 'parquet' %}
    select
        events_silver.event_id,
        events_silver.event_time,
        events_silver.event_type,
        events_silver.product_id,
        events_silver.category_id,
        events_silver.category_code,
        events_silver.brand,
        events_silver.price,
        events_silver.user_id,
        events_silver.user_session,
        events_silver.event_date
    from read_parquet('{{ var("silver_glob") }}', hive_partitioning = true) as events_silver
    {% else %}
        select
            events_silver.event_id,
            events_silver.event_time,
            events_silver.event_type,
            events_silver.product_id,
            events_silver.category_id,
            events_silver.category_code,
            events_silver.brand,
            events_silver.price,
            events_silver.user_id,
            events_silver.user_session,
            events_silver.event_date
        from {{ ref('events_silver') }} as events_silver
    {% endif %}
),

typed as (
    select
        cast(raw_events.event_id as varchar) as event_id,
        cast(raw_events.event_time as timestamp) as event_time,
        lower(cast(raw_events.event_type as varchar)) as event_type,
        cast(raw_events.product_id as bigint) as product_id,
        cast(raw_events.category_id as bigint) as category_id,
        nullif(cast(raw_events.category_code as varchar), '') as category_code,
        nullif(cast(raw_events.brand as varchar), '') as brand,
        cast(raw_events.price as double) as price,
        cast(raw_events.user_id as bigint) as user_id,
        cast(raw_events.user_session as varchar) as user_session,
        cast(raw_events.event_date as date) as event_date
    from raw_events
),

ranked as (
    select
        typed.event_id,
        typed.event_time,
        typed.event_type,
        typed.product_id,
        typed.category_id,
        typed.category_code,
        typed.brand,
        typed.price,
        typed.user_id,
        typed.user_session,
        typed.event_date,
        row_number() over (
            partition by typed.event_id
            order by typed.event_time
        ) as row_num
    from typed
)

select
    ranked.event_id,
    ranked.event_time,
    ranked.event_type,
    ranked.product_id,
    ranked.category_id,
    ranked.category_code,
    ranked.brand,
    ranked.price,
    ranked.user_id,
    ranked.user_session,
    ranked.event_date
from ranked
where
    ranked.row_num = 1
    and ranked.event_type in ('view', 'cart', 'remove_from_cart', 'purchase')
{{ event_window_filter('ranked.event_date') }}
