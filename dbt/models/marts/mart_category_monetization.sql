with view_counts as (
    select
        events.category_l1,
        count(*) as view_events
    from {{ ref('fct_events') }} as events
    where events.event_type = 'view'
    group by events.category_l1
),

purchase_amounts as (
    select
        events.category_l1,
        sum(events.price) as revenue
    from {{ ref('fct_events') }} as events
    where events.event_type = 'purchase'
    group by events.category_l1
),

combined as (
    select
        coalesce(view_counts.category_l1, purchase_amounts.category_l1) as category_l1,
        coalesce(view_counts.view_events, 0) as view_events,
        coalesce(purchase_amounts.revenue, 0) as revenue
    from view_counts
    full outer join purchase_amounts
        on view_counts.category_l1 = purchase_amounts.category_l1
)

select
    combined.category_l1,
    combined.view_events,
    combined.revenue,
    combined.view_events * 1.0 / nullif(sum(combined.view_events) over (), 0) as view_share,
    combined.revenue * 1.0 / nullif(sum(combined.revenue) over (), 0) as revenue_share,
    (combined.revenue * 1.0 / nullif(sum(combined.revenue) over (), 0))
    / nullif(combined.view_events * 1.0 / nullif(sum(combined.view_events) over (), 0), 0)
        as revenue_per_view_share
from combined
