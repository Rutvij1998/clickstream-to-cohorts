with users as (
    select
        dim_users.user_id,
        dim_users.cohort_week
    from {{ ref('dim_users') }} as dim_users
),

sizes as (
    select
        users.cohort_week,
        count(*) as cohort_size
    from users
    group by users.cohort_week
),

activity as (
    select distinct
        events.user_id,
        cast(date_trunc('week', events.event_date) as date) as activity_week
    from {{ ref('fct_events') }} as events
),

observed as (
    select
        users.cohort_week,
        date_diff('week', users.cohort_week, activity.activity_week) as weeks_since,
        count(distinct users.user_id) as active_users
    from users
    inner join activity
        on users.user_id = activity.user_id
    where activity.activity_week >= users.cohort_week
    group by users.cohort_week, date_diff('week', users.cohort_week, activity.activity_week)
),

week_index as (
    select distinct dim_date.week_start_date
    from {{ ref('dim_date') }} as dim_date
),

grid as (
    select
        sizes.cohort_week,
        sizes.cohort_size,
        date_diff('week', sizes.cohort_week, week_index.week_start_date) as weeks_since
    from sizes
    inner join week_index
        on sizes.cohort_week <= week_index.week_start_date
)

select
    grid.cohort_week,
    grid.weeks_since,
    grid.cohort_size,
    coalesce(observed.active_users, 0) as active_users,
    coalesce(observed.active_users, 0) * 1.0 / grid.cohort_size as retention_rate
from grid
left join observed
    on
        grid.cohort_week = observed.cohort_week
        and grid.weeks_since = observed.weeks_since
