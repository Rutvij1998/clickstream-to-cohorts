with users as (
    select
        dim_users.cohort_week,
        case
            when dim_users.first_purchase_at is null then null
            else date_diff('hour', dim_users.first_seen_at, dim_users.first_purchase_at)
        end as hours_to_first_purchase
    from {{ ref('dim_users') }} as dim_users
),

ranked as (
    select
        users.cohort_week,
        users.hours_to_first_purchase as hours,
        row_number() over (
            partition by users.cohort_week
            order by users.hours_to_first_purchase
        ) as hour_rank,
        count(*) over (partition by users.cohort_week) as purchaser_count
    from users
    where users.hours_to_first_purchase is not null
),

medians as (
    select
        ranked.cohort_week,
        avg(ranked.hours) as median_hours_to_first_purchase
    from ranked
    where
        ranked.hour_rank in (
            cast(floor((ranked.purchaser_count + 1) / 2.0) as integer),
            cast(floor((ranked.purchaser_count + 2) / 2.0) as integer)
        )
    group by ranked.cohort_week
),

cohorts as (
    select
        users.cohort_week,
        count(*) as cohort_size,
        sum(
            case
                when
                    users.hours_to_first_purchase is not null
                    and users.hours_to_first_purchase <= 168 then 1
                else 0
            end
        ) as activated_users_7d
    from users
    group by users.cohort_week
)

select
    cohorts.cohort_week,
    cohorts.cohort_size,
    cohorts.activated_users_7d,
    cohorts.activated_users_7d * 1.0 / cohorts.cohort_size as activation_rate_7d,
    medians.median_hours_to_first_purchase
from cohorts
left join medians
    on cohorts.cohort_week = medians.cohort_week
