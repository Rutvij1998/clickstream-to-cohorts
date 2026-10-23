with session_metrics as (
    select
        'session_cvr' as metric_name,
        sum(sessions.is_purchase) * 1.0 / count(*) as baseline_rate,
        count(*) * 1.0 / count(distinct sessions.session_start_date) as daily_volume
    from {{ ref('fct_sessions') }} as sessions

    union all

    select
        'view_to_cart' as metric_name,
        sum(case when sessions.view_events > 0 and sessions.cart_events > 0 then 1 else 0 end)
        * 1.0
        / nullif(sum(case when sessions.view_events > 0 then 1 else 0 end), 0) as baseline_rate,
        sum(case when sessions.view_events > 0 then 1 else 0 end) * 1.0
        / nullif(
            count(distinct case when sessions.view_events > 0 then sessions.session_start_date end),
            0
        ) as daily_volume
    from {{ ref('fct_sessions') }} as sessions

    union all

    select
        'cart_to_purchase' as metric_name,
        sum(case when sessions.cart_events > 0 and sessions.is_purchase = 1 then 1 else 0 end)
        * 1.0
        / nullif(sum(case when sessions.cart_events > 0 then 1 else 0 end), 0) as baseline_rate,
        sum(case when sessions.cart_events > 0 then 1 else 0 end) * 1.0
        / nullif(
            count(distinct case when sessions.cart_events > 0 then sessions.session_start_date end),
            0
        ) as daily_volume
    from {{ ref('fct_sessions') }} as sessions
),

lifts as (
    select 0.02 as mde_relative
    union all
    select 0.05 as mde_relative
),

scored as (
    select
        session_metrics.metric_name,
        session_metrics.baseline_rate,
        session_metrics.baseline_rate * (1 - session_metrics.baseline_rate) as baseline_variance,
        lifts.mde_relative,
        session_metrics.baseline_rate * lifts.mde_relative as mde_absolute,
        least(
            session_metrics.baseline_rate * (1 + lifts.mde_relative),
            0.999
        ) as comparison_rate,
        session_metrics.daily_volume,
        0.05 as alpha,
        0.80 as power,
        1.959963984540054 as z_alpha,
        0.841621233572914 as z_beta
    from session_metrics
    cross join lifts
    where
        session_metrics.baseline_rate > 0
        and session_metrics.baseline_rate < 1
        and session_metrics.daily_volume > 0
),

sized as (
    select
        scored.metric_name,
        scored.baseline_rate,
        scored.baseline_variance,
        scored.mde_relative,
        scored.mde_absolute,
        scored.comparison_rate,
        scored.alpha,
        scored.power,
        scored.z_alpha,
        scored.z_beta,
        scored.daily_volume,
        (scored.baseline_rate + scored.comparison_rate) / 2.0 as pooled_rate
    from scored
    where scored.comparison_rate > scored.baseline_rate
),

per_arm as (
    select
        sized.metric_name,
        sized.baseline_rate,
        sized.baseline_variance,
        sized.mde_relative,
        sized.mde_absolute,
        sized.comparison_rate,
        sized.alpha,
        sized.power,
        sized.z_alpha,
        sized.z_beta,
        sized.daily_volume,
        cast(ceil(
            power(
                (
                    sized.z_alpha * sqrt(2 * sized.pooled_rate * (1 - sized.pooled_rate))
                    + sized.z_beta * sqrt(
                        sized.baseline_rate * (1 - sized.baseline_rate)
                        + sized.comparison_rate * (1 - sized.comparison_rate)
                    )
                ) / (sized.comparison_rate - sized.baseline_rate),
                2
            )
        ) as bigint) as required_sample_size_per_arm
    from sized
)

select
    per_arm.metric_name,
    per_arm.baseline_rate,
    per_arm.baseline_variance,
    per_arm.mde_relative,
    per_arm.mde_absolute,
    per_arm.comparison_rate,
    per_arm.alpha,
    per_arm.power,
    per_arm.z_alpha,
    per_arm.z_beta,
    per_arm.daily_volume,
    per_arm.required_sample_size_per_arm,
    cast(
        ceil((2 * per_arm.required_sample_size_per_arm) / per_arm.daily_volume) as bigint
    ) as estimated_days_to_run
from per_arm
