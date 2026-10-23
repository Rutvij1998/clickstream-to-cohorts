select
    activity_date,
    revenue,
    sessions * cvr * aov as reconstructed_revenue
from {{ ref('mart_revenue_drivers_daily') }}
where abs(revenue - (sessions * cvr * aov)) > 0.000001 * greatest(abs(revenue), 1)
