select
    cohort_week,
    retention_rate
from {{ ref('mart_cohort_retention_weekly') }}
where
    weeks_since = 0
    and abs(retention_rate - 1) > 0.000000001
