select
    cast(spine.date_day as date) as date_day,
    cast(spine.year_number as integer) as year_number,
    cast(spine.month_number as integer) as month_number,
    cast(spine.day_of_month as integer) as day_of_month,
    cast(spine.iso_day_of_week as integer) as iso_day_of_week,
    spine.day_name,
    cast(spine.week_start_date as date) as week_start_date
from {{ ref('dim_date_spine') }} as spine
