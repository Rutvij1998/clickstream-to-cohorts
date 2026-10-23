select
    sum(view_share) as view_share_total,
    sum(revenue_share) as revenue_share_total
from {{ ref('mart_category_monetization') }}
having
    sum(view_share) is null
    or sum(revenue_share) is null
    or abs(sum(view_share) - 1) > 0.000001
    or abs(sum(revenue_share) - 1) > 0.000001
