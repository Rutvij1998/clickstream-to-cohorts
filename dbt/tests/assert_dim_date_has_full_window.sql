select count(*) as date_count
from {{ ref('dim_date') }}
having count(*) <> 61
