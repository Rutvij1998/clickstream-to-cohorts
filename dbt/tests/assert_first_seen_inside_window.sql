select
    user_id,
    first_seen_date
from {{ ref('int_user_first_activity') }}
where
    first_seen_date < cast('{{ var("start_date") }}' as date)
    or first_seen_date > cast('{{ var("end_date") }}' as date)
