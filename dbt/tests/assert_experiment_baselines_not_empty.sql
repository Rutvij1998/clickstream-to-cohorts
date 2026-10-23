select count(*) as baseline_rows
from {{ ref('mart_experiment_baselines') }}
having count(*) = 0
