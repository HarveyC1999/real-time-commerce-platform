select window_start, currency
from {{ ref('fct_hourly_commerce_metrics') }}
group by window_start, currency
having count(*) > 1
