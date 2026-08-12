with duplicate_metric_grains as (
    select window_start, currency
    from {{ source('analytics', 'order_metrics_hourly') }}
    group by window_start, currency
    having count(*) > 1
),

duplicate_status_grains as (
    select window_start, currency, order_status
    from {{ source('analytics', 'order_status_hourly') }}
    group by window_start, currency, order_status
    having count(*) > 1
)

select 'order_metrics_hourly' as source_name
from duplicate_metric_grains
union all
select 'order_status_hourly' as source_name
from duplicate_status_grains
