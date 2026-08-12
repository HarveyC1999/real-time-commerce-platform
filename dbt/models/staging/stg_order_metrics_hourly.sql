select
    window_start,
    window_end,
    currency,
    order_event_count,
    created_order_count,
    completed_order_count,
    revenue,
    average_order_value,
    aggregated_at,
    loaded_at
from {{ source('analytics', 'order_metrics_hourly') }}
