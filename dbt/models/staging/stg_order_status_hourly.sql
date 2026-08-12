select
    window_start,
    window_end,
    currency,
    order_status,
    status_event_count,
    aggregated_at,
    loaded_at
from {{ source('analytics', 'order_status_hourly') }}
