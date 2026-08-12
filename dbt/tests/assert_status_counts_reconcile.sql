select *
from {{ ref('fct_hourly_commerce_metrics') }}
where order_event_count != (
    pending_event_count
    + paid_event_count
    + processing_event_count
    + completed_status_event_count
    + cancelled_event_count
)
