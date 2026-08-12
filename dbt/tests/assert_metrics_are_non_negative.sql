select *
from {{ ref('fct_hourly_commerce_metrics') }}
where
    order_event_count < 0
    or created_order_count < 0
    or completed_order_count < 0
    or revenue < 0
    or pending_event_count < 0
    or paid_event_count < 0
    or processing_event_count < 0
    or completed_status_event_count < 0
    or cancelled_event_count < 0
