with status_counts as (
    select
        window_start,
        currency,
        sum(status_event_count) filter (
            where order_status = 'pending'
        ) as pending_event_count,
        sum(status_event_count) filter (
            where order_status = 'paid'
        ) as paid_event_count,
        sum(status_event_count) filter (
            where order_status = 'processing'
        ) as processing_event_count,
        sum(status_event_count) filter (
            where order_status = 'completed'
        ) as completed_status_event_count,
        sum(status_event_count) filter (
            where order_status = 'cancelled'
        ) as cancelled_event_count
    from {{ ref('stg_order_status_hourly') }}
    group by window_start, currency
)

select
    metrics.window_start,
    metrics.window_end,
    metrics.currency,
    metrics.order_event_count,
    metrics.created_order_count,
    metrics.completed_order_count,
    metrics.revenue,
    metrics.average_order_value,
    coalesce(status.pending_event_count, 0) as pending_event_count,
    coalesce(status.paid_event_count, 0) as paid_event_count,
    coalesce(status.processing_event_count, 0) as processing_event_count,
    coalesce(status.completed_status_event_count, 0) as completed_status_event_count,
    coalesce(status.cancelled_event_count, 0) as cancelled_event_count,
    metrics.aggregated_at,
    metrics.loaded_at
from {{ ref('stg_order_metrics_hourly') }} as metrics
left join status_counts as status
    on metrics.window_start = status.window_start
    and metrics.currency = status.currency
