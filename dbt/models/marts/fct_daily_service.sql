select
    date_trunc('day', event_time) as day,
    site,
    sum(case when event_type = 'demand' then 1 else 0 end) as orders,
    sum(case when event_type = 'backorder' then 1 else 0 end) as backorders,
    1 - sum(case when event_type = 'backorder' then 1 else 0 end)
        / sum(case when event_type = 'demand' then 1 else 0 end) as fill_rate
from {{ ref('stg_events') }}
where event_type in ('demand', 'backorder')
group by 1, 2
