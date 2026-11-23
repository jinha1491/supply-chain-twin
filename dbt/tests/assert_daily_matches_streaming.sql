-- Batch daily counts must equal the streaming job's counts for every window it closed.
select b.day, b.site, b.orders, s.orders as streamed_orders,
       b.backorders, s.backorders as streamed_backorders
from {{ ref('fct_daily_service') }} b
join {{ source('gold', 'daily_service') }} s
    on s.day = b.day and s.site = b.site
where b.orders != s.orders or b.backorders != s.backorders
