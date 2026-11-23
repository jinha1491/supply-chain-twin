with events as (
    select event_id, event_time, event_type, part_id, site, quantity
    from {{ ref('stg_events') }}
    {% if var('drop_event_id', none) %}
    where event_id != '{{ var("drop_event_id") }}'
    {% endif %}
),

changes as (
    select
        site,
        part_id,
        event_id,
        event_time,
        case
            when event_type = 'receipt' then quantity
            when event_type = 'fulfilled' then -quantity
            when event_type = 'shipment_dispatched'
                and site = '{{ var("dc_name", "central_dc") }}' then -quantity
            else 0
        end as delta,
        cast(null as int) as snapshot_on_hand
    from events
    where event_type in ('receipt', 'fulfilled', 'shipment_dispatched')
),

snapshots as (
    select site, part_id, event_id, event_time, 0 as delta, quantity as snapshot_on_hand
    from events
    where event_type = 'inventory_snapshot'
),

combined as (
    select * from changes where delta != 0
    union all
    select * from snapshots
),

running as (
    select
        *,
        sum(delta) over (
            partition by site, part_id
            order by event_id
            rows between unbounded preceding and current row
        ) as net_change
    from combined
)

select
    r.site,
    r.part_id,
    r.event_time as snapshot_time,
    r.snapshot_on_hand,
    p.order_up_to + r.net_change as derived_on_hand,
    p.order_up_to + r.net_change - r.snapshot_on_hand as difference
from running r
join {{ source('ref', 'policy') }} p
    on p.site = r.site and p.part_id = r.part_id
where r.snapshot_on_hand is not null
