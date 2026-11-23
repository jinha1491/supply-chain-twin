-- Every snapshot in the raw events must appear in the reconciliation exactly once.
with expected as (
    select count(*) as snapshots
    from {{ source('raw', 'events') }}
    where event_type = 'inventory_snapshot'
),
reconciled as (
    select count(*) as snapshots from {{ ref('fct_inventory_reconciliation') }}
)
select e.snapshots as expected, r.snapshots as reconciled
from expected e cross join reconciled r
where e.snapshots != r.snapshots
