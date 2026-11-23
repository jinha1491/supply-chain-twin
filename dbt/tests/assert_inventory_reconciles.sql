-- Stock derived from events must equal the simulator's recorded snapshot every time.
select site, part_id, snapshot_time, snapshot_on_hand, derived_on_hand, difference
from {{ ref('fct_inventory_reconciliation') }}
where difference != 0
