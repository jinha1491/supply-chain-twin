select
    event_id,
    event_time,
    event_type,
    part_id,
    site,
    quantity,
    ref_id,
    carrier_id
from {{ source('raw', 'events') }}
