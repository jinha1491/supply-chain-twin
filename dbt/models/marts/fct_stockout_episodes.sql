select
    e.site,
    e.part_id,
    p.supplier,
    e.out_start,
    e.out_end,
    e.duration_days,
    v.lead_time_mean as lead_time_mean_at_start
from {{ source('gold', 'stockout_episodes') }} e
join {{ source('md', 'parts') }} p
    on p.part_id = e.part_id
left join {{ ref('dim_supplier_versions') }} v
    on v.supplier = p.supplier
    and v.valid_from <= e.out_start
    and (v.valid_to is null or e.out_start < v.valid_to)
