with versions as (
    select
        name as supplier,
        lead_time_mean,
        lead_time_std,
        carrier,
        effective_at,
        row_number() over (
            partition by name, effective_at order by source_lsn desc
        ) as newest
    from {{ source('md', 'suppliers_history') }}
    where not deleted
)

select
    supplier,
    lead_time_mean,
    lead_time_std,
    carrier,
    effective_at as valid_from,
    lead(effective_at) over (partition by supplier order by effective_at) as valid_to
from versions
where newest = 1
