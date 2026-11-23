-- Each supplier version must end after it starts, and one supplier never has two
-- versions starting at the same time.
select supplier, valid_from, valid_to
from {{ ref('dim_supplier_versions') }}
where valid_to is not null and valid_to <= valid_from
union all
select supplier, valid_from, null as valid_to
from {{ ref('dim_supplier_versions') }}
group by supplier, valid_from
having count(*) > 1
