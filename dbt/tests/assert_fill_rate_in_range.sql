select day, site, fill_rate
from {{ ref('fct_daily_service') }}
where fill_rate < 0 or fill_rate > 1
