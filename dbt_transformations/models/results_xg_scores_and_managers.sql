{{ config(materialized='table') }}

with xg_results
as (
select * from "football_xg_results_clean"
)


-- joining on home team and date *should* be sufficient - this maps to a unique match
select
*
except
from
{{ ref("match_results_and_managers")}} results
left join xg_results
on results.home_fbref = xg_results.home
and results.date_new = xg_results.date
using (date, home)