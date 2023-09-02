{{ config(materialized='table') }}
-- TODO: implement incremental refresh later

with 
home_team_mapping as
(
select 
  fb_ref_name as home, 
  football_data_name as hometeam 
from 
  {{ ref("fb_ref_football_data_co_uk_mapping")}}
),

away_team_mapping as
(
select 
  fb_ref_name as away, 
  football_data_name as awayteam 
from 
  {{ ref("fb_ref_football_data_co_uk_mapping")}}
),

xg_results_with_fd_names as (
select * from
{{ ref('stg_expected_goals_results')}} xg 
left join 
  home_team_mapping 
using 
  (home)
left join 
  away_team_mapping
using
  (away)
)
 
  
select 
  *
from 
  {{ ref('stg_results_and_prices') }} results_fd
left join
  xg_results_with_fd_names xg
using(date, hometeam, awayteam)
--on xg.home_fd = results_fd.hometeam
--and xg.away_fd = results_fd.awayteam 
--and xg.date = results_fd.date 
where 
div in ('E0','E1','E2') -- xg data for top 3 divisions only so far
and date > '2014-07-01' -- xg ref goes back to 2014-06-01 for E0, E1, E2 
  