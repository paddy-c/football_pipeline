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

managers_and_lineups as (
select 
  date,
  home_team as home, -- home/away are the fb_ref names
  away_team as away,
  home_manager,
  away_manager,
  home_starting_11,
  away_starting_11
from
  {{ ref('dim_match_team_lineups_and_managers') }}
),

xg_results_with_fd_names as (
select * from
{{ ref('dim_match_expected_goals')}} xg 
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
  {{ ref('fct_match_results_and_odds') }} results_fd
left join
  xg_results_with_fd_names xg using(date, hometeam, awayteam)
left join
  managers_and_lineups using(date, home, away)
where 
div in ('E0','E1','E2') -- xg data for top 3 divisions only so far
and date > '2014-07-01' -- xg ref goes back to 2014-06-01 for E0, E1, E2 
  