{{ config(materialized='view')}}

with

home_team_matches as (
select 
div,
season_fbref,
date,
time,
time_fbref,
hometeam as team,
home as team_fbref,
home_manager as manager,
1 as is_home,
awayteam as opponent,
fthg as goals_for,
ftag as goals_against,
case when fthg > ftag then 3 when fthg = ftag then 1 else 0 end as points,
case 
  when fthg > ftag then 'W'
  when fthg = ftag then 'D'
  else 'L'
end as team_result,
hthg as ht_goals_for,
htag as ht_goals_against,
case
  when hthg > htag then 'W'
  when hthg = htag then 'D'
  else 'L'
end as ht_result,
case when hthg = 0 AND htag = 0 then 1 else 0 end as ht_nil_all,

hthg - htag as ht_goal_diff,
hs as shots_for,
"as" as shots_against,
hst as shots_on_target_for,
ast as shots_on_target_against,

hf as fouls_for,
af as fouls_against,

hc as corners_for,
ac as corners_against,

hy as yellow_cards_for,
ay as yellow_cards_opponent,

hr as red_cards_for,
ar as red_cards_opponent,

b365h as bet365_opening_win_price,
b365d as bet365_opening_draw_price,
b365a as bet365_opening_lose_price,

b365ch as bet365_closing_win_price,
b365cd as bet365_closing_draw_price,
b365ca as bet365_closing_lose_price,

avgh as avg_opening_win_price,
avgd as avg_opening_draw_price,
avga as avg_opening_lose_price,

avgch as avg_closing_win_price,
avgcd as avg_closing_draw_price,
avgca as avg_closing_lose_price,

home_xg as xg_for,
away_xg as xg_against,

referee,
referee_fbref,

home_starting_11 as team_starting_11,
away_starting_11 as opposition_starting_11
 from 
   {{ ref("obt_match_results_all_details") }}
),

-- now we need to flip the home and away teams
away_team_matches as (
select
div,
season_fbref,
date,
time,
time_fbref,
awayteam as team,
away as team_fbref,
away_manager as manager,
0 as is_home,
hometeam as opponent,
ftag as goals_for,
fthg as goals_against,
case when fthg > ftag then 0 when fthg = ftag then 1 else 3 end as points,
case 
  when fthg > ftag then 'L'
  when fthg = ftag then 'D'
  else 'W'
end as team_result,
htag as ht_goals_for,
hthg as ht_goals_against,
case
  when hthg > htag then 'L'
  when hthg = htag then 'D'
  else 'W'
end as ht_result,
case when hthg = 0 AND htag = 0 then 1 else 0 end as ht_nil_all,

htag - hthg as ht_goal_diff,
"as" as shots_for,
hs as shots_against,
ast as shots_on_target_for,
hst as shots_on_target_against,

af as fouls_for,
hf as fouls_against,

ac as corners_for,
hc as corners_against,

ay as yellow_cards_for,
hy as yellow_cards_opponent,

ar as red_cards_for,
hr as red_cards_opponent,

b365a as bet365_opening_win_price,
b365d as bet365_opening_draw_price,
b365h as bet365_opening_lose_price,

b365ca as bet365_closing_win_price,
b365cd as bet365_closing_draw_price,
b365ch as bet365_closing_lose_price,

avga as avg_opening_win_price,
avgd as avg_opening_draw_price,
avgh as avg_opening_lose_price,

avgca as avg_closing_win_price,
avgcd as avg_closing_draw_price,
avgch as avg_closing_lose_price,

away_xg as xg_for,
home_xg as xg_against,

referee,
referee_fbref,
away_starting_11 as team_starting_11,
home_starting_11 as opposition_starting_11
 from 
   {{ ref("obt_match_results_all_details") }}

),

all_matches_stg as (
select * from home_team_matches
union all
select * from away_team_matches
),

all_matches as (
  select 
  *,
  CASE WHEN goals_for > goals_against THEN 1 ELSE 0 END as team_win,
  CASE WHEN goals_for < goals_against THEN 1 ELSE 0 END as team_defeat,
  CASE WHEN goals_against = 0 THEN 1 ELSE 0 END as team_clean_sheet,
  xg_for/ NULLIF(shots_for, 0) as xg_for_per_shot,
  xg_against/ NULLIF(shots_against, 0) as xg_against_per_shot,
  avg_closing_win_price - avg_opening_win_price as avg_win_price_move_amount,
  1/avg_opening_win_price as avg_win_probability,
  1/avg_opening_lose_price as avg_lose_probability,
  1/bet365_opening_win_price as bet365_win_probability,
  1/bet365_closing_win_price as bet365_closing_win_probability
  from all_matches_stg
)

select * from all_matches