{{ config(materialized='view')}}

with

home_team_matches as (
select 
div,
date,
time,
time_fbref,
hometeam as team,
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

b365h as bet365_win_price,
b365d as bet365_draw_price,
b365a as bet365_lose_price,

home_xg as xg_for,
away_xg as xg_against,

referee,
referee_fbref
 from 
   {{ ref("england_combined_results_xg_prices") }}
),

-- now we need to flip the home and away teams
away_team_matches as (
select
div,
date,
time,
time_fbref,
awayteam as team,
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

b365a as bet365_win_price,
b365d as bet365_draw_price,
b365h as bet365_lose_price,

away_xg as xg_for,
home_xg as xg_against,

referee,
referee_fbref
 from 
   {{ ref("england_combined_results_xg_prices")}}

),

all_matches as (
select * from home_team_matches
union all
select * from away_team_matches
)

select * from all_matches
order by team, date