with

home_team_level as
(
select
    home as team,
    away as opposition,
    home_goals as goals_for,
    away_goals as goals_against,
    home_xg as xg_for,
    away_xg as xg_against,
    *
from
    {{ source('fb_ref', 'football_xg_results_clean')}}
),

away_team_level as
(
select
    away as team,
    home as opposition,
    away_goals as goals_for,
    home_goals as goals_against,
    away_xg as xg_for,
    home_xg as xg_against,
    *
from
    {{ source('fb_ref', 'football_xg_results_clean')}}
)

select
    round,
    week,
    day,
    date,
    time,
    team,
    opposition,
    goals_for,
    goals_against,
    xg_for,
    xg_against,
    venue,
    attendance,
    referee,
    league,
    season,
    date_dt
from
    home_team_level

union

select
    round,
    week,
    day,
    date,
    time,
    team,
    opposition,
    goals_for,
    goals_against,
    xg_for,
    xg_against,
    venue,
    attendance,
    referee,
    league,
    season,
    date_dt
from
    away_team_level