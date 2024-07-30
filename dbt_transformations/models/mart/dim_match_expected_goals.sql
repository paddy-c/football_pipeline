{{
  config(materialized='view')
}}

select
  week,
  day,
  date,
  time as time_fbref,
  home,
  away,
  score,
  attendance,
  venue,
  referee as referee_fbref,
  home_goals,
  away_goals,
  home_xg,
  away_xg,
  league as league_fbref,
  season as season_fbref

from
  {{ source('fb_ref', 'football_xg_results_clean') }}
where
  (home is not null) AND (away is not null) -- check for null rows
