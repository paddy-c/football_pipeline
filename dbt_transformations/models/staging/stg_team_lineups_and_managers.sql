{{
  config(
    materialized="view"
  )
}}

-- group lineups into arrays
SELECT
    date,
    home_team,
    away_team,
    home_manager,
    away_manager,
    array_agg(home_player) as home_starting_11,
    array_agg(away_player) as away_starting_11
FROM
    {{ source('fb_ref', 'football_lineups_and_managers') }}
group by
    date,
    home_team,
    away_team,
    home_manager,
    away_manager
