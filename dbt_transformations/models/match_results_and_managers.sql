{{ config(materialized='table') }}

with managers as
(
select distinct
    date,
    home_team as home_fbref,
    home_manager,
    away_team as away_fbref,
    away_manager
from
    {{ source( 'fb_ref', 'football_lineups_and_managers') }}
)

-- joining on home team and date *should* be sufficient - this maps to a unique match
select * from "match-results-and-odds" results
left join managers
using (home_fbref, away_fbref, date)
