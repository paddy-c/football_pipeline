{{
  config(materialized='view')
}}

-- TODO: good opportunity to carry out column renaming here...
-- TODO: add case statement to parse dates for non epl games 
select
  *
from
  {{ source('football_data_co_uk', 'results_and_odds_new_country_england') }}
where
  (fthg is not null) AND (ftag is not null) -- check for null rows
