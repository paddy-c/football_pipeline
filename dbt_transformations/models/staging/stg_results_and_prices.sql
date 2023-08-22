{{
  config(materialized='view')
}}

-- TODO: good opportunity to carry out column renaming here...
select
  *
from
  {{ source('football_data_co_uk', 'results_and_odds_country_england') }}
where
  (fthg is not null) AND (ftag is not null) -- check for null rows
