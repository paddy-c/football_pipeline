{{
  config(materialized='view')
}}

-- TODO: good opportunity to carry out column renaming here...
select
  *
from
  {{ source('fb_ref', 'football_xg_results_clean') }}
where
  (home is not null) AND (away is not null) -- check for null rows
