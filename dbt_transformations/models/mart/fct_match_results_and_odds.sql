{{
  config(materialized='view')
}}

-- TODO: good opportunity to carry out column renaming here...
-- TODO: add case statement to parse date formats for non epl/England games 
select
  {{ dbt_utils.star(from=source('football_data_co_uk', 'match_results_and_odds'), except=['b365h','b365d','b365a']) }},
  case when b365h = 0 then null else b365h end as b365h,
  case when b365d = 0 then null else b365d end as b365d,
  case when b365a = 0 then null else b365a end as b365a



from
  {{ source('football_data_co_uk', 'match_results_and_odds') }}
where
  (fthg is not null) AND (ftag is not null) -- check for null rows