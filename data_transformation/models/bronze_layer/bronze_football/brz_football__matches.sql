with source as (
    select * from {{ source('raw_football', 'matches') }}
),

cleaned as (
    select
        cast(match_id       as int64)       as match_id,
        cast(utc_date       as timestamp)   as match_timestamp,
        date(cast(utc_date  as timestamp))  as match_date,
        status,
        cast(matchday       as int64)       as matchday,
        cast(home_team_id   as int64)       as home_team_id,
        home_team_name,
        cast(away_team_id   as int64)       as away_team_id,
        away_team_name,
        cast(home_score     as int64)       as home_score,
        cast(away_score     as int64)       as away_score,
        winner,
        cast(season         as int64)       as season,
        cast(scraped_at     as timestamp)   as scraped_at

    from source
    where match_id is not null
)

select * from cleaned