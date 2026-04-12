with source as (
    select * from {{ source('raw_football', 'top_scorers') }}
),

cleaned as (
    select
        cast(player_id      as int64)      as player_id,
        player_name,
        nationality,
        position,
        cast(team_id        as int64)      as team_id,
        team_name,
        cast(goals          as int64)      as goals,
        cast(assists        as int64)      as assists,
        cast(penalties      as int64)      as penalties,
        cast(season         as int64)      as season,
        cast(scraped_at     as timestamp)  as scraped_at

    from source
    where player_id is not null
)

select * from cleaned