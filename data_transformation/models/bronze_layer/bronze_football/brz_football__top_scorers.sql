with source as (
    select * from {{ source('raw_football', 'standings') }}
),

cleaned as (
    select
        standing_type,
        cast(position           as int64)     as position,
        cast(team_id            as int64)      as team_id,
        team_name,
        team_short,
        cast(played             as int64)      as games_played,
        cast(won                as int64)      as won,
        cast(draw               as int64)      as draw,
        cast(lost               as int64)      as lost,
        cast(points             as int64)      as points,
        cast(goals_for          as int64)      as goals_for,
        cast(goals_against      as int64)      as goals_against,
        cast(goal_difference    as int64)      as goal_difference,
        form,
        cast(season             as int64)      as season,
        cast(scraped_at         as timestamp)  as scraped_at

    from source
    where team_id is not null
)

select * from cleaned