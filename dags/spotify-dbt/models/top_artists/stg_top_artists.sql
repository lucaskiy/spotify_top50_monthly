select
    md5(rank || date) as unique_id,
    rank as RANKING,
    date as DATA_COLETA,
    artist as ARTISTA,
    followers as SEGUIDORES,
    genre as GENERO,
    external_urls as LINK_SPOTIFY
from {{ source('artists', 'raw_artists') }}
order by rank, date