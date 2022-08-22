select
    md5(rank || date) as unique_id,
    rank as RANKING,
    date as DATA_COLETA,
    song_name as MUSICA,
    artist as CANTOR,
    album as ALBUM,
    {{ ms_to_minutes('track_time_ms') }} as DURACAO,
    release_date as DATA_LANCAMENTO,
    external_urls as LINK_SPOTIFY
from {{ source('tracks', 'raw_tracks') }}
order by rank, date