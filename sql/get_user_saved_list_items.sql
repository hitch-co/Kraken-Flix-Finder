WITH movies AS (
    SELECT DISTINCT
        m.idMovie,
        m.idFile,
        m.c00 as 'title_formatted',
        gl.genre_id,
        m.premiered as 'release_date',
        CAST(strftime('%Y', m.premiered) AS INTEGER) as 'release_year'
    FROM movie as m
    LEFT JOIN genre_link as gl 
        ON gl.media_id = m.idMovie
    WHERE m.idMovie IN ({str_movie_ids})
)

SELECT DISTINCT
    m.idMovie,
    m.title_formatted,
    m.release_date,
    m.release_year,
    f.idPath,
    p.strPath
FROM movies as m
LEFT JOIN files as f
    ON m.idFile = f.idFile
LEFT JOIN path as p
    ON f.idPath = p.idPath
ORDER BY 
    m.title_formatted, 
    m.release_date