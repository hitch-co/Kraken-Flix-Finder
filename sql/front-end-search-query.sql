WITH movies AS (
    SELECT 
        m.idMovie,
        m.idFile,
        m.c00 as 'title_formatted',
        gl.genre_id
    FROM movie as m
    LEFT JOIN genre_link as gl 
        ON gl.media_id = m.idMovie
    WHERE genre_id = '17'
    AND UPPER(m.c00) LIKE UPPER(:uinp_name)
)

SELECT
    m.idMovie,
    m.title_formatted,
    f.idPath,
    p.strPath
FROM movies as m
LEFT JOIN files as f
    ON m.idFile = f.idFile
LEFT JOIN path as p
    ON f.idPath = p.idPath