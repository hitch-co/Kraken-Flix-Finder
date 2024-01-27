with horror_movies AS (
    SELECT 
        m.idMovie,
        m.idFile,
        m.c00 as 'title_formatted',
        gl.genre_id
    FROM movie as m
    LEFT JOIN genre_link as gl 
        ON gl.media_id = m.idMovie
    WHERE genre_id = '17'
)

SELECT
    hm.idMovie,
    hm.title_formatted,
    f.idPath,
    p.strPath
FROM horror_movies as hm
LEFT JOIN files as f
    ON hm.idFile = f.idFile
LEFT JOIN path as p
    ON f.idPath = p.idPath