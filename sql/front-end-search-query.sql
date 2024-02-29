WITH movies AS (
    SELECT DISTINCT
        m.idMovie,
        m.idFile,
        m.c00 AS 'title_formatted',
        gl.genre_id,
        m.premiered AS 'release_date',
        CAST(strftime('%Y', m.premiered) AS INTEGER) AS 'release_year'
        -- a.name AS 'actor_name',
        -- d.name AS 'director_name'
    FROM movie AS m
    LEFT JOIN genre_link AS gl 
        ON gl.media_id = m.idMovie

    LEFT JOIN actor_link AS al
        ON al.media_id = m.idMovie
    LEFT JOIN actor AS a 
        ON a.actor_id = al.actor_id

    LEFT JOIN director_link AS dl
        ON dl.media_id = m.idMovie
    LEFT JOIN actor AS d  -- Alias 'd' for director
        ON d.actor_id = dl.actor_id  -- Assuming directors are also listed in the 'actor' table with their respective roles
    WHERE (:uinp_genre_id IS NULL OR gl.genre_id = :uinp_genre_id)
    AND (UPPER(m.c00) LIKE UPPER(:uinp_movie_name) OR :uinp_movie_name IS NULL)
    AND (CAST(strftime('%Y', m.premiered) AS INTEGER) BETWEEN :uinp_year_min AND :uinp_year_max OR :uinp_year_min IS NULL OR :uinp_year_max IS NULL)
    AND (a.name LIKE :uinp_actor_name OR :uinp_actor_name IS NULL)
    AND (d.name LIKE :uinp_director_name OR :uinp_director_name IS NULL)
)

SELECT DISTINCT
    m.idMovie,
    m.title_formatted,
    m.release_date,
    m.release_year,
    f.idPath,
    p.strPath
FROM movies AS m
LEFT JOIN files AS f
    ON m.idFile = f.idFile
LEFT JOIN path AS p
    ON f.idPath = p.idPath
ORDER BY 
    m.title_formatted, 
    m.release_date;
