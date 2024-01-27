--genre_link.media_id is the same as movies.idMovie???
SELECT 
    'genre_link media_id' as 'table', 
    COUNT(DISTINCT media_id) as counts 
FROM genre_link 

UNION ALL

SELECT
    'movies' as 'table', 
    COUNT(DISTINCT idMovie) as counts 
FROM movie
