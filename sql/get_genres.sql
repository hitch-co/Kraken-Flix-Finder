WITH genres AS (
    SELECT DISTINCT
        g.genre_id,
        g.name
    FROM genre as g
)

SELECT
    g.genre_id as id,
    g.name
FROM genres as g