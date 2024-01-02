SELECT
	  f.title AS film, c.name AS category
FROM {{ source('source', 'film') }} AS f
  LEFT JOIN {{ source('source', 'film_category') }} AS fc 
    on f.film_id = fc.film_id
  LEFT JOIN {{ source('source', 'category') }} AS c
    ON c.category_id = fc.category_id
ORDER BY f.title