SELECT
    film.title,
    rental.count
FROM {{ source('source', 'inventory') }}
JOIN {{ source('source', 'film') }} ON film.film_id = inventory.film_id
JOIN (
    SELECT inventory_id, count(*) as count
    FROM {{ source('source', 'rental') }}
    GROUP BY inventory_id
) rental ON rental.inventory_id = inventory.inventory_id
ORDER BY rental.count DESC;