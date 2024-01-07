SELECT
    first_name || ' ' || last_name as name,
    rental.count
FROM {{ source('source', 'customer') }}
INNER JOIN (
    SELECT customer_id, count(*) as count
    FROM {{ source('source', 'rental') }}
    GROUP BY 1
) rental ON rental.customer_id = customer.customer_id
ORDER BY rental.count DESC;