SELECT
    first_name || ' ' || last_name as name,
    rental.count
FROM {{ source('source', 'staff') }}
INNER JOIN (
    SELECT staff_id, count(*) as count
    FROM {{ source('source', 'rental') }}
    GROUP BY 1
) rental ON rental.staff_id = staff.staff_id
ORDER BY rental.count DESC;