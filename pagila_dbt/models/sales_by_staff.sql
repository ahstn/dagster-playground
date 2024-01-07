SELECT
    first_name || ' ' || last_name as name,
    rental.count
FROM {{ source('pagila', 'staff') }}
INNER JOIN (
    SELECT staff_id, count(*) as count
    FROM {{ source('pagila', 'rental') }}
    GROUP BY 1
) rental ON rental.staff_id = staff.staff_id
ORDER BY rental.count DESC;