-- NTILE(n): divides rows into n buckets and returns the bucket number.
-- NTILE infers as IntType (non-nullable).
SELECT
    NTILE(4) OVER (ORDER BY created_at) AS quartile,
    id,
    name,
    created_at
FROM users
