-- NTILE(n): divides rows into n buckets and returns the bucket number.
-- NTILE infers as IntType (non-nullable).
SELECT
    id,
    name,
    created_at,
    NTILE(4) OVER (ORDER BY created_at) AS quartile
FROM users
