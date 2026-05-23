-- Recursive CTE using WITH RECURSIVE.
-- Example: generate a sequence of numbers.
-- The recursive part unions with the initial seed until no new rows are produced.
WITH RECURSIVE counter (n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM counter
    WHERE n < 5
)

SELECT CAST(n AS TEXT) AS n FROM counter
