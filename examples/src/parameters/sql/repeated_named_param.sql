-- Repeated @name parameters: Marmot deduplicates them into a single argument.
-- Both sides of the UNION reference @pattern, generating one argument.
-- This is preferred over anonymous ? which would generate separate _2 arguments.
SELECT
    id,
    name
FROM users
WHERE
    name LIKE
    @pattern
UNION
SELECT
    id,
    name
FROM users
WHERE email LIKE @pattern
