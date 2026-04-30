-- WHERE with LIKE and NOT LIKE for pattern matching.
-- % matches any sequence; _ matches a single character.
SELECT id, name, email
FROM users
WHERE email LIKE @domain_pattern
  AND name NOT LIKE @exclude_pattern
