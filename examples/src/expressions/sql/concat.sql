-- String concatenation with || operator.
SELECT
  id,
  name || ' <' || email || '>' AS name_and_email
FROM users
