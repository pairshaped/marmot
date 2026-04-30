-- Simple CASE: compares one expression against multiple values.
SELECT
  name,
  active,
  CASE active
    WHEN 1 THEN 'yes'
    WHEN 0 THEN 'no'
    ELSE 'unknown'
  END AS active_status
FROM users
