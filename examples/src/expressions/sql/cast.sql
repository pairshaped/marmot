-- CAST: explicitly convert an expression to a target type.
-- Marmot recognizes CAST(expr AS INTEGER/REAL/TEXT/BLOB) and generates the
-- corresponding Gleam type (Int, Float, String, BitArray).
SELECT
  id,
  name,
  CAST(created_at AS TEXT) AS created_text
FROM users
