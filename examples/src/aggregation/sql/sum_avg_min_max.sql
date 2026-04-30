-- Aggregate functions: COUNT, SUM, AVG, MAX, MIN.
-- SUM and AVG infer as FloatType (nullable).
-- MAX and MIN wrapped in CAST to ensure correct type inference.
SELECT
  COUNT(*) AS total,
  SUM(price) AS revenue,
  AVG(price) AS avg_price,
  CAST(MAX(price) AS REAL) AS max_price,
  CAST(MIN(price) AS REAL) AS min_price
FROM orders
