-- Arithmetic expressions: +, -, *, / on table columns.
-- Marmot infers the result type from numeric literals in the expression.
-- price * 1.08: Float literal → FloatType result.
SELECT
  id,
  price,
  price * 1.08 AS price_with_tax,
  price / 100 AS price_in_cents
FROM orders
