---
# marmot-7td8
title: 'Fix computed expression type inference: arithmetic on typed columns falls back to Option(String)'
status: completed
type: task
priority: normal
created_at: 2026-04-30T21:54:46Z
updated_at: 2026-04-30T23:15:28Z
---

Location: `src/marmot/internal/sqlite/parse.gleam` — `infer_expression_type`

When a SELECT column is an arithmetic expression (e.g., `price * 1.08`,
`n + 1` in a CTE), Marmot has no type inference rule for it and falls back to
`Option(String)` (unknown type). SQLite returns the actual numeric type, so the
generated `decode.optional(decode.string)` fails at runtime with "expected String,
got Float" (or Int).

Examples that hit this:

- `price * 1.08` on a REAL column → inferred Option(String), SQLite returns Float
- `n + 1` in a recursive CTE → inferred Option(String), SQLite returns Int
- Any arithmetic involving known-typed columns or literals

The fix: walk the expression tree and infer the result type from operand types.
For arithmetic (`+`, `-`, `*`, `/`, `%`), if both operands have known numeric
types, the result should be the widest type (e.g., Int + Float → Float). For
operators on same-type operands, preserve the type. For mixed/unknown operands,
fall back to String as today.

Reproduction: `examples/src/expressions/sql/arithmetic.sql` (deleted from
examples because it fails), `examples/src/subqueries_ctes/sql/cte_recursive.sql`.
