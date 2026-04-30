---
# marmot-0iqn
title: 'Fix CAST type inference: result type should come from target, not source'
status: completed
type: task
priority: normal
created_at: 2026-04-30T21:54:46Z
updated_at: 2026-04-30T23:15:28Z
---

Location: `src/marmot/internal/sqlite/parse.gleam` — `infer_expression_type`

`CAST(expr AS target_type)` currently infers the result type from the source
expression instead of the CAST target. For example:

- `CAST(created_at AS TEXT)` on an INTEGER column → infers IntType (from source),
  generates `decode.int`, but SQLite returns TEXT → runtime decode failure
- `CAST(created_at AS REAL)` on an INTEGER column → infers IntType, generates
  `decode.int`, but SQLite returns REAL → runtime decode failure

The fix: use the CAST target type (`INTEGER`→IntType, `REAL`→FloatType,
`TEXT`→StringType, `BLOB`→BitArrayType) instead of the source expression type.

Reproduction: `examples/src/expressions/sql/cast.sql` (deleted from examples
because it fails). Any `CAST(col AS TEXT)` on a non-TEXT column triggers this.
