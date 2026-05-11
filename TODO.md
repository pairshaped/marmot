# TODO

## Parameter Inference Gaps

- [ ] Type params inside CAST wrappers from surrounding column context.
  - Example: `pr3.season = CAST(@season AS INTEGER)` should infer `season: Int`.
  - Current behavior: the param name is preserved, but the type can fall back to `String`.
  - Pinned by the participant count wrapper regression shape.

- [ ] Type params inside arithmetic expressions from nearby column context.
  - Example: `balance_cents + @min_delta >= 0` should infer `min_delta: Int` from `balance_cents`.
  - Current behavior: `min_delta` falls back to `String`.
  - Pinned by `introspect_update_with_column_arithmetic_param_test`.

- [ ] Collect typing evidence across all occurrences before deduping named params.
  - Example: `CAST(@start AS INTEGER) = 0 OR created_at >= CAST(@start AS INTEGER)`.
  - Current behavior: first occurrence can win before a later occurrence supplies stronger type evidence.
  - Pinned by `introspect_cast_conditional_bypass_param_test`.

## Release Notes

- [ ] Publish a patch release for the derived-table binder fix after review.
  - Fixes `SELECT COUNT(*) FROM (SELECT ... WHERE ... @param ...)` losing named params and falling back to opcode guesses.
