---
# marmot-ph48
title: Fix positional UPSERT parameter inference
status: completed
type: bug
priority: normal
created_at: 2026-06-10T19:52:27Z
updated_at: 2026-06-11T00:25:28Z
---

Gleam Marmot currently falls back to StringType for positional parameters in INSERT ... ON CONFLICT ... DO UPDATE statements. Named params in the same shape are inferred correctly, and Rust Marmot now infers positional VALUES, DO UPDATE SET, and WHERE params from the insert target table.\n\nEvidence:\n- test/birdie_snapshots/upsert_do_update_returning.accepted expects param, param_2, param_3, param_4, param_5 all StringType.\n- test/birdie_snapshots/upsert_do_update_set_with_where.accepted only infers the named version WHERE param, while positional INSERT/SET params fall back to StringType.\n\nExpected behavior: positional parameters should use the target table schema, including nullable integer primary-key insert params and non-null integer update/where params.\n\n- [x] Add a regression test for positional UPSERT DO UPDATE params.\n- [x] Make positional VALUES, DO UPDATE SET, and WHERE params infer from the target table.\n- [x] Update affected snapshots.

## Summary of Changes

Typed anonymous UPSERT DO UPDATE SET and WHERE parameters from the target table schema, preserving rowid insert nullability for VALUES parameters and non-null update/where parameter inference. Updated the UPSERT snapshots.
