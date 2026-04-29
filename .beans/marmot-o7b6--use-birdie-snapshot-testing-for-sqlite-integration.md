---
# marmot-o7b6
title: Use Birdie snapshot testing for sqlite integration tests
status: todo
type: task
priority: normal
created_at: 2026-04-29T16:38:34Z
updated_at: 2026-04-29T16:38:34Z
---

Replace verbose let assert [Column(...), ...] patterns in test/marmot/internal/sqlite_test.gleam with birdie.snap() calls.

Birdie is already used in codegen_test.gleam and error_test.gleam. The sqlite_test.gleam integration tests all manually assert every field of every Column/Parameter, which is verbose and error-prone. Birdie snapshots are more concise and catch regressions automatically.

Scope: every test in sqlite_test.gleam where the assertion is against result.columns, result.parameters, or similar data structures. Skip tests that validate specific error types or behavioral conditions (e.g., list.length checks, Error(_) checks).
