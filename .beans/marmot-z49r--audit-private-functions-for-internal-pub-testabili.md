---
# marmot-z49r
title: Audit private functions for @internal pub testability
status: todo
type: task
priority: normal
created_at: 2026-04-29T17:09:14Z
updated_at: 2026-04-29T17:09:14Z
---

Survey all private functions (fn, not pub fn) across the codebase and identify candidates for @internal pub fn so they can be unit tested directly.

Priority areas:
- src/marmot/internal/sqlite/parse.gleam: private helpers in the type inference pipeline
- src/marmot/internal/sqlite/opcode.gleam: private helpers for column/parameter resolution
- src/marmot/internal/sqlite/tokenize.gleam: private tokenizer utilities
- Other modules with non-trivial private logic

Criteria: a function is a good candidate if it has meaningful logic (not a trivial one-liner) and testing it through the public API would miss edge cases.
