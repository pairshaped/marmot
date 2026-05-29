---
# marmot-dpsv
title: Fix Seek opcode parameter inference for indexed columns
status: todo
type: bug
priority: critical
tags:
    - review
    - sqlite
    - correctness
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T17:53:52Z
parent: marmot-xhvm
---

opcode.resolve_seek_cursor_key currently resolves SeekGE/SeekGT/SeekLE/SeekLT parameters by taking the first column of the table schema. For an index on any other column, generated parameter types can be wrong.

Resolve the actual indexed column when possible, or fall back safely with a clear limitation. Add regression tests with an index on a non-first column such as email.
