---
# marmot-yse8
title: 'Test: Add direct unit tests for parse.gleam and opcode.gleam'
status: todo
type: task
priority: high
created_at: 2026-04-28T21:32:33Z
updated_at: 2026-04-28T21:32:33Z
---

Critical untested functions (all tested only indirectly via introspect_query):

1. `parse.infer_expression_type` + helpers (`infer_cast_type`, `infer_coalesce_type`, `infer_case_type`) -- ~330 lines of type inference. Wrong output = silently wrong Gleam types. Has state-machine logic for CAST/COALESCE/CASE/window functions with zero direct coverage.
2. `opcode.infer_parameter_type` -- ~80 lines matching Variable opcodes to comparison context. Wrong inference = generated code expects wrong SQLite type for a bound parameter.
3. `opcode.find_column_for_register` -- ~60 lines resolving ResultRow registers back to cursor+column-index. Has sorter-aware logic with a known fix history and no direct test.
