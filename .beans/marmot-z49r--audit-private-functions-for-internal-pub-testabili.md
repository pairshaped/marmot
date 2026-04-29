---
# marmot-z49r
title: Audit private functions for @internal pub testability
status: todo
type: task
priority: normal
created_at: 2026-04-29T17:09:14Z
updated_at: 2026-04-29T18:01:49Z
---

Audit rule: only expose a private function as @internal pub if BOTH:
1. It needs direct testing (meaningful logic, edge cases worth isolating)
2. Those tests are NOT already covered (or coverable) through public callers

Most audit candidates fail criterion #2:
- sql_error_hint: covered via error.to_string
- resolve_path: covered via validate_output
- codegen helpers: covered via Birdie module snapshots
- pipeline functions: covered via integration tests
- parse helpers (reverted): all testable via infer_expression_type dispatcher

@internal pub is an escape hatch, not the default. First try adding tests through the public API. Only expose if the test through the public API is genuinely awkward or can't reach the edge case.
