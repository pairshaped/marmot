---
# marmot-xiys
title: Preflight generated output collisions across database refs
status: completed
type: bug
priority: high
tags:
    - review
    - multi-db
    - codegen
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T21:38:04Z
parent: marmot-xhvm
---

generate checks duplicate output paths within one database target, but run_generate processes named refs one at a time. Two refs can write the same generated file and the later one overwrites the earlier one.

Compute all target output paths before writing and fail with a clear error when two refs would write the same path.

Implemented: run_generate now resolves all database targets before generation, preflights generated output paths across refs, reports cross-target collisions before writing, and preserves SQL directory discovery errors. Reviewed with tests for same-target dedupe, multi-target collisions, and unreadable sql_dir.
