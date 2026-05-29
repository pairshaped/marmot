---
# marmot-xiys
title: Preflight generated output collisions across database refs
status: todo
type: bug
priority: high
tags:
    - review
    - multi-db
    - codegen
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T17:53:52Z
parent: marmot-xhvm
---

generate checks duplicate output paths within one database target, but run_generate processes named refs one at a time. Two refs can write the same generated file and the later one overwrites the earlier one.

Compute all target output paths before writing and fail with a clear error when two refs would write the same path.
