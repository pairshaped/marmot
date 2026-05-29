---
# marmot-xhvm
title: Address validated code review findings
status: todo
type: epic
priority: high
tags:
    - review
created_at: 2026-05-29T17:53:47Z
updated_at: 2026-05-29T17:53:47Z
---

Tackle order: quick wins first, then the highest-risk correctness work.

1. marmot-zioc Remove DATABASE_URL from no-database guidance.
2. marmot-35qo Use gleam/bool.guard in SQL file validation.
3. marmot-ytd1 Preserve filesystem errors when creating generated output dirs.
4. marmot-euis Share duplicate-name detection helper.
5. marmot-zzig Validate malformed named database array entries.
6. marmot-a17p Report gleam.toml read and parse failures clearly.
7. marmot-zl3d Prevent --database from overriding named database refs.
8. marmot-xiys Preflight generated output collisions across database refs.
9. marmot-nsnb Implement SQLite affinity-style type mapping.
10. marmot-dpsv Fix Seek opcode parameter inference for indexed columns.
11. marmot-q3uf Add CI for format, tests, and lint.
12. marmot-c6ho Audit disabled glinter rules.
13. marmot-zo32 Remove stale v1 schema metadata path.
