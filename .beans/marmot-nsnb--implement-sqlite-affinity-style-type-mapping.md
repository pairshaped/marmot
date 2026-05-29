---
# marmot-nsnb
title: Implement SQLite affinity-style type mapping
status: completed
type: bug
priority: high
tags:
    - review
    - sqlite
    - codegen
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T21:49:54Z
parent: marmot-xhvm
---

query.parse_sqlite_type matches exact normalized type names after stripping parens. SQLite affinity rules are substring-based, so valid types like UNSIGNED BIG INT can fall through to StringType and generate wrong Gleam types.

Update mapping to follow SQLite affinity rules while preserving Marmot-specific bool/date/timestamp behavior where intentional. Add tests for variant spellings.
