---
# marmot-zo32
title: Remove stale v1 schema metadata path
status: todo
type: task
priority: low
tags:
    - review
    - cleanup
    - sqlite
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T17:53:52Z
parent: marmot-xhvm
---

schema.get_table_metadata appears to be legacy now that sqlite.gleam uses get_table_metadata_v2. Confirm no callers remain, remove the stale v1 path, and consolidate duplicated PRAGMA decoding where it still makes sense.
