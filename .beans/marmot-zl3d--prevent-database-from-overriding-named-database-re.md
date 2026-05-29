---
# marmot-zl3d
title: Prevent --database from overriding named database refs
status: todo
type: bug
priority: critical
tags:
    - review
    - multi-db
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T17:53:52Z
parent: marmot-xhvm
---

In named database mode, the CLI should select a ref with --database-name, not override the ref path with --database. The current parse/target flow preserves the old override behavior even when a named ref is selected.

Decide the compatibility shape, then make mixed --database + --database-name behavior explicit and tested. Multi-db refs should not silently run against a different SQLite file.
