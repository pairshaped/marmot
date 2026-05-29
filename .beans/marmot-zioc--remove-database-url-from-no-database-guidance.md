---
# marmot-zioc
title: Remove DATABASE_URL from no-database guidance
status: todo
type: task
priority: normal
tags:
    - review
    - quick-win
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T17:53:52Z
parent: marmot-xhvm
---

The README and llms.txt no longer advertise DATABASE_URL, but the runtime no-database error still lists it as an option.

Update src/marmot/internal/error.gleam and the corresponding Birdie snapshot so the message recommends only --database and [tools.marmot].database. Keep DATABASE_URL support in code for backwards compatibility.
