---
# marmot-35qo
title: Use gleam/bool.guard in SQL file validation
status: todo
type: task
priority: low
tags:
    - review
    - cleanup
    - quick-win
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T17:53:52Z
parent: marmot-xhvm
---

src/marmot/internal/sql_files.gleam reimplements bool_guard locally. Replace the private helper with gleam/bool.guard and keep the filename validation behavior unchanged.
