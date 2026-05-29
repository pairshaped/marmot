---
# marmot-euis
title: Share duplicate-name detection helper
status: todo
type: task
priority: low
tags:
    - review
    - cleanup
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T17:53:52Z
parent: marmot-xhvm
---

marmot.gleam and internal/codegen.gleam have duplicate find_duplicates implementations. Move the behavior to a shared internal helper or an existing suitable module without changing error output.
