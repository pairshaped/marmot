---
# marmot-ytd1
title: Preserve filesystem errors when creating generated output dirs
status: todo
type: task
priority: normal
tags:
    - review
    - error-handling
    - quick-win
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T17:53:52Z
parent: marmot-xhvm
---

marmot.ensure_parent_dir currently returns Error(Nil), so write_module_file can only print a generic parent-directory failure.

Return or render the underlying simplifile error so blocked paths, permission failures, and other filesystem problems give useful diagnostics. Update tests around ensure_parent_dir.
