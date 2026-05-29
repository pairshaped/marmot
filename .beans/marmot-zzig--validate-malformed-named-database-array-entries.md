---
# marmot-zzig
title: Validate malformed named database array entries
status: todo
type: bug
priority: high
tags:
    - review
    - config
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T17:53:52Z
parent: marmot-xhvm
---

[[tools.marmot.databases]] entries without name are silently ignored. Bad compact multi-db config should produce a clear config error instead of falling through to missing database or partial config behavior.

Add a ConfigError variant, render a descriptive error, and test missing/empty name cases.
