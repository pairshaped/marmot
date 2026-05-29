---
# marmot-q3uf
title: Add CI for format, tests, and lint
status: todo
type: task
priority: normal
tags:
    - review
    - ci
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T17:53:52Z
parent: marmot-xhvm
---

There is no .github workflow. Add CI that runs at least gleam format --check src test and gleam test. Include glinter if the existing disabled-rule state can be handled without blocking unrelated work.
