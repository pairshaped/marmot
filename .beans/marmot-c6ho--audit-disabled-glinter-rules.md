---
# marmot-c6ho
title: Audit disabled glinter rules
status: todo
type: task
priority: normal
tags:
    - review
    - cleanup
    - lint
created_at: 2026-05-29T17:53:25Z
updated_at: 2026-05-29T17:53:52Z
parent: marmot-xhvm
---

gleam.toml disables label_possible, unused_exports, and thrown_away_error. Audit the current findings, especially thrown_away_error and unused_exports, and either fix them or document why each disabled rule remains intentional.
