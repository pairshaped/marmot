---
# marmot-a17p
title: Report gleam.toml read and parse failures clearly
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

marmot.gleam reads gleam.toml with result.unwrap(""), and project.parse_toml_config turns TOML parse errors into an empty config. This makes missing, unreadable, or malformed config look like unrelated database configuration failures.

Keep absent gleam.toml acceptable if that is intended, but report malformed TOML and read failures that should not be ignored.
