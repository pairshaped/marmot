---
# marmot-hyvn
title: Fix RETURNING cast alias nullability inference
status: completed
type: bug
priority: normal
created_at: 2026-06-10T19:52:27Z
updated_at: 2026-06-11T00:26:30Z
---

Gleam Marmot currently marks INSERT ... RETURNING CAST(id AS TEXT) AS id_text as nullable even when id is a non-null primary key. Rust Marmot now treats the cast alias as String and non-null for this case.\n\nEvidence:\n- test/birdie_snapshots/returning_cast_as_alias.accepted expects Column("id_text", StringType, True).\n\nExpected behavior: RETURNING expression aliases should use the same expression inference as SELECT aliases, and a cast of a non-null returned column should preserve non-nullability when the input expression is known non-null.\n\n- [x] Add or update a regression test for RETURNING CAST(id AS TEXT) AS id_text.\n- [x] Apply expression inference to RETURNING aliases.\n- [x] Preserve non-nullability for casts of known non-null expressions.\n- [x] Update the snapshot.

## Summary of Changes

RETURNING expression aliases now run through expression inference, so CAST(id AS TEXT) AS id_text resolves as a non-null String result in the existing regression snapshot.
