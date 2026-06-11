---
# marmot-7lkz
title: Fix RETURNING direct-column alias inference
status: completed
type: bug
priority: normal
created_at: 2026-06-10T19:53:22Z
updated_at: 2026-06-11T00:25:53Z
---

Gleam Marmot currently loses origin type and nullability for direct columns when they are aliased in a RETURNING clause.\n\nEvidence:\n- test/birdie_snapshots/returning_alias_preserves_case.accepted expects Column("userId", StringType, True) and Column("userName", StringType, True) for RETURNING id AS userId, name AS userName, even though id is an integer primary key and name is TEXT NOT NULL.\n\nExpected behavior: preserve the alias text for generated field naming while inferring result type/nullability from the origin column: userId should be IntType non-null and userName should be StringType non-null.\n\n- [x] Add or update a regression test for aliased direct columns in RETURNING.\n- [x] Resolve RETURNING alias expressions back to origin table columns when possible.\n- [x] Update the snapshot.

## Summary of Changes

RETURNING clauses now parse full select items instead of alias strings, so aliased direct columns keep their origin type and nullability while preserving the alias text. Updated the direct alias snapshot.
