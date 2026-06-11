---
# marmot-n6dg
title: Infer named parameter type from IN-list integer columns
status: completed
type: bug
priority: normal
created_at: 2026-06-11T13:56:55Z
updated_at: 2026-06-11T15:49:41Z
---

Generating from a query that compares an integer column to multiple named parameters inferred one parameter as String.

SQL:
```sql
select count(*) as value
from app_fees
where club_id in (@club_id, @parent_id, @grandparent_id)
  and active = @active
```

Generated signature included:
```gleam
pub fn count_fees(
  db db: sqlight.Connection,
  club_id club_id: Int,
  parent_id parent_id: Int,
  grandparent_id grandparent_id: String,
  active active: Int,
)
```

Expected `grandparent_id` to be inferred as `Int`, matching `app_fees.club_id`. This was found while generating Gleam code for the sqlite_tests request-shaped benchmark.



## Summary of Changes

- Added a regression test for named parameters inside an integer `IN` list.
- Taught binder discovery to reuse the owning `IN` column for named list parameters, so all three named parameters infer `Int` from `app_fees.club_id`.
- Verified with `bin/test fast`.
