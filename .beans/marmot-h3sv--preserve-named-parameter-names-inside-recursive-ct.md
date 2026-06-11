---
# marmot-h3sv
title: Preserve named parameter names inside recursive CTEs
status: completed
type: bug
priority: normal
created_at: 2026-06-11T13:56:55Z
updated_at: 2026-06-11T15:49:41Z
---

Generating from a recursive CTE renamed the public function parameter from `club_id` to `id`.

SQL:
```sql
with recursive parents(id, parent_id) as (
    select id, parent_id from app_clubs where id = @club_id
    union all
    select c.id, c.parent_id
    from app_clubs c
    inner join parents p on p.parent_id = c.id
)
select cast(coalesce(sum(id), 0) as integer) as value from parents
```

Generated signature included:
```gleam
pub fn sum_parent_chain(
  db db: sqlight.Connection,
  id id: Int,
)
```

The generated SQL still binds `@club_id`, so the function works positionally, but the public API should preserve the named parameter label as `club_id`. This was found while generating Gleam code for the sqlite_tests request-shaped benchmark.



## Summary of Changes

- Added a regression test for recursive CTE named parameter preservation.
- Preserved named SQL placeholders when parameter extraction falls back to SQLite opcode inference, so `@club_id` remains `club_id` instead of becoming the compared column name `id`.
- Verified with `bin/test fast`.
