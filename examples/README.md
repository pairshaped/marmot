# Marmot Examples

A standalone Gleam project demonstrating every supported SQL pattern in Marmot.
Each category under `src/` is its own `sql/` directory and generates one Gleam module.

## Quick start

```sh
cd examples
sqlite3 demo.sqlite < migration.sql
gleam run -m marmot
```

Generated modules land in `src/generated/sql/`. Browse them to see exactly what
Marmot produces for each SQL pattern.

## Running the tests

Every generated function has a corresponding test. Tests use in-memory SQLite
databases (no setup needed beyond generating the code first):

```sh
gleam test
```

## Categories

| Directory | Module | What it covers |
|---|---|---|
| `src/basic/sql/` | `basic_sql` | SELECT, INSERT, UPDATE, DELETE, REPLACE |
| `src/joins/sql/` | `joins_sql` | INNER, LEFT, RIGHT, CROSS, NATURAL joins, USING, multiple joins |
| `src/filtering/sql/` | `filtering_sql` | WHERE with =, !=, <, >, LIKE, IS NULL, IN, BETWEEN, AND/OR, NOT |
| `src/aggregation/sql/` | `aggregation_sql` | COUNT, SUM, AVG, MAX, MIN, GROUP BY, HAVING, FILTER |
| `src/sorting/sql/` | `sorting_sql` | ORDER BY, LIMIT, OFFSET |
| `src/subqueries_ctes/sql/` | `subqueries_ctes_sql` | Subqueries in WHERE/SELECT, EXISTS, CTEs, recursive CTEs |
| `src/expressions/sql/` | `expressions_sql` | CAST, COALESCE, CASE (searched, simple, nested), arithmetic, string concat |
| `src/window_functions/sql/` | `window_functions_sql` | ROW_NUMBER, RANK, DENSE_RANK, NTILE |
| `src/upserts/sql/` | `upserts_sql` | INSERT OR REPLACE, INSERT OR IGNORE, ON CONFLICT |
| `src/returning/sql/` | `returning_sql` | RETURNING clause on INSERT, UPDATE, DELETE |
| `src/parameters/sql/` | `parameters_sql` | Anonymous `?`, named `@name`/`:name`/`$name` params |
| `src/modifiers/sql/` | `modifiers_sql` | DISTINCT, UNION, UNION ALL, INTERSECT, EXCEPT |
| `src/nullability_overrides/sql/` | `nullability_overrides_sql` | `!` and `?` nullability annotations |

## Seed data

`migration.sql` creates the schema and inserts enough rows for all queries to
return meaningful results. The database has: 5 users, 6 posts, 5 comments,
4 tags, 9 post-tags, and 5 orders.

To reset: delete `demo.sqlite` and re-run `sqlite3 demo.sqlite < migration.sql`.
