# Statement Skeleton Parser

## Background

Marmot today tokenizes SQL well, then parses it badly. The tokenizer at `src/marmot/internal/sqlite/tokenize.gleam` correctly handles operators, string literals, quoted identifiers, parameters, and comments. The parsers built on top of those tokens use heuristic walkers (`split_at_keyword`, `find_keyword`, `take_until_keywords`) that scan the entire token list for keyword text without grammatical context. `parse_select_items` even self-describes as "a heuristic token walker, not a SQL grammar."

Three documented limitations all trace back to this:

1. **Table names containing SQL keywords** (`returning`, `into`). `tokenize.has_keyword(tokens, "RETURNING")` matches any `Word("RETURNING")` at depth 0 regardless of position.
2. **`INSERT INTO t VALUES (?, ?)` without an explicit column list.** `parse_insert_columns` finds the first paren group, which in this shape is the VALUES list, and returns garbage column names.
3. **Silent miscoercion under "search all tables" parameter resolution.** `parameters.gleam:384` strips a column reference's table prefix and searches every table for the bare name. Two tables with overlapping column names produce wrong types with no warning.

The README currently calls this a tokenizer problem. It is not. The tokenizer is fine. The fix is grammar-aware parsing of statement skeletons and clause boundaries on top of the existing tokens.

## Goals

In scope:

- A new statement skeleton parser at `src/marmot/internal/sqlite/parse/statement_parser.gleam`. Input: token list. Output: typed `Statement` value carrying parsed structure for boundaries that need it (FROM aliases, INSERT shape, CTEs) and clause token slices for bodies that don't.
- INSERT grammar coverage: `INSERT [OR ...] INTO table [column-list]? (VALUES ... | SELECT ... | DEFAULT VALUES) [ON CONFLICT ...]? [RETURNING ...]?`
- Schema fallback for `INSERT INTO t VALUES (...)` with no column list, with a deliberate write-vs-read nullability rule for INTEGER PRIMARY KEY rowid aliases.
- Alias-aware table resolution at the current parsed statement scope. Two tables with overlapping column names produce a typed `AmbiguousColumn` error rather than a silent wrong type.
- Replace global keyword checks (`tokenize.has_keyword`, `find_keyword`-driven boundary detection) at consumer sites (`sqlite.gleam`, `results.gleam`, parameter extraction) with statement-aware queries derived from the new `Statement` value.
- Parser-unit and resolver-unit tests that exercise the new modules directly, so opcode fallback no longer hides parser bugs.
- README rewrite of the limitations section that drops the misleading tokenizer framing.

Out of scope, deferred to later incremental work:

- Full expression AST or new expression-type inference. CTE result columns, COALESCE, CASE, and complex SELECT expressions keep their existing fallback behavior.
- Subquery scoping. Alias-aware resolution applies only to the current statement scope. Subqueries continue to use the existing fallback inference.
- USING-join column merging. v1 reports bare references to USING-merged columns as ambiguous.
- Replacing token walkers that operate inside bounded clause bodies (`parse_select_items`, `parse_where_columns`, `parse_update_set_columns`). They keep their internal heuristics; this work only changes their inputs.

## Output shape

The parser returns a typed `Statement`. Names are placeholders for the implementation agent.

```gleam
pub type Statement {
  Select(SelectStmt)
  Insert(InsertStmt)
  Update(UpdateStmt)
  Delete(DeleteStmt)
  Unsupported(tokens: List(Token))
}

pub type SelectStmt {
  SelectStmt(ctes: List(CteDef), body: SelectBody)
}

pub type SelectBody {
  SelectBody(
    is_distinct: Bool,
    select_list: List(Token),
    from: List(FromItem),
    where: Option(List(Token)),
    group_by: Option(List(Token)),
    having: Option(List(Token)),
    order_by: Option(List(Token)),
    limit: Option(List(Token)),
  )
}

pub type FromItem {
  FromItem(
    table: TableRef,
    alias: Option(String),
    on: Option(List(Token)),
  )
}

pub type TableRef {
  TableRef(schema: Option(String), name: String, quoted: Bool)
}

pub type InsertStmt {
  InsertStmt(
    on_conflict_clause: OnConflictClause,
    table: TableRef,
    column_list: Option(List(String)),
    source: InsertSource,
    upsert: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type InsertSource {
  ValuesSource(raw: List(Token), rows: List(List(List(Token))))
  SelectSource(SelectStmt)
  DefaultValuesSource
}

pub type UpdateStmt {
  UpdateStmt(
    table: TableRef,
    set: List(Token),
    from: List(FromItem),
    where: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type DeleteStmt {
  DeleteStmt(
    table: TableRef,
    where: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type CteDef {
  CteDef(name: String, columns: List(String), body: List(Token))
}
```

Design notes:

- `SelectBody` has no `returning` field. SQLite SELECT does not support RETURNING; uniformity is not worth allowing invalid states.
- `ValuesSource` carries both the raw clause tokens and the per-row structured form. Structured rows drive positional binding; raw tokens are an escape hatch for fallback, tests, and incomplete cases.
- `SelectSource(SelectStmt)` is structural because parameter binding for `INSERT ... SELECT` needs alias-aware resolution on the source query.
- `CteDef.body` stays raw in v1. Recursive structural CTE parsing is deferred along with CTE result-type inference.
- `FromItem` does not carry join-modifier interpretation. Alias resolution does not need it. Storing modifier tokens unparsed is acceptable if needed later; v1 simply omits them.
- `TableRef` supports schema-qualified names (`main.users`) and preserves quoting. The parser must not hard-code "one word after FROM" thinking.
- `Unsupported` exists because SQLite accepts a wide grammar (CREATE, PRAGMA, ATTACH, etc.) that Marmot is not committing to handle. A typed unsupported variant is safer than crashing or misclassifying. Downstream consumers (codegen, parameter inference) treat `Unsupported` as a parse failure for code generation and surface a `MarmotError`. The parser itself does not error on it.

## INSERT VALUES schema fallback and write-vs-read nullability

When `column_list` is `None`, the parameter inference derives bindable columns from the table schema in declared order. The schema loader switches to `PRAGMA table_xinfo` to expose generated columns (which `PRAGMA table_info` omits), and to `PRAGMA table_list` to detect `WITHOUT ROWID` tables (the `wr` column).

Bindable columns are filtered by `hidden == 0`. Generated columns are skipped silently because the user cannot bind to them; making them count generated-column slots would be hostile.

Write-nullability differs from read-nullability for rowid aliases. An `INTEGER PRIMARY KEY` column on a normal (rowid) table is non-null on read because SQLite always assigns a rowid, but binding `NULL` on insert means "auto-assign next rowid." Same for `INTEGER PRIMARY KEY AUTOINCREMENT`. On `WITHOUT ROWID` tables the same column declaration is a regular primary key with no auto-assign, so write-nullability matches read-nullability. Multi-column primary keys are never rowid aliases.

The schema loader exposes richer internal metadata. The public `Column` shape stays unchanged.

```gleam
pub type ColumnMetadata {
  ColumnMetadata(
    column: Column,
    is_rowid_alias: Bool,
    is_generated: Bool,
    hidden: Int,
  )
}
```

Parameter inference computes:

```gleam
write_nullable = column.nullable || is_rowid_alias
```

Value-count mismatches surface a `MarmotError` rather than falling back to opcode-based inference. For multi-row VALUES, every row must have the same number of expressions and that count must match the bindable column count. Any divergence errors with a specific message identifying the offending row.

The rowid-alias flag is a property of the schema, not the parse, so it lives in `schema.gleam`.

## Alias-aware table resolution

A new module at `src/marmot/internal/sqlite/parse/resolver.gleam` builds an alias map from a statement's FROM clause and resolves column references against it. Resolution applies to the current parsed statement scope only.

Alias map registration rule:

- `FromItem` with explicit alias: register the alias only.
- `FromItem` with no alias: register the table name as an implicit alias.
- Two visible names colliding in the map: typed error.

This rule handles self-joins like `users u JOIN users manager ON manager.id = u.manager_id` correctly because both items have explicit aliases and the bare table name `users` is never registered.

Column resolution returns one of five outcomes. The resolver itself returns the typed `Resolution`; mapping to `MarmotError` happens at the consumer.

- `Resolved(table, column)`: the reference points to exactly one column in scope.
- `AmbiguousColumn`: a bare reference matches columns in multiple in-scope tables. Consumers map this to a `MarmotError` asking the user to qualify.
- `UnknownQualifiedAlias`: `u.email` where `u` is not in the alias map. Consumers map to a `MarmotError`.
- `UnknownColumnInKnownTable`: `u.nope` where `u` resolves but `nope` is not in that table. Consumers map to a `MarmotError`.
- `UnknownTableRef`: a known FROM name that has no schema entry (CTE, view, attached DB table that was not introspected). Returned as a non-error fallback signal so existing CTE behavior is preserved.

Module surface:

```gleam
pub type ResolvedColumn { ResolvedColumn(table: TableRef, column: Column) }

pub type Resolution {
  Resolved(ResolvedColumn)
  AmbiguousColumn
  UnknownQualifiedAlias
  UnknownColumnInKnownTable
  UnknownTableRef
}

pub fn build_alias_map(from: List(FromItem)) -> Result(Dict(String, TableRef), MarmotError)
pub fn resolve_qualified(map, alias, col_name, schemas) -> Resolution
pub fn resolve_bare(map, col_name, schemas) -> Resolution
```

Subqueries continue to use the existing fallback path. Correlated references and inner-scope shadowing are not modeled in v1.

## Migration of existing parsers

The existing whole-statement walkers stay, but each gains a sibling that operates on a pre-bounded clause slice. Internal call sites move to the body-level helpers via the new statement parser.

| Current entry point | New body-level helper |
| --- | --- |
| `parse_select_items(tokens)` | `parse_select_item_list(select_list_tokens)` |
| `parse_where_columns(tokens)` | `parse_where_body(where_tokens)` |
| `parse_update_set_columns(tokens)` | `parse_update_set_body(set_tokens)` |
| `parse_returning_columns(tokens)` | `parse_returning_body(returning_tokens)` (input is the slice from `Stmt.returning`) |
| `parse_from_tables(tokens)` | replaced by `SelectBody.from` / `UpdateStmt.from` / `DeleteStmt.table` |
| `parse_insert_columns(tokens)` | replaced by `InsertStmt.column_list` |
| `parse_values_placeholder_positions(tokens)` | replaced by walking `InsertSource.ValuesSource.rows` |

Acceptance: internal call sites in `sqlite.gleam`, `results.gleam`, and parameter extraction must derive boundary decisions from the `Statement`, not from fresh whole-statement keyword scans. The whole-statement entry points either become thin shims that call the new statement parser then delegate to the body helper, or get retired from internal use entirely. Tests and compatibility wrappers may keep using the old helpers; the rule targets consumer sites.

## Testing

Three layers.

**Statement parser unit tests** at `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`. The parser entry point is `List(Token) -> Result(Statement, MarmotError)`; tests use a small `tokenize |> parse` helper for readability. No database, no opcode fallback, no codegen. Fixtures target the documented bugs:

- `SELECT * FROM returning` produces `Select` with a single `FromItem` whose `table.name == "returning"`.
- `SELECT * FROM into` produces the equivalent.
- `INSERT INTO t VALUES (?, ?)` produces `Insert` with `column_list == None` and rows of length 1, each containing two parameter expressions.
- `INSERT INTO t (a, b) VALUES (?, ?), (?, ?)` produces `column_list == Some(["a", "b"])` and rows of length 2.
- `INSERT INTO t (a) VALUES (?), (?, ?)` produces a `MarmotError` identifying row 2.
- `INSERT INTO t DEFAULT VALUES` produces `DefaultValuesSource`.
- `INSERT OR IGNORE INTO t (a) VALUES (?)` populates `on_conflict_clause`.
- `WITH foo AS (SELECT 1) SELECT * FROM foo` captures the CTE with a raw body and parses the outer SELECT structurally.
- `SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id` produces two `FromItem` entries with aliases.
- `CREATE TABLE x (...)` produces `Unsupported`.

**Resolver unit tests** at `test/marmot/internal/sqlite/parse/resolver_test.gleam`. Pure: `(Statement, table_schemas) -> alias map / Resolution`. Fixtures:

- `users u JOIN orders o`: `u.email` resolves to users.email, `o.total` to orders.total, bare `email` (only in users) resolves, bare `id` (in both) is `AmbiguousColumn`.
- `users u JOIN users manager ON manager.id = u.manager_id`: both aliases resolve, no false-positive on duplicate table name.
- `users JOIN orders`: bare `email` resolves via implicit table-name registration.
- `WITH foo AS (...) SELECT * FROM foo`: `foo.x` resolves to `UnknownTableRef`, not an error.
- `SELECT * FROM users u WHERE u.nope = ?`: `UnknownColumnInKnownTable`.
- `SELECT * FROM users u WHERE x.id = ?`: `UnknownQualifiedAlias`.

**Integration regression tests** for the bug class. SQL fixtures plus tests that compile and run them:

- `keyword_table.sql` querying a table named `returning`.
- `bare_insert.sql` doing `INSERT INTO t VALUES (?, ?)` against a real schema, asserting parameter types and write-nullability for a rowid PK column.
- A failing fixture under `test/fixtures/sql_failures/` for the column-count mismatch case, with the expected error pattern encoded in the test (not under `examples/`, which reads as runnable user material).

**Schema-loader tests** for the new metadata:

- Table with a generated column: `is_generated == True`, hidden in fallback positional binding.
- Table with `INTEGER PRIMARY KEY`: `is_rowid_alias == True`. Same column on a `WITHOUT ROWID` table: `is_rowid_alias == False`.
- Multi-column primary key: `is_rowid_alias == False`.

**Test style.** Birdie snapshots for positive structural shape (matches existing project convention). Hand-written assertions for contract points that must never drift: `column_list == None`, row counts, alias map entries, exact error variants. A snapshot alone is too easy to approve without noticing the one field that matters.

**Snapshot review.** No bulk accept of changed snapshots from the existing suite. Each diff gets a short categorized explanation in the PR: previously wrong now right, still fallback unchanged by design, regression found and fixed, or cosmetic name/order change with reason.

**Acceptance criteria:**

- All existing tests still pass (538 baseline). Snapshot diffs allowed where the new resolver produces better types; each diff reviewed and categorized.
- New unit suites pass.
- The three documented bugs become supported behavior, with regression fixtures.
- `rg` check on consumer sites (`sqlite.gleam`, `results.gleam`, parameter extraction) finds no `tokenize.has_keyword(_, "RETURNING")` or comparable global boundary probes. Boundary decisions originate from `Statement`. Tests, compatibility shims, and parse module internals are exempt.

## Documentation updates

**`README.md` limitations section.** The "proper SQL tokenizer" framing goes away. The three "Fixable, worth doing eventually" items collapse:

- Table names containing SQL keywords: removed.
- `INSERT INTO t VALUES (?, ?)` without column list: removed; note that generated columns are skipped automatically and count mismatches are reported as errors.
- Complex expressions: kept, with updated framing. Expression-type inference is incremental; CTE result columns and complex SELECT expressions still fall back to `StringType` in some cases. Use `CAST` to guide inference, or alias with `!`/`?` for nullability.

Two new limitations:

- Subquery scoping. Alias-aware resolution is limited to the current parsed statement scope. Parameters inside subqueries still use the existing fallback inference and may require explicit qualification or `CAST`.
- USING joins. `JOIN x USING (col)` is supported but bare `col` references in WHERE may be reported as ambiguous. Use `ON` syntax or qualify the column.

The "Differences from Squirrel" section stays, except for any sentence that currently blames the tokenizer or string parsing in a way that becomes false after this work. The user-facing difference is unchanged: Squirrel uses Postgres' wire-protocol type info; Marmot reconstructs types from SQLite schema, EXPLAIN, and SQL context.

**`src/marmot/internal/README.md`.** Update the data flow from "tokenize then heuristic walkers" to "tokenize then statement parser then typed `Statement` then clause-body walkers and resolver." Update the diagram. Add a short section on the rowid-alias rule and `ColumnMetadata` so the next contributor does not reintroduce the read/write nullability bug.

**Module-level `////` doc comments** on `parse/statement_parser.gleam` and `parse/resolver.gleam`. Each explicitly lists what it does not parse: expressions, subquery scoping, USING column merging. The `parse_select_items` / `parse_where_columns` shim docstrings get updated to point at the body-level helpers as the preferred entry points for new code.
