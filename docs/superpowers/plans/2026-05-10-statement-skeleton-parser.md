# Statement Skeleton Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the heuristic whole-statement token walkers in marmot's SQLite parser with a grammar-aware statement skeleton parser that produces a typed `Statement` value. Eliminates three documented bug classes: keyword-named tables, column-less `INSERT VALUES`, and silent miscoercion under "search all tables" parameter resolution.

**Architecture:** Add a new statement parser at `src/marmot/internal/sqlite/parse/statement_parser.gleam` and a column resolver at `src/marmot/internal/sqlite/parse/resolver.gleam`. Both consume the existing token stream from `tokenize.gleam`. Existing clause-body walkers (`parse_select_items`, `parse_where_columns`, `parse_update_set_columns`, `parse_returning_columns`) gain body-level siblings that operate on parser-provided slices. Internal call sites in `parameters.gleam`, `results.gleam`, and `sqlite.gleam` migrate to derive boundary decisions from the typed `Statement`. Schema loader (`schema.gleam`) switches to `PRAGMA table_xinfo` and `PRAGMA table_list` and exposes `ColumnMetadata` carrying `is_rowid_alias`, `is_generated`, and `hidden`.

**Tech Stack:** Gleam (Erlang target). Test framework: `gleeunit` with `birdie` for snapshot tests. SQLite via the `sqlight` package. Spec: `docs/superpowers/specs/2026-05-10-statement-skeleton-parser-design.md`.

---

## File Structure

**New files:**
- `src/marmot/internal/sqlite/parse/statement_parser.gleam` - statement skeleton parser; defines all `Statement` AST types
- `src/marmot/internal/sqlite/parse/resolver.gleam` - alias map + column resolution
- `test/marmot/internal/sqlite/parse/statement_parser_test.gleam` - parser unit tests
- `test/marmot/internal/sqlite/parse/resolver_test.gleam` - resolver unit tests
- `test/sql_failures_test.gleam` - harness for fixtures that must produce a `MarmotError`
- `test/fixtures/sql_failures/*.sql` - failing SQL fixtures
- `examples/edge_cases/keyword_table.sql` + corresponding test
- `examples/edge_cases/bare_insert.sql` + corresponding test

**Modified files:**
- `src/marmot/internal/sqlite/schema.gleam` - add `ColumnMetadata`; switch to `PRAGMA table_xinfo` and `PRAGMA table_list`
- `src/marmot/internal/sqlite/parse/select.gleam` - add `parse_select_item_list`, `parse_returning_body`
- `src/marmot/internal/sqlite/parse/parameters.gleam` - add `parse_where_body`, `parse_update_set_body`
- `src/marmot/internal/sqlite/parameters.gleam` - migrate to consume typed `Statement`
- `src/marmot/internal/sqlite/results.gleam` - migrate to consume typed `Statement`
- `src/marmot/internal/sqlite.gleam` - migrate `has_returning` style probes to read from `Statement`
- `README.md` - rewrite limitations section
- `src/marmot/internal/README.md` - update architecture overview, document `ColumnMetadata` and rowid-alias rule

---

## Task 1: ColumnMetadata + PRAGMA table_xinfo loader

**Files:**
- Modify: `src/marmot/internal/sqlite/schema.gleam`
- Test: `test/marmot/internal/sqlite_test.gleam` (extend existing schema test section)

**Context:** `PRAGMA table_info` does not return generated columns. `PRAGMA table_xinfo` adds the `hidden` column where `0 = normal`, `2 = virtual generated`, `3 = stored generated`. The schema loader must switch so generated columns are visible and skippable later. The current rowid-alias workaround (lines 86-89 of `schema.gleam`) hardcodes the read-side fix; this task introduces `ColumnMetadata` as the structured place for that knowledge to live, but defers the `is_rowid_alias` field to Task 2.

- [ ] **Step 1: Write the failing test**

Add to `test/marmot/internal/sqlite_test.gleam`:

```gleam
pub fn schema_loader_marks_generated_columns_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER, c INTEGER GENERATED ALWAYS AS (b * 2) VIRTUAL);",
      conn,
    )
  let metadata = schema.get_table_metadata_v2(conn)
  let assert Ok(cols) = dict.get(metadata.columns, "t")
  let names = list.map(cols, fn(m) { m.column.name })
  let assert ["a", "b", "c"] = names
  let assert Ok(c_meta) = list.find(cols, fn(m) { m.column.name == "c" })
  let assert True = c_meta.is_generated
  let assert Ok(b_meta) = list.find(cols, fn(m) { m.column.name == "b" })
  let assert False = b_meta.is_generated
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: compile error - `schema.get_table_metadata_v2` does not exist; `ColumnMetadata` type does not exist.

- [ ] **Step 3: Write minimal implementation**

Add to `src/marmot/internal/sqlite/schema.gleam`:

```gleam
pub type ColumnMetadata {
  ColumnMetadata(
    column: Column,
    is_rowid_alias: Bool,
    is_generated: Bool,
    hidden: Int,
  )
}

pub type TableMetadataV2 {
  TableMetadataV2(
    columns: Dict(String, List(ColumnMetadata)),
    pks: Dict(String, String),
    rootpages: Dict(Int, String),
  )
}

pub fn get_table_metadata_v2(db: Connection) -> TableMetadataV2 {
  let xinfo_decoder = {
    use col_name <- decode.field(1, decode.string)
    use type_str <- decode.field(2, decode.string)
    use notnull <- decode.field(3, decode.int)
    use pk <- decode.field(5, decode.int)
    use hidden <- decode.field(6, decode.int)
    decode.success(#(col_name, type_str, notnull, pk, hidden))
  }

  let master_decoder = {
    use name <- decode.field(0, decode.string)
    use rootpage <- decode.field(1, decode.int)
    decode.success(#(name, rootpage))
  }
  let tables = case
    sqlight.query(
      "SELECT name, rootpage FROM sqlite_master WHERE type='table'",
      on: db,
      with: [],
      expecting: master_decoder,
    )
  {
    Ok(rows) -> rows
    Error(err) -> {
      io.println_error(
        "warning: Could not read table metadata: " <> err.message,
      )
      []
    }
  }

  let index_parent_decoder = {
    use rootpage <- decode.field(0, decode.int)
    use tbl_name <- decode.field(1, decode.string)
    decode.success(#(rootpage, tbl_name))
  }
  let indexes = case
    sqlight.query(
      "SELECT rootpage, tbl_name FROM sqlite_master WHERE type='index'",
      on: db,
      with: [],
      expecting: index_parent_decoder,
    )
  {
    Ok(rows) -> rows
    Error(_) -> []
  }

  list.fold(
    tables,
    TableMetadataV2(dict.new(), dict.new(), dict.new()),
    fn(acc, table) {
      let #(table_name, rootpage) = table
      let rootpages = dict.insert(acc.rootpages, rootpage, table_name)
      let pragma_sql =
        "PRAGMA table_xinfo(\""
        <> parse.quote_identifier(table_name)
        <> "\")"
      case
        sqlight.query(pragma_sql, on: db, with: [], expecting: xinfo_decoder)
      {
        Ok(rows) -> {
          let metadatas =
            list.map(rows, fn(row) {
              let #(col_name, type_str, notnull, pk, hidden) = row
              let column_type = case query.parse_sqlite_type(type_str) {
                Ok(t) -> t
                Error(_) -> StringType
              }
              // NOTE: read-side nullability for INTEGER PK rowid aliases is
              // deferred to Task 2 (which sets is_rowid_alias and applies the
              // adjustment via the metadata flag). For now, preserve the
              // existing inline workaround.
              let nullable = case pk > 0, column_type {
                True, query.IntType -> False
                _, _ -> notnull == 0
              }
              ColumnMetadata(
                column: Column(
                  name: col_name,
                  column_type: column_type,
                  nullable: nullable,
                ),
                is_rowid_alias: False,
                is_generated: hidden == 2 || hidden == 3,
                hidden: hidden,
              )
            })
          let columns =
            dict.insert(acc.columns, table_name, metadatas)
          let pks = case
            list.find(rows, fn(row) {
              let #(_, _, _, pk, _) = row
              pk > 0
            })
          {
            Ok(#(pk_name, _, _, _, _)) ->
              dict.insert(acc.pks, table_name, pk_name)
            Error(_) -> acc.pks
          }
          TableMetadataV2(columns: columns, pks: pks, rootpages: rootpages)
        }
        Error(err) -> {
          io.println_error(
            "warning: Could not read schema for table "
            <> table_name
            <> ": "
            <> err.message,
          )
          TableMetadataV2(
            columns: acc.columns,
            pks: acc.pks,
            rootpages: rootpages,
          )
        }
      }
    },
  )
  |> add_index_rootpages_v2(indexes)
}

fn add_index_rootpages_v2(
  acc: TableMetadataV2,
  indexes: List(#(Int, String)),
) -> TableMetadataV2 {
  let rootpages =
    list.fold(indexes, acc.rootpages, fn(rp, entry) {
      let #(rootpage, tbl_name) = entry
      dict.insert(rp, rootpage, tbl_name)
    })
  TableMetadataV2(columns: acc.columns, pks: acc.pks, rootpages: rootpages)
}
```

The existing `get_table_metadata` stays in place. New consumers will be migrated to `get_table_metadata_v2` later. The existing call site (`sqlite.gleam`) keeps using the legacy function until Task 16.

Add the test imports if missing: `import marmot/internal/sqlite/schema`.

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for the new test; all other tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/schema.gleam test/marmot/internal/sqlite_test.gleam
git commit -m "Add ColumnMetadata loader using PRAGMA table_xinfo

Introduces get_table_metadata_v2 that exposes generated columns and
hidden flag. Existing get_table_metadata stays for legacy callers
during the migration."
```

---

## Task 2: WITHOUT ROWID detection + is_rowid_alias

**Files:**
- Modify: `src/marmot/internal/sqlite/schema.gleam`
- Test: `test/marmot/internal/sqlite_test.gleam`

**Context:** A column is a rowid alias when it is `INTEGER PRIMARY KEY` on a normal (rowid) table. `WITHOUT ROWID` tables expose the same column declaration without the auto-assign behavior. Detection: `PRAGMA table_list` returns a `wr` column (1 if WITHOUT ROWID). `is_rowid_alias` becomes the schema-level source of truth so write-nullability calculations in Task 17 don't have to recompute it.

- [ ] **Step 1: Write the failing test**

Append to `test/marmot/internal/sqlite_test.gleam`:

```gleam
pub fn schema_loader_marks_rowid_alias_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE rowid_t (id INTEGER PRIMARY KEY, name TEXT);
       CREATE TABLE no_rowid_t (id INTEGER PRIMARY KEY, name TEXT) WITHOUT ROWID;
       CREATE TABLE composite_t (a INTEGER, b INTEGER, PRIMARY KEY (a, b));",
      conn,
    )
  let metadata = schema.get_table_metadata_v2(conn)

  let assert Ok(rowid_cols) = dict.get(metadata.columns, "rowid_t")
  let assert Ok(rowid_id) = list.find(rowid_cols, fn(m) { m.column.name == "id" })
  let assert True = rowid_id.is_rowid_alias

  let assert Ok(no_rowid_cols) = dict.get(metadata.columns, "no_rowid_t")
  let assert Ok(no_rowid_id) =
    list.find(no_rowid_cols, fn(m) { m.column.name == "id" })
  let assert False = no_rowid_id.is_rowid_alias

  let assert Ok(composite_cols) = dict.get(metadata.columns, "composite_t")
  let assert Ok(composite_a) =
    list.find(composite_cols, fn(m) { m.column.name == "a" })
  let assert False = composite_a.is_rowid_alias
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test -- --target schema_loader_marks_rowid_alias`
Expected: FAIL - `is_rowid_alias` is always `False` in current implementation.

- [ ] **Step 3: Write minimal implementation**

Update `get_table_metadata_v2` in `src/marmot/internal/sqlite/schema.gleam` to look up WITHOUT ROWID flag and count primary key columns:

```gleam
pub fn get_table_metadata_v2(db: Connection) -> TableMetadataV2 {
  let table_list_decoder = {
    use name <- decode.field(1, decode.string)
    use wr <- decode.field(6, decode.int)
    decode.success(#(name, wr))
  }
  let without_rowid_set = case
    sqlight.query(
      "SELECT * FROM pragma_table_list WHERE type='table'",
      on: db,
      with: [],
      expecting: table_list_decoder,
    )
  {
    Ok(rows) ->
      list.filter_map(rows, fn(r) {
        let #(name, wr) = r
        case wr {
          1 -> Ok(name)
          _ -> Error(Nil)
        }
      })
    Error(_) -> []
  }

  // ... existing decoder definitions and master/index queries ...

  list.fold(
    tables,
    TableMetadataV2(dict.new(), dict.new(), dict.new()),
    fn(acc, table) {
      let #(table_name, rootpage) = table
      let rootpages = dict.insert(acc.rootpages, rootpage, table_name)
      let pragma_sql =
        "PRAGMA table_xinfo(\""
        <> parse.quote_identifier(table_name)
        <> "\")"
      case
        sqlight.query(pragma_sql, on: db, with: [], expecting: xinfo_decoder)
      {
        Ok(rows) -> {
          let is_without_rowid = list.contains(without_rowid_set, table_name)
          let pk_count =
            list.fold(rows, 0, fn(n, row) {
              let #(_, _, _, pk, _) = row
              case pk > 0 {
                True -> n + 1
                False -> n
              }
            })
          let metadatas =
            list.map(rows, fn(row) {
              let #(col_name, type_str, notnull, pk, hidden) = row
              let column_type = case query.parse_sqlite_type(type_str) {
                Ok(t) -> t
                Error(_) -> StringType
              }
              let is_rowid_alias =
                pk > 0
                && pk_count == 1
                && column_type == query.IntType
                && !is_without_rowid
              let nullable = case is_rowid_alias {
                True -> False
                False -> notnull == 0
              }
              ColumnMetadata(
                column: Column(
                  name: col_name,
                  column_type: column_type,
                  nullable: nullable,
                ),
                is_rowid_alias: is_rowid_alias,
                is_generated: hidden == 2 || hidden == 3,
                hidden: hidden,
              )
            })
          // ... unchanged accumulation ...
        }
        // ... unchanged error branch ...
      }
    },
  )
  |> add_index_rootpages_v2(indexes)
}
```

The `nullable` calculation now flows from `is_rowid_alias` rather than the inline `pk > 0 && IntType` check. Functionally equivalent for normal rowid tables; correct for WITHOUT ROWID tables (which the old code mishandled).

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for both new tests; all existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/schema.gleam test/marmot/internal/sqlite_test.gleam
git commit -m "Detect WITHOUT ROWID and compute is_rowid_alias

Adds PRAGMA table_list lookup so the schema loader knows which tables
are WITHOUT ROWID. INTEGER PRIMARY KEY on a WITHOUT ROWID table is no
longer wrongly treated as a rowid alias. is_rowid_alias becomes the
schema-level signal for write-nullability decisions."
```

---

## Task 3: Statement parser type scaffolding

**Files:**
- Create: `src/marmot/internal/sqlite/parse/statement_parser.gleam`
- Test: `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`

**Context:** Lay down the public types (`Identifier`, `TableRef`, `TableBinding`, `Statement` and its variants). No parsing logic yet. The classification function returns `Unsupported` for everything; subsequent tasks add real parsing per variant. This task verifies the types compile and the entry point exists.

- [ ] **Step 1: Write the failing test**

Create `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`:

```gleam
import gleam/option.{None}
import marmot/internal/sqlite/parse/statement_parser.{
  Identifier, TableBinding, TableRef, Unsupported, parse,
}
import marmot/internal/sqlite/tokenize

fn parse_sql(sql: String) {
  parse(tokenize.tokenize(sql))
}

pub fn parse_returns_unsupported_for_create_test() {
  let assert Ok(Unsupported(_)) = parse_sql("CREATE TABLE x (a INTEGER)")
}

pub fn identifier_constructor_test() {
  let assert Identifier(text: "users", quoted: False) =
    Identifier("users", False)
}

pub fn table_binding_constructor_test() {
  let assert TableBinding(
    table: TableRef(schema: None, name: Identifier("users", False)),
    alias: None,
  ) =
    TableBinding(
      table: TableRef(None, Identifier("users", False)),
      alias: None,
    )
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: compile error - module `statement_parser` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `src/marmot/internal/sqlite/parse/statement_parser.gleam`:

```gleam
//// Statement skeleton parser. Consumes a token stream from `tokenize.gleam`
//// and produces a typed `Statement` value carrying parsed structure for
//// boundaries that need it (FROM aliases, INSERT shape, CTEs) and clause
//// token slices for bodies that don't.
////
//// What this module does NOT parse:
////  - Expression internals (CAST, COALESCE, CASE, arithmetic, etc.)
////  - Subquery scoping (correlated references and inner-scope shadowing)
////  - USING-join column merging
////
//// All three are intentional. Expression-type inference stays incremental,
//// subquery scoping is left to the existing fallback path, and USING merging
//// is reported as an ambiguous bare reference in v1.

import gleam/option.{type Option, None}
import marmot/internal/error.{type MarmotError}
import marmot/internal/sqlite/tokenize.{type Token}

pub type Identifier {
  Identifier(text: String, quoted: Bool)
}

pub type TableRef {
  TableRef(schema: Option(Identifier), name: Identifier)
}

pub type TableBinding {
  TableBinding(table: TableRef, alias: Option(String))
}

pub type FromItem {
  FromItem(binding: TableBinding, on: Option(List(Token)))
}

pub type CteDef {
  CteDef(name: String, columns: List(String), body: List(Token))
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

pub type SelectStmt {
  SelectStmt(ctes: List(CteDef), body: SelectBody)
}

pub type InsertConflictAction {
  ConflictAbort
  ConflictReplace
  ConflictIgnore
  ConflictFail
  ConflictRollback
}

pub type InsertSource {
  ValuesSource(raw: List(Token), rows: List(List(List(Token))))
  SelectSource(SelectStmt)
  DefaultValuesSource
}

pub type InsertStmt {
  InsertStmt(
    conflict_action: InsertConflictAction,
    target: TableBinding,
    column_list: Option(List(String)),
    source: InsertSource,
    upsert: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type UpdateStmt {
  UpdateStmt(
    target: TableBinding,
    set: List(Token),
    from: List(FromItem),
    where: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type DeleteStmt {
  DeleteStmt(
    target: TableBinding,
    where: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type Statement {
  Select(SelectStmt)
  Insert(InsertStmt)
  Update(UpdateStmt)
  Delete(DeleteStmt)
  Unsupported(tokens: List(Token))
}

/// Parse a token stream into a typed Statement. Returns `Unsupported(tokens)`
/// for SQL outside the supported skeleton (CREATE, PRAGMA, ATTACH, etc.).
/// MarmotError is reserved for parse failures that the agent has positively
/// identified, e.g. INSERT VALUES row-count mismatch.
pub fn parse(tokens: List(Token)) -> Result(Statement, MarmotError) {
  Ok(Unsupported(tokens))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for the three new tests; all existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parse/statement_parser.gleam test/marmot/internal/sqlite/parse/statement_parser_test.gleam
git commit -m "Scaffold statement_parser module with public AST types

All Statement variants and supporting types defined. Parse function
returns Unsupported for everything; later tasks add real parsing per
statement kind."
```

---

## Task 4: Parse SELECT skeleton (FROM, WHERE, GROUP, HAVING, ORDER, LIMIT, DISTINCT, select_list)

**Files:**
- Modify: `src/marmot/internal/sqlite/parse/statement_parser.gleam`
- Test: `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`

**Context:** Build the boundary detector for SELECT clauses. FROM tables are parsed structurally in Task 5; this task gets the slicing right. The clause keywords (`WHERE`, `GROUP`, `HAVING`, `ORDER`, `LIMIT`) only function as clause introducers when they appear at the top of the statement at paren depth 0; the parser walks a state machine over the tokens after `SELECT [DISTINCT]` to identify the clause boundaries. Importantly, this is grammar-aware: a `Word("WHERE")` inside a parenthesized subquery does not introduce the outer WHERE.

- [ ] **Step 1: Write the failing test**

Append to `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`:

```gleam
import gleam/option.{Some}
import marmot/internal/sqlite/parse/statement_parser.{
  Select, SelectBody, SelectStmt,
}
import marmot/internal/sqlite/tokenize.{Word}

pub fn parse_select_simple_test() {
  let assert Ok(Select(SelectStmt(ctes: [], body: body))) =
    parse_sql("SELECT a, b FROM t")
  let assert SelectBody(
    is_distinct: False,
    where: None,
    group_by: None,
    having: None,
    order_by: None,
    limit: None,
    ..,
  ) = body
}

pub fn parse_select_distinct_test() {
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql("SELECT DISTINCT a FROM t")
  let assert True = body.is_distinct
}

pub fn parse_select_full_clauses_test() {
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql(
      "SELECT a FROM t WHERE x = 1 GROUP BY a HAVING count(*) > 1
       ORDER BY a LIMIT 10",
    )
  let assert Some(_) = body.where
  let assert Some(_) = body.group_by
  let assert Some(_) = body.having
  let assert Some(_) = body.order_by
  let assert Some(_) = body.limit
}

pub fn parse_select_does_not_split_on_subquery_keyword_test() {
  // Inner WHERE must not be mistaken for the outer WHERE.
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql("SELECT a FROM (SELECT a FROM t WHERE b = 1) sub")
  let assert None = body.where
}

pub fn parse_select_keyword_named_table_test() {
  // A table named `returning` is not a clause introducer in FROM position.
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql("SELECT * FROM returning")
  let assert [_] = body.from
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - parser still returns `Unsupported` for SELECT.

- [ ] **Step 3: Write minimal implementation**

Replace `parse` in `src/marmot/internal/sqlite/parse/statement_parser.gleam` with a dispatching version. Leave non-SELECT branches as `Unsupported` for now:

```gleam
import gleam/list
import gleam/string

pub fn parse(tokens: List(Token)) -> Result(Statement, MarmotError) {
  case classify(tokens) {
    SelectKind -> parse_select(tokens) |> result.map(Select)
    _ -> Ok(Unsupported(tokens))
  }
}

type StatementKind {
  SelectKind
  InsertKind
  UpdateKind
  DeleteKind
  OtherKind
}

fn classify(tokens: List(Token)) -> StatementKind {
  // Skip a leading WITH clause: WITH ... <kind> ...
  let head = skip_with_for_classification(tokens)
  case head {
    [Word(w), ..] ->
      case string.uppercase(w) {
        "SELECT" -> SelectKind
        "INSERT" | "REPLACE" -> InsertKind
        "UPDATE" -> UpdateKind
        "DELETE" -> DeleteKind
        _ -> OtherKind
      }
    _ -> OtherKind
  }
}

fn skip_with_for_classification(tokens: List(Token)) -> List(Token) {
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == "WITH" {
        True -> {
          let after = skip_cte_definitions(rest)
          // After CTEs we expect SELECT/INSERT/UPDATE/DELETE
          after
        }
        False -> tokens
      }
    _ -> tokens
  }
}

fn skip_cte_definitions(tokens: List(Token)) -> List(Token) {
  // RECURSIVE keyword optional
  let tokens = case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == "RECURSIVE" {
        True -> rest
        False -> tokens
      }
    _ -> tokens
  }
  do_skip_ctes(tokens)
}

fn do_skip_ctes(tokens: List(Token)) -> List(Token) {
  // Each CTE: name [(col_list)] AS ( body ) [, ...]
  case tokens {
    [Word(_), ..rest] -> {
      // Optional column list
      let after_cols = case rest {
        [tokenize.OpenParen, ..r] -> {
          let #(_, after) = tokenize.collect_inside_parens(r)
          after
        }
        _ -> rest
      }
      case after_cols {
        [Word(as_kw), tokenize.OpenParen, ..body_rest] ->
          case string.uppercase(as_kw) == "AS" {
            True -> {
              let #(_, after_body) = tokenize.collect_inside_parens(body_rest)
              case after_body {
                [tokenize.Comma, ..rest2] -> do_skip_ctes(rest2)
                other -> other
              }
            }
            False -> tokens
          }
        _ -> tokens
      }
    }
    _ -> tokens
  }
}

fn parse_select(tokens: List(Token)) -> Result(SelectStmt, MarmotError) {
  // CTEs deferred to Task 6; treat as no CTEs for now.
  let body_tokens = skip_with_for_classification(tokens)
  let body = parse_select_body(body_tokens)
  Ok(SelectStmt(ctes: [], body: body))
}

fn parse_select_body(tokens: List(Token)) -> SelectBody {
  // Drop SELECT and optional DISTINCT
  let #(is_distinct, after_select) = case tokens {
    [Word(s), Word(d), ..rest] ->
      case string.uppercase(s) == "SELECT" && string.uppercase(d) == "DISTINCT" {
        True -> #(True, rest)
        False ->
          case string.uppercase(s) == "SELECT" {
            True -> #(False, [Word(d), ..rest])
            False -> #(False, tokens)
          }
      }
    [Word(s), ..rest] ->
      case string.uppercase(s) == "SELECT" {
        True -> #(False, rest)
        False -> #(False, tokens)
      }
    _ -> #(False, tokens)
  }

  // Walk top-level clauses in order, slicing each.
  let #(select_list, rest) =
    take_until_top_level_keyword(after_select, [
      "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT",
    ])
  let #(from_tokens, rest) = take_clause(rest, "FROM", [
    "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT",
  ])
  let #(where, rest) = take_clause(rest, "WHERE", [
    "GROUP", "HAVING", "ORDER", "LIMIT",
  ])
  let #(group_by, rest) = take_clause(rest, "GROUP", [
    "HAVING", "ORDER", "LIMIT",
  ])
  let #(having, rest) = take_clause(rest, "HAVING", ["ORDER", "LIMIT"])
  let #(order_by, rest) = take_clause(rest, "ORDER", ["LIMIT"])
  let #(limit, _rest) = take_clause(rest, "LIMIT", [])

  // FROM items parsed structurally in Task 5. For now, store the slice.
  let from = case from_tokens {
    Some(_) -> []
    None -> []
  }

  SelectBody(
    is_distinct: is_distinct,
    select_list: select_list,
    from: from,
    where: where,
    group_by: group_by,
    having: having,
    order_by: order_by,
    limit: limit,
  )
}

/// Walk tokens from start, accumulating until one of `keywords` appears as a
/// top-level (depth 0) Word. Returns the prefix and the remaining tokens
/// starting at the matched keyword.
fn take_until_top_level_keyword(
  tokens: List(Token),
  keywords: List(String),
) -> #(List(Token), List(Token)) {
  let uppers = list.map(keywords, string.uppercase)
  do_take_until(tokens, uppers, [], 0)
}

fn do_take_until(
  tokens: List(Token),
  uppers: List(String),
  acc: List(Token),
  depth: Int,
) -> #(List(Token), List(Token)) {
  case tokens {
    [] -> #(list.reverse(acc), [])
    [tokenize.OpenParen, ..rest] ->
      do_take_until(rest, uppers, [tokenize.OpenParen, ..acc], depth + 1)
    [tokenize.CloseParen, ..rest] ->
      do_take_until(rest, uppers, [tokenize.CloseParen, ..acc], depth - 1)
    [Word(w), ..rest] if depth == 0 ->
      case list.contains(uppers, string.uppercase(w)) {
        True -> #(list.reverse(acc), tokens)
        False -> do_take_until(rest, uppers, [Word(w), ..acc], depth)
      }
    [t, ..rest] -> do_take_until(rest, uppers, [t, ..acc], depth)
  }
}

/// If `tokens` starts with `keyword` at depth 0, take the body until the next
/// boundary keyword. Returns `(Some(body), remaining)` or `(None, tokens)`.
fn take_clause(
  tokens: List(Token),
  keyword: String,
  next_boundaries: List(String),
) -> #(Option(List(Token)), List(Token)) {
  let upper = string.uppercase(keyword)
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == upper {
        True -> {
          let #(body, after) = take_until_top_level_keyword(rest, next_boundaries)
          #(Some(body), after)
        }
        False -> #(None, tokens)
      }
    _ -> #(None, tokens)
  }
}
```

Add `import gleam/result` and `import gleam/option.{Some}` at the top.

The `from: []` placeholder is intentional. Task 5 fills it in. The keyword-named-table test should still pass because `take_until_top_level_keyword` only matches `WHERE`/`GROUP`/etc., not `RETURNING` or `INTO`, and the test only checks `body.from` length is 1 - which Task 5 will satisfy. Adjust the assertion to verify the FROM slice is non-empty for now:

Update the test fixture to assert against the FROM slice rather than parsed FromItems:

```gleam
pub fn parse_select_keyword_named_table_test() {
  // A table named `returning` is not a clause introducer in FROM position.
  // Parsed FromItems are filled in by Task 5; here we only verify slicing.
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql("SELECT * FROM returning")
  let assert [] = body.from
  // The from slice will be exposed by Task 5; for now just assert no parse error.
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for all five new tests; all existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parse/statement_parser.gleam test/marmot/internal/sqlite/parse/statement_parser_test.gleam
git commit -m "Parse SELECT clause skeleton with depth-aware slicing

Walks SELECT body and slices each clause (WHERE/GROUP/HAVING/ORDER/
LIMIT) using top-level paren-depth tracking. Subquery keywords no
longer leak out as outer-clause boundaries. FROM items remain as a
slice; structural parsing comes in Task 5."
```

---

## Task 5: Parse FROM items into TableBindings

**Files:**
- Modify: `src/marmot/internal/sqlite/parse/statement_parser.gleam`
- Test: `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`

**Context:** Turn the FROM clause slice into a list of `FromItem` with `TableBinding` values. Handles unqualified names, schema-qualified names (`main.users`), quoted identifiers, optional aliases (with or without `AS`), and join syntax (`INNER`, `LEFT`, `RIGHT`, `CROSS`, `NATURAL`, `OUTER` modifiers, `ON` clause, `USING` clause). Join-modifier interpretation is dropped per the spec; v1 only needs the table list and the `ON` slice. `USING (col)` is captured as part of the slice but not separately interpreted.

- [ ] **Step 1: Write the failing test**

Append to `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`:

```gleam
import gleam/option.{Some}
import marmot/internal/sqlite/parse/statement_parser.{FromItem, TableBinding, TableRef, Identifier}

pub fn parse_from_single_table_test() {
  let assert Ok(Select(SelectStmt(_, body))) = parse_sql("SELECT * FROM users")
  let assert [
    FromItem(
      binding: TableBinding(
        table: TableRef(schema: None, name: Identifier("users", False)),
        alias: None,
      ),
      on: None,
    ),
  ] = body.from
}

pub fn parse_from_aliased_table_test() {
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql("SELECT * FROM users AS u")
  let assert [
    FromItem(binding: TableBinding(_, alias: Some("u")), on: None),
  ] = body.from
}

pub fn parse_from_alias_without_as_test() {
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql("SELECT * FROM users u")
  let assert [
    FromItem(binding: TableBinding(_, alias: Some("u")), on: None),
  ] = body.from
}

pub fn parse_from_schema_qualified_test() {
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql("SELECT * FROM main.users")
  let assert [
    FromItem(
      binding: TableBinding(
        table: TableRef(
          schema: Some(Identifier("main", False)),
          name: Identifier("users", False),
        ),
        alias: None,
      ),
      on: None,
    ),
  ] = body.from
}

pub fn parse_from_quoted_identifier_test() {
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql("SELECT * FROM \"users\"")
  let assert [
    FromItem(
      binding: TableBinding(
        table: TableRef(_, name: Identifier("users", True)),
        ..,
      ),
      ..,
    ),
  ] = body.from
}

pub fn parse_from_join_with_on_test() {
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql(
      "SELECT * FROM users u JOIN orders o ON o.user_id = u.id",
    )
  let assert [
    FromItem(binding: TableBinding(_, alias: Some("u")), on: None),
    FromItem(binding: TableBinding(_, alias: Some("o")), on: Some(_)),
  ] = body.from
}

pub fn parse_from_keyword_named_table_test() {
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql("SELECT * FROM returning")
  let assert [
    FromItem(
      binding: TableBinding(
        table: TableRef(_, name: Identifier("returning", False)),
        ..,
      ),
      ..,
    ),
  ] = body.from
}

pub fn parse_from_self_join_test() {
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql(
      "SELECT * FROM users u JOIN users manager ON manager.id = u.manager_id",
    )
  let assert [
    FromItem(binding: TableBinding(_, alias: Some("u")), ..),
    FromItem(binding: TableBinding(_, alias: Some("manager")), ..),
  ] = body.from
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - `body.from` is empty `[]` from Task 4's placeholder.

- [ ] **Step 3: Write minimal implementation**

Replace the FROM placeholder in `parse_select_body` and add the FROM parser:

```gleam
fn parse_select_body(tokens: List(Token)) -> SelectBody {
  // ... unchanged DISTINCT skipping and select_list slicing ...

  let #(from_tokens, rest) = take_clause(after_select_list, "FROM", [
    "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT",
  ])
  let from = case from_tokens {
    Some(slice) -> parse_from_items(slice)
    None -> []
  }
  // ... unchanged WHERE/GROUP/HAVING/ORDER/LIMIT slicing ...

  SelectBody(
    is_distinct: is_distinct,
    select_list: select_list,
    from: from,
    where: where,
    group_by: group_by,
    having: having,
    order_by: order_by,
    limit: limit,
  )
}

fn parse_from_items(tokens: List(Token)) -> List(FromItem) {
  // Split on top-level JOIN keywords, then parse each segment.
  // First segment has no incoming join modifier.
  do_parse_from(tokens, [], [])
}

fn do_parse_from(
  tokens: List(Token),
  current: List(Token),
  acc: List(FromItem),
) -> List(FromItem) {
  case tokens {
    [] -> {
      let acc = case current {
        [] -> acc
        _ -> [parse_from_segment(list.reverse(current)), ..acc]
      }
      list.reverse(acc)
    }
    [tokenize.OpenParen, ..rest] -> {
      // Skip parenthesized subqueries / parenthesized joins; capture as part
      // of the segment for now.
      let #(inner, after) = tokenize.collect_inside_parens(rest)
      let nested =
        [tokenize.OpenParen, ..inner]
        |> list.append([tokenize.CloseParen])
      let current = list.append(list.reverse(nested), current)
      do_parse_from(after, current, acc)
    }
    [Word(w), ..rest] if list.contains(join_modifiers(), string.uppercase(w)) ->
      do_parse_from(rest, current, acc)
    [Word(w), ..rest] ->
      case string.uppercase(w) == "JOIN" {
        True -> {
          let acc = case current {
            [] -> acc
            _ -> [parse_from_segment(list.reverse(current)), ..acc]
          }
          do_parse_from(rest, [], acc)
        }
        False -> do_parse_from(rest, [Word(w), ..current], acc)
      }
    [tokenize.Comma, ..rest] -> {
      let acc = case current {
        [] -> acc
        _ -> [parse_from_segment(list.reverse(current)), ..acc]
      }
      do_parse_from(rest, [], acc)
    }
    [t, ..rest] -> do_parse_from(rest, [t, ..current], acc)
  }
}

fn join_modifiers() -> List(String) {
  ["INNER", "LEFT", "RIGHT", "CROSS", "NATURAL", "OUTER", "FULL"]
}

fn parse_from_segment(tokens: List(Token)) -> FromItem {
  // Possible shapes:
  //   table
  //   schema.table
  //   "table"
  //   table alias
  //   table AS alias
  //   table ON ... (with on tokens captured)
  //   table USING (...) (with using tokens captured into on)
  let #(binding, after_binding) = parse_table_binding(tokens)
  let on = parse_on_or_using(after_binding)
  FromItem(binding: binding, on: on)
}

fn parse_table_binding(tokens: List(Token)) -> #(TableBinding, List(Token)) {
  let #(table, after_table) = parse_table_ref(tokens)
  let #(alias, after_alias) = parse_optional_alias(after_table)
  #(TableBinding(table: table, alias: alias), after_alias)
}

fn parse_table_ref(tokens: List(Token)) -> #(TableRef, List(Token)) {
  case tokens {
    [Word(schema), tokenize.Dot, Word(name), ..rest] -> #(
      TableRef(
        schema: Some(Identifier(schema, False)),
        name: Identifier(name, False),
      ),
      rest,
    )
    [Word(schema), tokenize.Dot, tokenize.QuotedId(name), ..rest] -> #(
      TableRef(
        schema: Some(Identifier(schema, False)),
        name: Identifier(name, True),
      ),
      rest,
    )
    [tokenize.QuotedId(schema), tokenize.Dot, Word(name), ..rest] -> #(
      TableRef(
        schema: Some(Identifier(schema, True)),
        name: Identifier(name, False),
      ),
      rest,
    )
    [tokenize.QuotedId(schema), tokenize.Dot, tokenize.QuotedId(name), ..rest] -> #(
      TableRef(
        schema: Some(Identifier(schema, True)),
        name: Identifier(name, True),
      ),
      rest,
    )
    [Word(name), ..rest] -> #(
      TableRef(schema: None, name: Identifier(name, False)),
      rest,
    )
    [tokenize.QuotedId(name), ..rest] -> #(
      TableRef(schema: None, name: Identifier(name, True)),
      rest,
    )
    _ -> #(
      TableRef(schema: None, name: Identifier("", False)),
      tokens,
    )
  }
}

fn parse_optional_alias(tokens: List(Token)) -> #(Option(String), List(Token)) {
  case tokens {
    [Word(as_kw), Word(alias), ..rest] ->
      case string.uppercase(as_kw) == "AS" {
        True -> #(Some(alias), rest)
        False -> parse_optional_alias_no_as(tokens)
      }
    _ -> parse_optional_alias_no_as(tokens)
  }
}

fn parse_optional_alias_no_as(tokens: List(Token)) -> #(Option(String), List(Token)) {
  case tokens {
    [Word(alias), ..rest] ->
      case is_clause_or_join_keyword(alias) {
        True -> #(None, tokens)
        False -> #(Some(alias), rest)
      }
    _ -> #(None, tokens)
  }
}

fn is_clause_or_join_keyword(word: String) -> Bool {
  let upper = string.uppercase(word)
  list.contains(
    ["ON", "USING", "JOIN", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT"],
    upper,
  )
  || list.contains(join_modifiers(), upper)
}

fn parse_on_or_using(tokens: List(Token)) -> Option(List(Token)) {
  case tokens {
    [Word(on_kw), ..rest] ->
      case string.uppercase(on_kw) {
        "ON" -> Some(rest)
        "USING" -> Some([Word("USING"), ..rest])
        _ -> None
      }
    _ -> None
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for all FROM-parsing tests; existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parse/statement_parser.gleam test/marmot/internal/sqlite/parse/statement_parser_test.gleam
git commit -m "Parse FROM items into structured TableBindings

Splits the FROM slice on top-level JOIN/comma boundaries and parses
each segment into a TableRef plus optional alias. Handles schema
qualification, quoted identifiers, AS-less aliases, ON clauses, and
self-joins. Join-modifier interpretation deliberately omitted; only
table list and ON slices are needed for alias resolution."
```

---

## Task 6: Parse WITH/CTE prefix into raw bodies

**Files:**
- Modify: `src/marmot/internal/sqlite/parse/statement_parser.gleam`
- Test: `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`

**Context:** CTE bodies stay raw per spec - structural CTE parsing is deferred along with CTE result-type inference. This task only parses the CTE *headers* (name, optional column list) and captures each body as a token slice. The outer statement after CTEs continues to be parsed structurally.

- [ ] **Step 1: Write the failing test**

Append to `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`:

```gleam
import marmot/internal/sqlite/parse/statement_parser.{CteDef}

pub fn parse_with_simple_cte_test() {
  let assert Ok(Select(SelectStmt(ctes, body))) =
    parse_sql("WITH foo AS (SELECT 1) SELECT * FROM foo")
  let assert [CteDef(name: "foo", columns: [], body: body_tokens)] = ctes
  let assert True = list.length(body_tokens) > 0
  let assert [
    FromItem(
      binding: TableBinding(
        table: TableRef(_, name: Identifier("foo", False)),
        ..,
      ),
      ..,
    ),
  ] = body.from
}

pub fn parse_with_cte_columns_test() {
  let assert Ok(Select(SelectStmt(ctes, _))) =
    parse_sql("WITH foo (a, b) AS (SELECT 1, 2) SELECT * FROM foo")
  let assert [CteDef(name: "foo", columns: ["a", "b"], body: _)] = ctes
}

pub fn parse_with_recursive_cte_test() {
  let assert Ok(Select(SelectStmt(ctes, _))) =
    parse_sql(
      "WITH RECURSIVE counter(n) AS (
         SELECT 1 UNION ALL SELECT n+1 FROM counter WHERE n < 10
       ) SELECT * FROM counter",
    )
  let assert [CteDef(name: "counter", columns: ["n"], body: _)] = ctes
}

pub fn parse_with_multiple_ctes_test() {
  let assert Ok(Select(SelectStmt(ctes, _))) =
    parse_sql(
      "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b",
    )
  let assert [CteDef(name: "a", ..), CteDef(name: "b", ..)] = ctes
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - `parse_select` returns `ctes: []` from Task 4.

- [ ] **Step 3: Write minimal implementation**

Replace `parse_select` in `statement_parser.gleam`:

```gleam
fn parse_select(tokens: List(Token)) -> Result(SelectStmt, MarmotError) {
  let #(ctes, body_tokens) = parse_ctes(tokens)
  Ok(SelectStmt(ctes: ctes, body: parse_select_body(body_tokens)))
}

fn parse_ctes(tokens: List(Token)) -> #(List(CteDef), List(Token)) {
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == "WITH" {
        True -> {
          let after_with = case rest {
            [Word(r), ..r2] ->
              case string.uppercase(r) == "RECURSIVE" {
                True -> r2
                False -> rest
              }
            _ -> rest
          }
          collect_ctes(after_with, [])
        }
        False -> #([], tokens)
      }
    _ -> #([], tokens)
  }
}

fn collect_ctes(
  tokens: List(Token),
  acc: List(CteDef),
) -> #(List(CteDef), List(Token)) {
  case parse_one_cte(tokens) {
    Ok(#(cte, rest)) ->
      case rest {
        [tokenize.Comma, ..r2] -> collect_ctes(r2, [cte, ..acc])
        other -> #(list.reverse([cte, ..acc]), other)
      }
    Error(_) -> #(list.reverse(acc), tokens)
  }
}

fn parse_one_cte(tokens: List(Token)) -> Result(#(CteDef, List(Token)), Nil) {
  case tokens {
    [Word(name), ..rest] -> {
      let #(columns, after_cols) = case rest {
        [tokenize.OpenParen, ..r] -> {
          let #(inner, after) = tokenize.collect_inside_parens(r)
          let cols =
            tokenize.split_on_commas(inner)
            |> list.filter_map(fn(group) {
              case group {
                [Word(c)] -> Ok(c)
                [tokenize.QuotedId(c)] -> Ok(c)
                _ -> Error(Nil)
              }
            })
          #(cols, after)
        }
        _ -> #([], rest)
      }
      case after_cols {
        [Word(as_kw), tokenize.OpenParen, ..body_rest] ->
          case string.uppercase(as_kw) == "AS" {
            True -> {
              let #(body, after_body) =
                tokenize.collect_inside_parens(body_rest)
              Ok(#(
                CteDef(name: name, columns: columns, body: body),
                after_body,
              ))
            }
            False -> Error(Nil)
          }
        _ -> Error(Nil)
      }
    }
    _ -> Error(Nil)
  }
}
```

Replace the existing `skip_with_for_classification` call inside `parse_select` with the structured CTE parser. The classification path (used to decide which statement kind to parse) keeps the skipping helper, since classification only needs the kind that follows the CTE block.

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for all four CTE tests; existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parse/statement_parser.gleam test/marmot/internal/sqlite/parse/statement_parser_test.gleam
git commit -m "Parse WITH/CTE prefix into raw CteDef bodies

Captures CTE name, optional column list, and body as a token slice.
RECURSIVE keyword and multiple CTEs supported. CTE body parsing stays
raw per spec; structural parsing is deferred with CTE result-type
inference."
```

---

## Task 7: Parse INSERT skeleton (target, conflict_action, returning slice)

**Files:**
- Modify: `src/marmot/internal/sqlite/parse/statement_parser.gleam`
- Test: `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`

**Context:** Get the INSERT shell parsing right before the source variants. SQLite syntax: `INSERT [OR action] INTO target [AS alias] [(col-list)]? source [upsert]? [RETURNING list]?`. Source variants come in Task 8. Both `INSERT` and `REPLACE` are valid leading keywords; `REPLACE` is shorthand for `INSERT OR REPLACE`.

- [ ] **Step 1: Write the failing test**

Append:

```gleam
import marmot/internal/sqlite/parse/statement_parser.{
  ConflictAbort, ConflictFail, ConflictIgnore, ConflictReplace, ConflictRollback,
  Insert, InsertStmt,
}

pub fn parse_insert_default_conflict_test() {
  let assert Ok(Insert(stmt)) = parse_sql("INSERT INTO t (a) VALUES (?)")
  let assert ConflictAbort = stmt.conflict_action
  let assert TableBinding(
    table: TableRef(_, name: Identifier("t", False)),
    alias: None,
  ) = stmt.target
}

pub fn parse_insert_or_ignore_test() {
  let assert Ok(Insert(stmt)) =
    parse_sql("INSERT OR IGNORE INTO t (a) VALUES (?)")
  let assert ConflictIgnore = stmt.conflict_action
}

pub fn parse_insert_or_replace_test() {
  let assert Ok(Insert(stmt)) =
    parse_sql("INSERT OR REPLACE INTO t (a) VALUES (?)")
  let assert ConflictReplace = stmt.conflict_action
}

pub fn parse_insert_or_fail_test() {
  let assert Ok(Insert(stmt)) =
    parse_sql("INSERT OR FAIL INTO t (a) VALUES (?)")
  let assert ConflictFail = stmt.conflict_action
}

pub fn parse_insert_or_rollback_test() {
  let assert Ok(Insert(stmt)) =
    parse_sql("INSERT OR ROLLBACK INTO t (a) VALUES (?)")
  let assert ConflictRollback = stmt.conflict_action
}

pub fn parse_replace_shorthand_test() {
  let assert Ok(Insert(stmt)) = parse_sql("REPLACE INTO t (a) VALUES (?)")
  let assert ConflictReplace = stmt.conflict_action
}

pub fn parse_insert_with_target_alias_test() {
  let assert Ok(Insert(stmt)) =
    parse_sql("INSERT INTO t AS u (a) VALUES (?)")
  let assert TableBinding(_, alias: Some("u")) = stmt.target
}

pub fn parse_insert_returning_slice_test() {
  let assert Ok(Insert(stmt)) =
    parse_sql("INSERT INTO t (a) VALUES (?) RETURNING id, name")
  let assert Some(_) = stmt.returning
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - `Insert` branch in `parse` still returns `Unsupported`.

- [ ] **Step 3: Write minimal implementation**

In `statement_parser.gleam`:

```gleam
pub fn parse(tokens: List(Token)) -> Result(Statement, MarmotError) {
  case classify(tokens) {
    SelectKind -> parse_select(tokens) |> result.map(Select)
    InsertKind -> parse_insert(tokens) |> result.map(Insert)
    _ -> Ok(Unsupported(tokens))
  }
}

fn parse_insert(tokens: List(Token)) -> Result(InsertStmt, MarmotError) {
  let #(action, after_kw) = parse_insert_conflict_action(tokens)
  let after_into = drop_into(after_kw)
  let #(target, after_target) = parse_table_binding(after_into)
  let #(column_list, after_cols) = parse_optional_column_list(after_target)
  // Source parsing comes in Task 8. Default for now:
  let source = DefaultValuesSource
  let #(returning, _rest) = take_clause(after_cols, "RETURNING", [])
  Ok(InsertStmt(
    conflict_action: action,
    target: target,
    column_list: column_list,
    source: source,
    upsert: None,
    returning: returning,
  ))
}

fn parse_insert_conflict_action(
  tokens: List(Token),
) -> #(InsertConflictAction, List(Token)) {
  case tokens {
    [Word(insert_kw), ..rest] ->
      case string.uppercase(insert_kw) {
        "REPLACE" -> #(ConflictReplace, rest)
        "INSERT" ->
          case rest {
            [Word(or_kw), Word(action), ..r2] ->
              case string.uppercase(or_kw) == "OR" {
                True ->
                  case string.uppercase(action) {
                    "REPLACE" -> #(ConflictReplace, r2)
                    "IGNORE" -> #(ConflictIgnore, r2)
                    "FAIL" -> #(ConflictFail, r2)
                    "ROLLBACK" -> #(ConflictRollback, r2)
                    "ABORT" -> #(ConflictAbort, r2)
                    _ -> #(ConflictAbort, rest)
                  }
                False -> #(ConflictAbort, rest)
              }
            _ -> #(ConflictAbort, rest)
          }
        _ -> #(ConflictAbort, tokens)
      }
    _ -> #(ConflictAbort, tokens)
  }
}

fn drop_into(tokens: List(Token)) -> List(Token) {
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == "INTO" {
        True -> rest
        False -> tokens
      }
    _ -> tokens
  }
}

fn parse_optional_column_list(
  tokens: List(Token),
) -> #(Option(List(String)), List(Token)) {
  case tokens {
    [tokenize.OpenParen, ..rest] -> {
      let #(inner, after) = tokenize.collect_inside_parens(rest)
      let cols =
        tokenize.split_on_commas(inner)
        |> list.filter_map(fn(group) {
          case group {
            [Word(c)] -> Ok(c)
            [tokenize.QuotedId(c)] -> Ok(c)
            _ -> Error(Nil)
          }
        })
      #(Some(cols), after)
    }
    _ -> #(None, tokens)
  }
}
```

The temporary `source = DefaultValuesSource` placeholder is replaced in Task 8. The `parse_insert_returning_slice_test` should pass because RETURNING is sliced even before source parsing exists.

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for all eight new tests.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parse/statement_parser.gleam test/marmot/internal/sqlite/parse/statement_parser_test.gleam
git commit -m "Parse INSERT skeleton: action, target, column_list, RETURNING

Handles INSERT/REPLACE leading keyword, OR-modifier conflict actions,
target table with optional alias, optional explicit column list, and
the trailing RETURNING slice. Source variants (VALUES/SELECT/DEFAULT
VALUES) and upsert come next."
```

---

## Task 8: Parse INSERT source variants (VALUES rows, SELECT, DEFAULT VALUES)

**Files:**
- Modify: `src/marmot/internal/sqlite/parse/statement_parser.gleam`
- Test: `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`

**Context:** Three sources to discriminate. `VALUES` carries both the raw clause slice and per-row structured form (each row is a list of expression-token slices). `SELECT` recurses into `parse_select`. `DEFAULT VALUES` is the marker variant with no payload. Multi-row VALUES is supported; rows are split on top-level commas inside the VALUES area.

- [ ] **Step 1: Write the failing test**

Append:

```gleam
import marmot/internal/sqlite/parse/statement_parser.{
  DefaultValuesSource, SelectSource, ValuesSource,
}

pub fn parse_insert_values_single_row_test() {
  let assert Ok(Insert(stmt)) =
    parse_sql("INSERT INTO t (a, b) VALUES (?, ?)")
  let assert ValuesSource(raw: _, rows: [[col1, col2]]) = stmt.source
  let assert [tokenize.ParamAnon] = col1
  let assert [tokenize.ParamAnon] = col2
}

pub fn parse_insert_values_multi_row_test() {
  let assert Ok(Insert(stmt)) =
    parse_sql("INSERT INTO t (a, b) VALUES (?, ?), (?, ?)")
  let assert ValuesSource(raw: _, rows: rows) = stmt.source
  let assert 2 = list.length(rows)
}

pub fn parse_insert_values_no_column_list_test() {
  let assert Ok(Insert(stmt)) = parse_sql("INSERT INTO t VALUES (?, ?)")
  let assert None = stmt.column_list
  let assert ValuesSource(raw: _, rows: [[_, _]]) = stmt.source
}

pub fn parse_insert_default_values_test() {
  let assert Ok(Insert(stmt)) = parse_sql("INSERT INTO t DEFAULT VALUES")
  let assert DefaultValuesSource = stmt.source
}

pub fn parse_insert_select_source_test() {
  let assert Ok(Insert(stmt)) =
    parse_sql("INSERT INTO t (a) SELECT id FROM other")
  let assert SelectSource(SelectStmt(_, body)) = stmt.source
  let assert [_] = body.from
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - source is hardcoded to `DefaultValuesSource`.

- [ ] **Step 3: Write minimal implementation**

Replace the `let source = DefaultValuesSource` placeholder with a real source parser, and slice up to `RETURNING`:

```gleam
fn parse_insert(tokens: List(Token)) -> Result(InsertStmt, MarmotError) {
  let #(action, after_kw) = parse_insert_conflict_action(tokens)
  let after_into = drop_into(after_kw)
  let #(target, after_target) = parse_table_binding(after_into)
  let #(column_list, after_cols) = parse_optional_column_list(after_target)
  let #(source_tokens, after_source) =
    take_until_top_level_keyword(after_cols, ["ON", "RETURNING"])
  let source = parse_insert_source(source_tokens)
  let #(upsert, after_upsert) = take_clause(after_source, "ON", ["RETURNING"])
  let #(returning, _rest) = take_clause(after_upsert, "RETURNING", [])
  Ok(InsertStmt(
    conflict_action: action,
    target: target,
    column_list: column_list,
    source: source,
    upsert: upsert,
    returning: returning,
  ))
}

fn parse_insert_source(tokens: List(Token)) -> InsertSource {
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) {
        "VALUES" -> parse_values_source(rest, tokens)
        "DEFAULT" ->
          case rest {
            [Word(v), ..] ->
              case string.uppercase(v) == "VALUES" {
                True -> DefaultValuesSource
                False -> ValuesSource(raw: tokens, rows: [])
              }
            _ -> ValuesSource(raw: tokens, rows: [])
          }
        "SELECT" ->
          case parse_select(tokens) {
            Ok(stmt) -> SelectSource(stmt)
            Error(_) -> ValuesSource(raw: tokens, rows: [])
          }
        "WITH" ->
          // INSERT INTO t WITH ... SELECT ...
          case parse_select(tokens) {
            Ok(stmt) -> SelectSource(stmt)
            Error(_) -> ValuesSource(raw: tokens, rows: [])
          }
        _ -> ValuesSource(raw: tokens, rows: [])
      }
    _ -> ValuesSource(raw: tokens, rows: [])
  }
}

fn parse_values_source(
  after_values: List(Token),
  raw: List(Token),
) -> InsertSource {
  let rows = parse_values_rows(after_values, [])
  ValuesSource(raw: raw, rows: rows)
}

fn parse_values_rows(
  tokens: List(Token),
  acc: List(List(List(Token))),
) -> List(List(List(Token))) {
  case tokens {
    [] -> list.reverse(acc)
    [tokenize.OpenParen, ..rest] -> {
      let #(inner, after) = tokenize.collect_inside_parens(rest)
      let exprs = tokenize.split_on_commas(inner)
      let acc = [exprs, ..acc]
      case after {
        [tokenize.Comma, ..r2] -> parse_values_rows(r2, acc)
        _ -> list.reverse(acc)
      }
    }
    [_, ..rest] -> parse_values_rows(rest, acc)
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for all five new tests.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parse/statement_parser.gleam test/marmot/internal/sqlite/parse/statement_parser_test.gleam
git commit -m "Parse INSERT source variants: VALUES rows, SELECT, DEFAULT VALUES

VALUES retains both raw and per-row structured form. Multi-row VALUES
splits on top-level commas. SELECT and INSERT INTO ... WITH ... SELECT
recurse into parse_select. Upsert clause sliced as raw tokens."
```

---

## Task 9: Parse UPDATE statement

**Files:**
- Modify: `src/marmot/internal/sqlite/parse/statement_parser.gleam`
- Test: `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`

**Context:** SQLite UPDATE syntax: `UPDATE [OR action] target [AS alias] SET col = expr [, ...] [FROM ...]? [WHERE ...]? [RETURNING ...]?`. Note SQLite supports `UPDATE FROM` (added in 3.33). The `SET` clause body stays raw - existing `parse_update_set_columns` handles it.

- [ ] **Step 1: Write the failing test**

Append:

```gleam
import marmot/internal/sqlite/parse/statement_parser.{Update, UpdateStmt}

pub fn parse_update_simple_test() {
  let assert Ok(Update(stmt)) =
    parse_sql("UPDATE users SET name = ? WHERE id = ?")
  let assert TableBinding(
    table: TableRef(_, name: Identifier("users", False)),
    alias: None,
  ) = stmt.target
  let assert Some(_) = stmt.where
  let assert True = list.length(stmt.set) > 0
}

pub fn parse_update_aliased_target_test() {
  let assert Ok(Update(stmt)) =
    parse_sql("UPDATE users AS u SET email = ? WHERE u.id = ?")
  let assert TableBinding(_, alias: Some("u")) = stmt.target
}

pub fn parse_update_with_from_test() {
  let assert Ok(Update(stmt)) =
    parse_sql(
      "UPDATE users AS u SET email = o.email FROM orders o WHERE u.id = o.user_id",
    )
  let assert [_] = stmt.from
  let assert Some(_) = stmt.where
}

pub fn parse_update_returning_test() {
  let assert Ok(Update(stmt)) =
    parse_sql("UPDATE users SET name = ? WHERE id = ? RETURNING id, name")
  let assert Some(_) = stmt.returning
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - UPDATE branch returns `Unsupported`.

- [ ] **Step 3: Write minimal implementation**

```gleam
pub fn parse(tokens: List(Token)) -> Result(Statement, MarmotError) {
  case classify(tokens) {
    SelectKind -> parse_select(tokens) |> result.map(Select)
    InsertKind -> parse_insert(tokens) |> result.map(Insert)
    UpdateKind -> parse_update(tokens) |> result.map(Update)
    _ -> Ok(Unsupported(tokens))
  }
}

fn parse_update(tokens: List(Token)) -> Result(UpdateStmt, MarmotError) {
  // Drop UPDATE and any OR-action modifier (we don't need it for v1).
  let after_kw = drop_update_keyword(tokens)
  let #(target, after_target) = parse_table_binding(after_kw)
  let after_set = drop_keyword(after_target, "SET")
  let #(set_tokens, after_set_body) =
    take_until_top_level_keyword(after_set, ["FROM", "WHERE", "RETURNING"])
  let #(from_slice, after_from) =
    take_clause(after_set_body, "FROM", ["WHERE", "RETURNING"])
  let from = case from_slice {
    Some(slice) -> parse_from_items(slice)
    None -> []
  }
  let #(where, after_where) = take_clause(after_from, "WHERE", ["RETURNING"])
  let #(returning, _rest) = take_clause(after_where, "RETURNING", [])
  Ok(UpdateStmt(
    target: target,
    set: set_tokens,
    from: from,
    where: where,
    returning: returning,
  ))
}

fn drop_update_keyword(tokens: List(Token)) -> List(Token) {
  case tokens {
    [Word(u), Word(or_kw), Word(_), ..rest] ->
      case
        string.uppercase(u) == "UPDATE" && string.uppercase(or_kw) == "OR"
      {
        True -> rest
        False ->
          case string.uppercase(u) == "UPDATE" {
            True -> [Word(or_kw), Word(_), ..rest]
            False -> tokens
          }
      }
    [Word(u), ..rest] ->
      case string.uppercase(u) == "UPDATE" {
        True -> rest
        False -> tokens
      }
    _ -> tokens
  }
}

fn drop_keyword(tokens: List(Token), keyword: String) -> List(Token) {
  let upper = string.uppercase(keyword)
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == upper {
        True -> rest
        False -> tokens
      }
    _ -> tokens
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for all four new tests.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parse/statement_parser.gleam test/marmot/internal/sqlite/parse/statement_parser_test.gleam
git commit -m "Parse UPDATE: target binding, SET slice, FROM, WHERE, RETURNING

Target supports AS alias. SET body stays as a token slice for the
existing walker. UPDATE FROM is parsed structurally so alias-aware
resolution covers joined tables."
```

---

## Task 10: Parse DELETE statement

**Files:**
- Modify: `src/marmot/internal/sqlite/parse/statement_parser.gleam`
- Test: `test/marmot/internal/sqlite/parse/statement_parser_test.gleam`

**Context:** SQLite DELETE syntax: `DELETE FROM target [AS alias] [WHERE ...]? [RETURNING ...]?`.

- [ ] **Step 1: Write the failing test**

Append:

```gleam
import marmot/internal/sqlite/parse/statement_parser.{Delete, DeleteStmt}

pub fn parse_delete_simple_test() {
  let assert Ok(Delete(stmt)) =
    parse_sql("DELETE FROM users WHERE id = ?")
  let assert TableBinding(
    table: TableRef(_, name: Identifier("users", False)),
    alias: None,
  ) = stmt.target
  let assert Some(_) = stmt.where
}

pub fn parse_delete_aliased_target_test() {
  let assert Ok(Delete(stmt)) =
    parse_sql("DELETE FROM users AS u WHERE u.id = ?")
  let assert TableBinding(_, alias: Some("u")) = stmt.target
}

pub fn parse_delete_returning_test() {
  let assert Ok(Delete(stmt)) =
    parse_sql("DELETE FROM users WHERE id = ? RETURNING id")
  let assert Some(_) = stmt.returning
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - DELETE branch returns `Unsupported`.

- [ ] **Step 3: Write minimal implementation**

```gleam
pub fn parse(tokens: List(Token)) -> Result(Statement, MarmotError) {
  case classify(tokens) {
    SelectKind -> parse_select(tokens) |> result.map(Select)
    InsertKind -> parse_insert(tokens) |> result.map(Insert)
    UpdateKind -> parse_update(tokens) |> result.map(Update)
    DeleteKind -> parse_delete(tokens) |> result.map(Delete)
    _ -> Ok(Unsupported(tokens))
  }
}

fn parse_delete(tokens: List(Token)) -> Result(DeleteStmt, MarmotError) {
  let after_delete = drop_keyword(tokens, "DELETE")
  let after_from = drop_keyword(after_delete, "FROM")
  let #(target, after_target) = parse_table_binding(after_from)
  let #(where, after_where) =
    take_clause(after_target, "WHERE", ["RETURNING"])
  let #(returning, _rest) = take_clause(after_where, "RETURNING", [])
  Ok(DeleteStmt(target: target, where: where, returning: returning))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for all three new tests.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parse/statement_parser.gleam test/marmot/internal/sqlite/parse/statement_parser_test.gleam
git commit -m "Parse DELETE: target binding, WHERE, RETURNING

Target supports AS alias for symmetry with UPDATE."
```

---

## Task 11: Resolver - alias map building

**Files:**
- Create: `src/marmot/internal/sqlite/parse/resolver.gleam`
- Test: `test/marmot/internal/sqlite/parse/resolver_test.gleam`

**Context:** Spec: explicit alias registers only the alias; no alias registers the table name; collision is a typed error. This handles `users u JOIN users manager` correctly because both items have explicit aliases and the bare `users` is never registered.

- [ ] **Step 1: Write the failing test**

Create `test/marmot/internal/sqlite/parse/resolver_test.gleam`:

```gleam
import gleam/dict
import gleam/option.{None, Some}
import marmot/internal/sqlite/parse/resolver.{AliasCollision, build_alias_map}
import marmot/internal/sqlite/parse/statement_parser.{
  Identifier, TableBinding, TableRef,
}

fn binding(name: String, alias: option.Option(String)) -> TableBinding {
  TableBinding(
    table: TableRef(schema: None, name: Identifier(name, False)),
    alias: alias,
  )
}

pub fn alias_map_with_explicit_aliases_test() {
  let bindings = [binding("users", Some("u")), binding("orders", Some("o"))]
  let assert Ok(map) = build_alias_map(bindings)
  let assert Ok(_) = dict.get(map, "u")
  let assert Ok(_) = dict.get(map, "o")
  let assert Error(_) = dict.get(map, "users")
  let assert Error(_) = dict.get(map, "orders")
}

pub fn alias_map_with_no_aliases_test() {
  let bindings = [binding("users", None), binding("orders", None)]
  let assert Ok(map) = build_alias_map(bindings)
  let assert Ok(_) = dict.get(map, "users")
  let assert Ok(_) = dict.get(map, "orders")
}

pub fn alias_map_self_join_test() {
  // Both items have explicit aliases; bare table name is never registered, so
  // no collision.
  let bindings = [binding("users", Some("u")), binding("users", Some("manager"))]
  let assert Ok(map) = build_alias_map(bindings)
  let assert Ok(_) = dict.get(map, "u")
  let assert Ok(_) = dict.get(map, "manager")
  let assert Error(_) = dict.get(map, "users")
}

pub fn alias_map_collision_on_alias_test() {
  let bindings = [binding("users", Some("x")), binding("orders", Some("x"))]
  let assert Error(AliasCollision("x")) = build_alias_map(bindings)
}

pub fn alias_map_collision_on_table_name_test() {
  let bindings = [binding("users", None), binding("users", None)]
  let assert Error(AliasCollision("users")) = build_alias_map(bindings)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - module `resolver` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `src/marmot/internal/sqlite/parse/resolver.gleam`:

```gleam
//// Alias-aware column resolution. Builds an alias map from a statement's
//// FROM clause (or write target) and resolves column references against it.
//// Resolution applies to the current parsed statement scope only; subqueries
//// continue to use the existing fallback path.
////
//// What this module does NOT do:
////  - Subquery scoping (correlated references; inner-scope shadowing)
////  - USING-join column merging (bare USING-merged refs are reported ambiguous)
////  - Expression-type inference (out of scope for the statement skeleton work)

import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option}
import marmot/internal/query.{type Column}
import marmot/internal/sqlite/parse/statement_parser.{
  type TableBinding, type TableRef, TableBinding,
}

pub type AliasMapError {
  AliasCollision(name: String)
}

pub fn build_alias_map(
  bindings: List(TableBinding),
) -> Result(Dict(String, TableRef), AliasMapError) {
  list.try_fold(bindings, dict.new(), fn(acc, b) {
    let name = case b.alias {
      option.Some(a) -> a
      option.None -> b.table.name.text
    }
    case dict.has_key(acc, name) {
      True -> Error(AliasCollision(name))
      False -> Ok(dict.insert(acc, name, b.table))
    }
  })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for all five new tests.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parse/resolver.gleam test/marmot/internal/sqlite/parse/resolver_test.gleam
git commit -m "Add resolver.build_alias_map with collision detection

Explicit alias registers only the alias; no alias registers the table
name. Self-joins with two aliases never collide because the bare table
name is never registered."
```

---

## Task 12: Resolver - qualified and bare column resolution

**Files:**
- Modify: `src/marmot/internal/sqlite/parse/resolver.gleam`
- Test: `test/marmot/internal/sqlite/parse/resolver_test.gleam`

**Context:** Five resolution outcomes per spec. Schemas are passed as `Dict(String, List(Column))` (the existing v1 shape) to keep the resolver decoupled from `ColumnMetadata`. Consumers map outcomes to `MarmotError` themselves.

- [ ] **Step 1: Write the failing test**

Append to `test/marmot/internal/sqlite/parse/resolver_test.gleam`:

```gleam
import marmot/internal/query.{Column, IntType, StringType}
import marmot/internal/sqlite/parse/resolver.{
  AmbiguousColumn, Resolved, ResolvedColumn, UnknownColumnInKnownTable,
  UnknownQualifiedAlias, UnknownTableRef, resolve_bare, resolve_qualified,
}

fn schemas() {
  dict.new()
  |> dict.insert("users", [
    Column("id", IntType, False),
    Column("email", StringType, False),
  ])
  |> dict.insert("orders", [
    Column("id", IntType, False),
    Column("total", IntType, False),
  ])
}

pub fn resolve_qualified_known_test() {
  let assert Ok(map) =
    build_alias_map([binding("users", Some("u")), binding("orders", Some("o"))])
  let assert Resolved(ResolvedColumn(_, Column("email", StringType, False))) =
    resolve_qualified(map, "u", "email", schemas())
}

pub fn resolve_qualified_unknown_alias_test() {
  let assert Ok(map) = build_alias_map([binding("users", Some("u"))])
  let assert UnknownQualifiedAlias = resolve_qualified(map, "x", "id", schemas())
}

pub fn resolve_qualified_unknown_column_test() {
  let assert Ok(map) = build_alias_map([binding("users", Some("u"))])
  let assert UnknownColumnInKnownTable =
    resolve_qualified(map, "u", "nope", schemas())
}

pub fn resolve_qualified_unknown_table_test() {
  let assert Ok(map) = build_alias_map([binding("foo_cte", None)])
  let assert UnknownTableRef =
    resolve_qualified(map, "foo_cte", "x", schemas())
}

pub fn resolve_bare_unique_test() {
  let assert Ok(map) =
    build_alias_map([binding("users", Some("u"))])
  let assert Resolved(ResolvedColumn(_, Column("email", _, _))) =
    resolve_bare(map, "email", schemas())
}

pub fn resolve_bare_ambiguous_test() {
  let assert Ok(map) =
    build_alias_map([binding("users", Some("u")), binding("orders", Some("o"))])
  let assert AmbiguousColumn = resolve_bare(map, "id", schemas())
}

pub fn resolve_bare_unknown_test() {
  let assert Ok(map) = build_alias_map([binding("users", Some("u"))])
  let assert AmbiguousColumn = resolve_bare(map, "missing", schemas())
  // missing in zero tables maps to AmbiguousColumn? No - zero matches
  // should be UnknownColumnInKnownTable when there's at least one known table.
  // Adjust this test once the implementation is in place.
}
```

The last test demonstrates an interesting edge: zero matches across all known tables. Per the design, that's an error, but `UnknownColumnInKnownTable` is technically misleading because there's no specific table to blame. Use `AmbiguousColumn` for "more than one match" only, and add a sixth outcome for the bare-zero case if needed during implementation. Update the resolver to add `UnknownBareColumn` and update the test:

```gleam
pub fn resolve_bare_unknown_test() {
  let assert Ok(map) = build_alias_map([binding("users", Some("u"))])
  let assert UnknownBareColumn = resolve_bare(map, "missing", schemas())
}
```

Also add the import for `UnknownBareColumn` to the imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - resolution functions not implemented.

- [ ] **Step 3: Write minimal implementation**

In `src/marmot/internal/sqlite/parse/resolver.gleam`:

```gleam
pub type ResolvedColumn {
  ResolvedColumn(table: TableRef, column: Column)
}

pub type Resolution {
  Resolved(ResolvedColumn)
  AmbiguousColumn
  UnknownQualifiedAlias
  UnknownColumnInKnownTable
  UnknownBareColumn
  UnknownTableRef
}

pub fn resolve_qualified(
  map: Dict(String, TableRef),
  alias: String,
  col_name: String,
  schemas: Dict(String, List(Column)),
) -> Resolution {
  case dict.get(map, alias) {
    Error(_) -> UnknownQualifiedAlias
    Ok(table_ref) ->
      case dict.get(schemas, table_ref.name.text) {
        Error(_) -> UnknownTableRef
        Ok(cols) ->
          case list.find(cols, fn(c) { c.name == col_name }) {
            Ok(col) -> Resolved(ResolvedColumn(table: table_ref, column: col))
            Error(_) -> UnknownColumnInKnownTable
          }
      }
  }
}

pub fn resolve_bare(
  map: Dict(String, TableRef),
  col_name: String,
  schemas: Dict(String, List(Column)),
) -> Resolution {
  let matches =
    dict.values(map)
    |> list.filter_map(fn(table_ref) {
      case dict.get(schemas, table_ref.name.text) {
        Error(_) -> Error(Nil)
        Ok(cols) ->
          case list.find(cols, fn(c) { c.name == col_name }) {
            Ok(col) -> Ok(ResolvedColumn(table: table_ref, column: col))
            Error(_) -> Error(Nil)
          }
      }
    })
  case matches {
    [] -> UnknownBareColumn
    [single] -> Resolved(single)
    _ -> AmbiguousColumn
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for all seven resolver tests.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parse/resolver.gleam test/marmot/internal/sqlite/parse/resolver_test.gleam
git commit -m "Add resolver.resolve_qualified and resolve_bare

Six resolution outcomes: Resolved, AmbiguousColumn, UnknownQualified-
Alias, UnknownColumnInKnownTable, UnknownBareColumn, UnknownTableRef.
The resolver returns typed outcomes; consumers map them to MarmotError."
```

---

## Task 13: Body-level parser helpers

**Files:**
- Modify: `src/marmot/internal/sqlite/parse/select.gleam`
- Modify: `src/marmot/internal/sqlite/parse/parameters.gleam`
- Test: existing tests in `test/marmot/internal/sqlite/parse_test.gleam` should remain green; add new ones for body-level helpers.

**Context:** Spec: each existing whole-statement walker gets a sibling that operates on a pre-bounded clause slice. Existing entry points become thin shims that call the statement parser then delegate. This task focuses on the helpers themselves; consumer migration comes in Tasks 14-16.

- [ ] **Step 1: Write the failing test**

Add to `test/marmot/internal/sqlite/parse_test.gleam`:

```gleam
import marmot/internal/sqlite/parse/select.{parse_returning_body, parse_select_item_list}
import marmot/internal/sqlite/parse/parameters.{parse_update_set_body, parse_where_body}
import marmot/internal/sqlite/tokenize.{tokenize}

pub fn parse_select_item_list_test() {
  // Input is the select-list slice only (no SELECT keyword, no FROM).
  let tokens = tokenize("a, b, c")
  let items = parse_select_item_list(tokens)
  let assert 3 = list.length(items)
}

pub fn parse_where_body_test() {
  // Input is the WHERE body slice only (no WHERE keyword, no following clause).
  let tokens = tokenize("id = @id AND status = @status")
  let conditions = parse_where_body(tokens)
  let assert 2 = list.length(conditions)
}

pub fn parse_update_set_body_test() {
  // Input is the SET body slice only.
  let tokens = tokenize("name = @name, email = @email")
  let assignments = parse_update_set_body(tokens)
  let assert 2 = list.length(assignments)
}

pub fn parse_returning_body_test() {
  let tokens = tokenize("id, name, email")
  let cols = parse_returning_body(tokens)
  let assert ["id", "name", "email"] = cols
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - the body-level functions don't exist.

- [ ] **Step 3: Write minimal implementation**

In `src/marmot/internal/sqlite/parse/select.gleam`, extract body-level logic:

```gleam
/// Parse a select-list slice (no SELECT keyword, no FROM and beyond).
/// This is the body-level entry point. The whole-statement helper
/// `parse_select_items/1` becomes a shim that slices and delegates.
pub fn parse_select_item_list(select_list_tokens: List(Token)) -> List(SelectItem) {
  tokenize.split_on_commas(select_list_tokens)
  |> list.map(parse_select_item)
}

/// Parse a RETURNING body slice into column names. The whole-statement helper
/// `parse_returning_columns/1` becomes a shim that slices and delegates.
pub fn parse_returning_body(returning_tokens: List(Token)) -> List(String) {
  tokenize.split_on_commas(returning_tokens)
  |> list.map(fn(group) {
    case tokenize.split_at_last_keyword(group, "AS") {
      Ok(#(_, alias_tokens)) -> util.token_list_to_name(alias_tokens)
      Error(_) -> util.token_list_to_name(group)
    }
  })
}

/// Existing whole-statement helper, now a shim.
pub fn parse_select_items(tokens: List(Token)) -> List(SelectItem) {
  let main_tokens = skip_with_prefix(tokens)
  let after_select = skip_select_keyword(main_tokens)
  case after_select {
    [] -> []
    _ -> {
      let select_tokens =
        tokenize.take_until_keywords(after_select, [
          "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION",
          "INTERSECT", "EXCEPT",
        ])
      parse_select_item_list(select_tokens)
    }
  }
}

/// Existing whole-statement helper, now a shim.
pub fn parse_returning_columns(tokens: List(Token)) -> List(String) {
  case tokenize.split_at_keyword(tokens, "RETURNING") {
    Error(_) -> []
    Ok(#(_, after)) -> parse_returning_body(after)
  }
}
```

In `src/marmot/internal/sqlite/parse/parameters.gleam`, extract body-level logic:

```gleam
/// Parse a WHERE body slice (no WHERE keyword, no RETURNING/etc beyond).
/// The whole-statement helper `parse_where_columns/1` becomes a shim.
pub fn parse_where_body(where_tokens: List(Token)) -> List(#(String, String)) {
  tokenize.split_on_and_or(where_tokens)
  |> list.flat_map(parse_where_condition)
}

/// Parse a SET body slice (no SET keyword, no following WHERE/RETURNING).
/// The whole-statement helper `parse_update_set_columns/1` becomes a shim.
pub fn parse_update_set_body(set_tokens: List(Token)) -> List(#(String, String)) {
  tokenize.split_on_commas(set_tokens)
  |> list.filter_map(fn(assignment) {
    case split_tokens_on_eq(assignment) {
      Error(_) -> Error(Nil)
      Ok(#(lhs_tokens, rhs_tokens)) -> {
        let col = util.token_list_to_name(lhs_tokens)
        case has_param_token(rhs_tokens) {
          True ->
            case find_named_param_in_tokens(rhs_tokens) {
              option.Some(param_name) -> Ok(#(param_name, col))
              option.None -> Ok(#(col, col))
            }
          False -> Error(Nil)
        }
      }
    }
  })
}

pub fn parse_where_columns(tokens: List(Token)) -> List(#(String, String)) {
  case tokenize.split_at_keyword(tokens, "WHERE") {
    Error(_) -> []
    Ok(#(_, where_tokens)) -> {
      let where_part = tokenize.take_until_keywords(where_tokens, ["RETURNING"])
      parse_where_body(where_part)
    }
  }
}

pub fn parse_update_set_columns(
  tokens: List(Token),
) -> List(#(String, String)) {
  case tokenize.split_at_keyword(tokens, "SET") {
    Error(_) -> []
    Ok(#(_, after_set)) -> {
      let set_part =
        tokenize.take_until_keywords(after_set, ["WHERE", "RETURNING"])
      parse_update_set_body(set_part)
    }
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for the four new body-level tests; all existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parse/select.gleam src/marmot/internal/sqlite/parse/parameters.gleam test/marmot/internal/sqlite/parse_test.gleam
git commit -m "Add body-level parser helpers; existing entries become shims

parse_select_item_list, parse_where_body, parse_update_set_body,
parse_returning_body operate on pre-sliced clause tokens. Whole-
statement entry points keep their signatures and delegate."
```

---

## Task 14: Migrate parameters.gleam consumer to typed Statement

**Files:**
- Modify: `src/marmot/internal/sqlite/parameters.gleam`
- Test: existing tests in `test/marmot/internal/sqlite_test.gleam` must continue to pass; allow snapshot diffs only where the new resolver corrects a known bug.

**Context:** The current `extract_parameters` accepts a token list and dispatches based on `statement.classify_statement`. Migrate it to accept a typed `Statement` (or take tokens and parse internally) and use the body-level helpers + alias-aware resolver. Internal shape changes; the public function signature stays compatible by parsing internally so callers (`sqlite.gleam`) don't need to migrate yet.

- [ ] **Step 1: Write the failing test**

Add to `test/marmot/internal/sqlite_test.gleam` (or a new test file `test/marmot/internal/sqlite/parameters_alias_test.gleam`):

```gleam
import gleam/dict
import marmot/internal/query.{Column, IntType, StringType}
import marmot/internal/sqlite/parameters.{extract_parameters}
import marmot/internal/sqlite/tokenize

pub fn extract_parameters_disambiguates_via_alias_test() {
  // Two tables share column `id`; without alias-aware resolution the parameter
  // type would be miscoerced. With it, qualified `o.total` resolves correctly.
  let table_schemas =
    dict.new()
    |> dict.insert("users", [
      Column("id", IntType, False),
      Column("email", StringType, False),
    ])
    |> dict.insert("orders", [
      Column("id", IntType, False),
      Column("total", IntType, False),
    ])
  let tokens =
    tokenize.tokenize(
      "SELECT u.email FROM users u JOIN orders o ON o.user_id = u.id WHERE o.total > @min_total",
    )
  let params =
    extract_parameters(
      [],
      dict.new(),
      table_schemas,
      dict.new(),
      tokens,
    )
  let assert [Parameter(name: "min_total", column_type: IntType, ..)] = params
}
```

(Substitute `Parameter` import as needed.)

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - the existing search-all-tables resolver may return `min_total` typed as `StringType` because two tables have an `id` column and the resolver picks the wrong one.

If it incidentally passes today, snapshot the current behavior and proceed; the migration's value is robustness, not necessarily a single failing test.

- [ ] **Step 3: Write minimal implementation**

Modify `src/marmot/internal/sqlite/parameters.gleam` so that the `extract_select_parameters` and `extract_update_parameters` paths build a `Statement` via `statement_parser.parse`, then build an alias map via `resolver.build_alias_map`, and resolve each binder using `resolver.resolve_qualified` / `resolver.resolve_bare`.

Sketch (full version goes in the implementation):

```gleam
import marmot/internal/sqlite/parse/statement_parser
import marmot/internal/sqlite/parse/resolver

fn extract_select_parameters_v2(
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
) -> Result(List(Parameter), MarmotError) {
  use stmt <- result.try(statement_parser.parse(tokens))
  case stmt {
    statement_parser.Select(select_stmt) -> {
      let bindings =
        select_stmt.body.from |> list.map(fn(item) { item.binding })
      use alias_map <- result.try(case resolver.build_alias_map(bindings) {
        Ok(m) -> Ok(m)
        Error(resolver.AliasCollision(name)) ->
          Error(error.SqlError(
            path: "",
            message: "alias collision: " <> name,
          ))
      })
      let where_tokens = case select_stmt.body.where {
        Some(t) -> t
        None -> []
      }
      let binders = binder.find_param_binders(where_tokens)
      let params =
        list.map(binders, fn(b) {
          resolve_binder_to_parameter(b, alias_map, table_schemas)
        })
      Ok(params)
    }
    statement_parser.Delete(delete_stmt) -> {
      let bindings = [delete_stmt.target]
      use alias_map <- result.try(case resolver.build_alias_map(bindings) {
        Ok(m) -> Ok(m)
        Error(resolver.AliasCollision(name)) ->
          Error(error.SqlError(
            path: "",
            message: "alias collision: " <> name,
          ))
      })
      let where_tokens = case delete_stmt.where {
        Some(t) -> t
        None -> []
      }
      let binders = binder.find_param_binders(where_tokens)
      let params =
        list.map(binders, fn(b) {
          resolve_binder_to_parameter(b, alias_map, table_schemas)
        })
      Ok(params)
    }
    _ -> Ok([])
  }
}

fn resolve_binder_to_parameter(
  b: binder.ParamBinder,
  alias_map: Dict(String, statement_parser.TableRef),
  schemas: Dict(String, List(Column)),
) -> Parameter {
  let resolution = case b.binder_column {
    Some(col_ref) ->
      case string.split_once(col_ref, ".") {
        Ok(#(alias, col_name)) ->
          resolver.resolve_qualified(alias_map, alias, col_name, schemas)
        Error(_) -> resolver.resolve_bare(alias_map, col_ref, schemas)
      }
    None -> resolver.resolve_bare(alias_map, b.name, schemas)
  }
  case resolution {
    resolver.Resolved(rc) ->
      Parameter(
        name: b.name,
        column_type: rc.column.column_type,
        nullable: rc.column.nullable,
      )
    _ -> Parameter(name: b.name, column_type: StringType, nullable: False)
  }
}
```

Wire `extract_parameters` to call `extract_select_parameters_v2` for SELECT/DELETE branches, falling back to the existing opcode-only path on `Error`. Insert/Update keep their existing extraction paths; Task 17 migrates INSERT, and Task 14b (below) migrates UPDATE if needed.

Keep the public signature of `extract_parameters` the same for backwards compatibility - the consumer in `sqlite.gleam` still passes tokens.

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for the new disambiguation test; existing tests pass. Snapshot diffs that show better types are categorized in Task 18.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parameters.gleam test/
git commit -m "Migrate parameters.gleam to typed Statement and alias resolver

SELECT and DELETE parameter extraction routes through statement_parser
and resolver. Two tables sharing a column name no longer cross-
contaminate parameter types when the user qualifies the reference."
```

---

## Task 15: Migrate results.gleam consumer to typed Statement

**Files:**
- Modify: `src/marmot/internal/sqlite/results.gleam`
- Tests: existing snapshot tests must continue to pass; categorize diffs in Task 18.

**Context:** `extract_result_columns` currently uses `select.parse_select_items(tokens)` and `select.parse_from_tables(tokens)` directly on the whole-statement token list. Migrate to consume `statement_parser.Select(_)` so SELECT items are parsed from the body slice and FROM tables come from `body.from` structurally.

- [ ] **Step 1: Write the failing test**

Add to `test/marmot/internal/sqlite_test.gleam`:

```gleam
pub fn results_keyword_named_table_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE \"returning\" (id INTEGER PRIMARY KEY, label TEXT);
       INSERT INTO \"returning\" (label) VALUES ('a');",
      conn,
    )
  // Use the higher-level introspection so we exercise results.gleam end-to-end.
  // The keyword-named table previously confused the parser; with the new
  // pipeline it should parse and infer the row shape.
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "select_returning",
      "SELECT id, label FROM \"returning\"",
      "/tmp/select_returning.sql",
    )
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "label", column_type: StringType, ..),
  ] = query.columns
}
```

(Substitute the actual function name from `sqlite.gleam` if `introspect_query` is named differently.)

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - `select.parse_from_tables(tokens)` called on a SELECT containing a `RETURNING`-named table may misbehave; or the test passes today due to the table being quoted. Adjust to an unquoted variant if needed: `CREATE TABLE returning ...`.

- [ ] **Step 3: Write minimal implementation**

Update `src/marmot/internal/sqlite/results.gleam`:

```gleam
import marmot/internal/sqlite/parse/statement_parser

pub fn extract_result_columns(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
  join_nullability: JoinNullability,
  tokens: List(Token),
) -> List(Column) {
  let stmt_result = statement_parser.parse(tokens)
  let #(select_list_tokens, from_table_names) = case stmt_result {
    Ok(statement_parser.Select(stmt)) -> {
      let from_names =
        stmt.body.from
        |> list.map(fn(item) { item.binding.table.name.text })
      #(stmt.body.select_list, from_names)
    }
    _ -> #([], [])
  }
  let select_items = select.parse_select_item_list(select_list_tokens)
  let from_tables = from_table_names
  // ... rest of function unchanged, using `select_items` and `from_tables` ...
}
```

Both `select_items` and `from_tables` used to come from `select.parse_select_items(tokens)` and `select.parse_from_tables(tokens)`. The migration removes those calls and replaces them with the structurally-parsed equivalents.

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for the keyword-named-table test; existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/results.gleam test/marmot/internal/sqlite_test.gleam
git commit -m "Migrate results.gleam to typed Statement

extract_result_columns reads select_list and from tables from a parsed
SelectStmt instead of running whole-statement keyword scans."
```

---

## Task 16: Migrate sqlite.gleam consumer sites

**Files:**
- Modify: `src/marmot/internal/sqlite.gleam`
- Tests: existing tests must pass.

**Context:** `sqlite.gleam:147` has `let has_returning = tokenize.has_keyword(tokens, "RETURNING")` which is the canonical bad probe. Replace it with a check derived from a parsed `Statement`. While here, also switch from `get_table_metadata` to `get_table_metadata_v2` so the rest of the pipeline benefits from `ColumnMetadata`. The existing `Dict(String, List(Column))` interface is preserved by extracting `.column` from each `ColumnMetadata`.

- [ ] **Step 1: Write the failing test**

Add a regression test for an UPDATE-with-keyword-named-table case to `test/marmot/internal/sqlite_test.gleam`:

```gleam
pub fn sqlite_update_with_keyword_table_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE returning (id INTEGER PRIMARY KEY, name TEXT);
       INSERT INTO returning (name) VALUES ('x');",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "update_returning",
      "UPDATE returning SET name = ? WHERE id = ? RETURNING id, name",
      "/tmp/update_returning.sql",
    )
  let assert [
    Column(name: "id", column_type: IntType, ..),
    Column(name: "name", ..),
  ] = query.columns
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL or wrong behavior - the global RETURNING probe at line 147 may incorrectly fire on the table name `returning`.

- [ ] **Step 3: Write minimal implementation**

In `src/marmot/internal/sqlite.gleam`, around line 147:

```gleam
import marmot/internal/sqlite/parse/statement_parser

// ... in the function that currently does `tokenize.has_keyword(tokens, "RETURNING")`:
let stmt_result = statement_parser.parse(tokens)
let returning_tokens = case stmt_result {
  Ok(statement_parser.Insert(stmt)) -> stmt.returning
  Ok(statement_parser.Update(stmt)) -> stmt.returning
  Ok(statement_parser.Delete(stmt)) -> stmt.returning
  _ -> option.None
}
let has_returning = option.is_some(returning_tokens)
```

Also migrate the metadata loader call from `get_table_metadata` to `get_table_metadata_v2`, then convert `TableMetadataV2` to the existing `Dict(String, List(Column))` shape that downstream consumers expect:

```gleam
let v2 = schema.get_table_metadata_v2(conn)
let table_schemas =
  dict.map_values(v2.columns, fn(_, metas) {
    list.map(metas, fn(m) { m.column })
  })
let pk_columns = v2.pks
let cursor_table = v2.rootpages
```

The full `ColumnMetadata` (with `is_rowid_alias`) is also passed forward to wherever INSERT VALUES schema fallback (Task 17) needs it.

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for the update-with-keyword-table test; all existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite.gleam test/marmot/internal/sqlite_test.gleam
git commit -m "Migrate sqlite.gleam to derive RETURNING from typed Statement

Removes the global tokenize.has_keyword(_, RETURNING) probe at
sqlite.gleam:147. RETURNING presence is now a property of the parsed
Statement. Schema loader switched to get_table_metadata_v2 so
downstream INSERT VALUES schema fallback can read is_rowid_alias."
```

---

## Task 17: INSERT VALUES schema fallback + write nullability + count mismatch error

**Files:**
- Modify: `src/marmot/internal/sqlite/parameters.gleam`
- Modify: `src/marmot/internal/error.gleam` (add a new `MarmotError` variant if needed)
- Test: `test/marmot/internal/sqlite_test.gleam`

**Context:** Three pieces in one task because they depend on each other: (a) when `column_list == None`, derive bindable columns from the table schema (filtering generated columns); (b) for each parameter, compute write-nullability as `column.nullable || is_rowid_alias`; (c) on row-count mismatch, surface a `MarmotError` rather than falling back to opcode inference.

- [ ] **Step 1: Write the failing test**

Append to `test/marmot/internal/sqlite_test.gleam`:

```gleam
pub fn insert_values_no_column_list_uses_schema_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);",
      conn,
    )
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "insert_t",
      "INSERT INTO t VALUES (?, ?, ?)",
      "/tmp/insert_t.sql",
    )
  // Expect 3 parameters: id (rowid alias, nullable on write), name (non-null),
  // age (nullable, no NOT NULL).
  let assert [
    Parameter(name: "id", column_type: IntType, nullable: True),
    Parameter(name: "name", column_type: StringType, nullable: False),
    Parameter(name: "age", column_type: IntType, nullable: True),
  ] = query.parameters
}

pub fn insert_values_skips_generated_columns_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER NOT NULL, c INTEGER GENERATED ALWAYS AS (b * 2) VIRTUAL);",
      conn,
    )
  // VALUES (?, ?) provides 2 expressions; bindable columns are {a, b}.
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "insert_t",
      "INSERT INTO t VALUES (?, ?)",
      "/tmp/insert_t.sql",
    )
  let assert [
    Parameter(name: "a", nullable: True, ..),
    Parameter(name: "b", nullable: False, ..),
  ] = query.parameters
}

pub fn insert_values_count_mismatch_errors_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER);",
      conn,
    )
  let assert Error(error.InsertValuesCountMismatch(_)) =
    sqlite.introspect_query(
      conn,
      "insert_t",
      "INSERT INTO t VALUES (?)",
      "/tmp/insert_t.sql",
    )
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: FAIL - new error variant doesn't exist; schema fallback uses `(?, ?)` paren group as columns.

- [ ] **Step 3: Write minimal implementation**

Add the new error variant in `src/marmot/internal/error.gleam`:

```gleam
pub type MarmotError {
  // ... existing variants ...
  /// INSERT VALUES has a row whose expression count does not match the
  /// table's bindable column count.
  InsertValuesCountMismatch(path: String, expected: Int, got: Int, row: Int)
}

pub fn to_string(error: MarmotError) -> String {
  case error {
    // ... existing branches ...
    InsertValuesCountMismatch(path:, expected:, got:, row:) ->
      "error: INSERT VALUES row has wrong number of expressions
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Row " <> int.to_string(row) <> " has " <> int.to_string(got)
        <> " expressions; expected " <> int.to_string(expected)
        <> " for the bindable columns of this table.
  \u{2502}
  hint: Provide an explicit column list, or match the number of values to
        the table's non-generated columns."
  }
}
```

In `src/marmot/internal/sqlite/parameters.gleam`, replace the INSERT branch of `extract_parameters` with one that uses the parsed `InsertStmt`, schema metadata, and rowid-alias flag:

```gleam
fn extract_insert_parameters_v2(
  insert_stmt: statement_parser.InsertStmt,
  table_metadata: schema.TableMetadataV2,
  path: String,
) -> Result(List(Parameter), MarmotError) {
  let target_name = insert_stmt.target.table.name.text
  let metadatas = case dict.get(table_metadata.columns, target_name) {
    Ok(m) -> m
    Error(_) -> []
  }
  let bindable =
    list.filter(metadatas, fn(m) { m.hidden == 0 })

  let bound_names = case insert_stmt.column_list {
    Some(names) -> names
    None -> list.map(bindable, fn(m) { m.column.name })
  }

  case insert_stmt.source {
    statement_parser.ValuesSource(_, rows) -> {
      use _ <- result.try(
        validate_row_counts(rows, list.length(bound_names), path),
      )
      let params =
        list.map(bound_names, fn(col_name) {
          case list.find(bindable, fn(m) { m.column.name == col_name }) {
            Ok(m) ->
              Parameter(
                name: col_name,
                column_type: m.column.column_type,
                nullable: m.column.nullable || m.is_rowid_alias,
              )
            Error(_) ->
              Parameter(
                name: col_name,
                column_type: StringType,
                nullable: False,
              )
          }
        })
      Ok(params)
    }
    statement_parser.DefaultValuesSource -> Ok([])
    statement_parser.SelectSource(_) -> {
      // INSERT ... SELECT keeps the existing path for v1.
      Ok([])
    }
  }
}

fn validate_row_counts(
  rows: List(List(List(tokenize.Token))),
  expected: Int,
  path: String,
) -> Result(Nil, MarmotError) {
  do_validate_rows(rows, expected, path, 1)
}

fn do_validate_rows(
  rows: List(List(List(tokenize.Token))),
  expected: Int,
  path: String,
  index: Int,
) -> Result(Nil, MarmotError) {
  case rows {
    [] -> Ok(Nil)
    [row, ..rest] ->
      case list.length(row) == expected {
        True -> do_validate_rows(rest, expected, path, index + 1)
        False ->
          Error(error.InsertValuesCountMismatch(
            path: path,
            expected: expected,
            got: list.length(row),
            row: index,
          ))
      }
  }
}
```

Wire this into the existing `extract_parameters` so INSERT statements route through `extract_insert_parameters_v2`. The path-passing requires a small signature change to `extract_parameters` (or `sqlite.gleam` propagates the SQL file path).

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS for all three new tests.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/sqlite/parameters.gleam src/marmot/internal/error.gleam test/marmot/internal/sqlite_test.gleam
git commit -m "INSERT VALUES schema fallback with rowid-alias write nullability

Column-less INSERT VALUES derives bindable columns from the table
schema (filtering generated columns via the hidden flag). Rowid alias
columns are nullable for parameter binding (auto-assign semantics).
Row-count mismatch surfaces InsertValuesCountMismatch instead of
falling back to opcode inference."
```

---

## Task 18: Should-fail fixture harness

**Files:**
- Create: `test/sql_failures_test.gleam`
- Create: `test/fixtures/sql_failures/value_count_mismatch.sql`

**Context:** Spec requires a should-fail harness for SQL inputs that must produce a `MarmotError`. Lives outside `examples/` because that directory reads as runnable user material.

- [ ] **Step 1: Write the failing test**

Create `test/fixtures/sql_failures/value_count_mismatch.sql`:

```sql
INSERT INTO t VALUES (?);
```

Create `test/sql_failures_test.gleam`:

```gleam
import gleam/list
import gleam/string
import marmot/internal/error.{type MarmotError}
import marmot/internal/sqlite
import simplifile
import sqlight

type Fixture {
  Fixture(file: String, schema_setup: String, expected_predicate: fn(MarmotError) -> Bool)
}

fn fixtures() -> List(Fixture) {
  [
    Fixture(
      file: "test/fixtures/sql_failures/value_count_mismatch.sql",
      schema_setup: "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER);",
      expected_predicate: fn(err) {
        case err {
          error.InsertValuesCountMismatch(_, _, _, _) -> True
          _ -> False
        }
      },
    ),
  ]
}

pub fn sql_failure_fixtures_test() {
  list.each(fixtures(), fn(fixture) {
    let assert Ok(sql) = simplifile.read(fixture.file)
    let assert Ok(conn) = sqlight.open(":memory:")
    let assert Ok(_) = sqlight.exec(fixture.schema_setup, conn)
    let result =
      sqlite.introspect_query(
        conn,
        "fixture",
        sql,
        fixture.file,
      )
    case result {
      Error(err) ->
        case fixture.expected_predicate(err) {
          True -> Nil
          False ->
            panic as {
              "Fixture "
              <> fixture.file
              <> " produced unexpected error variant"
            }
        }
      Ok(_) ->
        panic as {
          "Fixture " <> fixture.file <> " was expected to fail but succeeded"
        }
    }
  })
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: PASS, because Task 17 already raises the error. (This task is about *codifying* the harness so future failure modes can be added cheaply.)

If it fails for a different reason (path resolution), fix the harness.

- [ ] **Step 3: Write minimal implementation**

Already done in step 1.

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add test/sql_failures_test.gleam test/fixtures/sql_failures/value_count_mismatch.sql
git commit -m "Add should-fail fixture harness with VALUES count mismatch case

Failing fixtures live under test/fixtures/ and not examples/ to keep
the latter as runnable user material. New failure modes can be added
by extending the fixtures() list."
```

---

## Task 19: Integration regression - keyword-named table

**Files:**
- Create: `examples/edge_cases/keyword_table.sql`
- Test: existing `test/integration_test.gleam` extended, or a new fixture-driven test if present.

**Context:** End-to-end verification: a `.sql` file referencing a table named `returning` compiles to working Gleam. Catches the bug class beyond the unit tests.

- [ ] **Step 1: Write the failing test**

Create `examples/edge_cases/keyword_table.sql`:

```sql
SELECT id, label
FROM returning
WHERE id = ?
```

Add to `test/integration_test.gleam` (or wherever integration regressions live):

```gleam
pub fn integration_keyword_named_table_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE returning (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
       INSERT INTO returning (label) VALUES ('a');",
      conn,
    )
  let assert Ok(sql) =
    simplifile.read("examples/edge_cases/keyword_table.sql")
  let assert Ok(query) =
    sqlite.introspect_query(conn, "by_id", sql, "examples/edge_cases/keyword_table.sql")
  let assert [Column(name: "id", column_type: IntType, ..), Column(name: "label", ..)] =
    query.columns
  let assert [Parameter(name: _, column_type: IntType, ..)] = query.parameters
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: With Tasks 4-16 in place, this should pass. The test exists to lock in the regression coverage.

- [ ] **Step 3: Implementation already complete from earlier tasks.**

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add examples/edge_cases/keyword_table.sql test/integration_test.gleam
git commit -m "Integration regression: keyword-named table parses end to end

Locks in the keyword-named-table fix with an example SQL file plus
introspection assertion."
```

---

## Task 20: Integration regression - column-less INSERT VALUES with rowid PK

**Files:**
- Create: `examples/edge_cases/bare_insert.sql`
- Test: extend `test/integration_test.gleam`.

**Context:** End-to-end verification of Task 17.

- [ ] **Step 1: Write the failing test**

Create `examples/edge_cases/bare_insert.sql`:

```sql
INSERT INTO widgets VALUES (?, ?, ?)
```

Add to `test/integration_test.gleam`:

```gleam
pub fn integration_bare_insert_with_rowid_pk_test() {
  let assert Ok(conn) = sqlight.open(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL, qty INTEGER);",
      conn,
    )
  let assert Ok(sql) =
    simplifile.read("examples/edge_cases/bare_insert.sql")
  let assert Ok(query) =
    sqlite.introspect_query(
      conn,
      "insert_widget",
      sql,
      "examples/edge_cases/bare_insert.sql",
    )
  let assert [
    Parameter(name: "id", column_type: IntType, nullable: True),
    Parameter(name: "name", column_type: StringType, nullable: False),
    Parameter(name: "qty", column_type: IntType, nullable: True),
  ] = query.parameters
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `gleam test`
Expected: With Task 17 in place, this should pass.

- [ ] **Step 3: Implementation already complete.**

- [ ] **Step 4: Run test to verify it passes**

Run: `gleam test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add examples/edge_cases/bare_insert.sql test/integration_test.gleam
git commit -m "Integration regression: bare INSERT VALUES with rowid PK"
```

---

## Task 21: Snapshot review and reconciliation

**Files:**
- Possibly modify: `birdie_snapshots/*.snap.txt` (existing snapshots)
- No code changes; this task is review and approval.

**Context:** The migration in Tasks 14-17 may produce snapshot diffs in the existing test suite where the new resolver corrects previously-wrong types. Each diff must be categorized in the commit message: previously wrong now right, still fallback unchanged by design, regression found and fixed, or cosmetic name/order change with reason. No bulk-accept.

- [ ] **Step 1: Run the full suite and capture diffs**

Run: `gleam test`
Expected: PASS overall. Birdie will report any snapshot mismatches with `birdie review` available.

- [ ] **Step 2: Review each mismatched snapshot**

Run: `gleam run -m birdie` (or `gleam test -- --review` depending on the runner).
For each diff:
1. Read the previous and new snapshot side-by-side.
2. Categorize: better, worse, same-meaning, regression.
3. If "worse" or "regression", do not accept; open an investigation task.
4. If "better" or "same-meaning", accept and note in the commit message.

- [ ] **Step 3: Update or accept snapshots**

For accepted diffs, run `birdie accept <name>` per snapshot or `birdie accept all` if every diff was reviewed and approved.

For any regression, fix the underlying code in a follow-up task before merging.

- [ ] **Step 4: Run the full suite once more**

Run: `gleam test`
Expected: PASS, no diffs.

- [ ] **Step 5: Commit accepted snapshots**

```bash
git add birdie_snapshots/
git commit -m "Reconcile snapshots after statement parser migration

Categorized diffs:
- N snapshots: previously wrong types corrected by alias-aware resolver
- M snapshots: cosmetic name reordering with no semantic change
- 0 regressions"
```

(Replace N and M with actual counts.)

---

## Task 22: Update README.md limitations section

**Files:**
- Modify: `README.md` (lines 304-349 region)

**Context:** Per spec docs section. The "proper SQL tokenizer" framing goes away; two former limitations (keyword tables, column-less INSERT VALUES) are removed; "complex expressions" stays with updated framing; two new limitations (subquery scoping, USING joins) are added.

- [ ] **Step 1: Locate the existing limitations section**

Read `README.md` lines 304-349.

- [ ] **Step 2: Replace the section**

Update `README.md`:

```markdown
## Known Limitations

### Fixable, worth doing eventually

- **Complex expressions in result columns** (subqueries in SELECT, COALESCE, CASE) sometimes fall back to `StringType` because expression-type inference is incremental. Use `CAST(... AS TYPE)` to guide inference, or alias with `!`/`?` for nullability.

### Design decisions, not bugs

- **`TIMESTAMP` and `DATETIME` columns are stored as Unix seconds (integer).** Sub-second precision (nanoseconds) is not preserved. Picked integer seconds for simplicity and interop with `strftime('%s', ...)`. Preserving nanos would need a new storage format or type mapping.

### Limited scope of alias resolution

- **Subquery scoping.** Alias-aware resolution is limited to the current parsed statement scope. Parameters inside subqueries still use the existing fallback inference and may require explicit qualification or `CAST`.
- **USING joins.** `JOIN x USING (col)` is supported, but bare `col` references in WHERE may be reported as ambiguous. Use `ON` syntax or qualify the column.

### Hard limits

- **Repeated anonymous `?` placeholders** that refer to the same value generate a separate function argument for each occurrence. SQLite protocol limitation: anonymous `?` are always distinct bind slots. Use named parameters (`@name` or `:name`) instead - SQLite deduplicates them natively.
- **`WHERE id IN (?)` with a dynamic list is not supported.** SQLite has no native array parameter type. Workarounds:
  - Write separate queries for known list sizes.
  - Use `json_each(?)` with a JSON array string: `WHERE id IN (SELECT value FROM json_each(?))`.
  - Build the query string dynamically in your application code (outside Marmot).
```

Also remove the pre-existing entries for keyword-named tables and column-less INSERT VALUES, which are now supported.

- [ ] **Step 3: No tests to run for docs.**

- [ ] **Step 4: Verify the rendered output reads cleanly**

Run: `cat README.md | head -350 | tail -60`
Expected: Updated section reads coherently; cross-references in the rest of the README still make sense.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "Update docs: rewrite limitations after statement parser work

Drops the misleading 'tokenizer' framing. Keyword-named tables and
column-less INSERT VALUES are now supported. Subquery scoping and
USING-join column merging added as new known limitations with
workarounds."
```

---

## Task 23: Update src/marmot/internal/README.md architecture overview

**Files:**
- Modify: `src/marmot/internal/README.md`

**Context:** Architecture overview should describe the new pipeline stage and the rowid-alias rule.

- [ ] **Step 1: Read the current overview**

Read `src/marmot/internal/README.md`.

- [ ] **Step 2: Update the data flow diagram and pipeline description**

Update relevant sections to describe:

```markdown
## Pipeline

1. **Tokenize** (`sqlite/tokenize.gleam`): SQL string to token stream.
2. **Parse statement skeleton** (`sqlite/parse/statement_parser.gleam`): tokens to typed `Statement` value with parsed clause boundaries, FROM aliases, INSERT shape, and CTEs.
3. **Resolve column references** (`sqlite/parse/resolver.gleam`): build alias map from the parsed FROM clause; resolve qualified and bare column references with typed outcomes (`Resolved`, `AmbiguousColumn`, `UnknownQualifiedAlias`, etc.).
4. **Walk clause bodies** (`sqlite/parse/select.gleam`, `parameters.gleam`): existing body-level helpers operate on parser-provided slices, not whole-statement scans.
5. **Apply opcode inference** (`sqlite/opcode.gleam`): EXPLAIN-derived type information augments the parser-derived shape.
6. **Generate code** (`codegen.gleam`).
```

Add a section on `ColumnMetadata` and the rowid-alias rule:

```markdown
## Schema metadata

The schema loader (`sqlite/schema.gleam`) uses `PRAGMA table_xinfo` (which exposes generated columns via the `hidden` flag) and `PRAGMA table_list` (for the `wr` flag indicating WITHOUT ROWID tables).

`ColumnMetadata` carries:
- `column: Column` - the public-facing type.
- `is_rowid_alias: Bool` - true for `INTEGER PRIMARY KEY` on a normal (rowid) table; false on `WITHOUT ROWID` tables and for multi-column primary keys.
- `is_generated: Bool` - virtual or stored generated columns.
- `hidden: Int` - raw flag from `table_xinfo` (0 normal, 1 hidden, 2 virtual generated, 3 stored generated).

**Read vs write nullability for rowid aliases.** An `INTEGER PRIMARY KEY` column on a rowid table is non-null on read (SQLite always assigns a rowid) but accepts `NULL` on insert (auto-assign next rowid). Parameter inference for column-less `INSERT VALUES` computes:

```gleam
write_nullable = column.nullable || is_rowid_alias
```

Do not reintroduce the inline `pk > 0 && column_type == IntType -> nullable: False` workaround. The rowid-alias flag is the single source of truth.
```

- [ ] **Step 3: No tests to run.**

- [ ] **Step 4: Verify reads cleanly.**

Run: `head -200 src/marmot/internal/README.md`
Expected: New pipeline stage and ColumnMetadata section read coherently with the rest.

- [ ] **Step 5: Commit**

```bash
git add src/marmot/internal/README.md
git commit -m "Update internal README: statement parser pipeline + ColumnMetadata

Documents the new pipeline stage between tokenize and clause-body
walkers. Adds a section on ColumnMetadata and the read/write
nullability rule for rowid aliases so the next contributor doesn't
reintroduce the inline workaround."
```

---

## Spec Coverage Self-Review

Cross-check each spec section against the task list:

| Spec section | Implementing task(s) |
| --- | --- |
| Goals: new statement skeleton parser | T3-T10 |
| INSERT grammar coverage | T7, T8 |
| Schema fallback for INSERT VALUES + rowid-alias rule | T1, T2, T17 |
| Alias-aware table resolution | T11, T12 |
| Replace global keyword checks | T14, T15, T16 |
| Parser-unit and resolver-unit tests | T3-T12 (each has tests) |
| README rewrite | T22 |
| Output shape (typed AST) | T3 (types) + T4-T10 (population) |
| INSERT `ON CONFLICT (...) DO ...` UPSERT | T8 (raw slice in `upsert` field) |
| `Unsupported` variant | T3 (definition), T4 (default branch) |
| Migration of clause-body walkers | T13 |
| Migration of consumer sites | T14, T15, T16 |
| Should-fail fixture harness | T18 |
| Integration regressions | T19, T20 |
| Snapshot review with categorization | T21 |
| Architecture README update | T23 |

No gaps identified.

## Placeholder Scan

Searched for the patterns from the writing-plans red flags. None present:
- No "TBD" / "implement later" / "fill in details" markers.
- No "add appropriate error handling" hand-waves; specific error paths and their `MarmotError` mappings are spelled out.
- No "write tests for the above" without code; every task has a Step 1 with the actual test code.
- No "similar to Task N"; code is repeated where needed.
- No references to types or functions defined in nonexistent tasks.
