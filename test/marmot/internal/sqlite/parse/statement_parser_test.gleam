import gleam/list
import gleam/option.{None, Some}
import marmot/internal/sqlite/parse/statement_parser.{
  ConflictAbort, ConflictFail, ConflictIgnore, ConflictReplace, ConflictRollback,
  CteDef, DefaultValuesSource, FromItem, Identifier, Insert, InsertStmt, Select,
  SelectBody, SelectSource, SelectStmt, TableBinding, TableRef, Unsupported,
  ValuesSource, parse,
}
import marmot/internal/sqlite/tokenize.{Word}

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
  let assert [
    FromItem(
      binding: TableBinding(
        table: TableRef(schema: None, name: Identifier("returning", False)),
        alias: None,
      ),
      on: None,
    ),
  ] = body.from
}

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
