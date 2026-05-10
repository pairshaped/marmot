import gleam/list
import gleam/option.{None, Some}
import marmot/internal/sqlite/parse/statement_parser.{
  CteDef, FromItem, Identifier, Select, SelectBody, SelectStmt, TableBinding,
  TableRef, Unsupported, parse,
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
