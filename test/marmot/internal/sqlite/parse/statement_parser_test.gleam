import gleam/option.{None, Some}
import marmot/internal/sqlite/parse/statement_parser.{
  Identifier, Select, SelectBody, SelectStmt, TableBinding, TableRef,
  Unsupported, parse,
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
  // Parsed FromItems are filled in by Task 5; here we only verify slicing
  // doesn't produce a parse error and that no spurious clause boundaries fire.
  let assert Ok(Select(SelectStmt(_, body))) =
    parse_sql("SELECT * FROM returning")
  let assert [] = body.from
}
