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
