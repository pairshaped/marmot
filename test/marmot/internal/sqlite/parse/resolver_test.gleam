import gleam/dict
import gleam/option.{None, Some}
import marmot/internal/query.{Column, IntType, StringType}
import marmot/internal/sqlite/parse/resolver.{
  AliasCollision, AmbiguousColumn, Resolved, ResolvedColumn, UnknownBareColumn,
  UnknownColumnInKnownTable, UnknownQualifiedAlias, UnknownTableRef,
  build_alias_map, resolve_bare, resolve_qualified,
}
import marmot/internal/sqlite/parse/statement_parser.{
  Identifier, TableBinding, TableRef,
}

fn binding(name: String, alias: option.Option(String)) {
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
  let bindings = [
    binding("users", Some("u")),
    binding("users", Some("manager")),
  ]
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
  let assert UnknownQualifiedAlias =
    resolve_qualified(map, "x", "id", schemas())
}

pub fn resolve_qualified_unknown_column_test() {
  let assert Ok(map) = build_alias_map([binding("users", Some("u"))])
  let assert UnknownColumnInKnownTable =
    resolve_qualified(map, "u", "nope", schemas())
}

pub fn resolve_qualified_unknown_table_test() {
  let assert Ok(map) = build_alias_map([binding("foo_cte", None)])
  let assert UnknownTableRef = resolve_qualified(map, "foo_cte", "x", schemas())
}

pub fn resolve_bare_unique_test() {
  let assert Ok(map) = build_alias_map([binding("users", Some("u"))])
  let assert Resolved(ResolvedColumn(_, Column("email", _, _))) =
    resolve_bare(map, "email", schemas())
}

pub fn resolve_bare_ambiguous_test() {
  let assert Ok(map) =
    build_alias_map([binding("users", Some("u")), binding("orders", Some("o"))])
  let assert AmbiguousColumn = resolve_bare(map, "id", schemas())
}

pub fn resolve_bare_unknown_when_all_tables_known_test() {
  let assert Ok(map) = build_alias_map([binding("users", Some("u"))])
  let assert UnknownBareColumn = resolve_bare(map, "missing", schemas())
}

pub fn resolve_bare_falls_back_when_unknown_table_in_scope_test() {
  // CTE/view fallback: a CTE-named table has no schema entry, so bare
  // references that don't match known columns return UnknownTableRef
  // (silent fallback) rather than UnknownBareColumn (hard error).
  let assert Ok(map) =
    build_alias_map([binding("users", Some("u")), binding("foo_cte", None)])
  let assert UnknownTableRef = resolve_bare(map, "x", schemas())
}
