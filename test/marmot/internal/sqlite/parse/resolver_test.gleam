import gleam/dict
import gleam/option.{None, Some}
import marmot/internal/sqlite/parse/resolver.{AliasCollision, build_alias_map}
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
