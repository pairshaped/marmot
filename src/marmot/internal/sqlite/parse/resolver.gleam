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
import gleam/option
import marmot/internal/sqlite/parse/statement_parser.{
  type TableBinding, type TableRef,
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
