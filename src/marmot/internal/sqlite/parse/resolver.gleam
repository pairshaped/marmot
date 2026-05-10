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
import marmot/internal/query.{type Column}
import marmot/internal/sqlite/parse/statement_parser.{
  type TableBinding, type TableRef,
}

pub type AliasMapError {
  AliasCollision(name: String)
}

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
  // Collect matches AND track whether any in-scope table has no schema entry.
  // The latter is a "could plausibly own this column" signal that gates the
  // CTE/view fallback path (UnknownTableRef) vs. a hard error (UnknownBareColumn).
  let #(matches, has_unknown_table) =
    list.fold(dict.values(map), #([], False), fn(acc, table_ref) {
      let #(matches, has_unknown) = acc
      case dict.get(schemas, table_ref.name.text) {
        Error(_) -> #(matches, True)
        Ok(cols) ->
          case list.find(cols, fn(c) { c.name == col_name }) {
            Ok(col) -> #(
              [ResolvedColumn(table: table_ref, column: col), ..matches],
              has_unknown,
            )
            Error(_) -> #(matches, has_unknown)
          }
      }
    })
  case matches, has_unknown_table {
    [single], _ -> Resolved(single)
    [], True -> UnknownTableRef
    [], False -> UnknownBareColumn
    _, _ -> AmbiguousColumn
  }
}
