//// Result column extraction and RETURNING column handling.

import gleam/dict.{type Dict}
import gleam/list
import gleam/option
import gleam/result
import marmot/internal/query.{type Column, Column, StringType}
import marmot/internal/sqlite/opcode.{type JoinNullability, type Opcode}
import marmot/internal/sqlite/parse/expression
import marmot/internal/sqlite/parse/select
import marmot/internal/sqlite/parse/util
import marmot/internal/sqlite/tokenize.{type Token}

/// Extract result columns for regular (non-INSERT) queries.
///
/// Strategy: combine opcode-based resolution with text-based fallback.
/// Opcode tracing gives authoritative types when the column maps to a real
/// table column. Text parsing of the SELECT list gives names/types when
/// opcode tracing can't resolve (sorter pseudo-cursors, complex expressions,
/// aggregates).
///
/// Known limitations:
/// - CTEs lose type information because SQLite's EXPLAIN output does not trace
///   column types through CTE boundaries. Columns from CTEs are inferred as
///   StringType/nullable. See "cte" test in sqlite_test.
/// - Complex expressions (subqueries in SELECT, COALESCE, CASE) fall back to
///   text-based name extraction which may miss types. Use CAST in SQL or
///   column aliases to guide inference.
pub fn extract_result_columns(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
  join_nullability: JoinNullability,
  tokens: List(Token),
) -> List(Column) {
  let result_row = list.find(opcodes, fn(op) { op.opcode == "ResultRow" })
  case result_row {
    Error(_) -> []
    Ok(rr) -> {
      let base_reg = rr.p1
      let count = rr.p2
      let result_regs = util.make_range(base_reg, count)
      let select_items = select.parse_select_items(tokens)
      let from_tables = select.parse_from_tables(tokens)

      list.index_map(result_regs, fn(reg, idx) {
        let opcode_column =
          opcode.find_column_for_register(
            reg,
            opcodes,
            cursor_table,
            table_schemas,
            pk_columns,
          )
          |> opcode.apply_cursor_nullability(reg, opcodes, join_nullability)
        let text_column =
          resolve_select_item(
            idx,
            select_items,
            from_tables,
            table_schemas,
            join_nullability,
          )
        let select_item = util.list_at(select_items, idx)
        case select_item {
          Error(_) -> result.unwrap(opcode_column, default_column())
          Ok(item) -> {
            let resolved_col = case item.bare_column, opcode_column {
              option.Some(_), Ok(op) ->
                case text_column {
                  Ok(tc) -> Column(..tc, nullable: tc.nullable || op.nullable)
                  Error(_) -> Column(..op, name: item.alias)
                }
              option.Some(_), Error(_) ->
                case text_column {
                  Ok(tc) -> tc
                  Error(_) ->
                    Column(
                      name: item.alias,
                      column_type: StringType,
                      nullable: True,
                    )
                }
              option.None, Ok(op) -> {
                let base_type = case text_column {
                  Ok(tc) -> tc.column_type
                  Error(_) -> op.column_type
                }
                Column(
                  name: item.alias,
                  column_type: base_type,
                  nullable: op.nullable,
                )
              }
              option.None, Error(_) ->
                case text_column {
                  Ok(tc) -> tc
                  Error(_) ->
                    Column(
                      name: item.alias,
                      column_type: StringType,
                      nullable: True,
                    )
                }
            }
            expression.apply_override(resolved_col, item.override)
          }
        }
      })
    }
  }
}

fn default_column() -> Column {
  Column(name: "unknown", column_type: StringType, nullable: True)
}

/// Resolve a result column via SELECT-list text parsing.
fn resolve_select_item(
  idx: Int,
  select_items: List(select.SelectItem),
  from_tables: List(String),
  table_schemas: Dict(String, List(Column)),
  join_nullability: JoinNullability,
) -> Result(Column, Nil) {
  case list.drop(select_items, idx) |> list.first {
    Error(_) -> Error(Nil)
    Ok(item) -> {
      let resolved =
        list.find_map(from_tables, fn(table) {
          case item.bare_column {
            option.Some(col_name) ->
              case query.find_column_ci(table_schemas, table, col_name) {
                Ok(col) -> {
                  let nullable = case
                    dict.has_key(join_nullability.nullable_tables, table)
                  {
                    True -> True
                    False -> col.nullable
                  }
                  Ok(Column(..col, name: item.alias, nullable: nullable))
                }
                Error(_) -> Error(Nil)
              }
            option.None -> Error(Nil)
          }
        })
      let col = case resolved {
        Ok(c) -> c
        Error(_) -> expression.infer_expression_type(item)
      }
      Ok(expression.apply_override(col, item.override))
    }
  }
}

/// Extract RETURNING columns by parsing the RETURNING clause from SQL
/// and looking up column metadata from the table schema.
pub fn extract_returning_columns(
  tokens: List(Token),
  table_name: String,
  table_schemas: Dict(String, List(Column)),
) -> List(Column) {
  let returning_cols = select.parse_returning_columns(tokens)
  case returning_cols {
    [] -> []
    ["*"] ->
      case dict.get(table_schemas, table_name) {
        Ok(table_cols) -> table_cols
        Error(_) -> []
      }
    cols ->
      list.map(cols, fn(col_name) {
        case query.find_column_ci(table_schemas, table_name, col_name) {
          Ok(col) -> Column(..col, name: col_name)
          Error(_) ->
            Column(name: col_name, column_type: StringType, nullable: True)
        }
      })
  }
}
