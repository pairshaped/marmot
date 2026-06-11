//// Result column extraction and RETURNING column handling.

import gleam/dict.{type Dict}
import gleam/list
import gleam/option
import gleam/result
import marmot/internal/query.{type Column, Column, StringType}
import marmot/internal/sqlite/opcode.{type JoinNullability, type Opcode}
import marmot/internal/sqlite/parse/expression
import marmot/internal/sqlite/parse/select
import marmot/internal/sqlite/parse/statement_parser
import marmot/internal/sqlite/parse/util
import marmot/internal/sqlite/tokenize.{type Token, Star}

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
      let #(select_list_tokens, from_tables) = parse_select_details(tokens)
      let select_items = select.parse_select_item_list(select_list_tokens)

      list.index_map(result_regs, fn(reg, idx) {
        extract_result_register_column(
          reg,
          idx,
          opcodes,
          cursor_table,
          table_schemas,
          pk_columns,
          join_nullability,
          select_items,
          from_tables,
        )
      })
    }
  }
}

fn parse_select_details(tokens: List(Token)) -> #(List(Token), List(String)) {
  case statement_parser.parse(tokens) {
    Ok(statement_parser.Select(stmt)) -> {
      let from_names =
        stmt.body.from
        |> list.map(fn(item) { item.binding.table.name.text })
      #(stmt.body.select_list, from_names)
    }
    _ -> #([], [])
  }
}

fn extract_result_register_column(
  reg: Int,
  idx: Int,
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
  join_nullability: JoinNullability,
  select_items: List(select.SelectItem),
  from_tables: List(String),
) -> Column {
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

  case util.list_at(select_items, idx) {
    Error(_) -> result.unwrap(opcode_column, default_column())
    Ok(item) -> resolve_result_select_item(item, opcode_column, text_column)
  }
}

fn resolve_result_select_item(
  item: select.SelectItem,
  opcode_column: Result(Column, Nil),
  text_column: Result(Column, Nil),
) -> Column {
  let resolved_col = case item.bare_column, opcode_column {
    option.Some(_), Ok(op) -> resolve_bare_result_column(item, op, text_column)
    option.Some(_), Error(_) ->
      result.unwrap(text_column, alias_string_column(item.alias))
    option.None, Ok(op) -> resolve_non_bare_opcode_column(item, op, text_column)
    option.None, Error(_) ->
      result.unwrap(text_column, alias_string_column(item.alias))
  }

  expression.apply_override(resolved_col, item.override)
}

fn resolve_bare_result_column(
  item: select.SelectItem,
  op: Column,
  text_column: Result(Column, Nil),
) -> Column {
  case text_column {
    Ok(tc) -> Column(..tc, nullable: tc.nullable || op.nullable)
    Error(_) -> Column(..op, name: item.alias)
  }
}

fn resolve_non_bare_opcode_column(
  item: select.SelectItem,
  op: Column,
  text_column: Result(Column, Nil),
) -> Column {
  let base_type = case text_column {
    Ok(tc) -> tc.column_type
    Error(_) -> op.column_type
  }

  Column(name: item.alias, column_type: base_type, nullable: op.nullable)
}

fn alias_string_column(alias: String) -> Column {
  Column(name: alias, column_type: StringType, nullable: True)
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
          resolve_select_item_from_table(
            item,
            table,
            table_schemas,
            join_nullability,
          )
        })
      let col = case resolved {
        Ok(c) -> c
        Error(_) -> expression.infer_expression_type(item)
      }
      Ok(expression.apply_override(col, item.override))
    }
  }
}

fn resolve_select_item_from_table(
  item: select.SelectItem,
  table: String,
  table_schemas: Dict(String, List(Column)),
  join_nullability: JoinNullability,
) -> Result(Column, Nil) {
  case item.bare_column {
    option.Some(col_name) ->
      resolve_select_item_column(
        item,
        table,
        col_name,
        table_schemas,
        join_nullability,
      )
    option.None -> Error(Nil)
  }
}

fn resolve_select_item_column(
  item: select.SelectItem,
  table: String,
  col_name: String,
  table_schemas: Dict(String, List(Column)),
  join_nullability: JoinNullability,
) -> Result(Column, Nil) {
  case query.find_column_ci(table_schemas, table, col_name) {
    Ok(col) -> {
      let nullable =
        dict.has_key(join_nullability.nullable_tables, table) || col.nullable
      Ok(Column(..col, name: item.alias, nullable: nullable))
    }
    Error(_) -> Error(Nil)
  }
}

/// Extract RETURNING columns from the parsed body slice.
///
/// `returning_tokens` is the slice the statement parser already isolated
/// (everything between `RETURNING` and the next clause boundary), so this
/// function does not need to re-scan the full statement for the keyword.
/// That distinction matters when a table is named `RETURNING`: a global
/// `split_at_keyword` walk would find the table name and produce a bogus
/// boundary; the parser-provided slice only contains the actual RETURNING
/// expressions.
pub fn extract_returning_columns(
  returning_tokens: List(Token),
  table_name: String,
  table_schemas: Dict(String, List(Column)),
) -> List(Column) {
  case returning_tokens {
    [] -> []
    [Star] ->
      case dict.get(table_schemas, table_name) {
        Ok(table_cols) -> table_cols
        Error(_) -> []
      }
    _ ->
      returning_tokens
      |> select.parse_select_item_list
      |> list.map(fn(item) {
        resolve_returning_item(item, table_name, table_schemas)
      })
  }
}

fn resolve_returning_item(
  item: select.SelectItem,
  table_name: String,
  table_schemas: Dict(String, List(Column)),
) -> Column {
  let col = case item.bare_column {
    option.Some(col_name) ->
      case query.find_column_ci(table_schemas, table_name, col_name) {
        Ok(col) -> Column(..col, name: item.alias)
        Error(_) -> alias_string_column(item.alias)
      }
    option.None -> expression.infer_expression_type(item)
  }

  expression.apply_override(col, item.override)
}
