//// Parameter extraction from opcodes plus parsed SQL context.

import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import marmot/internal/query.{type Column, type Parameter, Parameter, StringType}
import marmot/internal/sqlite/opcode.{type Opcode}
import marmot/internal/sqlite/parse/parameters
import marmot/internal/sqlite/parse/select
import marmot/internal/sqlite/parse/statement
import marmot/internal/sqlite/tokenize

/// Extract parameters
pub fn extract_parameters(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
  tokens: List(tokenize.Token),
) -> List(Parameter) {
  let variable_ops =
    list.filter(opcodes, fn(op) { op.opcode == "Variable" })
    |> list.sort(fn(a, b) { int.compare(a.p1, b.p1) })
    |> opcode.dedupe_variables_by_p1

  let param_count = list.length(variable_ops)
  case param_count {
    0 -> []
    _ -> {
      let stmt_type = statement.classify_statement(tokens)
      let opcode_fallback = fn() {
        list.map(variable_ops, fn(var_op) {
          opcode.infer_parameter_type(
            var_op,
            opcodes,
            cursor_table,
            table_schemas,
            pk_columns,
          )
        })
      }

      fix_limit_offset_param_types(
        case stmt_type {
          statement.Insert | statement.Replace -> {
            case tokenize.has_keyword(tokens, "VALUES") {
              True -> {
                let parsed = extract_insert_parameters(table_schemas, tokens)
                case list.length(parsed) == param_count {
                  True -> parsed
                  False -> opcode_fallback()
                }
              }
              False -> {
                let parsed =
                  extract_insert_select_parameters(table_schemas, tokens)
                case list.length(parsed) == param_count {
                  True -> parsed
                  False -> opcode_fallback()
                }
              }
            }
          }
          statement.Update -> {
            let parsed = extract_update_parameters(table_schemas, tokens)
            case list.length(parsed) == param_count {
              True -> parsed
              False -> opcode_fallback()
            }
          }
          statement.Select | statement.Delete -> {
            let parsed =
              extract_select_parameters(table_schemas, tokens, stmt_type)
            case list.length(parsed) == param_count {
              True -> parsed
              False -> opcode_fallback()
            }
          }
          statement.Other -> opcode_fallback()
        },
        tokens,
      )
    }
  }
}

/// Override LIMIT ? and OFFSET ? parameter types to IntType.
/// These never participate in column comparisons, so both text-based
/// and opcode-based inference miss them.
fn fix_limit_offset_param_types(
  params: List(Parameter),
  tokens: List(tokenize.Token),
) -> List(Parameter) {
  let limit_offset_positions =
    find_limit_offset_param_positions(tokens, 0, 0, [], False)
  list.index_map(params, fn(param, idx) {
    case list.contains(limit_offset_positions, idx) {
      True -> Parameter(..param, column_type: query.IntType, nullable: False)
      False -> param
    }
  })
}

fn find_limit_offset_param_positions(
  tokens: List(tokenize.Token),
  param_count: Int,
  depth: Int,
  acc: List(Int),
  prev_is_limit_offset: Bool,
) -> List(Int) {
  case tokens {
    [] -> list.reverse(acc)
    [tokenize.OpenParen, ..rest] ->
      find_limit_offset_param_positions(
        rest,
        param_count,
        depth + 1,
        acc,
        prev_is_limit_offset,
      )
    [tokenize.CloseParen, ..rest] ->
      find_limit_offset_param_positions(
        rest,
        param_count,
        depth - 1,
        acc,
        prev_is_limit_offset,
      )
    [tokenize.ParamAnon, ..rest] | [tokenize.ParamNamed(_), ..rest] -> {
      let new_acc = case prev_is_limit_offset && depth == 0 {
        True -> [param_count, ..acc]
        False -> acc
      }
      find_limit_offset_param_positions(
        rest,
        param_count + 1,
        depth,
        new_acc,
        False,
      )
    }
    [tokenize.Word(w), ..rest] -> {
      let upper = string.uppercase(w)
      let is_limit_or_offset = upper == "LIMIT" || upper == "OFFSET"
      let new_prev = case is_limit_or_offset && depth == 0 {
        True -> True
        False -> prev_is_limit_offset
      }
      find_limit_offset_param_positions(rest, param_count, depth, acc, new_prev)
    }
    [_, ..rest] ->
      find_limit_offset_param_positions(
        rest,
        param_count,
        depth,
        acc,
        prev_is_limit_offset,
      )
  }
}

/// For INSERT statements, parameters correspond to columns in the INSERT column list
fn extract_insert_parameters(
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
) -> List(Parameter) {
  let columns = parameters.parse_insert_columns(tokens)
  let table = statement.parse_insert_table_name(tokens)
  let values_positions = parameters.parse_values_placeholder_positions(tokens)
  let bound_columns = case values_positions {
    [] -> columns
    _ ->
      list.index_map(columns, fn(col_name, idx) { #(col_name, idx) })
      |> list.filter_map(fn(pair) {
        let #(col_name, idx) = pair
        case list.contains(values_positions, idx) {
          True -> Ok(col_name)
          False -> Error(Nil)
        }
      })
  }

  list.map(bound_columns, fn(col_name) {
    case query.find_column(table_schemas, table, col_name) {
      Ok(col) ->
        Parameter(
          name: col_name,
          column_type: col.column_type,
          nullable: col.nullable,
        )
      Error(_) ->
        Parameter(name: col_name, column_type: StringType, nullable: False)
    }
  })
}

/// For INSERT ... SELECT, match parameters positionally
fn extract_insert_select_parameters(
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
) -> List(Parameter) {
  let target_cols = parameters.parse_insert_columns(tokens)
  let target_table = statement.parse_insert_table_name(tokens)
  let target_col_types = case dict.get(table_schemas, target_table) {
    Ok(cols) -> cols
    Error(_) -> []
  }

  // Find SELECT after the ) that closes the column list
  case tokenize.split_at_keyword(tokens, "SELECT") {
    Error(_) -> []
    Ok(#(_, after_select)) -> {
      // SELECT list ends at FROM
      let select_tokens = tokenize.take_until_keywords(after_select, ["FROM"])
      let items = tokenize.split_on_commas(select_tokens)
      let select_params =
        items
        |> list.index_map(fn(item_tokens, idx) {
          let is_anon = item_tokens == [tokenize.ParamAnon]
          let is_named = case item_tokens {
            [tokenize.ParamNamed(_)] -> True
            _ -> False
          }
          case is_anon || is_named {
            True ->
              case list.drop(target_cols, idx) |> list.first {
                Ok(col_name) ->
                  case
                    list.find(target_col_types, fn(c) { c.name == col_name })
                  {
                    Ok(col) -> {
                      let param_name = case item_tokens {
                        [tokenize.ParamNamed(n)] -> n
                        _ -> col_name
                      }
                      option.Some(Parameter(
                        name: param_name,
                        column_type: col.column_type,
                        nullable: col.nullable,
                      ))
                    }
                    Error(_) -> {
                      let param_name = case item_tokens {
                        [tokenize.ParamNamed(n)] -> n
                        _ -> col_name
                      }
                      option.Some(Parameter(
                        name: param_name,
                        column_type: StringType,
                        nullable: False,
                      ))
                    }
                  }
                Error(_) -> option.None
              }
            False -> option.None
          }
        })
        |> list.filter_map(fn(o) {
          case o {
            option.Some(p) -> Ok(p)
            option.None -> Error(Nil)
          }
        })

      // Tokens from FROM onward for WHERE params
      let from_onwards = tokenize.drop_until_keyword(after_select, "FROM")
      let where_tables =
        parameters.find_all_subquery_tables(from_onwards)
        |> list.filter(fn(t) {
          case dict.get(table_schemas, t) {
            Ok(_) -> True
            Error(_) -> False
          }
        })
      let where_binders = parameters.find_param_binders(from_onwards)
      let select_param_names = list.map(select_params, fn(p) { p.name })
      let unmatched_where_binders =
        list.filter(where_binders, fn(b) {
          !list.contains(select_param_names, b.name)
        })
      let where_params =
        list.map(unmatched_where_binders, fn(binder) {
          let names_to_try = case binder.binder_column {
            option.Some(col) -> {
              let bare_col = case string.split_once(col, ".") {
                Ok(#(_, after)) -> after
                Error(_) -> col
              }
              case bare_col == binder.name {
                True -> [binder.name]
                False -> [bare_col, binder.name]
              }
            }
            option.None -> [binder.name]
          }
          case
            list.find_map(names_to_try, fn(name) {
              list.find_map(where_tables, fn(table) {
                query.find_column(table_schemas, table, name)
              })
            })
          {
            Ok(col) ->
              Parameter(
                name: binder.name,
                column_type: col.column_type,
                nullable: col.nullable,
              )
            Error(_) ->
              Parameter(
                name: binder.name,
                column_type: StringType,
                nullable: False,
              )
          }
        })
      list.append(select_params, where_params)
    }
  }
}

/// For SELECT/DELETE statements, scan tokens for `column OP ?` patterns
fn extract_select_parameters(
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
  stmt_type: statement.StatementType,
) -> List(Parameter) {
  let all_tables = collect_all_tables(tokens, stmt_type, table_schemas)
  let binders = parameters.find_param_binders(tokens)
  case list.is_empty(binders) {
    True -> []
    False ->
      list.map(binders, fn(binder) {
        let names_to_try = case binder.binder_column {
          option.Some(col) if col != binder.name -> [binder.name, col]
          _ -> [binder.name]
        }
        let resolved =
          resolve_binder_type(names_to_try, all_tables, table_schemas)
        case resolved {
          Ok(col) ->
            Parameter(
              name: binder.name,
              column_type: col.column_type,
              nullable: col.nullable,
            )
          Error(_) ->
            Parameter(
              name: binder.name,
              column_type: StringType,
              nullable: False,
            )
        }
      })
  }
}

fn resolve_binder_type(
  names: List(String),
  all_tables: List(String),
  table_schemas: Dict(String, List(Column)),
) -> Result(Column, Nil) {
  case names {
    [] -> Error(Nil)
    [name, ..rest] -> {
      let bare = case string.split_once(name, ".") {
        Ok(#(_, after)) -> after
        Error(_) -> name
      }
      let found =
        list.find_map(all_tables, fn(table) {
          query.find_column(table_schemas, table, bare)
        })
      case found {
        Ok(c) -> Ok(c)
        Error(_) -> resolve_binder_type(rest, all_tables, table_schemas)
      }
    }
  }
}

/// Collect every table referenced in the tokens
fn collect_all_tables(
  tokens: List(tokenize.Token),
  stmt_type: statement.StatementType,
  table_schemas: Dict(String, List(Column)),
) -> List(String) {
  let from_tables = case stmt_type {
    statement.Select -> select.parse_from_tables(tokens)
    statement.Delete -> [statement.parse_delete_table_name(tokens)]
    _ -> []
  }
  let subquery_tables = parameters.find_all_subquery_tables(tokens)
  let combined = list.append(from_tables, subquery_tables)
  combined
  |> list.filter(fn(t) {
    case dict.get(table_schemas, t) {
      Ok(_) -> True
      Error(_) -> False
    }
  })
  |> list.unique
}

/// For UPDATE statements
fn extract_update_parameters(
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
) -> List(Parameter) {
  let table_name = statement.parse_update_table_name(tokens)
  let set_params = parameters.parse_update_set_columns(tokens)
  let where_params = parameters.parse_where_columns(tokens)

  case dict.get(table_schemas, table_name) {
    Ok(table_cols) -> {
      let set_parameters =
        list.map(set_params, fn(param) {
          let #(param_name, lookup_col) = param
          case list.find(table_cols, fn(c) { c.name == lookup_col }) {
            Ok(col) ->
              Parameter(
                name: param_name,
                column_type: col.column_type,
                nullable: col.nullable,
              )
            Error(_) ->
              Parameter(
                name: param_name,
                column_type: StringType,
                nullable: False,
              )
          }
        })
      let where_parameters =
        list.map(where_params, fn(param) {
          let #(param_name, lookup_col) = param
          let found_col =
            list.find(table_cols, fn(c) { c.name == lookup_col })
            |> result.lazy_or(fn() {
              dict.values(table_schemas)
              |> list.flatten
              |> list.find(fn(c) { c.name == lookup_col })
            })
          case found_col {
            Ok(col) ->
              Parameter(
                name: param_name,
                column_type: col.column_type,
                nullable: col.nullable,
              )
            Error(_) ->
              Parameter(
                name: param_name,
                column_type: StringType,
                nullable: False,
              )
          }
        })
      list.append(set_parameters, where_parameters)
    }
    Error(_) -> {
      let set_parameters =
        list.map(set_params, fn(param) {
          let #(param_name, _lookup_col) = param
          Parameter(name: param_name, column_type: StringType, nullable: False)
        })
      let where_parameters =
        list.map(where_params, fn(param) {
          let #(param_name, _lookup_col) = param
          Parameter(name: param_name, column_type: StringType, nullable: False)
        })
      list.append(set_parameters, where_parameters)
    }
  }
}

// ---- Parameter deduplication ----

/// Ensure parameter names are unique by appending _2, _3, etc. for duplicates.
pub fn deduplicate_parameter_names(params: List(Parameter)) -> List(Parameter) {
  deduplicate_params_loop(params, dict.new(), [])
  |> list.reverse
}

fn deduplicate_params_loop(
  params: List(Parameter),
  seen: Dict(String, Int),
  acc: List(Parameter),
) -> List(Parameter) {
  case params {
    [] -> acc
    [p, ..rest] ->
      case dict.get(seen, p.name) {
        Ok(count) -> {
          let new_name = p.name <> "_" <> int.to_string(count + 1)
          deduplicate_params_loop(rest, dict.insert(seen, p.name, count + 1), [
            Parameter(..p, name: new_name),
            ..acc
          ])
        }
        Error(_) ->
          deduplicate_params_loop(rest, dict.insert(seen, p.name, 1), [p, ..acc])
      }
  }
}
