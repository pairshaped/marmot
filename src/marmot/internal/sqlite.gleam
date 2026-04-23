import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import marmot/internal/query.{
  type Column, type Parameter, Column, Parameter, StringType,
}
import marmot/internal/sqlite/opcode.{type JoinNullability, type Opcode, Opcode}
import marmot/internal/sqlite/parse
import sqlight.{type Connection}

/// Result of introspecting a query's structure.
/// `columns` contains the result columns (empty for INSERT/UPDATE/DELETE without RETURNING).
/// `parameters` contains the `?` parameter types inferred from comparison context.
pub type QueryInfo {
  QueryInfo(columns: List(Column), parameters: List(Parameter))
}

/// Introspect columns of a table using PRAGMA table_info.
/// Note: get_table_metadata below has similar PRAGMA decoding but also extracts
/// primary key info and builds multiple dicts in a single pass.
///
/// Safety: `table` must be a known table name (e.g. from sqlite_master),
/// not arbitrary user input. The PRAGMA context does not support parameterized
/// queries, so we rely on `quote_identifier` for escaping.
pub fn introspect_columns(
  db: Connection,
  table: String,
) -> Result(List(Column), sqlight.Error) {
  let sql = "PRAGMA table_info(\"" <> parse.quote_identifier(table) <> "\")"
  let decoder = {
    use name <- decode.field(1, decode.string)
    use type_str <- decode.field(2, decode.string)
    use notnull <- decode.field(3, decode.int)
    decode.success(#(name, type_str, notnull))
  }

  use rows <- result.try(sqlight.query(
    sql,
    on: db,
    with: [],
    expecting: decoder,
  ))

  let columns =
    list.map(rows, fn(row) {
      let #(name, type_str, notnull) = row
      let column_type = case query.parse_sqlite_type(type_str) {
        Ok(t) -> t
        Error(_) -> StringType
      }
      Column(name: name, column_type: column_type, nullable: notnull == 0)
    })

  Ok(columns)
}

/// Introspect a query using EXPLAIN to determine result columns and parameters
pub fn introspect_query(
  db: Connection,
  sql: String,
) -> Result(QueryInfo, sqlight.Error) {
  // Normalize whitespace (newlines/tabs -> spaces, collapse runs). All keyword
  // detection and SQL parsing below relies on single-space separators.
  let normalized_sql = parse.normalize_sql_whitespace(sql)

  // Get all table metadata in a single pass
  let #(table_schemas, pk_columns, rootpage_table) = get_table_metadata(db)

  // Get EXPLAIN output (strip Marmot-specific `!`/`?` suffixes from aliases
  // before handing to SQLite)
  let sanitized_sql = parse.strip_nullability_suffixes(sql)
  let explain_sql = "EXPLAIN " <> sanitized_sql
  let decoder = {
    use addr <- decode.field(0, decode.int)
    use op <- decode.field(1, decode.string)
    use p1 <- decode.field(2, decode.int)
    use p2 <- decode.field(3, decode.int)
    use p3 <- decode.field(4, decode.int)
    use p4 <- decode.field(5, opcode.flexible_string_decoder())
    use p5 <- decode.field(6, decode.int)
    decode.success(Opcode(
      addr: addr,
      opcode: op,
      p1: p1,
      p2: p2,
      p3: p3,
      p4: p4,
      p5: p5,
    ))
  }

  use opcodes <- result.try(sqlight.query(
    explain_sql,
    on: db,
    with: [],
    expecting: decoder,
  ))

  // Build cursor -> table mapping from OpenRead/OpenWrite opcodes
  let cursor_table =
    list.fold(opcodes, dict.new(), fn(acc, op) {
      case op.opcode {
        "OpenRead" | "OpenWrite" ->
          case dict.get(rootpage_table, op.p2) {
            Ok(table_name) -> dict.insert(acc, op.p1, table_name)
            Error(_) -> acc
          }
        _ -> acc
      }
    })

  let join_nullability =
    opcode.compute_join_nullability(opcodes, cursor_table)

  // Check statement type
  let stmt_type = parse.classify_statement(normalized_sql)
  let is_insert = stmt_type == parse.Insert
  let has_returning = parse.contains_keyword(normalized_sql, "RETURNING")

  // Determine result columns
  let columns = case has_returning {
    True -> {
      let table_name = case stmt_type {
        parse.Insert -> parse.parse_insert_table_name(normalized_sql)
        parse.Update -> parse.parse_update_table_name(normalized_sql)
        parse.Delete -> parse.parse_delete_table_name(normalized_sql)
        _ -> ""
      }
      extract_returning_columns(normalized_sql, table_name, table_schemas)
    }
    False ->
      case is_insert {
        True -> []
        False ->
          extract_result_columns(
            opcodes,
            cursor_table,
            table_schemas,
            pk_columns,
            join_nullability,
            normalized_sql,
          )
      }
  }

  // Determine parameters (use SQL with nullability suffixes stripped so that
  // `col_name?` and `col_name!` aliases are not mistaken for `?` placeholders)
  let param_sql = parse.strip_nullability_suffixes(normalized_sql)
  let parameters =
    extract_parameters(
      opcodes,
      cursor_table,
      table_schemas,
      pk_columns,
      param_sql,
    )

  let parameters = deduplicate_parameter_names(parameters)
  Ok(QueryInfo(columns: columns, parameters: parameters))
}

/// Delegate to parse module for public API compatibility
pub fn strip_nullability_suffixes(sql: String) -> String {
  parse.strip_nullability_suffixes(sql)
}

// ---- Result column extraction (bridges opcode + parse) ----

/// Extract result columns for regular (non-INSERT) queries.
///
/// Strategy: combine opcode-based resolution with text-based fallback.
/// Opcode tracing gives authoritative types when the column maps to a real
/// table column. Text parsing of the SELECT list gives names/types when
/// opcode tracing can't resolve (sorter pseudo-cursors, complex expressions,
/// aggregates).
fn extract_result_columns(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
  join_nullability: JoinNullability,
  sql: String,
) -> List(Column) {
  let result_row = list.find(opcodes, fn(op) { op.opcode == "ResultRow" })
  case result_row {
    Error(_) -> []
    Ok(rr) -> {
      let base_reg = rr.p1
      let count = rr.p2
      let result_regs = parse.make_range(base_reg, count)
      let select_items = parse.parse_select_items(sql)
      let from_tables = parse.parse_from_tables(sql)

      list.index_map(result_regs, fn(reg, idx) {
        let opcode_column = {
          let base =
            opcode.find_column_for_register(
              reg,
              opcodes,
              cursor_table,
              table_schemas,
              pk_columns,
            )
          opcode.apply_cursor_nullability(base, reg, opcodes, join_nullability)
        }
        let select_item = parse.list_at(select_items, idx)
        case select_item {
          Error(_) -> opcode_column
          Ok(item) -> {
            let resolved_col = case item.bare_column {
              option.Some(_) ->
                case
                  resolve_select_item(
                    idx,
                    select_items,
                    from_tables,
                    table_schemas,
                    join_nullability,
                  )
                {
                  Column(name: "unknown", ..) ->
                    Column(..opcode_column, name: item.alias)
                  resolved -> {
                    let opcode_nullable = case opcode_column.name {
                      "unknown" -> False
                      _ -> opcode_column.nullable
                    }
                    Column(
                      ..resolved,
                      nullable: resolved.nullable || opcode_nullable,
                    )
                  }
                }
              option.None ->
                case opcode_column.name {
                  "unknown" ->
                    resolve_select_item(
                      idx,
                      select_items,
                      from_tables,
                      table_schemas,
                      join_nullability,
                    )
                  _ -> Column(..opcode_column, name: item.alias)
                }
            }
            parse.apply_override(resolved_col, item.override)
          }
        }
      })
    }
  }
}

/// Resolve a result column via SELECT-list text parsing.
fn resolve_select_item(
  idx: Int,
  select_items: List(parse.SelectItem),
  from_tables: List(String),
  table_schemas: Dict(String, List(Column)),
  join_nullability: JoinNullability,
) -> Column {
  case parse.list_at(select_items, idx) {
    Error(_) -> Column(name: "unknown", column_type: StringType, nullable: True)
    Ok(item) -> {
      let resolved =
        list.find_map(from_tables, fn(table) {
          case item.bare_column {
            option.Some(col_name) ->
              case dict.get(table_schemas, table) {
                Ok(cols) -> {
                  let lower = string.lowercase(col_name)
                  case
                    list.find(cols, fn(c) { string.lowercase(c.name) == lower })
                  {
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
                }
                Error(_) -> Error(Nil)
              }
            option.None -> Error(Nil)
          }
        })
      let col = case resolved {
        Ok(c) -> c
        Error(_) -> parse.infer_expression_type(item)
      }
      parse.apply_override(col, item.override)
    }
  }
}

/// Extract RETURNING columns by parsing the RETURNING clause from SQL
/// and looking up column metadata from the table schema.
fn extract_returning_columns(
  sql: String,
  table_name: String,
  table_schemas: Dict(String, List(Column)),
) -> List(Column) {
  let returning_cols = parse.parse_returning_columns(sql)
  case returning_cols {
    [] -> []
    ["*"] ->
      case dict.get(table_schemas, table_name) {
        Ok(table_cols) -> table_cols
        Error(_) -> []
      }
    cols ->
      case dict.get(table_schemas, table_name) {
        Ok(table_cols) ->
          list.map(cols, fn(col_name) {
            let lower_name = string.lowercase(col_name)
            case
              list.find(table_cols, fn(c) {
                string.lowercase(c.name) == lower_name
              })
            {
              Ok(col) -> Column(..col, name: col_name)
              Error(_) ->
                Column(name: col_name, column_type: StringType, nullable: True)
            }
          })
        Error(_) ->
          list.map(cols, fn(col_name) {
            Column(name: col_name, column_type: StringType, nullable: True)
          })
      }
  }
}

// ---- Parameter extraction (bridges opcode + parse) ----

/// Extract parameters
fn extract_parameters(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
  sql: String,
) -> List(Parameter) {
  let variable_ops =
    list.filter(opcodes, fn(op) { op.opcode == "Variable" })
    |> list.sort(fn(a, b) { int.compare(a.p1, b.p1) })
    |> opcode.dedupe_variables_by_p1

  let param_count = list.length(variable_ops)
  case param_count {
    0 -> []
    _ -> {
      let stmt_type = parse.classify_statement(sql)
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

      case stmt_type {
        parse.Insert -> {
          case parse.contains_keyword(sql, "VALUES") {
            True -> {
              let parsed = extract_insert_parameters(table_schemas, sql)
              case list.length(parsed) == param_count {
                True -> parsed
                False -> opcode_fallback()
              }
            }
            False -> {
              let parsed = extract_insert_select_parameters(table_schemas, sql)
              case list.length(parsed) == param_count {
                True -> parsed
                False -> opcode_fallback()
              }
            }
          }
        }
        parse.Update -> {
          let parsed = extract_update_parameters(table_schemas, sql)
          case list.length(parsed) == param_count {
            True -> parsed
            False -> opcode_fallback()
          }
        }
        parse.Select | parse.Delete -> {
          let parsed = extract_select_parameters(table_schemas, sql, stmt_type)
          case list.length(parsed) == param_count {
            True -> parsed
            False -> opcode_fallback()
          }
        }
        parse.Other -> opcode_fallback()
      }
    }
  }
}

/// For INSERT statements, parameters correspond to columns in the INSERT column list
fn extract_insert_parameters(
  table_schemas: Dict(String, List(Column)),
  sql: String,
) -> List(Parameter) {
  let columns = parse.parse_insert_columns(sql)
  let table = parse.parse_insert_table_name(sql)
  let values_positions = parse.parse_values_placeholder_positions(sql)
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

  case dict.get(table_schemas, table) {
    Ok(table_cols) ->
      list.map(bound_columns, fn(col_name) {
        case list.find(table_cols, fn(c) { c.name == col_name }) {
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
    Error(_) ->
      list.map(bound_columns, fn(col_name) {
        Parameter(name: col_name, column_type: StringType, nullable: False)
      })
  }
}

/// For INSERT ... SELECT, match parameters positionally
fn extract_insert_select_parameters(
  table_schemas: Dict(String, List(Column)),
  sql: String,
) -> List(Parameter) {
  let target_cols = parse.parse_insert_columns(sql)
  let target_table = parse.parse_insert_table_name(sql)
  let target_col_types = case dict.get(table_schemas, target_table) {
    Ok(cols) -> cols
    Error(_) -> []
  }

  let upper = string.uppercase(sql)
  let marker = ") SELECT "
  case string.split_once(upper, marker) {
    Error(_) -> []
    Ok(#(before, _)) -> {
      let offset = string.length(before) + string.length(marker)
      let after_select = string.drop_start(sql, offset) |> string.trim_start
      let end_idx = case parse.find_top_level_from(after_select) {
        option.Some(idx) -> idx
        option.None -> string.length(after_select)
      }
      let select_list = string.slice(after_select, 0, end_idx)
      let items = parse.split_top_level_commas(select_list)
      let select_params =
        items
        |> list.index_map(fn(item, idx) {
          let trimmed = string.trim(item)
          let is_anon = trimmed == "?"
          let is_named = parse.starts_with_param_prefix(trimmed)
          case is_anon || is_named {
            True ->
              case parse.list_at(target_cols, idx) {
                Ok(col_name) ->
                  case
                    list.find(target_col_types, fn(c) { c.name == col_name })
                  {
                    Ok(col) -> {
                      let param_name = case is_named {
                        True -> string.drop_start(trimmed, 1)
                        False -> col_name
                      }
                      option.Some(Parameter(
                        name: param_name,
                        column_type: col.column_type,
                        nullable: col.nullable,
                      ))
                    }
                    Error(_) -> {
                      let param_name = case is_named {
                        True -> string.drop_start(trimmed, 1)
                        False -> col_name
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
      let from_onwards = string.drop_start(after_select, end_idx)
      let where_tables =
        parse.find_all_subquery_tables(from_onwards)
        |> list.filter(fn(t) {
          case dict.get(table_schemas, t) {
            Ok(_) -> True
            Error(_) -> False
          }
        })
      let where_binders = parse.find_param_binders(from_onwards, 0, [])
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
                case dict.get(table_schemas, table) {
                  Ok(cols) ->
                    case list.find(cols, fn(c) { c.name == name }) {
                      Ok(col) -> Ok(col)
                      Error(_) -> Error(Nil)
                    }
                  Error(_) -> Error(Nil)
                }
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

/// For SELECT/DELETE statements, scan SQL for `column OP ?` patterns
fn extract_select_parameters(
  table_schemas: Dict(String, List(Column)),
  sql: String,
  stmt_type: parse.StatementType,
) -> List(Parameter) {
  let all_tables = collect_all_tables(sql, stmt_type, table_schemas)
  let binders = parse.find_param_binders(sql, 0, [])
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
          case dict.get(table_schemas, table) {
            Ok(cols) ->
              case list.find(cols, fn(c) { c.name == bare }) {
                Ok(col) -> Ok(col)
                Error(_) -> Error(Nil)
              }
            Error(_) -> Error(Nil)
          }
        })
      case found {
        Ok(c) -> Ok(c)
        Error(_) -> resolve_binder_type(rest, all_tables, table_schemas)
      }
    }
  }
}

/// Collect every table referenced in the SQL
fn collect_all_tables(
  sql: String,
  stmt_type: parse.StatementType,
  table_schemas: Dict(String, List(Column)),
) -> List(String) {
  let from_tables = case stmt_type {
    parse.Select -> parse.parse_from_tables(sql)
    parse.Delete -> [parse.parse_delete_table_name(sql)]
    _ -> []
  }
  let subquery_tables = parse.find_all_subquery_tables(sql)
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
  sql: String,
) -> List(Parameter) {
  let table_name = parse.parse_update_table_name(sql)
  let set_params = parse.parse_update_set_columns(sql)
  let where_params = parse.parse_where_columns(sql)

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

// ---- Table metadata ----

/// Get all table metadata in a single pass: schemas, primary keys, and rootpage
/// mappings.
fn get_table_metadata(
  db: Connection,
) -> #(Dict(String, List(Column)), Dict(String, String), Dict(Int, String)) {
  let master_decoder = {
    use name <- decode.field(0, decode.string)
    use rootpage <- decode.field(1, decode.int)
    decode.success(#(name, rootpage))
  }

  let tables =
    sqlight.query(
      "SELECT name, rootpage FROM sqlite_master WHERE type='table'",
      on: db,
      with: [],
      expecting: master_decoder,
    )
    |> result.unwrap([])

  let index_parent_decoder = {
    use rootpage <- decode.field(0, decode.int)
    use tbl_name <- decode.field(1, decode.string)
    decode.success(#(rootpage, tbl_name))
  }
  let indexes =
    sqlight.query(
      "SELECT rootpage, tbl_name FROM sqlite_master WHERE type='index'",
      on: db,
      with: [],
      expecting: index_parent_decoder,
    )
    |> result.unwrap([])

  let pragma_decoder = {
    use col_name <- decode.field(1, decode.string)
    use type_str <- decode.field(2, decode.string)
    use notnull <- decode.field(3, decode.int)
    use pk <- decode.field(5, decode.int)
    decode.success(#(col_name, type_str, notnull, pk))
  }

  list.fold(tables, #(dict.new(), dict.new(), dict.new()), fn(acc, table) {
    let #(schemas, pks, rootpages) = acc
    let #(table_name, rootpage) = table
    let rootpages = dict.insert(rootpages, rootpage, table_name)

    let pragma_sql =
      "PRAGMA table_info(\"" <> parse.quote_identifier(table_name) <> "\")"
    case
      sqlight.query(pragma_sql, on: db, with: [], expecting: pragma_decoder)
    {
      Ok(rows) -> {
        let columns =
          list.map(rows, fn(row) {
            let #(col_name, type_str, notnull, pk) = row
            let column_type = case query.parse_sqlite_type(type_str) {
              Ok(t) -> t
              Error(_) -> StringType
            }
            // SQLite quirk: INTEGER PRIMARY KEY columns have notnull=0 in
            // PRAGMA output because they're rowid aliases (which auto-assign
            // on INSERT), but they're always NOT NULL at read time. Force
            // nullable=False for single-column integer primary keys.
            let nullable = case pk > 0, column_type {
              True, query.IntType -> False
              _, _ -> notnull == 0
            }
            Column(name: col_name, column_type: column_type, nullable: nullable)
          })
        let schemas = dict.insert(schemas, table_name, columns)
        let pks = case list.find(rows, fn(row) { row.3 > 0 }) {
          Ok(#(pk_name, _, _, _)) -> dict.insert(pks, table_name, pk_name)
          Error(_) -> pks
        }
        #(schemas, pks, rootpages)
      }
      Error(_) -> #(schemas, pks, rootpages)
    }
  })
  |> add_index_rootpages(indexes)
}

fn add_index_rootpages(
  acc: #(Dict(String, List(Column)), Dict(String, String), Dict(Int, String)),
  indexes: List(#(Int, String)),
) -> #(Dict(String, List(Column)), Dict(String, String), Dict(Int, String)) {
  let #(schemas, pks, rootpages) = acc
  let rootpages =
    list.fold(indexes, rootpages, fn(acc, entry) {
      let #(rootpage, tbl_name) = entry
      dict.insert(acc, rootpage, tbl_name)
    })
  #(schemas, pks, rootpages)
}

// ---- Parameter deduplication ----

/// Ensure parameter names are unique by appending _2, _3, etc. for duplicates.
fn deduplicate_parameter_names(params: List(Parameter)) -> List(Parameter) {
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

// ---- Returns annotation parser ----

pub type ReturnsAnnotationError {
  InvalidReturnsTypeName(name: String, reason: String)
}

/// Parse the `-- returns: Name` annotation from the top of a SQL file.
pub fn parse_returns_annotation(
  sql: String,
) -> Result(option.Option(String), ReturnsAnnotationError) {
  scan_for_returns(string.split(sql, "\n"))
}

fn scan_for_returns(
  lines: List(String),
) -> Result(option.Option(String), ReturnsAnnotationError) {
  case lines {
    [] -> Ok(option.None)
    [first, ..rest] -> {
      let trimmed = string.trim(first)
      case trimmed {
        "" -> scan_for_returns(rest)
        _ -> {
          case string.starts_with(trimmed, "--") {
            False -> Ok(option.None)
            True -> {
              let body = string.drop_start(trimmed, 2) |> string.trim
              case string.starts_with(body, "returns:") {
                False -> scan_for_returns(rest)
                True -> {
                  let name_part =
                    string.drop_start(body, 8)
                    |> string.trim
                  validate_returns_type_name(name_part)
                  |> result.map(option.Some)
                }
              }
            }
          }
        }
      }
    }
  }
}

fn validate_returns_type_name(
  name: String,
) -> Result(String, ReturnsAnnotationError) {
  case name {
    "" -> Error(InvalidReturnsTypeName(name, "type name is empty"))
    _ -> {
      case string.ends_with(name, "Row") {
        False ->
          Error(InvalidReturnsTypeName(
            name,
            "type name must end with `Row` (e.g., `OrgRow`)",
          ))
        True ->
          case is_valid_pascal_case_identifier(name) {
            False ->
              Error(InvalidReturnsTypeName(
                name,
                "type name must be PascalCase with only letters and digits",
              ))
            True -> Ok(name)
          }
      }
    }
  }
}

fn is_valid_pascal_case_identifier(name: String) -> Bool {
  case string.to_graphemes(name) {
    [] -> False
    [first, ..rest] -> {
      let first_code = char_code(first)
      is_upper(first_code)
      && list.all(rest, fn(ch) {
        let code = char_code(ch)
        is_upper(code) || is_lower(code) || is_ascii_digit(code)
      })
    }
  }
}

fn char_code(ch: String) -> Int {
  case string.to_utf_codepoints(ch) {
    [cp] -> string.utf_codepoint_to_int(cp)
    _ -> 0
  }
}

fn is_upper(code: Int) -> Bool {
  code >= 65 && code <= 90
}

fn is_lower(code: Int) -> Bool {
  code >= 97 && code <= 122
}

fn is_ascii_digit(code: Int) -> Bool {
  code >= 48 && code <= 57
}
