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
import marmot/internal/sqlite/opcode.{type Opcode, Opcode}
import marmot/internal/sqlite/parse
import marmot/internal/sqlite/parse/parameters
import marmot/internal/sqlite/parse/select
import marmot/internal/sqlite/parse/statement
import marmot/internal/sqlite/results
import marmot/internal/sqlite/schema
import marmot/internal/sqlite/tokenize.{
  type Token, CloseParen, OpenParen, ParamAnon, ParamNamed, Word,
}
import sqlight.{type Connection}

/// Result of introspecting a query's structure.
/// `columns` contains the result columns (empty for INSERT/UPDATE/DELETE without RETURNING).
/// `parameters` contains the `?` parameter types inferred from comparison context.
pub type QueryInfo {
  QueryInfo(columns: List(Column), parameters: List(Parameter))
}

/// Introspect columns of a table using PRAGMA table_info.
/// Note: schema.get_table_metadata has similar PRAGMA decoding but also extracts
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

/// Introspect a query using EXPLAIN to determine result columns and parameters.
///
/// This is a pipeline: each step delegates to a single-purpose helper. Kept as
/// one function because splitting would scatter the data flow (normalized_sql,
/// opcodes, cursor_table, join_nullability, tokens) across functions called
/// from exactly one call site.
pub fn introspect_query(
  db: Connection,
  sql: String,
) -> Result(QueryInfo, sqlight.Error) {
  // Normalize whitespace (newlines/tabs -> spaces, collapse runs). All keyword
  // detection and SQL parsing below relies on single-space separators.
  let normalized_sql = parse.normalize_sql_whitespace(sql)

  // Get all table metadata in a single pass
  let #(table_schemas, pk_columns, rootpage_table) =
    schema.get_table_metadata(db)

  // Get EXPLAIN output (strip Marmot-specific `!`/`?` suffixes from aliases
  // before handing to SQLite)
  let sanitized_sql = parse.strip_nullability_suffixes(normalized_sql)
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

  let join_nullability = opcode.compute_join_nullability(opcodes, cursor_table)

  // Tokenize once for all analysis
  let tokens = tokenize.tokenize(normalized_sql)

  // Check statement type
  let stmt_type = statement.classify_statement(tokens)
  let is_insert =
    stmt_type == statement.Insert || stmt_type == statement.Replace
  let has_returning = tokenize.has_keyword(tokens, "RETURNING")

  // Determine result columns
  let columns = case has_returning {
    True -> {
      let table_name = case stmt_type {
        statement.Insert | statement.Replace ->
          statement.parse_insert_table_name(tokens)
        statement.Update -> statement.parse_update_table_name(tokens)
        statement.Delete -> statement.parse_delete_table_name(tokens)
        _ -> ""
      }
      results.extract_returning_columns(tokens, table_name, table_schemas)
    }
    False ->
      case is_insert {
        True -> []
        False ->
          results.extract_result_columns(
            opcodes,
            cursor_table,
            table_schemas,
            pk_columns,
            join_nullability,
            tokens,
          )
      }
  }

  // Determine parameters. Tokenize the suffix-stripped SQL separately
  // so `col_name?` and `col_name!` aliases produce ParamAnon/NullableOverride
  // correctly in each context.
  let param_tokens = tokenize.tokenize(sanitized_sql)
  let parameters =
    extract_parameters(
      opcodes,
      cursor_table,
      table_schemas,
      pk_columns,
      param_tokens,
    )

  let parameters = deduplicate_parameter_names(parameters)
  Ok(QueryInfo(columns: columns, parameters: parameters))
}

/// Delegate to parse module for public API compatibility
pub fn strip_nullability_suffixes(sql: String) -> String {
  parse.strip_nullability_suffixes(sql)
}

// ---- Parameter extraction (bridges opcode + parse) ----

/// Extract parameters
fn extract_parameters(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
  tokens: List(Token),
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
  parameters: List(Parameter),
  tokens: List(Token),
) -> List(Parameter) {
  let limit_offset_positions =
    find_limit_offset_param_positions(tokens, 0, 0, [], False)
  list.index_map(parameters, fn(param, idx) {
    case list.contains(limit_offset_positions, idx) {
      True -> Parameter(..param, column_type: query.IntType, nullable: False)
      False -> param
    }
  })
}

fn find_limit_offset_param_positions(
  tokens: List(Token),
  param_count: Int,
  depth: Int,
  acc: List(Int),
  prev_is_limit_offset: Bool,
) -> List(Int) {
  case tokens {
    [] -> list.reverse(acc)
    [OpenParen, ..rest] ->
      find_limit_offset_param_positions(
        rest,
        param_count,
        depth + 1,
        acc,
        prev_is_limit_offset,
      )
    [CloseParen, ..rest] ->
      find_limit_offset_param_positions(
        rest,
        param_count,
        depth - 1,
        acc,
        prev_is_limit_offset,
      )
    [ParamAnon, ..rest] | [ParamNamed(_), ..rest] -> {
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
    [Word(w), ..rest] -> {
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
  tokens: List(Token),
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
  tokens: List(Token),
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
  tokens: List(Token),
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
  tokens: List(Token),
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
  tokens: List(Token),
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
      let first_code = query.char_code(first)
      // First char must be A-Z
      { first_code >= 65 && first_code <= 90 }
      && list.all(rest, fn(ch) {
        let code = query.char_code(ch)
        // A-Z, a-z, 0-9
        { code >= 65 && code <= 90 }
        || { code >= 97 && code <= 122 }
        || { code >= 48 && code <= 57 }
      })
    }
  }
}
