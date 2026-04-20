import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import marmot/internal/query.{
  type Column, type Parameter, Column, Parameter, StringType,
}
import sqlight.{type Connection}

/// Result of introspecting a query's structure.
/// `columns` contains the result columns (empty for INSERT/UPDATE/DELETE without RETURNING).
/// `parameters` contains the `?` parameter types inferred from comparison context.
pub type QueryInfo {
  QueryInfo(columns: List(Column), parameters: List(Parameter))
}

type StatementType {
  Select
  Insert
  Update
  Delete
  Other
}

fn classify_statement(sql: String) -> StatementType {
  let upper = string.uppercase(string.trim(sql))
  case string.starts_with(upper, "SELECT") {
    True -> Select
    False ->
      case string.starts_with(upper, "INSERT") {
        True -> Insert
        False ->
          case string.starts_with(upper, "UPDATE") {
            True -> Update
            False ->
              case string.starts_with(upper, "DELETE") {
                True -> Delete
                False -> Other
              }
          }
      }
  }
}

/// Introspect columns of a table using PRAGMA table_info.
/// Note: get_table_metadata below has similar PRAGMA decoding but also extracts
/// primary key info and builds multiple dicts in a single pass.
pub fn introspect_columns(
  db: Connection,
  table: String,
) -> Result(List(Column), sqlight.Error) {
  let sql = "PRAGMA table_info(\"" <> quote_identifier(table) <> "\")"
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

/// Opcode from EXPLAIN output
type Opcode {
  Opcode(
    addr: Int,
    opcode: String,
    p1: Int,
    p2: Int,
    p3: Int,
    p4: String,
    p5: Int,
  )
}

/// Introspect a query using EXPLAIN to determine result columns and parameters
pub fn introspect_query(
  db: Connection,
  sql: String,
) -> Result(QueryInfo, sqlight.Error) {
  // Get all table metadata in a single pass
  let #(table_schemas, pk_columns, rootpage_table) = get_table_metadata(db)

  // Get EXPLAIN output
  let explain_sql = "EXPLAIN " <> sql
  let decoder = {
    use addr <- decode.field(0, decode.int)
    use opcode <- decode.field(1, decode.string)
    use p1 <- decode.field(2, decode.int)
    use p2 <- decode.field(3, decode.int)
    use p3 <- decode.field(4, decode.int)
    use p4 <- decode.field(5, flexible_string_decoder())
    use p5 <- decode.field(6, decode.int)
    decode.success(Opcode(
      addr: addr,
      opcode: opcode,
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

  // Check statement type
  let stmt_type = classify_statement(sql)
  let is_insert = stmt_type == Insert
  let has_returning = contains_keyword(sql, "RETURNING")

  // Determine result columns
  let columns = case has_returning {
    True -> {
      let table_name = case stmt_type {
        Insert -> parse_insert_table_name(sql)
        Update -> parse_update_table_name(sql)
        Delete -> parse_delete_table_name(sql)
        _ -> ""
      }
      extract_returning_columns(sql, table_name, table_schemas)
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
          )
      }
  }

  // Determine parameters
  let parameters =
    extract_parameters(opcodes, cursor_table, table_schemas, pk_columns, sql)

  let parameters = deduplicate_parameter_names(parameters)
  Ok(QueryInfo(columns: columns, parameters: parameters))
}

/// Extract result columns for regular (non-INSERT) queries
fn extract_result_columns(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
) -> List(Column) {
  let result_row = list.find(opcodes, fn(op) { op.opcode == "ResultRow" })

  case result_row {
    Error(_) -> []
    Ok(rr) -> {
      let base_reg = rr.p1
      let count = rr.p2
      let result_regs = make_range(base_reg, count)

      list.map(result_regs, fn(reg) {
        find_column_for_register(
          reg,
          opcodes,
          cursor_table,
          table_schemas,
          pk_columns,
        )
      })
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
  let returning_cols = parse_returning_columns(sql)
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

/// Parse RETURNING column names from SQL, handling aliases (e.g., "id AS user_id")
fn parse_returning_columns(sql: String) -> List(String) {
  let upper = string.uppercase(sql)
  case split_on_keyword(upper, " RETURNING ") {
    Ok(#(before, _)) -> {
      // Skip past " RETURNING " in the original string
      let offset = string.length(before) + string.length(" RETURNING ")
      let original_rest = string.drop_start(sql, offset)
      original_rest
      |> string.trim
      |> string.split(",")
      |> list.map(fn(col) {
        let trimmed = string.trim(col)
        // Handle "expr AS alias" -- use the alias as the column name
        // Split on uppercased string to find position, then extract from original
        case string.split_once(string.uppercase(trimmed), " AS ") {
          Ok(#(before_as, _)) -> {
            let alias_start = string.length(before_as) + 4
            // " AS " is 4 chars
            string.drop_start(trimmed, alias_start) |> string.trim
          }
          Error(_) -> trimmed
        }
      })
    }
    Error(_) -> []
  }
}

/// Find the Column/Rowid opcode that writes to a given register and resolve type
fn find_column_for_register(
  reg: Int,
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
) -> Column {
  // Check for Rowid opcode first (p2=dest_register)
  let rowid_op =
    list.find(opcodes, fn(op) { op.opcode == "Rowid" && op.p2 == reg })

  case rowid_op {
    Ok(op) ->
      case dict.get(cursor_table, op.p1) {
        Ok(table) -> resolve_rowid_column(table, table_schemas, pk_columns)
        Error(_) ->
          Column(name: "rowid", column_type: query.IntType, nullable: False)
      }
    Error(_) -> {
      // Check for Column opcode (p3=dest_register)
      let column_op =
        list.find(opcodes, fn(op) { op.opcode == "Column" && op.p3 == reg })

      case column_op {
        Ok(op) -> resolve_column(op.p1, op.p2, cursor_table, table_schemas)
        Error(_) ->
          Column(name: "unknown", column_type: StringType, nullable: True)
      }
    }
  }
}

/// Resolve the rowid (INTEGER PRIMARY KEY) column for a table.
/// Uses the pk field from PRAGMA table_info to correctly identify the PK column.
fn resolve_rowid_column(
  table: String,
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
) -> Column {
  case dict.get(pk_columns, table) {
    Ok(pk_name) ->
      case dict.get(table_schemas, table) {
        Ok(table_cols) ->
          case list.find(table_cols, fn(c) { c.name == pk_name }) {
            Ok(col) -> col
            Error(_) ->
              Column(name: pk_name, column_type: query.IntType, nullable: False)
          }
        Error(_) ->
          Column(name: pk_name, column_type: query.IntType, nullable: False)
      }
    Error(_) ->
      Column(name: "rowid", column_type: query.IntType, nullable: False)
  }
}

/// Look up a column by cursor and index
fn resolve_column(
  cursor: Int,
  col_idx: Int,
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
) -> Column {
  case dict.get(cursor_table, cursor) {
    Ok(table_name) ->
      case dict.get(table_schemas, table_name) {
        Ok(table_cols) ->
          case list_at(table_cols, col_idx) {
            Ok(col) -> col
            Error(_) ->
              Column(name: "unknown", column_type: StringType, nullable: True)
          }
        Error(_) ->
          Column(name: "unknown", column_type: StringType, nullable: True)
      }
    Error(_) -> Column(name: "unknown", column_type: StringType, nullable: True)
  }
}

/// Create a list of integers from start to start+count-1
fn make_range(start: Int, count: Int) -> List(Int) {
  case count <= 0 {
    True -> []
    False ->
      int.range(from: start, to: start + count, with: [], run: fn(acc, i) {
        [i, ..acc]
      })
      |> list.reverse
  }
}

/// Strip table prefixes ("t.name" -> "name") and function wrappers
/// ("LOWER(name)" -> "name") from a column reference.
fn normalize_column_ref(raw: String) -> String {
  // Strip table prefix
  let name = case string.split_once(raw, ".") {
    Ok(#(_, col)) -> col
    Error(_) -> raw
  }
  // Strip function wrapper
  case string.split_once(name, "(") {
    Ok(#(_, rest)) ->
      case string.split_once(rest, ")") {
        Ok(#(inner, _)) -> string.trim(inner)
        Error(_) -> name
      }
    Error(_) -> name
  }
}

/// Split a string on a SQL keyword, matching only as a whole word.
/// The keyword should include surrounding spaces (e.g., " SET ", " WHERE ").
/// Returns the parts before and after the keyword (excluding the keyword).
fn split_on_keyword(
  haystack: String,
  keyword: String,
) -> Result(#(String, String), Nil) {
  case string.split_once(haystack, keyword) {
    Ok(#(before, after)) -> Ok(#(before, after))
    Error(_) -> {
      // Also try keyword at end of string (no trailing space)
      let trimmed_keyword = string.trim_end(keyword)
      case string.ends_with(haystack, trimmed_keyword) {
        True -> {
          let before_len =
            string.length(haystack) - string.length(trimmed_keyword)
          Ok(#(string.slice(haystack, 0, before_len), ""))
        }
        False -> Error(Nil)
      }
    }
  }
}

/// Check whether a SQL keyword appears as a whole word in the SQL string.
/// Avoids matching substrings (e.g., "RETURNING" inside a table name).
fn contains_keyword(sql: String, keyword: String) -> Bool {
  let upper = string.uppercase(sql)
  // Check with surrounding spaces
  case string.contains(upper, " " <> keyword <> " ") {
    True -> True
    False ->
      // Check at end of string with leading space
      case string.ends_with(string.trim(upper), keyword) {
        True -> {
          let trimmed = string.trim(upper)
          let idx = string.length(trimmed) - string.length(keyword)
          case idx > 0 {
            True -> {
              let before = string.slice(trimmed, idx - 1, 1)
              before == " " || before == "\n" || before == "\t"
            }
            False -> False
          }
        }
        False -> False
      }
  }
}

/// Escape double quotes in an identifier to prevent SQL injection.
fn quote_identifier(name: String) -> String {
  string.replace(name, "\"", "\"\"")
}

/// Get element at index from a list
fn list_at(lst: List(a), idx: Int) -> Result(a, Nil) {
  lst |> list.drop(idx) |> list.first
}

/// Extract parameters
fn extract_parameters(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
  sql: String,
) -> List(Parameter) {
  // Find all Variable opcodes sorted by p1 (parameter number)
  let variable_ops =
    list.filter(opcodes, fn(op) { op.opcode == "Variable" })
    |> list.sort(fn(a, b) { int.compare(a.p1, b.p1) })

  let param_count = list.length(variable_ops)
  case param_count {
    0 -> []
    _ -> {
      let stmt_type = classify_statement(sql)
      let opcode_fallback = fn() {
        list.map(variable_ops, fn(var_op) {
          infer_parameter_type(
            var_op,
            opcodes,
            cursor_table,
            table_schemas,
            pk_columns,
          )
        })
      }

      case stmt_type {
        Insert -> {
          let parsed = extract_insert_parameters(table_schemas, sql)
          // If the string parser's parameter count doesn't match actual
          // placeholders (e.g. INSERT...SELECT), fall back to EXPLAIN opcodes
          case list.length(parsed) == param_count {
            True -> parsed
            False -> opcode_fallback()
          }
        }
        Update -> {
          let parsed = extract_update_parameters(table_schemas, sql)
          case list.length(parsed) == param_count {
            True -> parsed
            False -> opcode_fallback()
          }
        }
        _ -> opcode_fallback()
      }
    }
  }
}

/// For INSERT statements, parameters correspond to columns in the INSERT column list
fn extract_insert_parameters(
  table_schemas: Dict(String, List(Column)),
  sql: String,
) -> List(Parameter) {
  let columns = parse_insert_columns(sql)
  let table = parse_insert_table_name(sql)

  case dict.get(table_schemas, table) {
    Ok(table_cols) ->
      list.map(columns, fn(col_name) {
        let col_type =
          list.find(table_cols, fn(c) { c.name == col_name })
          |> result.map(fn(c: Column) { c.column_type })
          |> result.unwrap(StringType)
        Parameter(name: col_name, column_type: col_type)
      })
    Error(_) ->
      list.map(columns, fn(col_name) {
        Parameter(name: col_name, column_type: StringType)
      })
  }
}

/// For UPDATE statements, parameters correspond to SET columns then WHERE columns
fn extract_update_parameters(
  table_schemas: Dict(String, List(Column)),
  sql: String,
) -> List(Parameter) {
  let table_name = parse_update_table_name(sql)
  let set_cols = parse_update_set_columns(sql)
  let where_cols = parse_where_columns(sql)
  let all_cols = list.append(set_cols, where_cols)

  case dict.get(table_schemas, table_name) {
    Ok(table_cols) ->
      list.map(all_cols, fn(col_name) {
        let col_type =
          list.find(table_cols, fn(c) { c.name == col_name })
          |> result.map(fn(c: Column) { c.column_type })
          |> result.unwrap(StringType)
        Parameter(name: col_name, column_type: col_type)
      })
    Error(_) ->
      list.map(all_cols, fn(col_name) {
        Parameter(name: col_name, column_type: StringType)
      })
  }
}

/// Infer parameter type from comparison context
/// Key insight: when a register is reused, find the Column opcode closest
/// to (but before) the comparison that uses it.
fn infer_parameter_type(
  var_op: Opcode,
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
) -> Parameter {
  let var_reg = var_op.p2
  let comparison_ops = [
    "Eq", "Ne", "Lt", "Le", "Gt", "Ge", "SeekGE", "SeekGT", "SeekLE", "SeekLT",
  ]

  // Find a comparison that involves our variable register
  let comparison =
    list.find(opcodes, fn(op) {
      list.contains(comparison_ops, op.opcode)
      && { op.p1 == var_reg || op.p3 == var_reg }
    })

  case comparison {
    Ok(cmp) -> {
      let other_reg = case cmp.p1 == var_reg {
        True -> cmp.p3
        False -> cmp.p1
      }
      // Find the Column opcode that writes to other_reg,
      // closest to (but before) the comparison instruction
      find_nearest_column_source(
        other_reg,
        cmp.addr,
        opcodes,
        cursor_table,
        table_schemas,
      )
    }
    Error(_) -> {
      // Check for SeekRowid - Variable used as rowid
      let seek_rowid =
        list.find(opcodes, fn(op) {
          op.opcode == "SeekRowid" && op.p3 == var_reg
        })
      case seek_rowid {
        Ok(sr) ->
          case dict.get(cursor_table, sr.p1) {
            Ok(table) ->
              case dict.get(pk_columns, table) {
                Ok(pk_name) ->
                  Parameter(name: pk_name, column_type: query.IntType)
                Error(_) -> Parameter(name: "id", column_type: query.IntType)
              }
            Error(_) -> Parameter(name: "id", column_type: query.IntType)
          }
        Error(_) -> Parameter(name: "param", column_type: StringType)
      }
    }
  }
}

/// Find the Column opcode that writes to target_reg, closest to but before cmp_addr
fn find_nearest_column_source(
  target_reg: Int,
  cmp_addr: Int,
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
) -> Parameter {
  // Filter Column opcodes that write to target_reg and are before cmp_addr
  let candidates =
    list.filter(opcodes, fn(op) {
      op.opcode == "Column" && op.p3 == target_reg && op.addr < cmp_addr
    })

  // Pick the one closest to cmp_addr (highest addr)
  let best: Result(Opcode, Nil) =
    list.fold(candidates, Error(Nil), fn(acc: Result(Opcode, Nil), op) {
      case acc {
        Error(_) -> Ok(op)
        Ok(prev) ->
          case op.addr > prev.addr {
            True -> Ok(op)
            False -> Ok(prev)
          }
      }
    })

  case best {
    Ok(cop) ->
      resolve_column_to_parameter(cop.p1, cop.p2, cursor_table, table_schemas)
    Error(_) -> Parameter(name: "param", column_type: StringType)
  }
}

/// Resolve a cursor + column index to a Parameter
fn resolve_column_to_parameter(
  cursor: Int,
  col_idx: Int,
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
) -> Parameter {
  case dict.get(cursor_table, cursor) {
    Ok(table_name) ->
      case dict.get(table_schemas, table_name) {
        Ok(table_cols) ->
          case list_at(table_cols, col_idx) {
            Ok(col) -> Parameter(name: col.name, column_type: col.column_type)
            Error(_) -> Parameter(name: "param", column_type: StringType)
          }
        Error(_) -> Parameter(name: "param", column_type: StringType)
      }
    Error(_) -> Parameter(name: "param", column_type: StringType)
  }
}

/// Extract the first word after a keyword in SQL (case-insensitive).
/// Used for parsing table names from INSERT INTO / UPDATE statements.
/// Handles quoted identifiers (double quotes and backticks).
fn extract_word_after_keyword(sql: String, keyword: String) -> String {
  let upper = string.uppercase(string.trim(sql))
  let upper_keyword = string.uppercase(keyword)
  case string.split_once(upper, upper_keyword) {
    Ok(#(before, _)) -> {
      let offset = string.length(before) + string.length(upper_keyword)
      let rest = string.drop_start(string.trim(sql), offset) |> string.trim
      case string.first(rest) {
        // Double-quoted identifier
        Ok("\"") -> {
          let inner = string.drop_start(rest, 1)
          case string.split_once(inner, "\"") {
            Ok(#(name, _)) -> name
            Error(_) -> rest
          }
        }
        // Backtick-quoted identifier
        Ok("`") -> {
          let inner = string.drop_start(rest, 1)
          case string.split_once(inner, "`") {
            Ok(#(name, _)) -> name
            Error(_) -> rest
          }
        }
        // Unquoted identifier
        _ ->
          case string.split_once(rest, " ") {
            Ok(#(word, _)) -> word
            Error(_) ->
              case string.split_once(rest, "(") {
                Ok(#(word, _)) -> string.trim(word)
                Error(_) -> rest
              }
          }
      }
    }
    Error(_) -> ""
  }
}

/// Parse the table name from an INSERT statement
fn parse_insert_table_name(sql: String) -> String {
  extract_word_after_keyword(sql, "INTO")
}

/// Parse table name from UPDATE statement
fn parse_update_table_name(sql: String) -> String {
  extract_word_after_keyword(sql, "UPDATE")
}

/// Parse table name from DELETE statement
fn parse_delete_table_name(sql: String) -> String {
  extract_word_after_keyword(sql, "FROM")
}

/// Parse SET column names from UPDATE statement
fn parse_update_set_columns(sql: String) -> List(String) {
  let upper = string.uppercase(sql)
  case split_on_keyword(upper, " SET ") {
    Ok(#(_, rest_upper)) -> {
      let offset = string.length(sql) - string.length(rest_upper)
      let rest = string.drop_start(sql, offset)
      // Get the part between SET and WHERE/RETURNING
      let set_part = case split_on_keyword(string.uppercase(rest), " WHERE ") {
        Ok(#(before, _)) -> string.slice(rest, 0, string.length(before))
        Error(_) ->
          case split_on_keyword(string.uppercase(rest), " RETURNING ") {
            Ok(#(before, _)) -> string.slice(rest, 0, string.length(before))
            Error(_) -> rest
          }
      }
      // Parse "col1 = ?, col2 = ?" -> ["col1", "col2"]
      set_part
      |> string.split(",")
      |> list.filter_map(fn(part) {
        case string.split_once(string.trim(part), "=") {
          Ok(#(col_name, _)) -> Ok(string.trim(col_name))
          Error(_) -> Error(Nil)
        }
      })
    }
    Error(_) -> []
  }
}

/// Parse WHERE column names from UPDATE/DELETE statement
fn parse_where_columns(sql: String) -> List(String) {
  let upper = string.uppercase(sql)
  case split_on_keyword(upper, " WHERE ") {
    Ok(#(before_where, _)) -> {
      let offset = string.length(before_where) + string.length(" WHERE ")
      let rest = string.drop_start(sql, offset)
      // Get part before RETURNING if present
      let where_part = case
        split_on_keyword(string.uppercase(rest), " RETURNING ")
      {
        Ok(#(before, _)) -> string.slice(rest, 0, string.length(before))
        Error(_) -> rest
      }
      // Replace AND/OR with commas (case-insensitive) to split conditions
      split_where_conditions(where_part)
      |> list.filter_map(fn(part) {
        let trimmed = string.trim(part)
        // Match patterns like "col = ?", "col > ?", "col=?", etc.
        case string.contains(trimmed, "?") {
          True -> {
            let col_name = extract_column_before_operator(trimmed)
            case col_name {
              "" -> Error(Nil)
              name -> Ok(normalize_column_ref(name))
            }
          }
          False -> Error(Nil)
        }
      })
    }
    Error(_) -> []
  }
}

/// Extract the column name before an operator in a WHERE condition.
/// Handles both "col = ?" and "col=?" (no spaces around operator).
fn extract_column_before_operator(condition: String) -> String {
  // Try splitting on common SQL comparison operators
  let operators = [">=", "<=", "!=", "<>", "=", ">", "<", " LIKE ", " IN "]
  extract_column_with_operators(string.trim(condition), operators)
}

fn extract_column_with_operators(
  condition: String,
  operators: List(String),
) -> String {
  case operators {
    [] ->
      // Fallback: try splitting on space
      case string.split_once(condition, " ") {
        Ok(#(col, _)) -> string.trim(col)
        Error(_) -> ""
      }
    [op, ..rest] -> {
      let check = case string.contains(op, " ") {
        True -> string.split_once(string.uppercase(condition), op)
        False -> string.split_once(condition, op)
      }
      case check {
        Ok(#(col, _)) -> string.trim(col)
        Error(_) -> extract_column_with_operators(condition, rest)
      }
    }
  }
}

/// Split WHERE conditions on AND/OR keywords (case-insensitive)
fn split_where_conditions(where_part: String) -> List(String) {
  let delimiter = "\u{0000}"
  let original_replaced =
    replace_keyword_ci(where_part, " AND ", delimiter)
    |> replace_keyword_ci(" OR ", delimiter)
  string.split(original_replaced, delimiter)
}

/// Case-insensitive keyword replacement
fn replace_keyword_ci(
  input: String,
  keyword: String,
  replacement: String,
) -> String {
  let upper_input = string.uppercase(input)
  let upper_keyword = string.uppercase(keyword)
  replace_keyword_ci_loop(input, upper_input, upper_keyword, replacement, "")
}

fn replace_keyword_ci_loop(
  original: String,
  upper: String,
  keyword: String,
  replacement: String,
  acc: String,
) -> String {
  case string.split_once(upper, keyword) {
    Ok(#(before, after)) -> {
      let before_len = string.length(before)
      let keyword_len = string.length(keyword)
      let original_before = string.slice(original, 0, before_len)
      let original_after = string.drop_start(original, before_len + keyword_len)
      replace_keyword_ci_loop(
        original_after,
        after,
        keyword,
        replacement,
        acc <> original_before <> replacement,
      )
    }
    Error(_) -> acc <> original
  }
}

/// Parse INSERT column names from SQL.
/// Uses depth-aware parenthesis matching to handle nested expressions
/// like VALUES (COALESCE(?, 'x'), ?).
fn parse_insert_columns(sql: String) -> List(String) {
  case string.split_once(sql, "(") {
    Ok(#(_, rest)) ->
      case find_matching_close_paren(rest, 1, "") {
        Ok(cols_str) ->
          cols_str
          |> string.split(",")
          |> list.map(string.trim)
        Error(_) -> []
      }
    Error(_) -> []
  }
}

/// Walk through a string tracking parenthesis depth to find the matching
/// close paren for an already-opened opening paren (depth starts at 1).
fn find_matching_close_paren(
  s: String,
  depth: Int,
  acc: String,
) -> Result(String, Nil) {
  case string.pop_grapheme(s) {
    Error(_) -> Error(Nil)
    Ok(#("(", rest)) -> find_matching_close_paren(rest, depth + 1, acc <> "(")
    Ok(#(")", rest)) ->
      case depth == 1 {
        True -> Ok(acc)
        False -> find_matching_close_paren(rest, depth - 1, acc <> ")")
      }
    Ok(#(char, rest)) -> find_matching_close_paren(rest, depth, acc <> char)
  }
}

/// Get all table metadata in a single pass: schemas, primary keys, and rootpage
/// mappings. Issues one sqlite_master query and one PRAGMA table_info per table.
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

  // Decode all fields we need from PRAGMA table_info in one pass per table
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
      "PRAGMA table_info(\"" <> quote_identifier(table_name) <> "\")"
    case
      sqlight.query(pragma_sql, on: db, with: [], expecting: pragma_decoder)
    {
      Ok(rows) -> {
        let columns =
          list.map(rows, fn(row) {
            let #(col_name, type_str, notnull, _) = row
            let column_type = case query.parse_sqlite_type(type_str) {
              Ok(t) -> t
              Error(_) -> StringType
            }
            Column(
              name: col_name,
              column_type: column_type,
              nullable: notnull == 0,
            )
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
}

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

/// A decoder that handles both string and non-string p4 values
fn flexible_string_decoder() -> decode.Decoder(String) {
  decode.one_of(decode.string, [
    decode.map(decode.int, int.to_string),
    decode.map(decode.float, fn(_) { "" }),
    decode.success(""),
  ])
}
