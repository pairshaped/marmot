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

/// Introspect columns of a table using PRAGMA table_info
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
  // Get all table schemas and primary key info
  let table_schemas = get_table_schemas(db)
  let pk_columns = get_pk_columns(db)

  // Get root page -> table name mapping
  let rootpage_table = get_rootpage_mapping(db)

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
  let has_returning = string.contains(string.uppercase(sql), "RETURNING")

  // Determine result columns
  let columns = case has_returning {
    True -> {
      let table_name = case is_insert {
        True -> parse_insert_table_name(sql)
        _ -> parse_update_table_name(sql)
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
  let parameters = extract_parameters(opcodes, cursor_table, table_schemas, sql)

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
    cols ->
      case dict.get(table_schemas, table_name) {
        Ok(table_cols) ->
          list.map(cols, fn(col_name) {
            case list.find(table_cols, fn(c) { c.name == col_name }) {
              Ok(col) -> col
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

/// Parse RETURNING column names from SQL
fn parse_returning_columns(sql: String) -> List(String) {
  let upper = string.uppercase(sql)
  case string.split_once(upper, "RETURNING") {
    Ok(#(_, rest)) -> {
      // Get the original case portion after RETURNING
      let offset = string.length(sql) - string.length(rest)
      let original_rest = string.drop_start(sql, offset)
      original_rest
      |> string.trim
      |> string.split(",")
      |> list.map(string.trim)
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
  sql: String,
) -> List(Parameter) {
  // Find all Variable opcodes sorted by p1 (parameter number)
  let variable_ops =
    list.filter(opcodes, fn(op) { op.opcode == "Variable" })
    |> list.sort(fn(a, b) { int.compare(a.p1, b.p1) })

  case list.length(variable_ops) {
    0 -> []
    _ -> {
      let stmt_type = classify_statement(sql)

      case stmt_type {
        Insert -> extract_insert_parameters(table_schemas, sql)
        Update -> extract_update_parameters(table_schemas, sql)
        _ ->
          list.map(variable_ops, fn(var_op) {
            infer_parameter_type(var_op, opcodes, cursor_table, table_schemas)
          })
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
              case dict.get(table_schemas, table) {
                Ok(table_cols) ->
                  case
                    list.find(table_cols, fn(c) {
                      c.column_type == query.IntType && c.nullable == False
                    })
                  {
                    Ok(col) ->
                      Parameter(name: col.name, column_type: col.column_type)
                    Error(_) ->
                      Parameter(name: "id", column_type: query.IntType)
                  }
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

/// Parse the table name from an INSERT statement
fn parse_insert_table_name(sql: String) -> String {
  // "INSERT INTO tablename (...)"
  let upper = string.uppercase(string.trim(sql))
  case string.split_once(upper, "INTO") {
    Ok(#(_, rest)) -> {
      let trimmed = string.trim(rest)
      // Take the first word (table name)
      case string.split_once(trimmed, " ") {
        Ok(#(_table_upper, _)) -> {
          // Get the original-case version
          let offset = string.length(sql) - string.length(string.trim(rest))
          let original_rest = string.drop_start(string.trim(sql), offset)
          case string.split_once(original_rest, " ") {
            Ok(#(table, _)) -> table
            Error(_) -> original_rest
          }
        }
        Error(_) -> trimmed
      }
    }
    Error(_) -> ""
  }
}

/// Parse table name from UPDATE statement
fn parse_update_table_name(sql: String) -> String {
  // "UPDATE tablename SET ..."
  let upper = string.uppercase(string.trim(sql))
  case string.split_once(upper, "UPDATE") {
    Ok(#(_, rest)) -> {
      let trimmed = string.trim(rest)
      case string.split_once(trimmed, " ") {
        Ok(#(_, _)) -> {
          let offset = string.length(sql) - string.length(string.trim(rest))
          let original_rest = string.drop_start(string.trim(sql), offset)
          case string.split_once(original_rest, " ") {
            Ok(#(table, _)) -> table
            Error(_) -> original_rest
          }
        }
        Error(_) -> trimmed
      }
    }
    Error(_) -> ""
  }
}

/// Parse SET column names from UPDATE statement
fn parse_update_set_columns(sql: String) -> List(String) {
  let upper = string.uppercase(sql)
  case string.split_once(upper, "SET") {
    Ok(#(_, rest_upper)) -> {
      let offset = string.length(sql) - string.length(rest_upper)
      let rest = string.drop_start(sql, offset)
      // Get the part between SET and WHERE/RETURNING
      let set_part = case string.split_once(string.uppercase(rest), "WHERE") {
        Ok(#(before, _)) -> string.slice(rest, 0, string.length(before))
        Error(_) ->
          case string.split_once(string.uppercase(rest), "RETURNING") {
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
  case string.split_once(upper, "WHERE") {
    Ok(#(_, rest_upper)) -> {
      let offset = string.length(sql) - string.length(rest_upper)
      let rest = string.drop_start(sql, offset)
      // Get part before RETURNING if present
      let where_part = case
        string.split_once(string.uppercase(rest), "RETURNING")
      {
        Ok(#(before, _)) -> string.slice(rest, 0, string.length(before))
        Error(_) -> rest
      }
      // Parse "col1 = ? AND col2 > ?" -> ["col1", "col2"]
      where_part
      |> string.replace("AND", ",")
      |> string.replace("and", ",")
      |> string.replace("OR", ",")
      |> string.replace("or", ",")
      |> string.split(",")
      |> list.filter_map(fn(part) {
        let trimmed = string.trim(part)
        // Match patterns like "col = ?", "col > ?", "col < ?", etc.
        case string.contains(trimmed, "?") {
          True ->
            case string.split_once(trimmed, " ") {
              Ok(#(col_name, _)) -> Ok(string.trim(col_name))
              Error(_) -> Error(Nil)
            }
          False -> Error(Nil)
        }
      })
    }
    Error(_) -> []
  }
}

/// Parse INSERT column names from SQL
fn parse_insert_columns(sql: String) -> List(String) {
  case string.split_once(sql, "(") {
    Ok(#(_, rest)) ->
      case string.split_once(rest, ")") {
        Ok(#(cols_str, _)) ->
          cols_str
          |> string.split(",")
          |> list.map(string.trim)
        Error(_) -> []
      }
    Error(_) -> []
  }
}

/// Get all table schemas from the database
fn get_table_schemas(db: Connection) -> Dict(String, List(Column)) {
  let tables_decoder = {
    use name <- decode.field(0, decode.string)
    decode.success(name)
  }

  let tables =
    sqlight.query(
      "SELECT name FROM sqlite_master WHERE type='table'",
      on: db,
      with: [],
      expecting: tables_decoder,
    )
    |> result.unwrap([])

  list.fold(tables, dict.new(), fn(acc, table_name) {
    case introspect_columns(db, table_name) {
      Ok(columns) -> dict.insert(acc, table_name, columns)
      Error(_) -> acc
    }
  })
}

/// Get primary key column name for each table using PRAGMA table_info pk field.
/// Returns a dict mapping table name to the PK column name.
fn get_pk_columns(db: Connection) -> Dict(String, String) {
  let tables =
    sqlight.query(
      "SELECT name FROM sqlite_master WHERE type='table'",
      on: db,
      with: [],
      expecting: {
        use name <- decode.field(0, decode.string)
        decode.success(name)
      },
    )
    |> result.unwrap([])

  let pk_decoder = {
    use name <- decode.field(1, decode.string)
    use pk <- decode.field(5, decode.int)
    decode.success(#(name, pk))
  }

  list.fold(tables, dict.new(), fn(acc, table_name) {
    let sql = "PRAGMA table_info(\"" <> quote_identifier(table_name) <> "\")"
    case sqlight.query(sql, on: db, with: [], expecting: pk_decoder) {
      Ok(rows) ->
        case list.find(rows, fn(row) { row.1 > 0 }) {
          Ok(#(col_name, _)) -> dict.insert(acc, table_name, col_name)
          Error(_) -> acc
        }
      Error(_) -> acc
    }
  })
}

/// Get rootpage -> table name mapping from sqlite_master
fn get_rootpage_mapping(db: Connection) -> Dict(Int, String) {
  let decoder = {
    use name <- decode.field(0, decode.string)
    use rootpage <- decode.field(1, decode.int)
    decode.success(#(name, rootpage))
  }

  let rows =
    sqlight.query(
      "SELECT name, rootpage FROM sqlite_master WHERE type='table'",
      on: db,
      with: [],
      expecting: decoder,
    )
    |> result.unwrap([])

  list.fold(rows, dict.new(), fn(acc, row) {
    let #(name, rootpage) = row
    dict.insert(acc, rootpage, name)
  })
}

/// A decoder that handles both string and non-string p4 values
fn flexible_string_decoder() -> decode.Decoder(String) {
  decode.one_of(decode.string, [
    decode.map(decode.int, int.to_string),
    decode.map(decode.float, fn(_) { "" }),
    decode.success(""),
  ])
}
