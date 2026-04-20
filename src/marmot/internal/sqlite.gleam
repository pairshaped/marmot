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

/// Find cursor IDs that may produce NULL rows due to LEFT JOIN semantics.
/// SQLite emits `NullRow` on cursor P1 when an outer join has no matching
/// inner row — after this, any `Column` read from that cursor returns NULL.
/// `IfNullRow` tests this state. Any cursor that is the target of either
/// opcode is a "nullable cursor": columns resolved against it must be
/// marked nullable in the generated type.
fn find_nullable_cursors(opcodes: List(Opcode)) -> dict.Dict(Int, Nil) {
  list.fold(opcodes, dict.new(), fn(acc, op) {
    case op.opcode {
      "NullRow" | "IfNullRow" -> dict.insert(acc, op.p1, Nil)
      _ -> acc
    }
  })
}

/// Nullable-cursor set + the set of tables those cursors read from.
/// We track both because text-based column resolution (resolve_select_item)
/// goes by table name, while opcode-based resolution goes by cursor id.
type JoinNullability {
  JoinNullability(
    nullable_cursors: dict.Dict(Int, Nil),
    nullable_tables: dict.Dict(String, Nil),
  )
}

/// Build a mapping from each autoindex/sorter cursor to its source table cursor.
/// SQLite builds transient autoindexes when no suitable index exists for a JOIN.
/// The pattern: `OpenAutoindex p1=auto_cursor` followed by a loop that reads
/// from the source cursor via Column/Rowid opcodes and inserts into the autoindex
/// via IdxInsert. By scanning for `IdxInsert p1=auto_cursor` and looking back at
/// the nearest preceding `Column` or `Rowid` opcode that reads from a real-table
/// cursor, we can map auto_cursor → source_cursor (and thus → table_name).
fn build_autoindex_source(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
) -> Dict(Int, Int) {
  // Find all autoindex/sorter cursors from OpenAutoindex opcodes
  let autoindex_cursors =
    list.filter_map(opcodes, fn(op) {
      case op.opcode {
        "OpenAutoindex" -> Ok(#(op.p1, op.addr))
        _ -> Error(Nil)
      }
    })

  list.fold(autoindex_cursors, dict.new(), fn(acc, entry) {
    let #(auto_cursor, open_addr) = entry
    // Find IdxInsert targeting this autoindex cursor
    let idx_insert =
      list.find(opcodes, fn(op) {
        op.opcode == "IdxInsert" && op.p1 == auto_cursor && op.addr > open_addr
      })
    case idx_insert {
      Error(_) -> acc
      Ok(insert_op) -> {
        // Look back from IdxInsert to find the nearest Column/Rowid/IdxRowid
        // opcode reading from a real-table cursor (in cursor_table)
        let source =
          list.find_map(
            list.reverse(
              list.filter(opcodes, fn(op) {
                op.addr < insert_op.addr && op.addr > open_addr
              }),
            ),
            fn(op) {
              case op.opcode {
                "Column" | "Rowid" | "IdxRowid" ->
                  case dict.has_key(cursor_table, op.p1) {
                    True -> Ok(op.p1)
                    False -> Error(Nil)
                  }
                _ -> Error(Nil)
              }
            },
          )
        case source {
          Ok(src_cursor) -> dict.insert(acc, auto_cursor, src_cursor)
          Error(_) -> acc
        }
      }
    }
  })
}

fn compute_join_nullability(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
) -> JoinNullability {
  let nullable_cursors = find_nullable_cursors(opcodes)
  let autoindex_source = build_autoindex_source(opcodes, cursor_table)
  let nullable_tables =
    dict.fold(nullable_cursors, dict.new(), fn(acc, cursor_id, _) {
      // First try direct lookup in cursor_table (real-table cursor)
      case dict.get(cursor_table, cursor_id) {
        Ok(table_name) -> dict.insert(acc, table_name, Nil)
        Error(_) ->
          // Not a real-table cursor — check if it's an autoindex cursor
          // whose source is a known real-table cursor
          case dict.get(autoindex_source, cursor_id) {
            Ok(source_cursor) ->
              case dict.get(cursor_table, source_cursor) {
                Ok(table_name) -> dict.insert(acc, table_name, Nil)
                Error(_) -> acc
              }
            Error(_) -> acc
          }
      }
    })
  JoinNullability(
    nullable_cursors: nullable_cursors,
    nullable_tables: nullable_tables,
  )
}

/// If the Column opcode that produced this result register reads from a
/// nullable cursor (LEFT-JOIN right side), mark the column nullable. The
/// `Column` opcode format is `Column P1=cursor P2=col_idx P3=dest_reg`.
fn apply_cursor_nullability(
  base: Column,
  dest_reg: Int,
  opcodes: List(Opcode),
  join_nullability: JoinNullability,
) -> Column {
  let producer =
    list.find(opcodes, fn(op) { op.opcode == "Column" && op.p3 == dest_reg })
  case producer {
    Ok(op) ->
      case dict.has_key(join_nullability.nullable_cursors, op.p1) {
        True -> Column(..base, nullable: True)
        False -> base
      }
    Error(_) -> base
  }
}

/// Introspect a query using EXPLAIN to determine result columns and parameters
pub fn introspect_query(
  db: Connection,
  sql: String,
) -> Result(QueryInfo, sqlight.Error) {
  // Normalize whitespace (newlines/tabs → spaces, collapse runs). All keyword
  // detection and SQL parsing below relies on single-space separators.
  // SQLite's EXPLAIN accepts the original SQL with newlines, so we keep the
  // original for that and use the normalized form for everything else.
  let normalized_sql = normalize_sql_whitespace(sql)

  // Get all table metadata in a single pass
  let #(table_schemas, pk_columns, rootpage_table) = get_table_metadata(db)

  // Get EXPLAIN output (strip Marmot-specific `!`/`?` suffixes from aliases
  // before handing to SQLite — they're valid Marmot syntax but not valid SQL)
  let sanitized_sql = strip_nullability_suffixes(sql)
  let explain_sql = "EXPLAIN " <> sanitized_sql
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

  let join_nullability = compute_join_nullability(opcodes, cursor_table)

  // Check statement type
  let stmt_type = classify_statement(normalized_sql)
  let is_insert = stmt_type == Insert
  let has_returning = contains_keyword(normalized_sql, "RETURNING")

  // Determine result columns
  let columns = case has_returning {
    True -> {
      let table_name = case stmt_type {
        Insert -> parse_insert_table_name(normalized_sql)
        Update -> parse_update_table_name(normalized_sql)
        Delete -> parse_delete_table_name(normalized_sql)
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

  // Determine parameters — use SQL with nullability suffixes stripped so that
  // `col_name?` and `col_name!` aliases are not mistaken for `?` placeholders.
  let param_sql = strip_nullability_suffixes(normalized_sql)
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

/// Normalize SQL whitespace: strip line comments (-- ... up to newline),
/// convert newlines and tabs to spaces, then collapse runs of spaces into
/// single spaces. Trims leading/trailing whitespace. Safe on SQL because
/// string literals aren't split across lines in our queries and whitespace
/// inside identifiers isn't allowed.
/// Strip the Marmot-specific nullability suffixes `!` / `?` from alias names
/// before sending SQL to SQLite's EXPLAIN. We only strip when the suffix
/// appears directly after an identifier character and is followed by
/// whitespace, comma, end-of-string, or closing paren. This avoids
/// mangling legitimate SQL like `WHERE x != y` or `?` placeholders.
pub fn strip_nullability_suffixes(sql: String) -> String {
  do_strip_nullability_suffixes(sql, "", False, False)
}

fn do_strip_nullability_suffixes(
  remaining: String,
  acc: String,
  in_single: Bool,
  in_double: Bool,
) -> String {
  case string.pop_grapheme(remaining) {
    Error(_) -> acc
    Ok(#("'", rest)) ->
      do_strip_nullability_suffixes(rest, acc <> "'", !in_single, in_double)
    Ok(#("\"", rest)) ->
      do_strip_nullability_suffixes(rest, acc <> "\"", in_single, !in_double)
    Ok(#(ch, rest)) ->
      case in_single || in_double {
        True ->
          do_strip_nullability_suffixes(rest, acc <> ch, in_single, in_double)
        False ->
          case ch == "!" || ch == "?" {
            True -> {
              let prev_ok = case string_last(acc) {
                Ok(p) -> is_ident_char(p)
                Error(_) -> False
              }
              let next_char = case string.pop_grapheme(rest) {
                Ok(#(c, _)) -> c
                Error(_) -> " "
              }
              let next_ok = case next_char {
                " " | "," | ")" | "\t" | "\n" -> True
                _ -> False
              }
              case prev_ok && next_ok {
                True ->
                  do_strip_nullability_suffixes(rest, acc, in_single, in_double)
                False ->
                  do_strip_nullability_suffixes(
                    rest,
                    acc <> ch,
                    in_single,
                    in_double,
                  )
              }
            }
            False ->
              do_strip_nullability_suffixes(
                rest,
                acc <> ch,
                in_single,
                in_double,
              )
          }
      }
  }
}

fn is_ident_char(c: String) -> Bool {
  case c {
    "a"
    | "b"
    | "c"
    | "d"
    | "e"
    | "f"
    | "g"
    | "h"
    | "i"
    | "j"
    | "k"
    | "l"
    | "m"
    | "n"
    | "o"
    | "p"
    | "q"
    | "r"
    | "s"
    | "t"
    | "u"
    | "v"
    | "w"
    | "x"
    | "y"
    | "z" -> True
    "A"
    | "B"
    | "C"
    | "D"
    | "E"
    | "F"
    | "G"
    | "H"
    | "I"
    | "J"
    | "K"
    | "L"
    | "M"
    | "N"
    | "O"
    | "P"
    | "Q"
    | "R"
    | "S"
    | "T"
    | "U"
    | "V"
    | "W"
    | "X"
    | "Y"
    | "Z" -> True
    "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" -> True
    "_" -> True
    _ -> False
  }
}

fn string_last(s: String) -> Result(String, Nil) {
  case string.length(s) {
    0 -> Error(Nil)
    n -> Ok(string.slice(s, n - 1, 1))
  }
}

fn normalize_sql_whitespace(sql: String) -> String {
  sql
  |> strip_line_comments
  |> string.replace("\r\n", " ")
  |> string.replace("\r", " ")
  |> string.replace("\n", " ")
  |> string.replace("\t", " ")
  |> collapse_spaces
  |> string.trim
}

/// Remove `-- line comments` from SQL. A line comment starts with `--`
/// outside of a string literal and extends to end of line. Preserves
/// newlines so subsequent whitespace collapsing works naturally.
fn strip_line_comments(sql: String) -> String {
  do_strip_line_comments(sql, "", False, False, False)
}

fn do_strip_line_comments(
  remaining: String,
  acc: String,
  in_single: Bool,
  in_double: Bool,
  in_comment: Bool,
) -> String {
  case string.pop_grapheme(remaining) {
    Error(_) -> acc
    Ok(#("\n", rest)) ->
      // Newline ends comment and is kept as whitespace
      do_strip_line_comments(rest, acc <> "\n", in_single, in_double, False)
    Ok(#(_, rest)) if in_comment ->
      do_strip_line_comments(rest, acc, in_single, in_double, True)
    Ok(#("'", rest)) ->
      case in_double {
        True -> do_strip_line_comments(rest, acc <> "'", in_single, True, False)
        False ->
          do_strip_line_comments(rest, acc <> "'", !in_single, False, False)
      }
    Ok(#("\"", rest)) ->
      case in_single {
        True ->
          do_strip_line_comments(rest, acc <> "\"", True, in_double, False)
        False ->
          do_strip_line_comments(rest, acc <> "\"", False, !in_double, False)
      }
    Ok(#("-", rest)) ->
      case in_single || in_double {
        True ->
          do_strip_line_comments(rest, acc <> "-", in_single, in_double, False)
        False ->
          case string.pop_grapheme(rest) {
            Ok(#("-", rest2)) ->
              do_strip_line_comments(rest2, acc, False, False, True)
            _ ->
              do_strip_line_comments(
                rest,
                acc <> "-",
                in_single,
                in_double,
                False,
              )
          }
      }
    Ok(#(char, rest)) ->
      do_strip_line_comments(rest, acc <> char, in_single, in_double, False)
  }
}

fn collapse_spaces(sql: String) -> String {
  case string.contains(sql, "  ") {
    True -> collapse_spaces(string.replace(sql, "  ", " "))
    False -> sql
  }
}

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
      let result_regs = make_range(base_reg, count)
      let select_items = parse_select_items(sql)
      let from_tables = parse_from_tables(sql)

      list.index_map(result_regs, fn(reg, idx) {
        let opcode_column = {
          let base =
            find_column_for_register(
              reg,
              opcodes,
              cursor_table,
              table_schemas,
              pk_columns,
            )
          apply_cursor_nullability(base, reg, opcodes, join_nullability)
        }
        let select_item = list_at(select_items, idx)
        case select_item {
          // No SELECT list available: use opcode result as-is.
          Error(_) -> opcode_column
          Ok(item) -> {
            // Prefer SELECT parsing when the item is a bare column reference:
            // text-based schema lookup gives correct name + type + nullability
            // without the opcode-tracing pitfalls (sorter pseudo-cursors,
            // IdxRowid-returning-target-PK-name, etc.). For expressions
            // (CAST, COALESCE, COUNT, etc.) or when the bare column can't be
            // found, fall back to opcode-traced type with the SELECT alias.
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
                    // If opcode tracing resolved a real column (not the "unknown"
                    // fallback), propagate its nullability — e.g. an autoindex
                    // cursor that NullRow targets marks its column nullable.
                    // When opcode_column is "unknown" it means the column was read
                    // from a pseudo-cursor (sorter, ephemeral, window function
                    // machinery) that isn't in cursor_table; its nullable=True is
                    // just the safe fallback default, not a genuine signal, so we
                    // don't propagate it.
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
            apply_override(resolved_col, item.override)
          }
        }
      })
    }
  }
}

/// Resolve a result column via SELECT-list text parsing.
/// Returns (alias_name, StringType, nullable=True) as a safe default when
/// the expression isn't a plain column reference.
fn resolve_select_item(
  idx: Int,
  select_items: List(SelectItem),
  from_tables: List(String),
  table_schemas: Dict(String, List(Column)),
  join_nullability: JoinNullability,
) -> Column {
  case list_at(select_items, idx) {
    Error(_) -> Column(name: "unknown", column_type: StringType, nullable: True)
    Ok(item) -> {
      // Try each FROM table to find a matching column, use the alias/name.
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
                      // Override nullability if the source table is on the
                      // right side of a LEFT JOIN.
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
        Error(_) -> infer_expression_type(item)
      }
      apply_override(col, item.override)
    }
  }
}

fn apply_override(col: Column, override: NullabilityOverride) -> Column {
  case override {
    OverrideNonNull -> Column(..col, nullable: False)
    OverrideNullable -> Column(..col, nullable: True)
    OverrideNone -> col
  }
}

/// When an expression-based SELECT item can't be resolved to a table column,
/// infer its type from the expression shape. Handles common aggregates and
/// explicit CASTs. Falls back to StringType nullable.
fn infer_expression_type(item: SelectItem) -> Column {
  let upper = string.uppercase(item.expression)
  // Literal constants: "0", "1", "42", "'text'", "3.14"
  case infer_literal_type(string.trim(item.expression)) {
    option.Some(t) -> Column(name: item.alias, column_type: t, nullable: False)
    option.None ->
      case string.starts_with(upper, "COUNT(") {
        True ->
          Column(name: item.alias, column_type: query.IntType, nullable: False)
        False ->
          case string.starts_with(upper, "EXISTS(") {
            True ->
              Column(
                name: item.alias,
                column_type: query.IntType,
                nullable: False,
              )
            False ->
              case string.starts_with(upper, "CAST(") {
                True -> infer_cast_type(item.alias, upper)
                False ->
                  case string.starts_with(upper, "COALESCE(") {
                    True -> infer_coalesce_type(item.alias, item.expression)
                    False ->
                      case string.starts_with(upper, "SUM(") {
                        True ->
                          Column(
                            name: item.alias,
                            column_type: query.IntType,
                            nullable: True,
                          )
                        False ->
                          case is_integer_window_function(upper) {
                            True ->
                              Column(
                                name: item.alias,
                                column_type: query.IntType,
                                nullable: False,
                              )
                            False ->
                              Column(
                                name: item.alias,
                                column_type: StringType,
                                nullable: True,
                              )
                          }
                      }
                  }
              }
          }
      }
  }
}

/// When COALESCE's last argument is a literal, the result is non-null with
/// that literal's inferred type. E.g., `COALESCE(SUM(x), 0)` → Int non-null.
fn infer_coalesce_type(alias: String, expr: String) -> Column {
  let inner = case string.split_once(expr, "(") {
    Ok(#(_, after)) -> after
    Error(_) -> expr
  }
  let inner = case string.ends_with(inner, ")") {
    True -> string.drop_end(inner, 1)
    False -> inner
  }
  let args = split_top_level_commas(inner)
  case list.last(args) {
    Error(_) -> Column(name: alias, column_type: StringType, nullable: True)
    Ok(last_arg) -> {
      let trimmed = string.trim(last_arg)
      case infer_literal_type(trimmed) {
        option.Some(t) -> Column(name: alias, column_type: t, nullable: False)
        option.None ->
          Column(name: alias, column_type: StringType, nullable: True)
      }
    }
  }
}

/// Recognize SQL window functions that always return a non-null integer.
/// Detects ROW_NUMBER(), RANK(), DENSE_RANK(), NTILE(...) followed by OVER.
/// Matches on the uppercased expression, anchored at the start.
fn is_integer_window_function(upper: String) -> Bool {
  let starts_with_fn =
    string.starts_with(upper, "ROW_NUMBER(")
    || string.starts_with(upper, "RANK(")
    || string.starts_with(upper, "DENSE_RANK(")
    || string.starts_with(upper, "NTILE(")
  starts_with_fn && string.contains(upper, ") OVER")
}

fn infer_literal_type(s: String) -> option.Option(query.ColumnType) {
  case string.first(s) {
    Error(_) -> option.None
    Ok("'") -> option.Some(StringType)
    Ok(c) ->
      case is_digit_char(c) || c == "-" {
        True ->
          case string.contains(s, ".") {
            True -> option.Some(query.FloatType)
            False -> option.Some(query.IntType)
          }
        False -> option.None
      }
  }
}

fn is_digit_char(c: String) -> Bool {
  case c {
    "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" -> True
    _ -> False
  }
}

/// Parse `CAST(... AS type)` to find the target type. Uses the uppercased
/// expression for keyword matching.
fn infer_cast_type(name: String, upper_expr: String) -> Column {
  // Find the last " AS " at top level inside the CAST parens
  case string.split_once(upper_expr, " AS ") {
    Ok(#(_, after)) -> {
      let target = string.trim(after)
      // Strip trailing ) and anything after
      let target = case string.split_once(target, ")") {
        Ok(#(t, _)) -> string.trim(t)
        Error(_) -> target
      }
      case target {
        "INTEGER" | "INT" | "BIGINT" ->
          Column(name: name, column_type: query.IntType, nullable: False)
        "REAL" | "FLOAT" | "DOUBLE" ->
          Column(name: name, column_type: query.FloatType, nullable: False)
        "TEXT" | "VARCHAR" | "CHAR" ->
          Column(name: name, column_type: StringType, nullable: False)
        "BLOB" ->
          Column(name: name, column_type: query.BitArrayType, nullable: False)
        _ -> Column(name: name, column_type: StringType, nullable: True)
      }
    }
    Error(_) -> Column(name: name, column_type: StringType, nullable: True)
  }
}

type NullabilityOverride {
  OverrideNonNull
  OverrideNullable
  OverrideNone
}

type SelectItem {
  SelectItem(
    /// The display/field name (from AS alias or the expression itself)
    alias: String,
    /// The raw expression (left side of AS, or the whole item)
    expression: String,
    /// If the expression is a bare column reference (possibly "table.col"),
    /// this is the column name (without table prefix). None for expressions
    /// like COUNT(*), COALESCE(...), subqueries, etc.
    bare_column: option.Option(String),
    /// Nullability override from alias suffix (`name!` / `name?`).
    override: NullabilityOverride,
  )
}

/// Parse the SELECT list from a normalized SQL string.
/// Returns one SelectItem per comma-separated top-level expression.
/// Handles nested parentheses (COALESCE(...), CAST(... AS ...), etc) and
/// CTEs (WITH ... AS (...), main SELECT).
fn parse_select_items(sql: String) -> List(SelectItem) {
  // Skip any WITH [RECURSIVE] cte AS (...) prefix to find the main SELECT
  let main_sql = skip_with_prefix(sql)
  let upper = string.uppercase(main_sql)
  let start_prefix = case string.starts_with(upper, "SELECT DISTINCT ") {
    True -> "SELECT DISTINCT "
    False ->
      case string.starts_with(upper, "SELECT ") {
        True -> "SELECT "
        False -> ""
      }
  }
  case start_prefix {
    "" -> []
    prefix -> {
      let after_select = string.drop_start(main_sql, string.length(prefix))
      // Find end of SELECT list: FROM at top level, or end of string
      // for queries without FROM (SELECT EXISTS(...), SELECT COALESCE(...),
      // etc). Terminate at other top-level keywords too.
      let end_idx = case find_top_level_from(after_select) {
        option.Some(idx) -> idx
        option.None -> {
          // No top-level FROM (e.g. `SELECT EXISTS(...) AS alias`). The
          // SELECT list ends at the next top-level WHERE/GROUP/ORDER/LIMIT,
          // or at end of string. Use the same paren-aware scan so we don't
          // truncate at a keyword that's inside a subquery.
          [
            do_find_top_level_keyword(after_select, " WHERE ", 0, 0),
            do_find_top_level_keyword(after_select, " GROUP BY ", 0, 0),
            do_find_top_level_keyword(after_select, " ORDER BY ", 0, 0),
            do_find_top_level_keyword(after_select, " LIMIT ", 0, 0),
          ]
          |> list.filter_map(fn(x) {
            case x {
              option.Some(i) -> Ok(i)
              option.None -> Error(Nil)
            }
          })
          |> list.sort(int.compare)
          |> list.first
          |> result.unwrap(string.length(after_select))
        }
      }
      let select_list = string.slice(after_select, 0, end_idx)
      split_top_level_commas(select_list)
      |> list.map(parse_select_item)
    }
  }
}

/// Skip a WITH [RECURSIVE] prefix: walk past `WITH` through matching
/// parens for each named CTE, landing on the main SELECT.
/// Input that doesn't start with WITH is returned unchanged.
fn skip_with_prefix(sql: String) -> String {
  let upper = string.uppercase(sql)
  case
    string.starts_with(upper, "WITH RECURSIVE "),
    string.starts_with(upper, "WITH ")
  {
    True, _ -> skip_cte_definitions(string.drop_start(sql, 15))
    _, True -> skip_cte_definitions(string.drop_start(sql, 5))
    _, _ -> sql
  }
}

/// Walk past `name [(cols)] AS ( ... ) [, another AS ( ... )]` until the
/// next top-level SELECT.
fn skip_cte_definitions(s: String) -> String {
  let trimmed = string.trim_start(s)
  // Find the first ( at depth 0 after any `AS `
  case find_matching_paren_after_as(trimmed) {
    option.None -> trimmed
    option.Some(after_close) -> {
      let rest = string.trim_start(after_close)
      case string.starts_with(rest, ",") {
        True -> skip_cte_definitions(string.drop_start(rest, 1))
        False -> rest
      }
    }
  }
}

/// Find the " AS (" top-level start, then walk through matching parens,
/// returning the substring after the closing `)`.
fn find_matching_paren_after_as(s: String) -> option.Option(String) {
  let upper = string.uppercase(s)
  case find_top_level_keyword_offset(upper, " AS ") {
    option.None -> option.None
    option.Some(idx) -> {
      let after_as = string.drop_start(s, idx + 4) |> string.trim_start
      case string.pop_grapheme(after_as) {
        Ok(#("(", rest)) -> option.Some(walk_matching_paren(rest, 1))
        _ -> option.None
      }
    }
  }
}

fn find_top_level_keyword_offset(
  s: String,
  keyword: String,
) -> option.Option(Int) {
  do_find_top_level_keyword(s, keyword, 0, 0)
}

fn walk_matching_paren(s: String, depth: Int) -> String {
  case depth {
    0 -> s
    _ ->
      case string.pop_grapheme(s) {
        Error(_) -> s
        Ok(#("(", rest)) -> walk_matching_paren(rest, depth + 1)
        Ok(#(")", rest)) -> walk_matching_paren(rest, depth - 1)
        Ok(#(_, rest)) -> walk_matching_paren(rest, depth)
      }
  }
}

/// Parse the FROM clause to get the list of tables.
/// Returns the primary table plus any JOINed tables. Handles "FROM table AS alias"
/// and "FROM table JOIN other ON ...". Does not resolve aliases back to tables
/// (we match against all tables, so aliases don't matter for lookup).
fn parse_from_tables(sql: String) -> List(String) {
  let main_sql = skip_with_prefix(sql)
  let upper = string.uppercase(main_sql)
  let after_select = case string.starts_with(upper, "SELECT DISTINCT ") {
    True -> string.drop_start(main_sql, 16)
    False ->
      case string.starts_with(upper, "SELECT ") {
        True -> string.drop_start(main_sql, 7)
        False -> main_sql
      }
  }
  case find_top_level_from(after_select) {
    option.None -> []
    option.Some(from_idx) -> {
      let rest =
        after_select
        |> string.drop_start(from_idx + 5)
        |> string.trim
      // Terminate at WHERE/GROUP/HAVING/ORDER/LIMIT/RETURNING
      let rest_upper = string.uppercase(rest)
      let end_idx =
        [
          find_keyword_idx(rest_upper, "WHERE"),
          find_keyword_idx(rest_upper, "GROUP BY"),
          find_keyword_idx(rest_upper, "HAVING"),
          find_keyword_idx(rest_upper, "ORDER BY"),
          find_keyword_idx(rest_upper, "LIMIT"),
          find_keyword_idx(rest_upper, "RETURNING"),
        ]
        |> list.filter_map(fn(x) {
          case x {
            option.Some(i) -> Ok(i)
            option.None -> Error(Nil)
          }
        })
        |> list.sort(int.compare)
        |> list.first
      let from_part = case end_idx {
        Ok(i) -> string.slice(rest, 0, i)
        Error(_) -> rest
      }
      // Split on JOIN keywords
      extract_table_names_from_from(from_part)
    }
  }
}

/// Extract table names from a FROM clause text, handling JOINs.
/// Input example: `accounts u JOIN account_emails ue ON ue.account_id = u.id`
fn extract_table_names_from_from(from_part: String) -> List(String) {
  // Replace all JOIN variants with a sentinel, then split on it
  let normalized =
    from_part
    |> replace_ignore_case(" LEFT JOIN ", "|||")
    |> replace_ignore_case(" LEFT OUTER JOIN ", "|||")
    |> replace_ignore_case(" RIGHT JOIN ", "|||")
    |> replace_ignore_case(" INNER JOIN ", "|||")
    |> replace_ignore_case(" CROSS JOIN ", "|||")
    |> replace_ignore_case(" JOIN ", "|||")
  let parts = string.split(normalized, "|||")
  list.filter_map(parts, fn(part) {
    // Strip " ON ..." clause
    let upper = string.uppercase(part)
    let no_on = case string.split_once(upper, " ON ") {
      Ok(_) -> {
        let idx = case string.split_once(upper, " ON ") {
          Ok(#(before, _)) -> string.length(before)
          Error(_) -> string.length(part)
        }
        string.slice(part, 0, idx)
      }
      Error(_) -> part
    }
    // First word is the table name; optional second word is alias
    case string.split(string.trim(no_on), " ") {
      [table, ..] -> {
        let cleaned = string.trim(table)
        case cleaned {
          "" -> Error(Nil)
          _ -> Ok(cleaned)
        }
      }
      [] -> Error(Nil)
    }
  })
}

fn replace_ignore_case(
  haystack: String,
  needle: String,
  replacement: String,
) -> String {
  // Case-insensitive replace by walking through and finding uppercase matches
  do_replace_ignore_case(haystack, needle, replacement, "")
}

fn do_replace_ignore_case(
  remaining: String,
  needle: String,
  replacement: String,
  acc: String,
) -> String {
  let needle_len = string.length(needle)
  case string.length(remaining) < needle_len {
    True -> acc <> remaining
    False -> {
      let head = string.slice(remaining, 0, needle_len)
      case string.uppercase(head) == string.uppercase(needle) {
        True ->
          do_replace_ignore_case(
            string.drop_start(remaining, needle_len),
            needle,
            replacement,
            acc <> replacement,
          )
        False -> {
          let first = string.slice(remaining, 0, 1)
          do_replace_ignore_case(
            string.drop_start(remaining, 1),
            needle,
            replacement,
            acc <> first,
          )
        }
      }
    }
  }
}

fn find_keyword_idx(upper: String, keyword: String) -> option.Option(Int) {
  // Look for " KEYWORD " (with surrounding spaces) or keyword at end
  let with_space = " " <> keyword <> " "
  case string.split_once(upper, with_space) {
    Ok(#(before, _)) -> option.Some(string.length(before))
    Error(_) ->
      case string.ends_with(upper, " " <> keyword) {
        True -> option.Some(string.length(upper) - string.length(keyword) - 1)
        False -> option.None
      }
  }
}

/// Find the index of the top-level `FROM` keyword in a SELECT's from-part.
/// Respects nested parentheses (subqueries).
fn find_top_level_from(s: String) -> option.Option(Int) {
  do_find_top_level_keyword(s, " FROM ", 0, 0)
}

fn do_find_top_level_keyword(
  s: String,
  keyword: String,
  idx: Int,
  depth: Int,
) -> option.Option(Int) {
  let keyword_len = string.length(keyword)
  case string.length(s) < keyword_len {
    True -> option.None
    False -> {
      let head_char = string.slice(s, 0, 1)
      case head_char {
        "(" ->
          do_find_top_level_keyword(
            string.drop_start(s, 1),
            keyword,
            idx + 1,
            depth + 1,
          )
        ")" ->
          do_find_top_level_keyword(
            string.drop_start(s, 1),
            keyword,
            idx + 1,
            depth - 1,
          )
        _ ->
          case depth == 0 {
            True -> {
              let head = string.slice(s, 0, keyword_len)
              case string.uppercase(head) == string.uppercase(keyword) {
                True -> option.Some(idx)
                False ->
                  do_find_top_level_keyword(
                    string.drop_start(s, 1),
                    keyword,
                    idx + 1,
                    depth,
                  )
              }
            }
            False ->
              do_find_top_level_keyword(
                string.drop_start(s, 1),
                keyword,
                idx + 1,
                depth,
              )
          }
      }
    }
  }
}

/// Split a string on top-level commas (ignoring commas inside parens).
fn split_top_level_commas(s: String) -> List(String) {
  do_split_top_level_commas(s, "", [], 0)
}

fn do_split_top_level_commas(
  remaining: String,
  current: String,
  acc: List(String),
  depth: Int,
) -> List(String) {
  case string.pop_grapheme(remaining) {
    Error(_) ->
      case current {
        "" -> list.reverse(acc)
        _ -> list.reverse([string.trim(current), ..acc])
      }
    Ok(#("(", rest)) ->
      do_split_top_level_commas(rest, current <> "(", acc, depth + 1)
    Ok(#(")", rest)) ->
      do_split_top_level_commas(rest, current <> ")", acc, depth - 1)
    Ok(#(",", rest)) ->
      case depth {
        0 ->
          do_split_top_level_commas(rest, "", [string.trim(current), ..acc], 0)
        _ -> do_split_top_level_commas(rest, current <> ",", acc, depth)
      }
    Ok(#(char, rest)) ->
      do_split_top_level_commas(rest, current <> char, acc, depth)
  }
}

/// Parse a single SELECT-list item: `expr [AS alias]`.
/// Extracts the alias (or expression if no alias) and detects whether the
/// expression is a bare column reference (possibly with a table prefix).
fn parse_select_item(raw: String) -> SelectItem {
  let trimmed = string.trim(raw)
  // Split on last " AS " (case-insensitive, top-level)
  let #(expr, alias) = case rsplit_on_as(trimmed) {
    option.Some(#(e, a)) -> #(string.trim(e), string.trim(a))
    option.None -> #(trimmed, trimmed)
  }
  // For the alias, if it's the full expression (no explicit AS), derive a
  // clean name from it: strip table prefix, strip parens content.
  let clean_alias = case alias == expr {
    True -> {
      // Strip "table." prefix for aliased column refs
      case string.split_once(alias, ".") {
        Ok(#(_, after)) ->
          case is_simple_identifier(after) {
            True -> after
            False -> alias
          }
        Error(_) -> alias
      }
    }
    False -> alias
  }
  let bare_column = case is_simple_identifier(expr) {
    True -> option.Some(expr)
    False ->
      case string.split_once(expr, ".") {
        Ok(#(_, after)) ->
          case is_simple_identifier(after) {
            True -> option.Some(after)
            False -> option.None
          }
        Error(_) -> option.None
      }
  }
  let #(final_alias, override) = extract_nullability_override(clean_alias)
  SelectItem(
    alias: final_alias,
    expression: expr,
    bare_column: bare_column,
    override: override,
  )
}

fn extract_nullability_override(alias: String) -> #(String, NullabilityOverride) {
  case string.ends_with(alias, "!") {
    True -> #(string.drop_end(alias, 1), OverrideNonNull)
    False ->
      case string.ends_with(alias, "?") {
        True -> #(string.drop_end(alias, 1), OverrideNullable)
        False -> #(alias, OverrideNone)
      }
  }
}

/// Split on the LAST top-level " AS " (case-insensitive).
fn rsplit_on_as(s: String) -> option.Option(#(String, String)) {
  // Find all " AS " positions at depth 0, take the last.
  let upper = string.uppercase(s)
  let positions = find_all_top_level_as(upper, 0, 0, [])
  case list.last(positions) {
    Error(_) -> option.None
    Ok(pos) -> {
      let before = string.slice(s, 0, pos)
      let after = string.drop_start(s, pos + 4)
      option.Some(#(before, after))
    }
  }
}

fn find_all_top_level_as(
  s: String,
  idx: Int,
  depth: Int,
  acc: List(Int),
) -> List(Int) {
  case string.length(s) < 4 {
    True -> list.reverse(acc)
    False -> {
      let head = string.slice(s, 0, 1)
      case head {
        "(" ->
          find_all_top_level_as(
            string.drop_start(s, 1),
            idx + 1,
            depth + 1,
            acc,
          )
        ")" ->
          find_all_top_level_as(
            string.drop_start(s, 1),
            idx + 1,
            depth - 1,
            acc,
          )
        _ ->
          case depth == 0 && string.starts_with(s, " AS ") {
            True ->
              find_all_top_level_as(string.drop_start(s, 4), idx + 4, depth, [
                idx,
                ..acc
              ])
            False ->
              find_all_top_level_as(
                string.drop_start(s, 1),
                idx + 1,
                depth,
                acc,
              )
          }
      }
    }
  }
}

/// A "simple identifier" is an alpha/underscore-start followed by word chars.
fn is_simple_identifier(s: String) -> Bool {
  let trimmed = string.trim(s)
  case string.length(trimmed) {
    0 -> False
    _ -> {
      let graphemes = string.to_graphemes(trimmed)
      list.all(graphemes, is_identifier_char)
    }
  }
}

fn is_identifier_char(c: String) -> Bool {
  case c {
    "_" -> True
    _ -> {
      let code = case string.to_utf_codepoints(c) {
        [cp] -> string.utf_codepoint_to_int(cp)
        _ -> 0
      }
      // 0-9, A-Z, a-z
      { code >= 48 && code <= 57 }
      || { code >= 65 && code <= 90 }
      || { code >= 97 && code <= 122 }
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
  // Rowid-like opcodes write the rowid (PK) of cursor p1 into register p2.
  //   Rowid       — direct table cursor
  //   IdxRowid    — index cursor (resolves via parent table)
  //   SeekRowid   — seeks by rowid, writes result to p2
  let rowid_op =
    list.find(opcodes, fn(op) {
      case op.opcode {
        "Rowid" | "IdxRowid" | "SeekRowid" -> op.p2 == reg
        _ -> False
      }
    })

  case rowid_op {
    Ok(op) ->
      case dict.get(cursor_table, op.p1) {
        Ok(table) -> resolve_rowid_column(table, table_schemas, pk_columns)
        Error(_) ->
          Column(name: "rowid", column_type: query.IntType, nullable: False)
      }
    Error(_) -> {
      // Find the ResultRow address so we can prefer Column opcodes in the
      // output phase (after sorting/ephemeral fill) over those in the fill
      // phase. When ORDER BY is used, the same register may be written to
      // twice: once during the sorter-fill phase (writing a different value)
      // and once during the output phase (writing the actual result). We want
      // the output-phase write, which is the LAST Column writing to this
      // register before ResultRow.
      let result_row_addr =
        list.fold(opcodes, 0, fn(acc, op) {
          case op.opcode == "ResultRow" {
            True -> op.addr
            False -> acc
          }
        })

      let column_op =
        list.find(
          list.reverse(
            list.filter(opcodes, fn(op) { op.addr < result_row_addr }),
          ),
          fn(op) { op.opcode == "Column" && op.p3 == reg },
        )

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
  // Dedupe by parameter number (p1): SQLite sometimes emits multiple
  // Variable opcodes for a single `?` when the parameter is used in
  // multiple contexts (e.g., an indexed seek plus a post-seek filter).
  // We want one Parameter per `?` not per Variable opcode.
  let variable_ops =
    list.filter(opcodes, fn(op) { op.opcode == "Variable" })
    |> list.sort(fn(a, b) { int.compare(a.p1, b.p1) })
    |> dedupe_variables_by_p1

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
          case contains_keyword(sql, "VALUES") {
            True -> {
              let parsed = extract_insert_parameters(table_schemas, sql)
              case list.length(parsed) == param_count {
                True -> parsed
                False -> opcode_fallback()
              }
            }
            False -> {
              // INSERT ... SELECT: parameters come from the SELECT list
              // (positional, matched to INSERT target columns) plus any
              // WHERE conditions in the SELECT.
              let parsed = extract_insert_select_parameters(table_schemas, sql)
              case list.length(parsed) == param_count {
                True -> parsed
                False -> opcode_fallback()
              }
            }
          }
        }
        Update -> {
          let parsed = extract_update_parameters(table_schemas, sql)
          case list.length(parsed) == param_count {
            True -> parsed
            False -> opcode_fallback()
          }
        }
        Select | Delete -> {
          // Text-based WHERE clause parsing: match parameter positions
          // against columns mentioned in WHERE conditions. Covers simple
          // `col = ?` patterns; mixes with opcode fallback when counts
          // don't line up (e.g. subqueries, HAVING, LIMIT with params).
          let parsed = extract_select_parameters(table_schemas, sql, stmt_type)
          case list.length(parsed) == param_count {
            True -> parsed
            False -> opcode_fallback()
          }
        }
        Other -> opcode_fallback()
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
  // Parse the VALUES clause to find which positions have `?` placeholders
  // vs literals. The INSERT column at position i corresponds to the i-th
  // entry in VALUES(...); only the `?` entries become parameters.
  let values_positions = parse_values_placeholder_positions(sql)
  // Align column list with VALUES positions; keep only the ones bound
  // to a `?`. If positions list is empty (no VALUES or parse failed),
  // fall through with all columns and let the caller's count check
  // trigger opcode fallback.
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

/// Parse the VALUES(...) clause and return the 0-based positions where
/// a `?` placeholder appears. Used to align INSERT column names with
/// the subset that's bound by a parameter.
///
/// For `VALUES (?, 0, 0, ?, ?)` returns `[0, 3, 4]`.
fn parse_values_placeholder_positions(sql: String) -> List(Int) {
  let upper = string.uppercase(sql)
  case string.split_once(upper, " VALUES ") {
    Error(_) -> []
    Ok(#(before, _)) -> {
      let offset = string.length(before) + 8
      let rest = string.drop_start(sql, offset) |> string.trim_start
      // Take content inside the outermost parens
      case string.starts_with(rest, "(") {
        False -> []
        True -> {
          let inner_and_rest = string.drop_start(rest, 1)
          let inner = walk_matching_paren(inner_and_rest, 1)
          // inner is now everything AFTER the matching ) — we want BEFORE
          let inner_part =
            string.slice(
              inner_and_rest,
              0,
              string.length(inner_and_rest) - string.length(inner) - 1,
            )
          let parts = split_top_level_commas(inner_part)
          list.index_map(parts, fn(part, idx) { #(part, idx) })
          |> list.filter_map(fn(pair) {
            let #(part, idx) = pair
            let trimmed = string.trim(part)
            case trimmed {
              "?" -> Ok(idx)
              _ ->
                case string.starts_with(trimmed, "@") {
                  True -> Ok(idx)
                  False -> Error(Nil)
                }
            }
          })
        }
      }
    }
  }
}

fn dedupe_variables_by_p1(ops: List(Opcode)) -> List(Opcode) {
  do_dedupe_variables(ops, -1, [])
}

fn do_dedupe_variables(
  remaining: List(Opcode),
  last_p1: Int,
  acc: List(Opcode),
) -> List(Opcode) {
  case remaining {
    [] -> list.reverse(acc)
    [op, ..rest] ->
      case op.p1 == last_p1 {
        True -> do_dedupe_variables(rest, last_p1, acc)
        False -> do_dedupe_variables(rest, op.p1, [op, ..acc])
      }
  }
}

/// For INSERT ... SELECT, match parameters positionally across both the
/// SELECT list (where `?` at position i in the SELECT maps to the i-th
/// INSERT target column) and any WHERE clause in the SELECT.
fn extract_insert_select_parameters(
  table_schemas: Dict(String, List(Column)),
  sql: String,
) -> List(Parameter) {
  let target_cols = parse_insert_columns(sql)
  let target_table = parse_insert_table_name(sql)
  let target_col_types = case dict.get(table_schemas, target_table) {
    Ok(cols) -> cols
    Error(_) -> []
  }

  // Extract the SELECT ... FROM ... part that follows the INSERT column list
  let upper = string.uppercase(sql)
  let marker = ") SELECT "
  case string.split_once(upper, marker) {
    Error(_) -> []
    Ok(#(before, _)) -> {
      let offset = string.length(before) + string.length(marker)
      let after_select = string.drop_start(sql, offset) |> string.trim_start
      // SELECT-list ends at " FROM " at top level
      let end_idx = case
        do_find_top_level_keyword(after_select, " FROM ", 0, 0)
      {
        option.Some(idx) -> idx
        option.None -> string.length(after_select)
      }
      let select_list = string.slice(after_select, 0, end_idx)
      let items = split_top_level_commas(select_list)
      // For each item that is `?` or `@name`, use the target column at that position
      let select_params =
        items
        |> list.index_map(fn(item, idx) {
          let trimmed = string.trim(item)
          let is_anon = trimmed == "?"
          let is_named = string.starts_with(trimmed, "@")
          case is_anon || is_named {
            True ->
              case list_at(target_cols, idx) {
                Ok(col_name) ->
                  case
                    list.find(target_col_types, fn(c) { c.name == col_name })
                  {
                    Ok(col) -> {
                      // For named params, use the @name (stripped of @) as the
                      // parameter name; for positional ?, use the column name.
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
      // Now also look in the SELECT's FROM/WHERE part for `?` bindings
      let from_onwards = string.drop_start(after_select, end_idx)
      let where_tables =
        find_all_subquery_tables(from_onwards)
        |> list.filter(fn(t) {
          case dict.get(table_schemas, t) {
            Ok(_) -> True
            Error(_) -> False
          }
        })
      let where_binders = find_param_binders(from_onwards, 0, [])
      // Filter out named params that were already matched in the SELECT list
      let select_param_names = list.map(select_params, fn(p) { p.name })
      let unmatched_where_binders =
        list.filter(where_binders, fn(b) {
          !list.contains(select_param_names, b.name)
        })
      let where_params =
        list.map(unmatched_where_binders, fn(binder) {
          // Build a list of column names to try: the binder_column (with table
          // prefix stripped) takes priority, then fall back to binder.name itself.
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

/// For SELECT/DELETE statements, scan the SQL for `column OP ?` patterns
/// (in any scope, including subqueries) and resolve each `?` to a Parameter
/// in positional order. Returns an empty list when any `?` can't be cleanly
/// mapped to a preceding column — the caller falls back to opcode inference.
fn extract_select_parameters(
  table_schemas: Dict(String, List(Column)),
  sql: String,
  stmt_type: StatementType,
) -> List(Parameter) {
  let all_tables = collect_all_tables(sql, stmt_type, table_schemas)
  let binders = find_param_binders(sql, 0, [])
  case list.is_empty(binders) {
    True -> []
    False ->
      list.map(binders, fn(binder) {
        // Look up column by the binder name OR (for named params) by the
        // column found on the comparison LHS.
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

/// Collect every table referenced in the SQL: the main FROM, any JOINs,
/// plus tables introduced by subquery FROM clauses.
fn collect_all_tables(
  sql: String,
  stmt_type: StatementType,
  table_schemas: Dict(String, List(Column)),
) -> List(String) {
  let from_tables = case stmt_type {
    Select -> parse_from_tables(sql)
    Delete -> [parse_delete_table_name(sql)]
    _ -> []
  }
  let subquery_tables = find_all_subquery_tables(sql)
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

fn find_all_subquery_tables(sql: String) -> List(String) {
  do_find_subquery_tables(sql, [])
}

fn do_find_subquery_tables(sql: String, acc: List(String)) -> List(String) {
  let upper = string.uppercase(sql)
  case string.split_once(upper, " FROM ") {
    Error(_) -> list.reverse(acc)
    Ok(#(before, _)) -> {
      let offset = string.length(before) + 6
      let rest = string.drop_start(sql, offset) |> string.trim_start
      let table =
        rest
        |> string.to_graphemes
        |> list.take_while(fn(c) { c == "_" || is_alphanumeric_char(c) })
        |> string.join("")
      let new_acc = case table {
        "" -> acc
        name -> [name, ..acc]
      }
      do_find_subquery_tables(rest, new_acc)
    }
  }
}

fn is_alphanumeric_char(c: String) -> Bool {
  case c {
    "_" -> True
    _ -> {
      let code = case string.to_utf_codepoints(c) {
        [cp] -> string.utf_codepoint_to_int(cp)
        _ -> 0
      }
      { code >= 48 && code <= 57 }
      || { code >= 65 && code <= 90 }
      || { code >= 97 && code <= 122 }
    }
  }
}

/// Walk through the SQL left-to-right finding each top-level `?` or named
/// parameter (`@name`, `:name`), and for each `?` look backwards for the
/// nearest `column op` that binds it. Named parameters use the given name
/// directly. Returns column/param names in positional order, or [] if any
/// `?` fails to resolve.
fn find_param_binders(sql: String, idx: Int, acc: List(Binder)) -> List(Binder) {
  let found = find_next_placeholder(sql, idx)
  case found {
    PlaceholderNone -> list.reverse(acc)
    PlaceholderAnon(pos, after) -> {
      let before = string.slice(sql, 0, pos)
      case extract_column_binder(before) {
        option.None -> []
        option.Some(col) -> {
          // Strip any `table.` prefix — the Parameter name becomes a Gleam
          // identifier, and dots get stripped which would mangle the name.
          let bare = case string.split_once(col, ".") {
            Ok(#(_, after)) -> after
            Error(_) -> col
          }
          find_param_binders(sql, after, [
            Binder(name: bare, binder_column: option.Some(col)),
            ..acc
          ])
        }
      }
    }
    PlaceholderNamed(name, after) ->
      case list.find(acc, fn(b) { b.name == name }) {
        Ok(_) -> find_param_binders(sql, after, acc)
        Error(_) -> {
          // For named params, also look for a column binder on the LHS
          // of the comparison — lets us infer the type even when the
          // param name doesn't match a schema column.
          let before = string.slice(sql, 0, after - string.length(name) - 1)
          let column = extract_column_binder(before)
          find_param_binders(sql, after, [
            Binder(name: name, binder_column: column),
            ..acc
          ])
        }
      }
  }
}

type Binder {
  Binder(name: String, binder_column: option.Option(String))
}

type Placeholder {
  PlaceholderNone
  PlaceholderAnon(pos: Int, after: Int)
  PlaceholderNamed(name: String, after: Int)
}

/// Find the next parameter placeholder (either `?`, `@name`, or `:name`)
/// at or after from_idx, skipping single-quoted string literals.
fn find_next_placeholder(sql: String, from_idx: Int) -> Placeholder {
  let rest = string.drop_start(sql, from_idx)
  do_find_placeholder(rest, from_idx, False)
}

fn do_find_placeholder(s: String, idx: Int, in_string: Bool) -> Placeholder {
  case string.pop_grapheme(s) {
    Error(_) -> PlaceholderNone
    Ok(#("'", rest)) -> do_find_placeholder(rest, idx + 1, !in_string)
    Ok(#("?", rest)) ->
      case in_string {
        True -> do_find_placeholder(rest, idx + 1, True)
        False -> PlaceholderAnon(idx, idx + 1)
      }
    Ok(#("@", rest)) ->
      case in_string {
        True -> do_find_placeholder(rest, idx + 1, True)
        False -> read_named_placeholder(rest, idx + 1)
      }
    Ok(#(":", rest)) ->
      case in_string {
        True -> do_find_placeholder(rest, idx + 1, True)
        False -> read_named_placeholder(rest, idx + 1)
      }
    Ok(#(_, rest)) -> do_find_placeholder(rest, idx + 1, in_string)
  }
}

fn read_named_placeholder(s: String, start_idx: Int) -> Placeholder {
  do_read_named(s, start_idx, "")
}

fn do_read_named(s: String, idx: Int, acc: String) -> Placeholder {
  case string.pop_grapheme(s) {
    Error(_) ->
      case acc {
        "" -> PlaceholderNone
        n -> PlaceholderNamed(n, idx)
      }
    Ok(#(char, rest)) ->
      case is_alphanumeric_char(char) {
        True -> do_read_named(rest, idx + 1, acc <> char)
        False ->
          case acc {
            "" -> do_find_placeholder(s, idx, False)
            n -> PlaceholderNamed(n, idx)
          }
      }
  }
}

/// Given the SQL text before a `?`, extract the column name being compared
/// against it. Looks for trailing ` column OP ` pattern. Handles:
/// - Symbolic operators: `=`, `>=`, `<=`, `!=`, `<>`, `>`, `<`
/// - Keyword operators: LIKE, IS, IS NOT
/// - BETWEEN: `col BETWEEN ?` and `col BETWEEN ? AND ?` (both bind to col)
/// - Function-call LHS: `LOWER(col) = ?` → unwrap to `col`
fn extract_column_binder(before: String) -> option.Option(String) {
  let trimmed = string.trim_end(before)
  let upper = string.uppercase(trimmed)
  // Check for BETWEEN bindings first. Two forms:
  //   1. `col BETWEEN ? AND` — the first `?` of a BETWEEN
  //   2. `col BETWEEN literal AND` or `col BETWEEN ? AND` — the second
  //      `?` (where `AND` is the separator inside BETWEEN, NOT a boolean
  //      connector).
  case extract_between_column(trimmed, upper) {
    option.Some(col) -> option.Some(col)
    option.None -> {
      // Try symbolic operators first
      let sym_operators = [">=", "<=", "!=", "<>", "=", ">", "<"]
      let without_sym = strip_trailing_operator(trimmed, sym_operators)
      let without_op = case without_sym == trimmed {
        True -> strip_trailing_keyword_operator(trimmed)
        False -> without_sym
      }
      let trimmed2 = string.trim_end(without_op)
      let id = case take_trailing_identifier(trimmed2) {
        "" -> extract_identifier_from_trailing_parens(trimmed2)
        name -> name
      }
      case id {
        "" -> option.None
        name ->
          case is_simple_column_ref(name) {
            True -> option.Some(name)
            False -> option.None
          }
      }
    }
  }
}

/// Recognize the BETWEEN pattern and return the column name.
/// - `col BETWEEN` → first `?` of a BETWEEN. Returns col.
/// - `col BETWEEN <literal> AND` or `col BETWEEN ? AND` → second `?`. Returns col.
///
/// Uses the original (not uppercased) trimmed string for column extraction
/// so downstream schema lookups match the column name's real case.
fn extract_between_column(
  trimmed: String,
  upper: String,
) -> option.Option(String) {
  // Case 1: ends with " BETWEEN"
  case string.ends_with(upper, " BETWEEN") {
    True -> {
      let without_kw = string.drop_end(trimmed, 8)
      let name = take_trailing_identifier(string.trim_end(without_kw))
      case is_simple_column_ref(name) {
        True -> option.Some(name)
        False -> option.None
      }
    }
    False ->
      // Case 2: ends with " AND" and earlier has " BETWEEN "
      case string.ends_with(upper, " AND") {
        True -> {
          let upper_before_and = string.drop_end(upper, 4)
          case string.split_once(upper_before_and, " BETWEEN ") {
            Ok(#(before_between_upper, _)) -> {
              // Use original-case string — cut at the length of the upper
              // prefix (ASCII, so char count matches byte count).
              let prefix_len = string.length(before_between_upper)
              let before_between = string.slice(trimmed, 0, prefix_len)
              let trimmed_before_between = string.trim_end(before_between)
              let name = take_trailing_identifier(trimmed_before_between)
              case is_simple_column_ref(name) {
                True -> option.Some(name)
                False -> option.None
              }
            }
            Error(_) -> option.None
          }
        }
        False -> option.None
      }
  }
}

/// Given text ending in `...(inner)`, return the last identifier inside
/// the matched parens. Used for `LOWER(email) = ?` and similar.
fn extract_identifier_from_trailing_parens(s: String) -> String {
  let trimmed = string.trim_end(s)
  case string.ends_with(trimmed, ")") {
    False -> ""
    True -> {
      // Walk backwards matching parens to find the opening `(`.
      let without_close = string.slice(trimmed, 0, string.length(trimmed) - 1)
      let inside = find_matching_paren_content(without_close, 1, "")
      // Take the last identifier inside (could be "table.col" or just "col")
      take_trailing_identifier(string.trim_end(inside))
    }
  }
}

fn find_matching_paren_content(s: String, depth: Int, acc: String) -> String {
  case depth {
    0 -> acc
    _ ->
      case string.pop_grapheme(string.reverse(s)) {
        Error(_) -> acc
        Ok(#(")", rev_rest)) -> {
          let remaining = string.reverse(rev_rest)
          find_matching_paren_content(remaining, depth + 1, ")" <> acc)
        }
        Ok(#("(", rev_rest)) -> {
          let remaining = string.reverse(rev_rest)
          case depth {
            1 -> acc
            _ -> find_matching_paren_content(remaining, depth - 1, "(" <> acc)
          }
        }
        Ok(#(char, rev_rest)) -> {
          let remaining = string.reverse(rev_rest)
          find_matching_paren_content(remaining, depth, char <> acc)
        }
      }
  }
}

/// Strip trailing `LIKE`, `IS NOT`, or `IS` (case-insensitive) from s.
/// Returns s unchanged if no match.
fn strip_trailing_keyword_operator(s: String) -> String {
  let upper = string.uppercase(s)
  let keywords = [" LIKE", " IS NOT", " IS"]
  do_strip_trailing_keyword(s, upper, keywords)
}

fn do_strip_trailing_keyword(
  s: String,
  upper: String,
  keywords: List(String),
) -> String {
  case keywords {
    [] -> s
    [kw, ..rest] -> {
      case string.ends_with(upper, kw) {
        True -> string.slice(s, 0, string.length(s) - string.length(kw))
        False -> do_strip_trailing_keyword(s, upper, rest)
      }
    }
  }
}

fn strip_trailing_operator(s: String, operators: List(String)) -> String {
  case operators {
    [] -> s
    [op, ..rest] -> {
      let len = string.length(op)
      case string.length(s) >= len {
        True -> {
          let tail = string.slice(s, string.length(s) - len, len)
          case string.uppercase(tail) == op {
            True -> string.slice(s, 0, string.length(s) - len)
            False -> strip_trailing_operator(s, rest)
          }
        }
        False -> strip_trailing_operator(s, rest)
      }
    }
  }
}

fn take_trailing_identifier(s: String) -> String {
  let graphemes = string.to_graphemes(s)
  take_trailing_id_chars(list.reverse(graphemes), [])
  |> string.join("")
}

fn take_trailing_id_chars(
  reversed: List(String),
  acc: List(String),
) -> List(String) {
  case reversed {
    [] -> acc
    [char, ..rest] ->
      case is_ident_or_dot(char) {
        True -> take_trailing_id_chars(rest, [char, ..acc])
        False -> acc
      }
  }
}

fn is_ident_or_dot(c: String) -> Bool {
  c == "." || is_alphanumeric_char(c) || c == "_"
}

fn is_simple_column_ref(s: String) -> Bool {
  // Accept "col" or "table.col" where col and table are simple identifiers.
  case string.split_once(s, ".") {
    Ok(#(table, col)) ->
      is_simple_identifier(table) && is_simple_identifier(col)
    Error(_) -> is_simple_identifier(s)
  }
}

/// For UPDATE statements, parameters correspond to SET columns then WHERE columns.
/// set_params is a list of #(param_name, lookup_col) tuples — the param_name is
/// used as the generated function argument label, and lookup_col is used to find
/// the column type from the table schema (they differ when RHS is an expression
/// like COALESCE(@gender, gender) or balance_cents + @amount_cents).
fn extract_update_parameters(
  table_schemas: Dict(String, List(Column)),
  sql: String,
) -> List(Parameter) {
  let table_name = parse_update_table_name(sql)
  let set_params = parse_update_set_columns(sql)
  let where_params = parse_where_columns(sql)

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
          // First try the main UPDATE table, then search all tables (for subquery params)
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
  // Eq/Ne/Lt/Le/Gt/Ge: p1 and p3 are both registers being compared.
  // SeekGE/SeekGT/SeekLE/SeekLT: p1 is a CURSOR; p3 is the key register.
  let eq_ops = ["Eq", "Ne", "Lt", "Le", "Gt", "Ge"]
  let seek_ops = ["SeekGE", "SeekGT", "SeekLE", "SeekLT"]

  // Find a comparison that involves our variable register
  let comparison =
    list.find(opcodes, fn(op) {
      case list.contains(eq_ops, op.opcode) {
        True -> op.p1 == var_reg || op.p3 == var_reg
        False ->
          case list.contains(seek_ops, op.opcode) {
            True -> op.p3 == var_reg
            False -> False
          }
      }
    })

  case comparison {
    Ok(cmp) -> {
      let other_reg = case list.contains(seek_ops, cmp.opcode) {
        // For Seek opcodes, the "other side" is the index column which
        // is implicit: the first column of the index p1. Look at the
        // most recent Column opcode on the same cursor before this seek.
        True -> -1
        False ->
          case cmp.p1 == var_reg {
            True -> cmp.p3
            False -> cmp.p1
          }
      }
      case other_reg {
        -1 -> resolve_seek_cursor_key(cmp, cursor_table, table_schemas)
        _ ->
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
                  Parameter(
                    name: pk_name,
                    column_type: query.IntType,
                    nullable: False,
                  )
                Error(_) ->
                  Parameter(
                    name: "id",
                    column_type: query.IntType,
                    nullable: False,
                  )
              }
            Error(_) ->
              Parameter(name: "id", column_type: query.IntType, nullable: False)
          }
        Error(_) ->
          Parameter(name: "param", column_type: StringType, nullable: False)
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
    Error(_) ->
      Parameter(name: "param", column_type: StringType, nullable: False)
  }
}

/// For SeekGE/GT/LE/LT on an index, the key register holds a value being
/// compared against the FIRST column of the index. Look up the index's
/// parent table and get that column's metadata.
fn resolve_seek_cursor_key(
  seek_op: Opcode,
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
) -> Parameter {
  let cursor = seek_op.p1
  case dict.get(cursor_table, cursor) {
    Ok(table_name) ->
      case dict.get(table_schemas, table_name) {
        Ok(table_cols) ->
          // We don't know the index column directly. Fall back to looking
          // at the nearest Column opcode with the same cursor before this
          // seek (which gives us a column hint). If none, use a safe
          // default of Int (most common seek key type).
          case list.first(table_cols) {
            Ok(col) ->
              Parameter(
                name: col.name,
                column_type: col.column_type,
                nullable: col.nullable,
              )
            Error(_) ->
              Parameter(
                name: "param",
                column_type: query.IntType,
                nullable: False,
              )
          }
        Error(_) ->
          Parameter(name: "param", column_type: query.IntType, nullable: False)
      }
    Error(_) ->
      Parameter(name: "param", column_type: query.IntType, nullable: False)
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
            Ok(col) ->
              Parameter(
                name: col.name,
                column_type: col.column_type,
                nullable: col.nullable,
              )
            Error(_) ->
              Parameter(name: "param", column_type: StringType, nullable: False)
          }
        Error(_) ->
          Parameter(name: "param", column_type: StringType, nullable: False)
      }
    Error(_) ->
      Parameter(name: "param", column_type: StringType, nullable: False)
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
/// Parse SET assignments from an UPDATE statement.
/// Returns a list of #(param_name, lookup_col) tuples where:
/// - param_name: the generated function argument label (from @name if present, else col_name)
/// - lookup_col: the table column used for type lookup (always the LHS col_name)
///
/// Handles simple assignments (`col = ?` or `col = @col`), expressions with named
/// params (`col = COALESCE(@param, col)` or `col = col + @param`), and skips
/// assignments with no bound parameter (`col = other_col`, `col = col + 1`).
/// Uses split_top_level_commas to avoid breaking on commas inside COALESCE/subqueries.
fn parse_update_set_columns(sql: String) -> List(#(String, String)) {
  let upper = string.uppercase(sql)
  case split_on_keyword(upper, " SET ") {
    Ok(#(_, rest_upper)) -> {
      let offset = string.length(sql) - string.length(rest_upper)
      let rest = string.drop_start(sql, offset)
      // Get the part between SET and WHERE/RETURNING (top-level only)
      let set_part = case find_top_level_keyword_offset(rest, " WHERE ") {
        option.Some(idx) -> string.slice(rest, 0, idx)
        option.None ->
          case find_top_level_keyword_offset(rest, " RETURNING ") {
            option.Some(idx) -> string.slice(rest, 0, idx)
            option.None -> rest
          }
      }
      set_part
      |> split_top_level_commas
      |> list.filter_map(fn(part) {
        case string.split_once(string.trim(part), "=") {
          Ok(#(col_name, rhs)) -> {
            let col = string.trim(col_name)
            let rhs_trimmed = string.trim(rhs)
            case string.contains(rhs_trimmed, "?") {
              True ->
                // Positional param: use col name as both param name and lookup col
                Ok(#(col, col))
              False ->
                case extract_named_param_from_rhs(rhs_trimmed) {
                  Ok(param_name) ->
                    // Named param: use extracted @name as param name, LHS col for type lookup
                    Ok(#(param_name, col))
                  Error(_) ->
                    // No bound parameter — skip (e.g., `col = other_col + 1`)
                    Error(Nil)
                }
            }
          }
          Error(_) -> Error(Nil)
        }
      })
    }
    Error(_) -> []
  }
}

/// Extract the first @name identifier from an RHS expression.
/// E.g. "COALESCE(@gender, gender)" -> Ok("gender")
///      "@updated_at" -> Ok("updated_at")
///      "balance_cents + @amount_cents" -> Ok("amount_cents")
///      "other_col" -> Error(Nil)
fn extract_named_param_from_rhs(rhs: String) -> Result(String, Nil) {
  case string.split_once(rhs, "@") {
    Error(_) -> Error(Nil)
    Ok(#(_, after_at)) -> {
      // Collect chars until non-identifier character
      Ok(take_identifier_chars(after_at, ""))
    }
  }
}

fn take_identifier_chars(s: String, acc: String) -> String {
  case string.pop_grapheme(s) {
    Error(_) -> acc
    Ok(#(char, rest)) -> {
      case is_identifier_char(char) {
        True -> take_identifier_chars(rest, acc <> char)
        False -> acc
      }
    }
  }
}

/// Parse WHERE column names from UPDATE/DELETE statement.
/// Uses depth-aware top-level WHERE detection so subquery WHERE clauses
/// (inside parentheses) are not mistaken for the main WHERE.
/// Returns a list of #(param_name, lookup_col) tuples for WHERE parameters.
/// - param_name: the generated function argument label
/// - lookup_col: the schema column to use for type inference
/// For simple conditions like "col = ?" or "col = @name", both are the same col.
/// For arithmetic conditions like "balance_cents + @min_delta >= 0",
/// param_name is "min_delta" and lookup_col is "balance_cents".
/// For IN (subquery) conditions like "col IN (SELECT ... WHERE x = @param)",
/// param_name is extracted from the subquery's @named param.
fn parse_where_columns(sql: String) -> List(#(String, String)) {
  case find_top_level_keyword_offset(sql, " WHERE ") {
    option.None -> []
    option.Some(where_offset) -> {
      let offset = where_offset + string.length(" WHERE ")
      let rest = string.drop_start(sql, offset)
      // Get part before RETURNING if present (also top-level only)
      let where_part = case find_top_level_keyword_offset(rest, " RETURNING ") {
        option.Some(ret_offset) -> string.slice(rest, 0, ret_offset)
        option.None -> rest
      }
      // Replace AND/OR with commas (case-insensitive) to split conditions
      split_where_conditions(where_part)
      |> list.filter_map(fn(part) {
        let trimmed = string.trim(part)
        // Match patterns like "col = ?", "col > ?", "col=?", or "col = @name"
        case string.contains(trimmed, "?") || string.contains(trimmed, "@") {
          True -> {
            // Special case: "col IN (subquery)" or "col = (subquery)" where
            // @param is inside the subquery. Extract the first @named param
            // from the subquery RHS. Type lookup searches all tables since
            // the column may come from a JOINed table, not the main UPDATE table.
            let upper = string.uppercase(trimmed)
            let has_subquery =
              string.contains(upper, " IN (SELECT")
              || string.contains(upper, " IN ( SELECT")
              || string.contains(upper, "= (SELECT")
              || string.contains(upper, "= ( SELECT")
            case has_subquery {
              True -> {
                case extract_named_param_from_rhs(trimmed) {
                  Ok(param_name) -> Ok(#(param_name, param_name))
                  Error(_) -> Error(Nil)
                }
              }
              False -> {
                let lhs = extract_column_before_operator(trimmed)
                case lhs {
                  "" -> Error(Nil)
                  _ -> {
                    // If LHS contains "@", it's an arithmetic expr like "balance_cents + @min_delta"
                    // Extract the @param_name and use the base column for type lookup
                    case string.contains(lhs, "@") {
                      True -> {
                        case extract_named_param_from_rhs(lhs) {
                          Ok(param_name) -> {
                            // Find the base column (everything before the @)
                            let lookup_col = case string.split_once(lhs, "@") {
                              Ok(#(before_at, _)) ->
                                // Strip the arithmetic operator and spaces
                                before_at
                                |> string.trim
                                |> fn(s) {
                                  case
                                    string.ends_with(s, "+")
                                    || string.ends_with(s, "-")
                                    || string.ends_with(s, "*")
                                    || string.ends_with(s, "/")
                                  {
                                    True -> string.drop_end(s, 1) |> string.trim
                                    False -> s
                                  }
                                }
                              Error(_) -> param_name
                            }
                            Ok(#(param_name, normalize_column_ref(lookup_col)))
                          }
                          Error(_) ->
                            Ok(#(
                              normalize_column_ref(lhs),
                              normalize_column_ref(lhs),
                            ))
                        }
                      }
                      False -> {
                        let col_name = normalize_column_ref(lhs)
                        Ok(#(col_name, col_name))
                      }
                    }
                  }
                }
              }
            }
          }
          False -> Error(Nil)
        }
      })
    }
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

  // Also map index rootpages to their parent table. When queries use
  // indexed lookups, SQLite emits OpenRead/IdxRowid against the index
  // cursor; resolving that back to the parent table lets us infer the
  // rowid/PK column correctly.
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

// --- Returns annotation parser ---

pub type ReturnsAnnotationError {
  InvalidReturnsTypeName(name: String, reason: String)
}

/// Parse the `-- returns: Name` annotation from the top of a SQL file.
/// Returns `Ok(None)` when no annotation exists, `Ok(Some(name))` when a valid
/// annotation is present at the top (before any SQL statement), and
/// `Error(...)` when an annotation is present but malformed (invalid name,
/// missing Row suffix).
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

/// A decoder that handles both string and non-string p4 values
fn flexible_string_decoder() -> decode.Decoder(String) {
  decode.one_of(decode.string, [
    decode.map(decode.int, int.to_string),
    decode.map(decode.float, fn(_) { "" }),
    decode.success(""),
  ])
}
