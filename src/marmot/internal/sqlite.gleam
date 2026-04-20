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
            normalized_sql,
          )
      }
  }

  // Determine parameters
  let parameters =
    extract_parameters(
      opcodes,
      cursor_table,
      table_schemas,
      pk_columns,
      normalized_sql,
    )

  let parameters = deduplicate_parameter_names(parameters)
  Ok(QueryInfo(columns: columns, parameters: parameters))
}

/// Normalize SQL whitespace: strip line comments (-- ... up to newline),
/// convert newlines and tabs to spaces, then collapse runs of spaces into
/// single spaces. Trims leading/trailing whitespace. Safe on SQL because
/// string literals aren't split across lines in our queries and whitespace
/// inside identifiers isn't allowed.
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
        let opcode_column =
          find_column_for_register(
            reg,
            opcodes,
            cursor_table,
            table_schemas,
            pk_columns,
          )
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
            case item.bare_column {
              option.Some(_) ->
                case
                  resolve_select_item(
                    idx,
                    select_items,
                    from_tables,
                    table_schemas,
                  )
                {
                  Column(name: "unknown", ..) ->
                    Column(..opcode_column, name: item.alias)
                  resolved -> resolved
                }
              option.None ->
                case opcode_column.name {
                  "unknown" ->
                    resolve_select_item(
                      idx,
                      select_items,
                      from_tables,
                      table_schemas,
                    )
                  _ -> Column(..opcode_column, name: item.alias)
                }
            }
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
                    Ok(col) -> Ok(Column(..col, name: item.alias))
                    Error(_) -> Error(Nil)
                  }
                }
                Error(_) -> Error(Nil)
              }
            option.None -> Error(Nil)
          }
        })
      case resolved {
        Ok(col) -> col
        Error(_) ->
          Column(name: item.alias, column_type: StringType, nullable: True)
      }
    }
  }
}

type SelectItem {
  SelectItem(
    /// The display/field name (from AS alias or the expression itself)
    alias: String,
    /// If the expression is a bare column reference (possibly "table.col"),
    /// this is the column name (without table prefix). None for expressions
    /// like COUNT(*), COALESCE(...), subqueries, etc.
    bare_column: option.Option(String),
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
          let upper = string.uppercase(after_select)
          [
            find_keyword_idx(upper, "WHERE"),
            find_keyword_idx(upper, "GROUP BY"),
            find_keyword_idx(upper, "ORDER BY"),
            find_keyword_idx(upper, "LIMIT"),
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
  SelectItem(alias: clean_alias, bare_column: bare_column)
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

  case dict.get(table_schemas, table) {
    Ok(table_cols) ->
      list.map(columns, fn(col_name) {
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
      list.map(columns, fn(col_name) {
        Parameter(name: col_name, column_type: StringType, nullable: False)
      })
  }
}

/// For SELECT/DELETE statements, parameters correspond to columns mentioned
/// in WHERE conditions (in positional order). Column types come from any
/// table in the FROM list. Useful for `col = ?`, `col > ?`, etc.
///
/// Returns an empty list if the WHERE clause contains subqueries, complex
/// expressions, or any condition whose extracted column name isn't a simple
/// identifier. The caller falls back to opcode-based inference in that case.
fn extract_select_parameters(
  table_schemas: Dict(String, List(Column)),
  sql: String,
  stmt_type: StatementType,
) -> List(Parameter) {
  let cols = parse_where_columns(sql)
  // Reject if any extracted "column" isn't a clean identifier or qualified
  // identifier like `t.col`. Protects against subqueries (`id IN (SELECT
  // ...)`), complex predicates, etc.
  let all_simple = list.all(cols, fn(name) { is_simple_column_ref(name) })
  case all_simple {
    False -> []
    True -> {
      let from_tables = case stmt_type {
        Select -> parse_from_tables(sql)
        Delete -> [parse_delete_table_name(sql)]
        _ -> []
      }
      list.map(cols, fn(col_name) {
        // Strip table prefix for schema lookup
        let bare_name = case string.split_once(col_name, ".") {
          Ok(#(_, after)) -> after
          Error(_) -> col_name
        }
        // Find the column in any of the FROM tables
        let resolved =
          list.find_map(from_tables, fn(table) {
            case dict.get(table_schemas, table) {
              Ok(cols) ->
                case list.find(cols, fn(c) { c.name == bare_name }) {
                  Ok(col) -> Ok(col)
                  Error(_) -> Error(Nil)
                }
              Error(_) -> Error(Nil)
            }
          })
        case resolved {
          Ok(col) ->
            Parameter(
              name: bare_name,
              column_type: col.column_type,
              nullable: col.nullable,
            )
          Error(_) ->
            Parameter(name: bare_name, column_type: StringType, nullable: False)
        }
      })
    }
  }
}

fn is_simple_column_ref(s: String) -> Bool {
  // Accept "col" or "table.col" where col and table are simple identifiers.
  case string.split_once(s, ".") {
    Ok(#(table, col)) ->
      is_simple_identifier(table) && is_simple_identifier(col)
    Error(_) -> is_simple_identifier(s)
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
      list.map(all_cols, fn(col_name) {
        Parameter(name: col_name, column_type: StringType, nullable: False)
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

/// A decoder that handles both string and non-string p4 values
fn flexible_string_decoder() -> decode.Decoder(String) {
  decode.one_of(decode.string, [
    decode.map(decode.int, int.to_string),
    decode.map(decode.float, fn(_) { "" }),
    decode.success(""),
  ])
}
