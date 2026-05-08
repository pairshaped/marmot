import gleam/dict
import gleam/dynamic/decode
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import marmot/internal/query.{type Column, type Parameter, Column, StringType}
import marmot/internal/sqlite/opcode.{Opcode}
import marmot/internal/sqlite/parameters
import marmot/internal/sqlite/parse
import marmot/internal/sqlite/parse/statement
import marmot/internal/sqlite/results
import marmot/internal/sqlite/schema
import marmot/internal/sqlite/tokenize
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
    parameters.extract_parameters(
      opcodes,
      cursor_table,
      table_schemas,
      pk_columns,
      param_tokens,
    )

  let parameters = parameters.deduplicate_parameter_names(parameters)
  Ok(QueryInfo(columns: columns, parameters: parameters))
}

/// Delegate to parse module for public API compatibility
pub fn strip_nullability_suffixes(sql: String) -> String {
  parse.strip_nullability_suffixes(sql)
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
