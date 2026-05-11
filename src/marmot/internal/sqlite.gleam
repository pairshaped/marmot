//// Central introspection pipeline: given a DB connection and SQL text, return
//// result columns and parameter types.
////
//// The pipeline normalizes SQL, loads schema metadata, runs EXPLAIN, maps
//// cursors to tables, computes join nullability, tokenizes, classifies the
//// statement, and extracts results + parameters. Each stage is a focused
//// helper in this module or a delegate to the sqlite/ sub-modules.
////
//// What lives here: pipeline orchestration (introspect_query), PRAGMA-level
//// helpers (introspect_columns), and the returns-annotation parser.
//// What lives elsewhere: opcode analysis -> opcode.gleam, tokenizing ->
//// tokenize.gleam, text parsing -> parse/*.gleam, result extraction ->
//// results.gleam, parameter extraction -> parameters.gleam.

import gleam/bool
import gleam/dict
import gleam/dynamic/decode
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import marmot/internal/error.{type MarmotError}
import marmot/internal/query.{type Column, type Parameter, Column, StringType}
import marmot/internal/sqlite/opcode.{Opcode}
import marmot/internal/sqlite/parameters
import marmot/internal/sqlite/parse
import marmot/internal/sqlite/parse/statement_parser
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
///
/// The `path` argument is attached to any error returned for the user; it is
/// the SQL file path (or any placeholder for in-memory tests).
pub fn introspect_query(
  db: Connection,
  path: String,
  sql: String,
) -> Result(QueryInfo, MarmotError) {
  // Normalize whitespace (newlines/tabs -> spaces, collapse runs). All keyword
  // detection and SQL parsing below relies on single-space separators.
  let normalized_sql = parse.normalize_sql_whitespace(sql)

  let v2 = schema.get_table_metadata_v2(db)
  let table_schemas =
    dict.map_values(v2.columns, fn(_, metas) {
      list.map(metas, fn(m) { m.column })
    })
  let pk_columns = v2.pks
  let rootpage_table = v2.rootpages

  // Pre-flight: validate INSERT VALUES row counts against the schema before
  // handing to EXPLAIN. SQLite rejects count-mismatched INSERT VALUES with a
  // SqlError before EXPLAIN can run, so we must catch this case ourselves and
  // return our typed error instead.
  //
  // `parameters.extract_insert_parameters_v2` also runs a row-count check
  // (`validate_row_counts`) during extraction. The two are intentional: this
  // pre-flight runs *before* EXPLAIN to intercept SQLite's generic message,
  // while the extraction-stage check is a safety net for the param pipeline
  // (different error type, different layer). Keep both.
  let preflight_tokens = tokenize.tokenize(normalized_sql)
  use _ <- result.try(validate_insert_values_counts(preflight_tokens, v2, path))

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

  use opcodes <- result.try(
    sqlight.query(explain_sql, on: db, with: [], expecting: decoder)
    |> result.map_error(fn(err) {
      error.SqlError(path: path, message: err.message)
    }),
  )

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

  let tokens = tokenize.tokenize(normalized_sql)

  let parsed_stmt = statement_parser.parse(tokens)
  let returning_tokens = case parsed_stmt {
    Ok(statement_parser.Insert(stmt)) -> stmt.returning
    Ok(statement_parser.Update(stmt)) -> stmt.returning
    Ok(statement_parser.Delete(stmt)) -> stmt.returning
    _ -> option.None
  }
  let has_returning = option.is_some(returning_tokens)
  let is_insert = case parsed_stmt {
    Ok(statement_parser.Insert(_)) -> True
    _ -> False
  }

  let columns = case has_returning {
    True -> {
      let table_name = case parsed_stmt {
        Ok(statement_parser.Insert(stmt)) -> stmt.target.table.name.text
        Ok(statement_parser.Update(stmt)) -> stmt.target.table.name.text
        Ok(statement_parser.Delete(stmt)) -> stmt.target.table.name.text
        _ -> ""
      }
      let returning_body = option.unwrap(returning_tokens, [])
      results.extract_returning_columns(
        returning_body,
        table_name,
        table_schemas,
      )
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

  // Determine parameters. Tokenize the suffix-stripped SQL separately.
  // The first tokenization (above) runs against normalized_sql which still
  // has `!`/`?` alias suffixes; those produce NullOverride/NullableOverride
  // tokens. The suffix-stripped version has those removed, so `?` characters
  // in the text are guaranteed to be real SQL placeholders (ParamAnon), not
  // nullability markers on column aliases. This gives parameter extraction
  // a clean token stream without false positives from alias syntax.
  let param_tokens = tokenize.tokenize(sanitized_sql)
  use raw_parameters <- result.try(
    parameters.extract_parameters(
      opcodes,
      cursor_table,
      v2,
      pk_columns,
      param_tokens,
    )
    |> result.map_error(fn(e) { resolution_error_to_marmot_error(path, e) }),
  )

  let parameters = parameters.deduplicate_parameter_names(raw_parameters)
  Ok(QueryInfo(columns: columns, parameters: parameters))
}

fn resolution_error_to_marmot_error(
  path: String,
  err: parameters.ParameterResolutionError,
) -> MarmotError {
  case err {
    parameters.AmbiguousColumn(column:, candidates:) ->
      error.AmbiguousColumnReference(
        path: path,
        column: column,
        candidates: candidates,
      )
    parameters.UnknownAlias(alias:) ->
      error.UnknownColumnAlias(path: path, alias: alias)
    parameters.UnknownColumnInTable(table:, column:) ->
      error.UnknownColumnInTable(path: path, table: table, column: column)
    parameters.UnknownColumn(column:) ->
      error.UnknownColumnReference(path: path, column: column)
    // AliasMapCollision is structurally similar to "two FROM bindings share an
    // alias", which user-facing-wise reads as "your query has two `users`
    // tables in scope". Surface as ambiguous (with the colliding alias as the
    // sole candidate) rather than inventing a new public variant for an
    // internal collision.
    parameters.AliasMapCollision(name:) ->
      error.AmbiguousColumnReference(path: path, column: name, candidates: [
        name,
      ])
    parameters.InsertValuesCountMismatch(expected:, got:, row:) ->
      error.InsertValuesCountMismatch(
        path: path,
        expected: expected,
        got: got,
        row: row,
      )
  }
}

/// Pre-flight check for INSERT VALUES row counts.
///
/// Parses the statement and, if it is an INSERT with VALUES, validates that
/// every row has the same number of expressions as the number of bindable
/// columns (from the explicit column list, or the schema minus generated/hidden
/// columns). Returns our typed error before EXPLAIN can surface SQLite's own
/// count-mismatch message.
fn validate_insert_values_counts(
  tokens: List(tokenize.Token),
  v2: schema.TableMetadataV2,
  path: String,
) -> Result(Nil, MarmotError) {
  case statement_parser.parse(tokens) {
    Ok(statement_parser.Insert(stmt)) ->
      validate_insert_statement_values_counts(stmt, v2, path)
    _ -> Ok(Nil)
  }
}

fn validate_insert_statement_values_counts(
  stmt: statement_parser.InsertStmt,
  v2: schema.TableMetadataV2,
  path: String,
) -> Result(Nil, MarmotError) {
  case stmt.source {
    statement_parser.ValuesSource(_, rows) ->
      validate_insert_values_source(stmt, rows, v2, path)
    _ -> Ok(Nil)
  }
}

fn validate_insert_values_source(
  stmt: statement_parser.InsertStmt,
  rows: List(List(List(tokenize.Token))),
  v2: schema.TableMetadataV2,
  path: String,
) -> Result(Nil, MarmotError) {
  let target_name = stmt.target.table.name.text

  case dict.get(v2.columns, target_name) {
    // Unknown table: skip pre-flight, let EXPLAIN report the real error.
    Error(_) -> Ok(Nil)
    Ok(metadatas) -> {
      let bindable = list.filter(metadatas, fn(m) { m.hidden == 0 })
      let bound_count = case stmt.column_list {
        option.Some(names) -> list.length(names)
        option.None -> list.length(bindable)
      }
      validate_rows_preflight(rows, bound_count, 1, path)
    }
  }
}

fn validate_rows_preflight(
  rows: List(List(List(tokenize.Token))),
  expected: Int,
  index: Int,
  path: String,
) -> Result(Nil, MarmotError) {
  case rows {
    [] -> Ok(Nil)
    [row, ..rest] ->
      case list.length(row) == expected {
        True -> validate_rows_preflight(rest, expected, index + 1, path)
        False ->
          Error(error.InsertValuesCountMismatch(
            path: path,
            expected: expected,
            got: list.length(row),
            row: index,
          ))
      }
  }
}

/// Kept here so older internal callers do not need to know about
/// `sqlite/parse.gleam`.
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
        _ -> scan_for_returns_line(trimmed, rest)
      }
    }
  }
}

fn scan_for_returns_line(
  trimmed: String,
  rest: List(String),
) -> Result(option.Option(String), ReturnsAnnotationError) {
  use <- bool.guard(!string.starts_with(trimmed, "--"), Ok(option.None))
  scan_for_returns_comment(trimmed, rest)
}

fn scan_for_returns_comment(
  trimmed: String,
  rest: List(String),
) -> Result(option.Option(String), ReturnsAnnotationError) {
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
      { first_code >= 65 && first_code <= 90 }
      && list.all(rest, fn(ch) {
        let code = query.char_code(ch)
        { code >= 65 && code <= 90 }
        || { code >= 97 && code <= 122 }
        || { code >= 48 && code <= 57 }
      })
    }
  }
}
