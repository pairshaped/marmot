import gleam/dict
import gleam/int
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import marmot/internal/error
import marmot/internal/query.{
  type Column, type ColumnType, type Parameter, type Query, BitArrayType,
  BoolType, DateType, FloatType, IntType, StringType, TimestampType,
}

/// Parsed `query_function` config, split into parts needed for codegen.
///
/// `module_path` is the full dotless import path (e.g., "server/db").
/// `module_alias` is the last "/"-segment of the module path (e.g., "db").
/// Gleam's default import brings a module into scope under that alias.
/// `function` is the function name inside the module (e.g., "query").
///
/// Note on parameter naming: the generated query function has a labelled
/// parameter named `db` (the sqlight connection). If `module_alias` is also
/// "db" the generated code looks like `db.query(..., on: db, ...)`. Gleam's
/// resolver disambiguates this correctly: `db.query` is a module-qualified
/// call, while `on: db` references the parameter value. Verified by compile
/// test at codegen design time.
pub type QueryFunctionConfig {
  QueryFunctionConfig(
    module_path: String,
    module_alias: String,
    function: String,
  )
}

/// Parse a `query_function` config string of the form "module/path.function".
/// Returns `None` if input is `None` or malformed (no dot, empty parts).
pub fn parse_query_function(raw: Option(String)) -> Option(QueryFunctionConfig) {
  use value <- option_then(raw)
  // Split on the LAST "." so that paths with dots are handled correctly:
  // "server/db.query" -> #("server/db", "query").
  let parts = string.split(value, ".")
  case list.last(parts) {
    Error(_) | Ok("") -> option.None
    Ok(function) -> {
      let module_path =
        list.take(parts, list.length(parts) - 1)
        |> string.join(".")
      case module_path {
        "" -> option.None
        _ -> {
          // Reject paths containing ".." segments to prevent directory traversal
          let segments = string.split(module_path, "/")
          case list.contains(segments, "..") {
            True -> option.None
            False -> {
              // Split on both "/" and "." to get individual name parts,
              // since module paths can use either separator
              // (e.g., "server/db" or "some.module.path")
              let all_parts =
                segments
                |> list.flat_map(fn(s) { string.split(s, ".") })
              // Validate that all parts and the function are valid Gleam
              // identifiers (lowercase, alphanumeric + underscore)
              case
                list.all(all_parts, is_valid_gleam_name)
                && is_valid_gleam_name(function)
              {
                False -> option.None
                True -> {
                  let alias =
                    list.last(all_parts)
                    |> result.unwrap(module_path)
                  option.Some(QueryFunctionConfig(
                    module_path: module_path,
                    module_alias: alias,
                    function: function,
                  ))
                }
              }
            }
          }
        }
      }
    }
  }
}

fn option_then(opt: Option(a), next: fn(a) -> Option(b)) -> Option(b) {
  case opt {
    option.Some(value) -> next(value)
    option.None -> option.None
  }
}

/// Check that a name is a valid Gleam module segment or function name:
/// non-empty, starts with a lowercase letter or underscore, and contains
/// only lowercase letters, digits, and underscores.
fn is_valid_gleam_name(name: String) -> Bool {
  case string.to_graphemes(name) {
    [] -> False
    [first, ..rest] ->
      is_lower_or_underscore(first)
      && list.all(rest, fn(c) { is_lower_or_underscore(c) || is_ascii_digit(c) })
  }
}

fn is_lower_or_underscore(c: String) -> Bool {
  c == "_"
  || {
    let code = query.char_code(c)
    code >= 97 && code <= 122
  }
}

fn is_ascii_digit(c: String) -> Bool {
  let code = query.char_code(c)
  code >= 48 && code <= 57
}

/// Sanitize a database column or parameter name for use in generated Gleam code.
fn sanitize_name(name: String) -> String {
  name |> query.sanitize_identifier |> query.safe_name
}

/// Generate the complete Gleam source for a single query function,
/// including its Row type if it has return columns. Uses `sqlight.query`
/// directly.
pub fn generate_function(q: Query) -> String {
  generate_function_with_config(q, option.None)
}

/// Generate a complete module from a list of queries. Uses `sqlight.query`
/// directly.
pub fn generate_module(
  queries: List(Query),
) -> Result(String, error.MarmotError) {
  generate_module_with_config(queries, option.None)
}

/// Generate the complete Gleam source for a single query function.
/// When `query_function` is set (e.g., `Some("server/db.query")`) the
/// generated code calls through the configured wrapper function instead of
/// `sqlight.query`.
pub fn generate_function_with_config(
  q: Query,
  query_function: Option(String),
) -> String {
  let config = parse_query_function(query_function)
  case query.has_return_columns(q) {
    True -> generate_row_type(q) <> "\n\n" <> generate_query_function(q, config)
    False -> generate_exec_function(q, config)
  }
}

/// Generate a complete module from a list of queries.
/// When `query_function` is set (e.g., `Some("server/db.query")`) the
/// generated code calls through the configured wrapper function and adds the
/// wrapper's `import` line.
pub fn generate_module_with_config(
  queries: List(Query),
  query_function: Option(String),
) -> Result(String, error.MarmotError) {
  let config = parse_query_function(query_function)
  let imports = generate_imports(queries, config)
  let needs_date_decoder =
    list.any(queries, fn(q) {
      list.any(q.columns, fn(c) { c.column_type == DateType })
    })
  let needs_date_encoder =
    list.any(queries, fn(q) {
      list.any(q.parameters, fn(p) { p.column_type == DateType })
    })
  let needs_timestamp =
    list.any(queries, fn(q) {
      list.any(q.parameters, fn(p) { p.column_type == TimestampType })
    })
  let helper_parts = []
  let helper_parts = case needs_timestamp {
    True -> [timestamp_helpers(), ..helper_parts]
    False -> helper_parts
  }
  let helper_parts = case needs_date_decoder {
    True -> [date_decoder_helper(), ..helper_parts]
    False -> helper_parts
  }
  let helper_parts = case needs_date_encoder {
    True -> [date_to_string_helper(), ..helper_parts]
    False -> helper_parts
  }
  let helpers = case helper_parts {
    [] -> ""
    parts -> "\n\n" <> string.join(list.reverse(parts), "\n\n")
  }

  use #(shared_groups, _plain_queries) <- result.try(group_shared_queries(
    queries,
  ))

  // Shared types (one per group)
  let shared_types = case shared_groups {
    [] -> ""
    _ ->
      shared_groups
      |> list.map(fn(g) { generate_row_type_named(g.name, g.columns) })
      |> string.join("\n\n")
      |> fn(s) { "\n\n" <> s }
  }

  // Shared decoders (one per group)
  let shared_decoders = case shared_groups {
    [] -> ""
    _ ->
      shared_groups
      |> list.map(fn(g) { generate_shared_decoder(g.name, g.columns) })
      |> string.join("\n\n")
      |> fn(s) { "\n\n" <> s }
  }

  // Functions: for each query, generate appropriately
  let functions =
    queries
    |> list.map(fn(q) {
      case query.has_return_columns(q) {
        False -> generate_exec_function(q, config)
        True -> {
          case q.custom_type_name {
            option.Some(type_name) ->
              generate_shared_query_function(q, type_name, config)
            option.None ->
              generate_row_type(q)
              <> "\n\n"
              <> generate_query_function(q, config)
          }
        }
      }
    })
    |> string.join("\n\n")

  Ok(
    imports
    <> helpers
    <> shared_types
    <> shared_decoders
    <> "\n\n"
    <> functions
    <> "\n",
  )
}

fn generate_imports(
  queries: List(Query),
  query_function: Option(QueryFunctionConfig),
) -> String {
  let needs_option =
    list.any(queries, fn(q) {
      list.any(q.columns, fn(c) { c.nullable })
      || list.any(q.parameters, fn(p) { p.nullable })
    })
  let needs_timestamp =
    list.any(queries, fn(q) {
      list.any(q.columns, fn(c) { c.column_type == TimestampType })
      || list.any(q.parameters, fn(p) { p.column_type == TimestampType })
    })
  let needs_date =
    list.any(queries, fn(q) {
      list.any(q.columns, fn(c) { c.column_type == DateType })
      || list.any(q.parameters, fn(p) { p.column_type == DateType })
    })

  let wrapper_import = case query_function {
    option.Some(cfg) -> [
      #(True, "import " <> cfg.module_path),
    ]
    option.None -> []
  }

  let imports =
    [
      #(True, "import sqlight"),
      #(True, "import gleam/dynamic/decode"),
      #(needs_date, "import gleam/int"),
      #(needs_option, "import gleam/option.{type Option}"),
      #(needs_date, "import gleam/string"),
      #(needs_timestamp, "import gleam/time/timestamp.{type Timestamp}"),
      #(
        needs_date,
        "import gleam/time/calendar.{type Date, January, month_from_int, month_to_int}",
      ),
    ]
    |> list.append(wrapper_import)

  imports
  |> list.filter(fn(i) { i.0 })
  |> list.map(fn(i) { i.1 })
  |> list.sort(string.compare)
  |> string.join("\n")
}

fn generate_row_type(q: Query) -> String {
  generate_row_type_named(query.row_type_name(q.name), q.columns)
}

fn generate_row_type_named(type_name: String, columns: List(Column)) -> String {
  let fields =
    columns
    |> list.map(fn(col) {
      let type_str = case col.nullable {
        True -> "Option(" <> query.gleam_type(col.column_type) <> ")"
        False -> query.gleam_type(col.column_type)
      }
      "    " <> sanitize_name(col.name) <> ": " <> type_str <> ","
    })
    |> string.join("\n")
  "pub type "
  <> type_name
  <> " {\n  "
  <> type_name
  <> "(\n"
  <> fields
  <> "\n  )\n}"
}

pub fn shared_decoder_name(type_name: String) -> String {
  pascal_to_snake(type_name) <> "_decoder"
}

fn pascal_to_snake(name: String) -> String {
  string.to_graphemes(name)
  |> list.index_map(fn(ch, idx) {
    case is_upper_char(ch), idx {
      True, 0 -> string.lowercase(ch)
      True, _ -> "_" <> string.lowercase(ch)
      _, _ -> ch
    }
  })
  |> string.join("")
}

fn is_upper_char(ch: String) -> Bool {
  ch == string.uppercase(ch) && ch != string.lowercase(ch)
}

fn generate_shared_decoder(type_name: String, columns: List(Column)) -> String {
  let decoder_name = shared_decoder_name(type_name)
  let fields =
    columns
    |> list.index_map(fn(col, idx) {
      let idx_str = int.to_string(idx)
      let decoder = column_decoder(col)
      let name = decoder_var_name(col)
      "  use "
      <> name
      <> " <- decode.field("
      <> idx_str
      <> ", "
      <> decoder
      <> ")"
    })
    |> string.join("\n")

  let constructor_args =
    columns
    |> list.map(fn(col) {
      let name = sanitize_name(col.name)
      case col.column_type {
        TimestampType ->
          case col.nullable {
            True ->
              "    "
              <> name
              <> ": option.map("
              <> name
              <> "_raw, timestamp.from_unix_seconds),"
            False ->
              "    "
              <> name
              <> ": timestamp.from_unix_seconds("
              <> name
              <> "_raw),"
          }
        _ -> "    " <> name <> ":,"
      }
    })
    |> string.join("\n")

  "fn "
  <> decoder_name
  <> "() -> decode.Decoder("
  <> type_name
  <> ") {\n"
  <> fields
  <> "\n"
  <> "  decode.success("
  <> type_name
  <> "(\n"
  <> constructor_args
  <> "\n"
  <> "  ))\n"
  <> "}"
}

fn generate_shared_query_function(
  q: Query,
  type_name: String,
  query_function: Option(QueryFunctionConfig),
) -> String {
  let params = generate_param_list(q.parameters)
  let with_args = generate_with_args(q.parameters)
  let decoder_name = shared_decoder_name(type_name)
  "pub fn "
  <> q.name
  <> "(db db: sqlight.Connection"
  <> params
  <> ") -> Result(List("
  <> type_name
  <> "), sqlight.Error) {\n"
  <> "  "
  <> query_call(query_function)
  <> "(\n"
  <> "    \""
  <> escape_sql(q.sql)
  <> "\",\n"
  <> "    on: db,\n"
  <> "    with: ["
  <> with_args
  <> "],\n"
  <> "    expecting: "
  <> decoder_name
  <> "(),\n"
  <> "  )\n"
  <> "}"
}

fn query_call(query_function: Option(QueryFunctionConfig)) -> String {
  case query_function {
    option.Some(cfg) -> cfg.module_alias <> "." <> cfg.function
    option.None -> "sqlight.query"
  }
}

fn generate_query_function(
  q: Query,
  query_function: Option(QueryFunctionConfig),
) -> String {
  let params = generate_param_list(q.parameters)
  let with_args = generate_with_args(q.parameters)
  let decoder = generate_decoder(q)
  let row_type = query.row_type_name(q.name)
  "pub fn "
  <> q.name
  <> "(db db: sqlight.Connection"
  <> params
  <> ") -> Result(List("
  <> row_type
  <> "), sqlight.Error) {\n"
  <> "  "
  <> query_call(query_function)
  <> "(\n"
  <> "    \""
  <> escape_sql(q.sql)
  <> "\",\n"
  <> "    on: db,\n"
  <> "    with: ["
  <> with_args
  <> "],\n"
  <> "    expecting: {\n"
  <> decoder
  <> "    },\n"
  <> "  )\n"
  <> "}"
}

fn generate_exec_function(
  q: Query,
  query_function: Option(QueryFunctionConfig),
) -> String {
  let params = generate_param_list(q.parameters)
  let with_args = generate_with_args(q.parameters)

  "pub fn "
  <> q.name
  <> "(db db: sqlight.Connection"
  <> params
  <> ") -> Result(List(Nil), sqlight.Error) {\n"
  <> "  "
  <> query_call(query_function)
  <> "(\n"
  <> "    \""
  <> escape_sql(q.sql)
  <> "\",\n"
  <> "    on: db,\n"
  <> "    with: ["
  <> with_args
  <> "],\n"
  <> "    expecting: decode.success(Nil),\n"
  <> "  )\n"
  <> "}"
}

fn generate_param_list(params: List(Parameter)) -> String {
  case params {
    [] -> ""
    _ ->
      params
      |> list.map(fn(p) {
        let name = sanitize_name(p.name)
        let base_type = query.gleam_type(p.column_type)
        let type_str = case p.nullable {
          True -> "Option(" <> base_type <> ")"
          False -> base_type
        }
        ", " <> name <> " " <> name <> ": " <> type_str
      })
      |> string.join("")
  }
}

fn generate_with_args(params: List(Parameter)) -> String {
  params
  |> list.map(fn(p) {
    sqlight_encoder(sanitize_name(p.name), p.column_type, p.nullable)
  })
  |> string.join(", ")
}

fn sqlight_encoder(name: String, col_type: ColumnType, nullable: Bool) -> String {
  let base_fn = case col_type {
    IntType -> "sqlight.int"
    FloatType -> "sqlight.float"
    StringType -> "sqlight.text"
    BitArrayType -> "sqlight.blob"
    BoolType -> "sqlight.bool"
    TimestampType -> "sqlight.int"
    DateType -> "sqlight.text"
  }
  let value_expr = case col_type {
    TimestampType -> "timestamp_to_int(" <> name <> ")"
    DateType -> "date_to_string(" <> name <> ")"
    _ -> name
  }
  case nullable {
    True ->
      // sqlight.nullable(encoder, Option(T)) — but the encoder takes T,
      // not the timestamp/date-converted value. For nullable Timestamp/Date
      // we need to map before passing: option.map(ts, timestamp_to_int)
      // then sqlight.nullable(sqlight.int, ...).
      case col_type {
        TimestampType ->
          "sqlight.nullable(sqlight.int, option.map("
          <> name
          <> ", timestamp_to_int))"
        DateType ->
          "sqlight.nullable(sqlight.text, option.map("
          <> name
          <> ", date_to_string))"
        _ -> "sqlight.nullable(" <> base_fn <> ", " <> name <> ")"
      }
    False ->
      case col_type {
        TimestampType | DateType -> base_fn <> "(" <> value_expr <> ")"
        _ -> base_fn <> "(" <> name <> ")"
      }
  }
}

fn generate_decoder(q: Query) -> String {
  let type_name = query.row_type_name(q.name)
  let fields =
    q.columns
    |> list.index_map(fn(col, idx) {
      let idx_str = int.to_string(idx)
      let decoder = column_decoder(col)
      let name = decoder_var_name(col)
      "      use "
      <> name
      <> " <- decode.field("
      <> idx_str
      <> ", "
      <> decoder
      <> ")"
    })
    |> string.join("\n")

  let constructor_args =
    q.columns
    |> list.map(fn(col) {
      let name = sanitize_name(col.name)
      case col.column_type {
        TimestampType ->
          case col.nullable {
            True ->
              "        "
              <> name
              <> ": option.map("
              <> name
              <> "_raw, timestamp.from_unix_seconds),"
            False ->
              "        "
              <> name
              <> ": timestamp.from_unix_seconds("
              <> name
              <> "_raw),"
          }
        _ -> "        " <> name <> ":,"
      }
    })
    |> string.join("\n")

  fields
  <> "\n"
  <> "      decode.success("
  <> type_name
  <> "(\n"
  <> constructor_args
  <> "\n"
  <> "      ))\n"
}

fn decoder_var_name(col: Column) -> String {
  let name = sanitize_name(col.name)
  case col.column_type {
    TimestampType -> name <> "_raw"
    _ -> name
  }
}

fn column_decoder(col: Column) -> String {
  let base = case col.column_type {
    IntType -> "decode.int"
    FloatType -> "decode.float"
    StringType -> "decode.string"
    BitArrayType -> "decode.bit_array"
    BoolType -> "sqlight.decode_bool()"
    TimestampType -> "decode.int"
    DateType -> "date_decoder()"
  }
  case col.nullable {
    True -> "decode.optional(" <> base <> ")"
    False -> base
  }
}

fn escape_sql(sql: String) -> String {
  sql
  // Strip comments BEFORE collapsing newlines, otherwise a leading
  // `-- comment\nSELECT ...` becomes `-- comment SELECT ...` which comments
  // out the actual SQL at runtime.
  |> query.strip_comments
  |> string.replace("\\", "\\\\")
  |> string.replace("\"", "\\\"")
  |> string.replace("\r\n", " ")
  |> string.replace("\r", " ")
  |> string.replace("\n", " ")
  |> string.replace("\t", " ")
  |> query.collapse_spaces
}

fn timestamp_helpers() -> String {
  "/// Convert a Timestamp to a Unix seconds integer for SQLite storage.
/// Note: sub-second precision is not preserved (nanoseconds are discarded).
fn timestamp_to_int(ts: Timestamp) -> Int {
  let #(s, _) = timestamp.to_unix_seconds_and_nanoseconds(ts)
  s
}"
}

fn date_decoder_helper() -> String {
  "/// Decode an ISO 8601 date string (YYYY-MM-DD) from the database into a Date.
/// Returns a decode error if the string is not a valid date format.
/// Note: day validation is intentionally permissive (1-31 for all months)
/// since data comes from the database and strict calendar validation would
/// reject valid database rows on read.
fn date_decoder() -> decode.Decoder(Date) {
  use iso <- decode.then(decode.string)
  case string.split(iso, \"-\") {
    [year_str, month_str, day_str] ->
      case int.parse(year_str), int.parse(month_str), int.parse(day_str) {
        Ok(year), Ok(month_int), Ok(day) ->
          case month_from_int(month_int) {
            Ok(month) ->
              case day >= 1 && day <= 31 {
                True -> decode.success(Date(year:, month:, day:))
                False -> decode.failure(Date(0, January, 1), \"ISO 8601 date (YYYY-MM-DD)\")
              }
            Error(_) -> decode.failure(Date(0, January, 1), \"ISO 8601 date (YYYY-MM-DD)\")
          }
        _, _, _ -> decode.failure(Date(0, January, 1), \"ISO 8601 date (YYYY-MM-DD)\")
      }
    _ -> decode.failure(Date(0, January, 1), \"ISO 8601 date (YYYY-MM-DD)\")
  }
}"
}

/// Check whether two column lists match exactly: names, types, nullability,
/// and order.
pub fn columns_equal(a: List(Column), b: List(Column)) -> Bool {
  case a, b {
    [], [] -> True
    [col_a, ..rest_a], [col_b, ..rest_b] ->
      col_a.name == col_b.name
      && col_a.column_type == col_b.column_type
      && col_a.nullable == col_b.nullable
      && columns_equal(rest_a, rest_b)
    _, _ -> False
  }
}

pub type SharedGroup {
  SharedGroup(name: String, queries: List(Query), columns: List(Column))
}

pub fn group_shared_queries(
  queries: List(Query),
) -> Result(#(List(SharedGroup), List(Query)), error.MarmotError) {
  let #(annotated, unannotated) =
    list.partition(queries, fn(q) {
      case q.custom_type_name {
        option.Some(_) -> True
        option.None -> False
      }
    })

  let by_name =
    list.fold(annotated, dict.new(), fn(acc, q) {
      case q.custom_type_name {
        option.Some(name) ->
          dict.upsert(acc, name, fn(existing) {
            case existing {
              option.Some(existing_list) -> [q, ..existing_list]
              option.None -> [q]
            }
          })
        option.None -> acc
      }
    })

  let validated =
    dict.to_list(by_name)
    |> list.try_map(fn(pair) {
      let #(name, queries) = pair
      let reversed = list.reverse(queries)
      case validate_group_shapes(name, reversed) {
        Ok(cols) ->
          Ok(SharedGroup(name: name, queries: reversed, columns: cols))
        Error(e) -> Error(e)
      }
    })

  case validated {
    Ok(groups) -> Ok(#(groups, unannotated))
    Error(e) -> Error(e)
  }
}

fn validate_group_shapes(
  name: String,
  queries: List(Query),
) -> Result(List(Column), error.MarmotError) {
  case queries {
    [] -> Ok([])
    [first, ..rest] -> {
      case list.find(rest, fn(q) { !columns_equal(first.columns, q.columns) }) {
        Error(Nil) -> Ok(first.columns)
        Ok(mismatched) ->
          Error(
            error.SharedTypeMismatch(name: name, conflicts: [
              #(first.path, first.columns),
              #(mismatched.path, mismatched.columns),
            ]),
          )
      }
    }
  }
}

fn date_to_string_helper() -> String {
  "fn date_to_string(date: Date) -> String {
  let year_str = int.to_string(date.year) |> string.pad_start(4, \"0\")
  let month_str = int.to_string(month_to_int(date.month)) |> string.pad_start(2, \"0\")
  let day_str = int.to_string(date.day) |> string.pad_start(2, \"0\")
  year_str <> \"-\" <> month_str <> \"-\" <> day_str
}"
}
