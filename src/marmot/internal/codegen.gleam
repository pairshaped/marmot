import gleam/int
import gleam/list
import gleam/string
import marmot/internal/query.{
  type Column, type ColumnType, type Parameter, type Query, BitArrayType,
  BoolType, DateType, FloatType, IntType, StringType, TimestampType,
}

/// Sanitize a database column or parameter name for use in generated Gleam code.
fn sanitize_name(name: String) -> String {
  name |> query.sanitize_identifier |> query.safe_name
}

/// Generate the complete Gleam source for a single query function,
/// including its Row type if it has return columns.
pub fn generate_function(q: Query) -> String {
  case query.has_return_columns(q) {
    True -> generate_row_type(q) <> "\n\n" <> generate_query_function(q)
    False -> generate_exec_function(q)
  }
}

/// Generate a complete module from a list of queries.
pub fn generate_module(queries: List(Query)) -> String {
  let imports = generate_imports(queries)
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
  let functions =
    queries
    |> list.map(generate_function)
    |> string.join("\n\n")
  imports <> helpers <> "\n\n" <> functions <> "\n"
}

fn generate_imports(queries: List(Query)) -> String {
  let needs_option =
    list.any(queries, fn(q) { list.any(q.columns, fn(c) { c.nullable }) })
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

  let imports = [
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

  imports
  |> list.filter(fn(i) { i.0 })
  |> list.map(fn(i) { i.1 })
  |> list.sort(string.compare)
  |> string.join("\n")
}

fn generate_row_type(q: Query) -> String {
  let type_name = query.row_type_name(q.name)
  let fields =
    q.columns
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

fn generate_query_function(q: Query) -> String {
  let params = generate_param_list(q.parameters)
  let with_args = generate_with_args(q.parameters)
  let decoder = generate_decoder(q)
  "pub fn "
  <> q.name
  <> "(db: sqlight.Connection"
  <> params
  <> ") {\n"
  <> "  sqlight.query(\n"
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

fn generate_exec_function(q: Query) -> String {
  let params = generate_param_list(q.parameters)
  let with_args = generate_with_args(q.parameters)

  "pub fn "
  <> q.name
  <> "(db: sqlight.Connection"
  <> params
  <> ") {\n"
  <> "  sqlight.query(\n"
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
        ", " <> sanitize_name(p.name) <> ": " <> query.gleam_type(p.column_type)
      })
      |> string.join("")
  }
}

fn generate_with_args(params: List(Parameter)) -> String {
  params
  |> list.map(fn(p) { sqlight_encoder(sanitize_name(p.name), p.column_type) })
  |> string.join(", ")
}

fn sqlight_encoder(name: String, col_type: ColumnType) -> String {
  case col_type {
    IntType -> "sqlight.int(" <> name <> ")"
    FloatType -> "sqlight.float(" <> name <> ")"
    StringType -> "sqlight.text(" <> name <> ")"
    BitArrayType -> "sqlight.blob(" <> name <> ")"
    BoolType -> "sqlight.bool(" <> name <> ")"
    TimestampType -> {
      "sqlight.int(timestamp_to_int(" <> name <> "))"
    }
    DateType -> {
      // Convert Date to ISO 8601 text for storage
      // Generated code will need a date_to_string helper
      "sqlight.text(date_to_string(" <> name <> "))"
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
  |> string.replace("\\", "\\\\")
  |> string.replace("\"", "\\\"")
  |> string.replace("\r\n", " ")
  |> string.replace("\r", " ")
  |> string.replace("\n", " ")
  |> string.replace("\t", " ")
  |> collapse_whitespace
}

fn collapse_whitespace(s: String) -> String {
  s
  |> string.split(" ")
  |> list.filter(fn(part) { part != "" })
  |> string.join(" ")
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
fn date_decoder() -> decode.Decoder(Date) {
  use iso <- decode.then(decode.string)
  case string.split(iso, \"-\") {
    [year_str, month_str, day_str] ->
      case int.parse(year_str), int.parse(month_str), int.parse(day_str) {
        Ok(year), Ok(month_int), Ok(day) ->
          case month_from_int(month_int) {
            Ok(month) -> decode.success(Date(year:, month:, day:))
            Error(_) -> decode.failure(Date(0, January, 1), \"ISO 8601 date (YYYY-MM-DD)\")
          }
        _, _, _ -> decode.failure(Date(0, January, 1), \"ISO 8601 date (YYYY-MM-DD)\")
      }
    _ -> decode.failure(Date(0, January, 1), \"ISO 8601 date (YYYY-MM-DD)\")
  }
}"
}

fn date_to_string_helper() -> String {
  "fn date_to_string(date: Date) -> String {
  let year_str = int.to_string(date.year) |> string.pad_start(4, \"0\")
  let month_str = int.to_string(month_to_int(date.month)) |> string.pad_start(2, \"0\")
  let day_str = int.to_string(date.day) |> string.pad_start(2, \"0\")
  year_str <> \"-\" <> month_str <> \"-\" <> day_str
}"
}
