import gleam/list
import gleam/option.{type Option}
import gleam/string

pub type ColumnType {
  IntType
  FloatType
  StringType
  BitArrayType
  BoolType
  TimestampType
  DateType
}

pub type Column {
  Column(name: String, column_type: ColumnType, nullable: Bool)
}

pub type Parameter {
  Parameter(name: String, column_type: ColumnType, nullable: Bool)
}

pub type Query {
  Query(
    name: String,
    sql: String,
    path: String,
    parameters: List(Parameter),
    columns: List(Column),
    custom_type_name: Option(String),
  )
}

pub fn gleam_type(column_type: ColumnType) -> String {
  case column_type {
    IntType -> "Int"
    FloatType -> "Float"
    StringType -> "String"
    BitArrayType -> "BitArray"
    BoolType -> "Bool"
    TimestampType -> "timestamp.Timestamp"
    DateType -> "calendar.Date"
  }
}

pub fn parse_sqlite_type(raw: String) -> Result(ColumnType, Nil) {
  // Strip parenthesized parameters like VARCHAR(255) or DECIMAL(10,2)
  let base_type = case string.split_once(raw, "(") {
    Ok(#(base, _)) -> string.trim(base)
    Error(_) -> raw
  }
  case string.uppercase(base_type) {
    "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" | "MEDIUMINT" ->
      Ok(IntType)
    "REAL" | "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" -> Ok(FloatType)
    "TEXT" | "VARCHAR" | "CHAR" | "NVARCHAR" | "NCHAR" | "CLOB" ->
      Ok(StringType)
    "BLOB" -> Ok(BitArrayType)
    "BOOLEAN" | "BOOL" -> Ok(BoolType)
    "TIMESTAMP" | "DATETIME" -> Ok(TimestampType)
    "DATE" -> Ok(DateType)
    _ -> Error(Nil)
  }
}

pub fn function_name(filename: String) -> Result(String, Nil) {
  let base = case string.ends_with(filename, ".sql") {
    True -> string.drop_end(filename, 4)
    False -> filename
  }
  case sanitize_identifier(base) {
    "" -> Error(Nil)
    name -> Ok(name)
  }
}

/// Sanitize a string to be a valid Gleam identifier:
/// - Replace hyphens and spaces with underscores
/// - Strip any characters that aren't alphanumeric or underscore
/// - Prepend underscore if it starts with a digit
/// - Lowercase the result
pub fn sanitize_identifier(name: String) -> String {
  let cleaned =
    name
    |> string.lowercase
    |> string.replace("-", "_")
    |> string.replace(" ", "_")
    |> string.to_graphemes
    |> list.filter(is_identifier_char)
    |> string.join("")
  case string.first(cleaned) {
    Ok(c) ->
      case is_digit(c) {
        True -> "_" <> cleaned
        False -> cleaned
      }
    _ -> cleaned
  }
}

fn is_identifier_char(c: String) -> Bool {
  c == "_"
  || c == "a"
  || c == "b"
  || c == "c"
  || c == "d"
  || c == "e"
  || c == "f"
  || c == "g"
  || c == "h"
  || c == "i"
  || c == "j"
  || c == "k"
  || c == "l"
  || c == "m"
  || c == "n"
  || c == "o"
  || c == "p"
  || c == "q"
  || c == "r"
  || c == "s"
  || c == "t"
  || c == "u"
  || c == "v"
  || c == "w"
  || c == "x"
  || c == "y"
  || c == "z"
  || is_digit(c)
}

fn is_digit(c: String) -> Bool {
  c == "0"
  || c == "1"
  || c == "2"
  || c == "3"
  || c == "4"
  || c == "5"
  || c == "6"
  || c == "7"
  || c == "8"
  || c == "9"
}

/// Gleam reserved words that cannot be used as identifiers.
/// See https://gleam.run/book/tour/reserved-words/
const reserved_words = [
  "as", "assert", "auto", "case", "const", "delegate", "derive", "echo", "else",
  "external", "fn", "if", "implement", "import", "let", "macro", "opaque",
  "panic", "pub", "test", "todo", "type", "use",
]

pub fn safe_name(name: String) -> String {
  case list.contains(reserved_words, name) {
    True -> name <> "_"
    False -> name
  }
}

pub fn row_type_name(name: String) -> String {
  name
  |> to_pascal_case
  |> string.append("Row")
}

fn to_pascal_case(snake: String) -> String {
  snake
  |> string.split("_")
  |> list.map(string.capitalise)
  |> string.join("")
}

pub fn has_return_columns(query: Query) -> Bool {
  query.columns != []
}
