import gleam/list
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
  Parameter(name: String, column_type: ColumnType)
}

pub type Query {
  Query(
    name: String,
    sql: String,
    path: String,
    parameters: List(Parameter),
    columns: List(Column),
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
    "INTEGER" | "INT" -> Ok(IntType)
    "REAL" | "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" -> Ok(FloatType)
    "TEXT" | "VARCHAR" | "CHAR" | "NVARCHAR" | "NCHAR" | "CLOB" -> Ok(StringType)
    "BLOB" -> Ok(BitArrayType)
    "BOOLEAN" | "BOOL" -> Ok(BoolType)
    "TIMESTAMP" | "DATETIME" -> Ok(TimestampType)
    "DATE" -> Ok(DateType)
    _ -> Error(Nil)
  }
}

pub fn function_name(filename: String) -> String {
  case string.ends_with(filename, ".sql") {
    True -> string.drop_end(filename, 4)
    False -> filename
  }
}

/// Gleam reserved words that cannot be used as identifiers
const reserved_words = [
  "as", "assert", "auto", "case", "const", "echo", "external", "fn", "if",
  "import", "let", "macro", "opaque", "panic", "pub", "test", "todo", "type",
  "use",
]

pub fn safe_name(name: String) -> String {
  case list.contains(reserved_words, name) {
    True -> name <> "_"
    False -> name
  }
}

pub fn row_type_name(filename: String) -> String {
  filename
  |> function_name
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
