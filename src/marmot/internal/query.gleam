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
  case string.uppercase(raw) {
    "INTEGER" | "INT" -> Ok(IntType)
    "REAL" | "FLOAT" | "DOUBLE" -> Ok(FloatType)
    "TEXT" | "VARCHAR" | "CHAR" -> Ok(StringType)
    "BLOB" -> Ok(BitArrayType)
    "BOOLEAN" | "BOOL" -> Ok(BoolType)
    "TIMESTAMP" | "DATETIME" -> Ok(TimestampType)
    "DATE" -> Ok(DateType)
    _ -> Error(Nil)
  }
}

pub fn function_name(filename: String) -> String {
  string.replace(filename, ".sql", "")
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
