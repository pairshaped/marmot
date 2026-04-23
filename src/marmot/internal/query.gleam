import gleam/bool
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

/// Convert a single grapheme to its UTF codepoint integer.
pub fn char_code(c: String) -> Int {
  case string.to_utf_codepoints(c) {
    [cp] -> string.utf_codepoint_to_int(cp)
    _ -> 0
  }
}

fn is_identifier_char(c: String) -> Bool {
  case c {
    "_" -> True
    _ -> {
      let code = char_code(c)
      // a-z (input is already lowercased), 0-9
      { code >= 97 && code <= 122 } || { code >= 48 && code <= 57 }
    }
  }
}

fn is_digit(c: String) -> Bool {
  let code = char_code(c)
  code >= 48 && code <= 57
}

/// Gleam reserved words that cannot be used as identifiers.
/// See https://gleam.run/book/tour/reserved-words/
const reserved_words = [
  "as", "assert", "auto", "case", "const", "delegate", "derive", "echo", "else",
  "external", "fn", "if", "implement", "import", "let", "macro", "opaque",
  "panic", "pub", "test", "todo", "type", "use",
]

pub fn safe_name(name: String) -> String {
  use <- bool.guard(list.contains(reserved_words, name), name <> "_")
  name
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

/// Check if a character is a valid SQL identifier character (letters, digits,
/// underscore). Handles both upper and lowercase.
pub fn is_sql_ident_char(c: String) -> Bool {
  case c {
    "_" -> True
    _ -> {
      let code = char_code(c)
      // 0-9, A-Z, a-z
      { code >= 48 && code <= 57 }
      || { code >= 65 && code <= 90 }
      || { code >= 97 && code <= 122 }
    }
  }
}

/// Remove `-- line comments` and `/* block comments */` from SQL, preserving
/// string literals and the original spacing. Scans grapheme-by-grapheme,
/// tracking quote state and comment boundaries.
pub fn strip_comments(sql: String) -> String {
  do_strip_comments(string.to_graphemes(sql), [], False, False, False, False)
  |> list.reverse
  |> string.join("")
}

fn do_strip_comments(
  chars: List(String),
  acc: List(String),
  in_single: Bool,
  in_double: Bool,
  in_line_comment: Bool,
  in_block_comment: Bool,
) -> List(String) {
  case chars {
    [] -> acc
    // Inside line comment: skip until newline
    ["\n", ..rest] if in_line_comment ->
      do_strip_comments(rest, ["\n", ..acc], in_single, in_double, False, False)
    [_, ..rest] if in_line_comment ->
      do_strip_comments(rest, acc, in_single, in_double, True, False)
    // Block comment end: insert a space so adjacent tokens don't fuse
    // (e.g., SELECT/**/id must not become SELECTid).
    // Preserve in_single/in_double since quote state is independent of comments.
    ["*", "/", ..rest] if in_block_comment ->
      do_strip_comments(rest, [" ", ..acc], in_single, in_double, False, False)
    // Inside block comment: skip (including newlines)
    [_, ..rest] if in_block_comment ->
      do_strip_comments(rest, acc, in_single, in_double, False, True)
    // Newline outside any comment: preserve
    ["\n", ..rest] ->
      do_strip_comments(
        rest,
        ["\n", ..acc],
        in_single,
        in_double,
        False,
        False,
      )
    // Single-quoted strings
    ["'", ..rest] ->
      case in_double {
        True ->
          do_strip_comments(rest, ["'", ..acc], in_single, True, False, False)
        False ->
          case in_single {
            True ->
              case rest {
                ["'", ..rest2] ->
                  do_strip_comments(
                    rest2,
                    ["'", "'", ..acc],
                    True,
                    False,
                    False,
                    False,
                  )
                _ ->
                  do_strip_comments(
                    rest,
                    ["'", ..acc],
                    False,
                    False,
                    False,
                    False,
                  )
              }
            False ->
              do_strip_comments(rest, ["'", ..acc], True, False, False, False)
          }
      }
    // Double-quoted identifiers
    ["\"", ..rest] ->
      case in_single {
        True ->
          do_strip_comments(rest, ["\"", ..acc], True, in_double, False, False)
        False ->
          do_strip_comments(
            rest,
            ["\"", ..acc],
            False,
            !in_double,
            False,
            False,
          )
      }
    // Line comment start (outside quotes)
    ["-", "-", ..rest] ->
      case in_single || in_double {
        True ->
          do_strip_comments(
            ["-", ..rest],
            ["-", ..acc],
            in_single,
            in_double,
            False,
            False,
          )
        False -> do_strip_comments(rest, acc, False, False, True, False)
      }
    // Block comment start (outside quotes)
    ["/", "*", ..rest] ->
      case in_single || in_double {
        True ->
          do_strip_comments(
            ["*", ..rest],
            ["/", ..acc],
            in_single,
            in_double,
            False,
            False,
          )
        False -> do_strip_comments(rest, acc, False, False, False, True)
      }
    // Everything else
    [c, ..rest] ->
      do_strip_comments(rest, [c, ..acc], in_single, in_double, False, False)
  }
}

/// Collapse runs of spaces into single spaces.
pub fn collapse_spaces(s: String) -> String {
  s
  |> string.split(" ")
  |> list.filter(fn(part) { part != "" })
  |> string.join(" ")
}
