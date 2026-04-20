import gleam/option
import marmot/internal/query.{
  BitArrayType, BoolType, Column, DateType, FloatType, IntType, Parameter, Query,
  StringType, TimestampType,
}

pub fn column_type_to_gleam_type_test() {
  let assert "Int" = query.gleam_type(IntType)
  let assert "Float" = query.gleam_type(FloatType)
  let assert "String" = query.gleam_type(StringType)
  let assert "BitArray" = query.gleam_type(BitArrayType)
  let assert "Bool" = query.gleam_type(BoolType)
  let assert "timestamp.Timestamp" = query.gleam_type(TimestampType)
  let assert "calendar.Date" = query.gleam_type(DateType)
}

pub fn sqlite_type_to_column_type_test() {
  let assert Ok(IntType) = query.parse_sqlite_type("INTEGER")
  let assert Ok(FloatType) = query.parse_sqlite_type("REAL")
  let assert Ok(StringType) = query.parse_sqlite_type("TEXT")
  let assert Ok(BitArrayType) = query.parse_sqlite_type("BLOB")
  let assert Ok(BoolType) = query.parse_sqlite_type("BOOLEAN")
  let assert Ok(TimestampType) = query.parse_sqlite_type("TIMESTAMP")
  let assert Ok(TimestampType) = query.parse_sqlite_type("DATETIME")
  let assert Ok(DateType) = query.parse_sqlite_type("DATE")
}

pub fn sqlite_type_case_insensitive_test() {
  let assert Ok(IntType) = query.parse_sqlite_type("integer")
  let assert Ok(FloatType) = query.parse_sqlite_type("real")
  let assert Ok(StringType) = query.parse_sqlite_type("text")
  let assert Ok(BoolType) = query.parse_sqlite_type("boolean")
  let assert Ok(TimestampType) = query.parse_sqlite_type("timestamp")
  let assert Ok(DateType) = query.parse_sqlite_type("date")
}

pub fn sqlite_type_unknown_test() {
  let assert Error(Nil) = query.parse_sqlite_type("POLYGON")
  let assert Error(Nil) = query.parse_sqlite_type("CUSTOM_TYPE")
}

pub fn sqlite_type_parameterized_test() {
  let assert Ok(StringType) = query.parse_sqlite_type("VARCHAR(255)")
  let assert Ok(FloatType) = query.parse_sqlite_type("DECIMAL(10,2)")
  let assert Ok(IntType) = query.parse_sqlite_type("INTEGER(8)")
  let assert Ok(StringType) = query.parse_sqlite_type("NVARCHAR(100)")
  let assert Ok(FloatType) = query.parse_sqlite_type("NUMERIC(5,2)")
}

pub fn safe_name_reserved_word_test() {
  let assert "type_" = query.safe_name("type")
  let assert "let_" = query.safe_name("let")
  let assert "case_" = query.safe_name("case")
  let assert "fn_" = query.safe_name("fn")
  let assert "use_" = query.safe_name("use")
}

pub fn safe_name_non_reserved_test() {
  let assert "name" = query.safe_name("name")
  let assert "id" = query.safe_name("id")
  let assert "email" = query.safe_name("email")
}

pub fn function_name_from_filename_test() {
  let assert Ok("find_user") = query.function_name("find_user.sql")
  let assert Ok("list_all_posts") = query.function_name("list_all_posts.sql")
  let assert Ok("delete_user") = query.function_name("delete_user.sql")
}

pub fn function_name_preserves_sql_in_name_test() {
  let assert Ok("fix_sql_injection") =
    query.function_name("fix_sql_injection.sql")
  let assert Ok("sql_backup") = query.function_name("sql_backup.sql")
}

pub fn row_type_name_from_name_test() {
  let assert "FindUserRow" = query.row_type_name("find_user")
  let assert "ListAllPostsRow" = query.row_type_name("list_all_posts")
  let assert "DeleteUserRow" = query.row_type_name("delete_user")
}

pub fn query_has_return_columns_test() {
  let q =
    Query(
      name: "find_user",
      sql: "SELECT id FROM users WHERE id = ?",
      path: "src/app/sql/find_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [Column(name: "id", column_type: IntType, nullable: False)],
      custom_type_name: option.None,
    )
  let assert True = query.has_return_columns(q)
}

pub fn function_name_sanitizes_hyphens_test() {
  let assert Ok("get_users") = query.function_name("get-users.sql")
}

pub fn function_name_sanitizes_leading_digit_test() {
  let assert Ok("_1_get_users") = query.function_name("1-get-users.sql")
}

pub fn function_name_sanitizes_spaces_test() {
  let assert Ok("my_query") = query.function_name("my query.sql")
}

pub fn function_name_sanitizes_uppercase_test() {
  let assert Ok("find_user") = query.function_name("Find_User.sql")
}

pub fn function_name_strips_special_chars_test() {
  let assert Ok("finduser") = query.function_name("find@user!.sql")
}

pub fn function_name_rejects_empty_identifier_test() {
  let assert Error(Nil) = query.function_name("@#$.sql")
  let assert Error(Nil) = query.function_name(".sql")
}

pub fn sqlite_type_integer_aliases_test() {
  let assert Ok(IntType) = query.parse_sqlite_type("BIGINT")
  let assert Ok(IntType) = query.parse_sqlite_type("SMALLINT")
  let assert Ok(IntType) = query.parse_sqlite_type("TINYINT")
  let assert Ok(IntType) = query.parse_sqlite_type("MEDIUMINT")
}

pub fn query_has_no_return_columns_test() {
  let q =
    Query(
      name: "delete_user",
      sql: "DELETE FROM users WHERE id = ?",
      path: "src/app/sql/delete_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [],
      custom_type_name: option.None,
    )
  let assert False = query.has_return_columns(q)
}
