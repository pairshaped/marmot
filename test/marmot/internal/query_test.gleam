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

pub fn function_name_from_filename_test() {
  let assert "find_user" = query.function_name("find_user.sql")
  let assert "list_all_posts" = query.function_name("list_all_posts.sql")
  let assert "delete_user" = query.function_name("delete_user.sql")
}

pub fn row_type_name_from_filename_test() {
  let assert "FindUserRow" = query.row_type_name("find_user.sql")
  let assert "ListAllPostsRow" = query.row_type_name("list_all_posts.sql")
  let assert "DeleteUserRow" = query.row_type_name("delete_user.sql")
}

pub fn query_has_return_columns_test() {
  let q =
    Query(
      name: "find_user",
      sql: "SELECT id FROM users WHERE id = ?",
      path: "src/app/sql/find_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [Column(name: "id", column_type: IntType, nullable: False)],
    )
  let assert True = query.has_return_columns(q)
}

pub fn query_has_no_return_columns_test() {
  let q =
    Query(
      name: "delete_user",
      sql: "DELETE FROM users WHERE id = ?",
      path: "src/app/sql/delete_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [],
    )
  let assert False = query.has_return_columns(q)
}
