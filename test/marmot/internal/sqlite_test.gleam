import gleam/list
import marmot/internal/query.{
  BitArrayType, BoolType, Column, DateType, FloatType, IntType, Parameter,
  StringType, TimestampType,
}
import marmot/internal/sqlite
import sqlight

pub fn introspect_integer_column_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec("CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY)", on: db)
  let assert Ok(columns) = sqlite.introspect_columns(db, "t")
  let assert [Column(name: "id", column_type: IntType, nullable: False)] =
    columns
}

pub fn introspect_all_types_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (
        a INTEGER NOT NULL,
        b REAL NOT NULL,
        c TEXT NOT NULL,
        d BLOB NOT NULL,
        e BOOLEAN NOT NULL,
        f TIMESTAMP NOT NULL,
        g DATE NOT NULL
      )",
      on: db,
    )
  let assert Ok(columns) = sqlite.introspect_columns(db, "t")
  let assert 7 = list.length(columns)
  let assert [
    Column(name: "a", column_type: IntType, nullable: False),
    Column(name: "b", column_type: FloatType, nullable: False),
    Column(name: "c", column_type: StringType, nullable: False),
    Column(name: "d", column_type: BitArrayType, nullable: False),
    Column(name: "e", column_type: BoolType, nullable: False),
    Column(name: "f", column_type: TimestampType, nullable: False),
    Column(name: "g", column_type: DateType, nullable: False),
  ] = columns
}

pub fn introspect_nullable_column_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (
        a INTEGER NOT NULL,
        b TEXT
      )",
      on: db,
    )
  let assert Ok(columns) = sqlite.introspect_columns(db, "t")
  let assert [
    Column(name: "a", column_type: IntType, nullable: False),
    Column(name: "b", column_type: StringType, nullable: True),
  ] = columns
}

pub fn introspect_query_select_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id, username FROM users WHERE email = ?",
    )
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "username", column_type: StringType, nullable: False),
  ] = result.columns
  let assert [Parameter(name: "email", column_type: StringType)] =
    result.parameters
}

pub fn introspect_query_insert_returning_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "INSERT INTO users (username, created_at) VALUES (?, ?) RETURNING id, created_at",
    )
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "created_at", column_type: TimestampType, nullable: False),
  ] = result.columns
  let assert [
    Parameter(name: "username", column_type: StringType),
    Parameter(name: "created_at", column_type: TimestampType),
  ] = result.parameters
}

pub fn introspect_query_no_return_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "DELETE FROM users WHERE id = ?")
  let assert [] = result.columns
  let assert [Parameter(name: "id", column_type: IntType)] = result.parameters
}

pub fn introspect_query_no_params_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT id, name FROM users")
  let assert [] = result.parameters
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name", column_type: StringType, nullable: False),
  ] = result.columns
}

pub fn introspect_query_multiple_params_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "SELECT id FROM users WHERE name = ? AND age > ?",
    )
  let assert [
    Parameter(name: "name", column_type: StringType),
    Parameter(name: "age", column_type: IntType),
  ] = result.parameters
}

pub fn introspect_query_nullable_result_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        bio TEXT
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "SELECT id, bio FROM users WHERE id = ?")
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "bio", column_type: StringType, nullable: True),
  ] = result.columns
}

pub fn introspect_query_update_no_return_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(db, "UPDATE users SET name = ? WHERE id = ?")
  let assert [] = result.columns
  let assert [
    Parameter(name: "name", column_type: StringType),
    Parameter(name: "id", column_type: IntType),
  ] = result.parameters
}

pub fn introspect_query_update_returning_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        updated_at TIMESTAMP NOT NULL
      )",
      on: db,
    )
  let assert Ok(result) =
    sqlite.introspect_query(
      db,
      "UPDATE users SET name = ?, updated_at = ? WHERE id = ? RETURNING id, updated_at",
    )
  let assert [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "updated_at", column_type: TimestampType, nullable: False),
  ] = result.columns
}
