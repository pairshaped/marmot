import gleam/dynamic/decode
import gleam/list
import gleam/option
import gleam/string
import marmot/internal/codegen
import marmot/internal/query
import marmot/internal/sqlite
import sqlight

// --- Type Round-Trip Tests ---
// These test the full cycle: write a value, read it back, verify

pub fn roundtrip_integer_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val INTEGER NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec("INSERT INTO t (id, val) VALUES (1, 42)", on: db)
  let assert Ok([#(42)]) =
    sqlight.query(
      "SELECT val FROM t WHERE id = ?",
      on: db,
      with: [
        sqlight.int(1),
      ],
      expecting: {
        use val <- decode.field(0, decode.int)
        decode.success(#(val))
      },
    )
}

pub fn roundtrip_float_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val REAL NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.query(
      "INSERT INTO t (id, val) VALUES (?, ?)",
      on: db,
      with: [
        sqlight.int(1),
        sqlight.float(3.14),
      ],
      expecting: decode.success(Nil),
    )
  let assert Ok([#(v)]) =
    sqlight.query(
      "SELECT val FROM t WHERE id = ?",
      on: db,
      with: [
        sqlight.int(1),
      ],
      expecting: {
        use val <- decode.field(0, decode.float)
        decode.success(#(val))
      },
    )
  let assert True = v >. 3.13 && v <. 3.15
}

pub fn roundtrip_text_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.query(
      "INSERT INTO t (id, val) VALUES (?, ?)",
      on: db,
      with: [
        sqlight.int(1),
        sqlight.text("hello"),
      ],
      expecting: decode.success(Nil),
    )
  let assert Ok([#("hello")]) =
    sqlight.query(
      "SELECT val FROM t WHERE id = ?",
      on: db,
      with: [
        sqlight.int(1),
      ],
      expecting: {
        use val <- decode.field(0, decode.string)
        decode.success(#(val))
      },
    )
}

pub fn roundtrip_blob_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val BLOB NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.query(
      "INSERT INTO t (id, val) VALUES (?, ?)",
      on: db,
      with: [
        sqlight.int(1),
        sqlight.blob(<<1, 2, 3>>),
      ],
      expecting: decode.success(Nil),
    )
  let assert Ok([#(<<1, 2, 3>>)]) =
    sqlight.query(
      "SELECT val FROM t WHERE id = ?",
      on: db,
      with: [
        sqlight.int(1),
      ],
      expecting: {
        use val <- decode.field(0, decode.bit_array)
        decode.success(#(val))
      },
    )
}

pub fn roundtrip_boolean_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val BOOLEAN NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.query(
      "INSERT INTO t (id, val) VALUES (?, ?)",
      on: db,
      with: [
        sqlight.int(1),
        sqlight.bool(True),
      ],
      expecting: decode.success(Nil),
    )
  let assert Ok(_) =
    sqlight.query(
      "INSERT INTO t (id, val) VALUES (?, ?)",
      on: db,
      with: [
        sqlight.int(2),
        sqlight.bool(False),
      ],
      expecting: decode.success(Nil),
    )
  let assert Ok([#(True)]) =
    sqlight.query(
      "SELECT val FROM t WHERE id = ?",
      on: db,
      with: [
        sqlight.int(1),
      ],
      expecting: {
        use val <- decode.field(0, sqlight.decode_bool())
        decode.success(#(val))
      },
    )
  let assert Ok([#(False)]) =
    sqlight.query(
      "SELECT val FROM t WHERE id = ?",
      on: db,
      with: [
        sqlight.int(2),
      ],
      expecting: {
        use val <- decode.field(0, sqlight.decode_bool())
        decode.success(#(val))
      },
    )
}

pub fn roundtrip_timestamp_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TIMESTAMP NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.query(
      "INSERT INTO t (id, val) VALUES (?, ?)",
      on: db,
      with: [
        sqlight.int(1),
        sqlight.int(1_700_000_000),
      ],
      expecting: decode.success(Nil),
    )
  let assert Ok([#(raw_val)]) =
    sqlight.query(
      "SELECT val FROM t WHERE id = ?",
      on: db,
      with: [
        sqlight.int(1),
      ],
      expecting: {
        use val <- decode.field(0, decode.int)
        decode.success(#(val))
      },
    )
  let assert True = raw_val == 1_700_000_000
}

pub fn roundtrip_date_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val DATE NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.query(
      "INSERT INTO t (id, val) VALUES (?, ?)",
      on: db,
      with: [
        sqlight.int(1),
        sqlight.text("2024-12-25"),
      ],
      expecting: decode.success(Nil),
    )
  let assert Ok([#("2024-12-25")]) =
    sqlight.query(
      "SELECT val FROM t WHERE id = ?",
      on: db,
      with: [
        sqlight.int(1),
      ],
      expecting: {
        use val <- decode.field(0, decode.string)
        decode.success(#(val))
      },
    )
}

pub fn roundtrip_nullable_some_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.query(
      "INSERT INTO t (id, val) VALUES (?, ?)",
      on: db,
      with: [
        sqlight.int(1),
        sqlight.text("present"),
      ],
      expecting: decode.success(Nil),
    )
  let assert Ok([#(option.Some("present"))]) =
    sqlight.query(
      "SELECT val FROM t WHERE id = ?",
      on: db,
      with: [
        sqlight.int(1),
      ],
      expecting: {
        use val <- decode.field(0, decode.optional(decode.string))
        decode.success(#(val))
      },
    )
}

pub fn roundtrip_nullable_none_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, val TEXT)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.query(
      "INSERT INTO t (id, val) VALUES (?, ?)",
      on: db,
      with: [
        sqlight.int(1),
        sqlight.null(),
      ],
      expecting: decode.success(Nil),
    )
  let assert Ok([#(option.None)]) =
    sqlight.query(
      "SELECT val FROM t WHERE id = ?",
      on: db,
      with: [
        sqlight.int(1),
      ],
      expecting: {
        use val <- decode.field(0, decode.optional(decode.string))
        decode.success(#(val))
      },
    )
}

// --- Query Pattern Tests ---

pub fn select_single_row_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec("INSERT INTO users (id, name) VALUES (1, 'alice')", on: db)
  let assert Ok([#(1, "alice")]) =
    sqlight.query(
      "SELECT id, name FROM users WHERE id = ?",
      on: db,
      with: [sqlight.int(1)],
      expecting: {
        use id <- decode.field(0, decode.int)
        use name <- decode.field(1, decode.string)
        decode.success(#(id, name))
      },
    )
}

pub fn select_multiple_rows_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec(
      "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
      on: db,
    )
  let assert Ok(rows) =
    sqlight.query("SELECT id, name FROM users", on: db, with: [], expecting: {
      use id <- decode.field(0, decode.int)
      use name <- decode.field(1, decode.string)
      decode.success(#(id, name))
    })
  let assert 3 = list.length(rows)
}

pub fn select_empty_result_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok([]) =
    sqlight.query(
      "SELECT id, name FROM users WHERE id = ?",
      on: db,
      with: [sqlight.int(999)],
      expecting: {
        use id <- decode.field(0, decode.int)
        use name <- decode.field(1, decode.string)
        decode.success(#(id, name))
      },
    )
}

pub fn insert_returning_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok([#(1, "alice")]) =
    sqlight.query(
      "INSERT INTO users (name) VALUES (?) RETURNING id, name",
      on: db,
      with: [sqlight.text("alice")],
      expecting: {
        use id <- decode.field(0, decode.int)
        use name <- decode.field(1, decode.string)
        decode.success(#(id, name))
      },
    )
}

pub fn update_returning_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec("INSERT INTO users VALUES (1, 'alice')", on: db)
  let assert Ok([#(1, "bob")]) =
    sqlight.query(
      "UPDATE users SET name = ? WHERE id = ? RETURNING id, name",
      on: db,
      with: [sqlight.text("bob"), sqlight.int(1)],
      expecting: {
        use id <- decode.field(0, decode.int)
        use name <- decode.field(1, decode.string)
        decode.success(#(id, name))
      },
    )
}

pub fn delete_exec_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )
  let assert Ok(_) =
    sqlight.exec("INSERT INTO users VALUES (1, 'alice')", on: db)
  let assert Ok([]) =
    sqlight.query(
      "DELETE FROM users WHERE id = ?",
      on: db,
      with: [
        sqlight.int(1),
      ],
      expecting: decode.success(Nil),
    )
  // Verify deleted
  let assert Ok([]) =
    sqlight.query("SELECT id FROM users", on: db, with: [], expecting: {
      use id <- decode.field(0, decode.int)
      decode.success(#(id))
    })
}

// --- Introspection Pipeline Tests ---

pub fn introspect_and_codegen_select_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL,
        bio TEXT,
        is_active BOOLEAN NOT NULL,
        created_at TIMESTAMP NOT NULL
      )",
      on: db,
    )
  let sql =
    "SELECT id, username, bio, is_active, created_at FROM users WHERE email = ?"
  let assert Ok(result) = sqlite.introspect_query(db, sql)

  // Verify introspection found correct columns
  let assert 5 = list.length(result.columns)
  let assert 1 = list.length(result.parameters)

  // Generate code and verify it looks right
  let q =
    query.Query(
      name: "find_user_by_email",
      sql: sql,
      path: "src/app/sql/find_user_by_email.sql",
      parameters: result.parameters,
      columns: result.columns,
      custom_type_name: option.None,
    )
  let code = codegen.generate_function(q)
  // Code should contain the function name and row type
  let assert True = string.contains(code, "find_user_by_email")
  let assert True = string.contains(code, "FindUserByEmailRow")
  let assert True = string.contains(code, "sqlight.Connection")
}
