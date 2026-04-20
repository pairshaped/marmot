import birdie
import gleam/option
import marmot/internal/codegen
import marmot/internal/query.{
  BitArrayType, BoolType, Column, DateType, FloatType, IntType, Parameter, Query,
  StringType, TimestampType,
}

pub fn codegen_select_single_param_test() {
  let q =
    Query(
      name: "find_user",
      sql: "SELECT id, username, email FROM users WHERE username = ?",
      path: "src/app/sql/find_user.sql",
      parameters: [
        Parameter(name: "username", column_type: StringType, nullable: False),
      ],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "username", column_type: StringType, nullable: False),
        Column(name: "email", column_type: StringType, nullable: False),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen select single param")
}

pub fn codegen_select_no_params_test() {
  let q =
    Query(
      name: "list_users",
      sql: "SELECT id, username FROM users",
      path: "src/app/sql/list_users.sql",
      parameters: [],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "username", column_type: StringType, nullable: False),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen select no params")
}

pub fn codegen_select_nullable_test() {
  let q =
    Query(
      name: "find_user_bio",
      sql: "SELECT id, username, bio FROM users WHERE id = ?",
      path: "src/app/sql/find_user_bio.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "username", column_type: StringType, nullable: False),
        Column(name: "bio", column_type: StringType, nullable: True),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen select nullable column")
}

pub fn codegen_select_timestamp_test() {
  let q =
    Query(
      name: "find_user",
      sql: "SELECT id, created_at FROM users WHERE id = ?",
      path: "src/app/sql/find_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "created_at", column_type: TimestampType, nullable: False),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen select with timestamp")
}

pub fn codegen_select_date_test() {
  let q =
    Query(
      name: "find_event",
      sql: "SELECT id, event_date FROM events WHERE id = ?",
      path: "src/app/sql/find_event.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "event_date", column_type: DateType, nullable: False),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen select with date")
}

pub fn codegen_select_boolean_test() {
  let q =
    Query(
      name: "find_active_users",
      sql: "SELECT id, is_active FROM users",
      path: "src/app/sql/find_active_users.sql",
      parameters: [],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "is_active", column_type: BoolType, nullable: False),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen select with boolean")
}

pub fn codegen_select_float_test() {
  let q =
    Query(
      name: "find_product",
      sql: "SELECT id, price FROM products WHERE id = ?",
      path: "src/app/sql/find_product.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "price", column_type: FloatType, nullable: False),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen select with float")
}

pub fn codegen_select_blob_test() {
  let q =
    Query(
      name: "find_avatar",
      sql: "SELECT id, avatar FROM users WHERE id = ?",
      path: "src/app/sql/find_avatar.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "avatar", column_type: BitArrayType, nullable: False),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen select with blob")
}

pub fn codegen_insert_returning_test() {
  let q =
    Query(
      name: "create_user",
      sql: "INSERT INTO users (username, email) VALUES (?, ?) RETURNING id, created_at",
      path: "src/app/sql/create_user.sql",
      parameters: [
        Parameter(name: "username", column_type: StringType, nullable: False),
        Parameter(name: "email", column_type: StringType, nullable: False),
      ],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "created_at", column_type: TimestampType, nullable: False),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen insert returning")
}

pub fn codegen_exec_no_return_test() {
  let q =
    Query(
      name: "delete_user",
      sql: "DELETE FROM users WHERE id = ?",
      path: "src/app/sql/delete_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen exec no return")
}

pub fn codegen_exec_no_params_test() {
  let q =
    Query(
      name: "delete_all_users",
      sql: "DELETE FROM users",
      path: "src/app/sql/delete_all_users.sql",
      parameters: [],
      columns: [],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen exec no params")
}

pub fn codegen_multiple_params_test() {
  let q =
    Query(
      name: "find_users_by_age",
      sql: "SELECT id, name FROM users WHERE age > ? AND age < ?",
      path: "src/app/sql/find_users_by_age.sql",
      parameters: [
        Parameter(name: "age_min", column_type: IntType, nullable: False),
        Parameter(name: "age_max", column_type: IntType, nullable: False),
      ],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "name", column_type: StringType, nullable: False),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen multiple params")
}

pub fn codegen_nullable_timestamp_test() {
  let q =
    Query(
      name: "find_user",
      sql: "SELECT id, deleted_at FROM users WHERE id = ?",
      path: "src/app/sql/find_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "deleted_at", column_type: TimestampType, nullable: True),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen nullable timestamp")
}

pub fn codegen_timestamp_param_test() {
  let q =
    Query(
      name: "update_last_seen",
      sql: "UPDATE users SET last_seen = ? WHERE id = ?",
      path: "src/app/sql/update_last_seen.sql",
      parameters: [
        Parameter(
          name: "last_seen",
          column_type: TimestampType,
          nullable: False,
        ),
        Parameter(name: "id", column_type: IntType, nullable: False),
      ],
      columns: [],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen timestamp parameter")
}

pub fn codegen_full_module_test() {
  let queries = [
    Query(
      name: "find_user",
      sql: "SELECT id, username FROM users WHERE id = ?",
      path: "src/app/sql/find_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "username", column_type: StringType, nullable: False),
      ],
      custom_type_name: option.None,
    ),
    Query(
      name: "delete_user",
      sql: "DELETE FROM users WHERE id = ?",
      path: "src/app/sql/delete_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [],
      custom_type_name: option.None,
    ),
  ]
  codegen.generate_module(queries)
  |> birdie.snap(title: "codegen full module")
}

pub fn codegen_exec_only_module_test() {
  let queries = [
    Query(
      name: "delete_user",
      sql: "DELETE FROM users WHERE id = ?",
      path: "src/app/sql/delete_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [],
      custom_type_name: option.None,
    ),
    Query(
      name: "delete_all_posts",
      sql: "DELETE FROM posts",
      path: "src/app/sql/delete_all_posts.sql",
      parameters: [],
      columns: [],
      custom_type_name: option.None,
    ),
  ]
  codegen.generate_module(queries)
  |> birdie.snap(title: "codegen exec only module")
}

pub fn codegen_reserved_word_column_test() {
  let q =
    Query(
      name: "find_by_type",
      sql: "SELECT id, type FROM items WHERE type = ?",
      path: "src/app/sql/find_by_type.sql",
      parameters: [
        Parameter(name: "type", column_type: StringType, nullable: False),
      ],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "type", column_type: StringType, nullable: False),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen reserved word column")
}

pub fn codegen_timestamp_param_module_test() {
  let queries = [
    Query(
      name: "update_last_seen",
      sql: "UPDATE users SET last_seen = ? WHERE id = ?",
      path: "src/app/sql/update_last_seen.sql",
      parameters: [
        Parameter(
          name: "last_seen",
          column_type: TimestampType,
          nullable: False,
        ),
        Parameter(name: "id", column_type: IntType, nullable: False),
      ],
      columns: [],
      custom_type_name: option.None,
    ),
  ]
  codegen.generate_module(queries)
  |> birdie.snap(title: "codegen timestamp param module with helper")
}

pub fn codegen_date_param_module_test() {
  let queries = [
    Query(
      name: "create_event",
      sql: "INSERT INTO events (name, event_date) VALUES (?, ?)",
      path: "src/app/sql/create_event.sql",
      parameters: [
        Parameter(name: "name", column_type: StringType, nullable: False),
        Parameter(name: "event_date", column_type: DateType, nullable: False),
      ],
      columns: [],
      custom_type_name: option.None,
    ),
  ]
  codegen.generate_module(queries)
  |> birdie.snap(title: "codegen date param module with encoder")
}

pub fn codegen_date_and_timestamp_module_test() {
  let queries = [
    Query(
      name: "find_event",
      sql: "SELECT id, event_date FROM events WHERE id = ?",
      path: "src/app/sql/find_event.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "event_date", column_type: DateType, nullable: False),
      ],
      custom_type_name: option.None,
    ),
    Query(
      name: "update_last_seen",
      sql: "UPDATE events SET last_seen = ? WHERE id = ?",
      path: "src/app/sql/update_last_seen.sql",
      parameters: [
        Parameter(
          name: "last_seen",
          column_type: TimestampType,
          nullable: False,
        ),
        Parameter(name: "id", column_type: IntType, nullable: False),
      ],
      columns: [],
      custom_type_name: option.None,
    ),
  ]
  codegen.generate_module(queries)
  |> birdie.snap(title: "codegen date and timestamp module")
}

pub fn codegen_nullable_date_test() {
  let q =
    Query(
      name: "find_event",
      sql: "SELECT id, event_date FROM events WHERE id = ?",
      path: "src/app/sql/find_event.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "event_date", column_type: DateType, nullable: True),
      ],
      custom_type_name: option.None,
    )
  codegen.generate_function(q)
  |> birdie.snap(title: "codegen nullable date column")
}

pub fn codegen_date_module_test() {
  let queries = [
    Query(
      name: "find_event",
      sql: "SELECT id, event_date FROM events WHERE id = ?",
      path: "src/app/sql/find_event.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "event_date", column_type: DateType, nullable: False),
      ],
      custom_type_name: option.None,
    ),
  ]
  codegen.generate_module(queries)
  |> birdie.snap(title: "codegen date module with helpers")
}

pub fn codegen_module_without_query_function_test() {
  // Sanity: generate_module_with_config(queries, None) matches the default
  // behaviour produced by generate_module — uses sqlight.query directly.
  let queries = [
    Query(
      name: "find_user",
      sql: "SELECT id, username FROM users WHERE id = ?",
      path: "src/app/sql/find_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "username", column_type: StringType, nullable: False),
      ],
      custom_type_name: option.None,
    ),
    Query(
      name: "delete_user",
      sql: "DELETE FROM users WHERE id = ?",
      path: "src/app/sql/delete_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [],
      custom_type_name: option.None,
    ),
  ]
  codegen.generate_module_with_config(queries, option.None)
  |> birdie.snap(title: "codegen module without query_function")
}

pub fn codegen_module_with_query_function_test() {
  // When query_function is set, generated code imports the wrapper module
  // and routes calls through it (e.g., `db.query(...)` instead of
  // `sqlight.query(...)`). `sqlight` is still imported for value encoders
  // (`sqlight.int`, `sqlight.text`, etc.) and for the `sqlight.Connection`
  // type on the `db` parameter.
  let queries = [
    Query(
      name: "find_user",
      sql: "SELECT id, username FROM users WHERE id = ?",
      path: "src/app/sql/find_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "username", column_type: StringType, nullable: False),
      ],
      custom_type_name: option.None,
    ),
    Query(
      name: "delete_user",
      sql: "DELETE FROM users WHERE id = ?",
      path: "src/app/sql/delete_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType, nullable: False)],
      columns: [],
      custom_type_name: option.None,
    ),
  ]
  codegen.generate_module_with_config(queries, option.Some("server/db.query"))
  |> birdie.snap(title: "codegen module with query_function")
}

pub fn columns_equal_identical_test() {
  let a = [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name", column_type: StringType, nullable: False),
  ]
  let b = [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name", column_type: StringType, nullable: False),
  ]
  let assert True = codegen.columns_equal(a, b)
}

pub fn columns_equal_different_name_test() {
  let a = [Column(name: "id", column_type: IntType, nullable: False)]
  let b = [Column(name: "pk", column_type: IntType, nullable: False)]
  let assert False = codegen.columns_equal(a, b)
}

pub fn columns_equal_different_type_test() {
  let a = [Column(name: "id", column_type: IntType, nullable: False)]
  let b = [Column(name: "id", column_type: StringType, nullable: False)]
  let assert False = codegen.columns_equal(a, b)
}

pub fn columns_equal_different_nullability_test() {
  let a = [Column(name: "id", column_type: IntType, nullable: False)]
  let b = [Column(name: "id", column_type: IntType, nullable: True)]
  let assert False = codegen.columns_equal(a, b)
}

pub fn columns_equal_different_order_test() {
  let a = [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name", column_type: StringType, nullable: False),
  ]
  let b = [
    Column(name: "name", column_type: StringType, nullable: False),
    Column(name: "id", column_type: IntType, nullable: False),
  ]
  let assert False = codegen.columns_equal(a, b)
}

pub fn columns_equal_different_lengths_test() {
  let a = [
    Column(name: "id", column_type: IntType, nullable: False),
    Column(name: "name", column_type: StringType, nullable: False),
  ]
  let b = [Column(name: "id", column_type: IntType, nullable: False)]
  let assert False = codegen.columns_equal(a, b)
}
