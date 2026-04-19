import birdie
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
      parameters: [Parameter(name: "username", column_type: StringType)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "username", column_type: StringType, nullable: False),
        Column(name: "email", column_type: StringType, nullable: False),
      ],
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
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "username", column_type: StringType, nullable: False),
        Column(name: "bio", column_type: StringType, nullable: True),
      ],
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
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "created_at", column_type: TimestampType, nullable: False),
      ],
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
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "event_date", column_type: DateType, nullable: False),
      ],
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
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "price", column_type: FloatType, nullable: False),
      ],
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
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "avatar", column_type: BitArrayType, nullable: False),
      ],
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
        Parameter(name: "username", column_type: StringType),
        Parameter(name: "email", column_type: StringType),
      ],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "created_at", column_type: TimestampType, nullable: False),
      ],
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
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [],
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
        Parameter(name: "age_min", column_type: IntType),
        Parameter(name: "age_max", column_type: IntType),
      ],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "name", column_type: StringType, nullable: False),
      ],
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
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "deleted_at", column_type: TimestampType, nullable: True),
      ],
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
        Parameter(name: "last_seen", column_type: TimestampType),
        Parameter(name: "id", column_type: IntType),
      ],
      columns: [],
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
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [
        Column(name: "id", column_type: IntType, nullable: False),
        Column(name: "username", column_type: StringType, nullable: False),
      ],
    ),
    Query(
      name: "delete_user",
      sql: "DELETE FROM users WHERE id = ?",
      path: "src/app/sql/delete_user.sql",
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [],
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
      parameters: [Parameter(name: "id", column_type: IntType)],
      columns: [],
    ),
    Query(
      name: "delete_all_posts",
      sql: "DELETE FROM posts",
      path: "src/app/sql/delete_all_posts.sql",
      parameters: [],
      columns: [],
    ),
  ]
  codegen.generate_module(queries)
  |> birdie.snap(title: "codegen exec only module")
}
