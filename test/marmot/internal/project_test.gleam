import gleam/dict
import gleam/list
import gleam/option
import gleam/string
import marmot/internal/project.{Config}
import simplifile

/// Run a test body with guaranteed cleanup of a temp directory, even if
/// the body panics on a failed assertion.
fn with_temp_dir(base: String, body: fn() -> Nil) -> Nil {
  let result = rescue(body)
  let _ = simplifile.delete(base)
  case result {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}

@external(erlang, "marmot_test_ffi", "rescue")
fn rescue(body: fn() -> Nil) -> Result(Nil, String)

pub fn parse_config_empty_toml_test() {
  let config = project.parse_config("", [], option.None)
  let assert Config(
    database: option.None,
    database_name: option.None,
    output: option.None,
    query_function: option.None,
    sql_dir: option.None,
    migrations_dir: option.None,
    seeds_dir: option.None,
    databases: _,
    error: option.None,
  ) = config
}

pub fn parse_config_from_toml_test() {
  let toml =
    "[tools.marmot]
database = \"dev.sqlite\"
output = \"src/app/generated\"
"
  let config = project.parse_config(toml, [], option.None)
  let assert Config(
    database: option.Some("dev.sqlite"),
    database_name: option.None,
    output: option.Some("src/app/generated"),
    query_function: option.None,
    sql_dir: option.None,
    migrations_dir: option.None,
    seeds_dir: option.None,
    databases: _,
    error: option.None,
  ) = config
}

pub fn should_parse_query_function_from_toml_test() {
  let toml =
    "[tools.marmot]
database = \"dev.sqlite\"
query_function = \"server/db.query\"
"
  let config = project.parse_config(toml, [], option.None)
  let assert Config(
    database: option.Some("dev.sqlite"),
    database_name: option.None,
    output: option.None,
    query_function: option.Some("server/db.query"),
    sql_dir: option.None,
    migrations_dir: option.None,
    seeds_dir: option.None,
    databases: _,
    error: option.None,
  ) = config
}

pub fn should_default_query_function_to_none_test() {
  let toml =
    "[tools.marmot]
database = \"dev.sqlite\"
"
  let config = project.parse_config(toml, [], option.None)
  let assert Config(query_function: option.None, ..) = config
}

pub fn parse_config_cli_overrides_toml_test() {
  let toml =
    "[tools.marmot]
database = \"dev.sqlite\"
output = \"src/app/generated\"
"
  let config =
    project.parse_config(
      toml,
      ["--database", "test.sqlite", "--output", "src/other/"],
      option.None,
    )
  let assert Config(
    database: option.Some("test.sqlite"),
    database_name: option.None,
    output: option.Some("src/other/"),
    query_function: option.None,
    sql_dir: option.None,
    migrations_dir: option.None,
    seeds_dir: option.None,
    databases: _,
    error: option.None,
  ) = config
}

pub fn parse_config_cli_overrides_env_test() {
  let toml =
    "[tools.marmot]
database = \"dev.sqlite\"
"
  let config =
    project.parse_config(
      toml,
      ["--database", "test.sqlite"],
      option.Some("env.sqlite"),
    )
  let assert Config(database: option.Some("test.sqlite"), ..) = config
}

pub fn parse_config_env_overrides_toml_test() {
  let toml =
    "[tools.marmot]
database = \"dev.sqlite\"
"
  let config = project.parse_config(toml, [], option.Some("env.sqlite"))
  let assert Config(database: option.Some("env.sqlite"), ..) = config
}

pub fn find_sql_directories_test() {
  use <- with_temp_dir("test_tmp")

  let assert Ok(_) = simplifile.create_directory_all("test_tmp/src/app/sql")
  let assert Ok(_) =
    simplifile.write("test_tmp/src/app/sql/find_user.sql", "SELECT 1")
  let assert Ok(_) = simplifile.create_directory_all("test_tmp/src/other/sql")
  let assert Ok(_) =
    simplifile.write("test_tmp/src/other/sql/list_items.sql", "SELECT 1")

  let dirs = project.find_sql_directories("test_tmp/src", option.None)
  let assert True = list.contains(dirs, "test_tmp/src/app/sql")
  let assert True = list.contains(dirs, "test_tmp/src/other/sql")
  let assert 2 = list.length(dirs)
  Nil
}

pub fn find_sql_directories_with_sql_dir_test() {
  use <- with_temp_dir("test_tmp_sqldir")

  let assert Ok(_) =
    simplifile.create_directory_all("test_tmp_sqldir/src/sql/likes")
  let assert Ok(_) =
    simplifile.write("test_tmp_sqldir/src/sql/likes/get_likes.sql", "SELECT 1")
  let assert Ok(_) =
    simplifile.create_directory_all("test_tmp_sqldir/src/sql/articles")
  let assert Ok(_) =
    simplifile.write(
      "test_tmp_sqldir/src/sql/articles/get_articles.sql",
      "SELECT 1",
    )

  let dirs =
    project.find_sql_directories(
      "test_tmp_sqldir/src",
      option.Some("test_tmp_sqldir/src/sql"),
    )
  let assert True = list.contains(dirs, "test_tmp_sqldir/src/sql/likes")
  let assert True = list.contains(dirs, "test_tmp_sqldir/src/sql/articles")
  let assert 2 = list.length(dirs)
  Nil
}

pub fn find_sql_directories_with_sql_dir_root_files_test() {
  use <- with_temp_dir("test_tmp_sqldir2")

  let assert Ok(_) =
    simplifile.create_directory_all("test_tmp_sqldir2/src/sql/likes")
  let assert Ok(_) =
    simplifile.write("test_tmp_sqldir2/src/sql/likes/get_likes.sql", "SELECT 1")
  let assert Ok(_) =
    simplifile.write("test_tmp_sqldir2/src/sql/get_settings.sql", "SELECT 1")

  let dirs =
    project.find_sql_directories(
      "test_tmp_sqldir2/src",
      option.Some("test_tmp_sqldir2/src/sql"),
    )
  let assert True = list.contains(dirs, "test_tmp_sqldir2/src/sql")
  let assert True = list.contains(dirs, "test_tmp_sqldir2/src/sql/likes")
  let assert 2 = list.length(dirs)
  Nil
}

pub fn find_sql_directories_with_sql_dir_empty_subdir_test() {
  use <- with_temp_dir("test_tmp_sqldir3")

  let assert Ok(_) =
    simplifile.create_directory_all("test_tmp_sqldir3/src/sql/empty")
  let assert Ok(_) =
    simplifile.create_directory_all("test_tmp_sqldir3/src/sql/likes")
  let assert Ok(_) =
    simplifile.write("test_tmp_sqldir3/src/sql/likes/get_likes.sql", "SELECT 1")

  let dirs =
    project.find_sql_directories(
      "test_tmp_sqldir3/src",
      option.Some("test_tmp_sqldir3/src/sql"),
    )
  let assert True = list.contains(dirs, "test_tmp_sqldir3/src/sql/likes")
  let assert 1 = list.length(dirs)
  Nil
}

pub fn parse_config_sql_dir_from_toml_test() {
  let toml =
    "[tools.marmot]
database = \"dev.sqlite\"
sql_dir = \"src/sql\"
"
  let config = project.parse_config(toml, [], option.None)
  let assert Config(sql_dir: option.Some("src/sql"), ..) = config
}

pub fn parse_config_migration_and_seed_dirs_from_toml_test() {
  let toml =
    "[tools.marmot]
database = \"dev.sqlite\"
migrations_dir = \"db/migrations/curling\"
seeds_dir = \"db/seeds/curling\"
"
  let config = project.parse_config(toml, [], option.None)
  let assert Config(
    migrations_dir: option.Some("db/migrations/curling"),
    seeds_dir: option.Some("db/seeds/curling"),
    ..,
  ) = config
}

pub fn parse_config_database_refs_are_not_selected_without_cli_test() {
  let toml =
    "[tools.marmot.databases.curling]
path = \"db/curling.db\"
migrations_dir = \"db/migrations/curling\"
seeds_dir = \"db/seeds/curling\"
"
  let config = project.parse_config(toml, [], option.Some("db/env.db"))
  let assert Config(
    database: option.None,
    database_name: option.None,
    migrations_dir: option.None,
    seeds_dir: option.None,
    ..,
  ) = config
}

pub fn parse_config_cli_named_database_selects_database_ref_test() {
  let toml =
    "[tools.marmot.databases.shared]
path = \"db/shared.db\"
migrations_dir = \"db/migrations/shared\"
seeds_dir = \"db/seeds/shared\"

[tools.marmot.databases.curling]
path = \"db/curling.db\"
migrations_dir = \"db/migrations/curling\"
seeds_dir = \"db/seeds/curling\"
"
  let config =
    project.parse_config(toml, ["--database-name", "shared"], option.None)
  let assert Config(
    database: option.Some("db/shared.db"),
    database_name: option.Some("shared"),
    migrations_dir: option.Some("db/migrations/shared"),
    seeds_dir: option.Some("db/seeds/shared"),
    ..,
  ) = config
}

pub fn parse_config_named_database_derives_default_paths_test() {
  let toml =
    "[tools.marmot.databases.primary]
"
  let config =
    project.parse_config(toml, ["--database-name", "primary"], option.None)
  let assert Config(
    database: option.Some("db/primary.sqlite"),
    database_name: option.Some("primary"),
    output: option.Some("src/generated/sql/primary"),
    sql_dir: option.Some("src/sql/primary"),
    migrations_dir: option.Some("db/migrations/primary"),
    seeds_dir: option.Some("db/seeds/primary"),
    ..,
  ) = config
}

pub fn parse_config_named_database_array_uses_name_test() {
  let toml =
    "[[tools.marmot.databases]]
name = \"primary\"
"
  let config =
    project.parse_config(toml, ["--database-name", "primary"], option.None)
  let assert Config(
    database: option.Some("db/primary.sqlite"),
    database_name: option.Some("primary"),
    output: option.Some("src/generated/sql/primary"),
    sql_dir: option.Some("src/sql/primary"),
    migrations_dir: option.Some("db/migrations/primary"),
    seeds_dir: option.Some("db/seeds/primary"),
    ..,
  ) = config
}

pub fn parse_config_named_database_allows_overrides_test() {
  let toml =
    "[[tools.marmot.databases]]
name = \"primary\"
path = \"priv/db/main.sqlite\"
migrations_dir = \"priv/db/migrations\"
seeds_dir = \"priv/db/seeds\"
sql_dir = \"src/db/sql\"
output = \"src/db/generated\"
"
  let config =
    project.parse_config(toml, ["--database-name", "primary"], option.None)
  let assert Config(
    database: option.Some("priv/db/main.sqlite"),
    output: option.Some("src/db/generated"),
    sql_dir: option.Some("src/db/sql"),
    migrations_dir: option.Some("priv/db/migrations"),
    seeds_dir: option.Some("priv/db/seeds"),
    ..,
  ) = config
}

pub fn parse_config_named_database_appends_name_to_global_dirs_test() {
  let toml =
    "[tools.marmot]
migrations_dir = \"priv/migrations\"
seeds_dir = \"priv/seeds\"
sql_dir = \"src/database_sql\"
output = \"src/generated/database_sql\"

[[tools.marmot.databases]]
name = \"primary\"
"
  let config =
    project.parse_config(toml, ["--database-name", "primary"], option.None)
  let assert Config(
    output: option.Some("src/generated/database_sql/primary"),
    sql_dir: option.Some("src/database_sql/primary"),
    migrations_dir: option.Some("priv/migrations/primary"),
    seeds_dir: option.Some("priv/seeds/primary"),
    ..,
  ) = config
}

pub fn parse_config_named_database_does_not_double_configured_namespace_test() {
  let toml =
    "[tools.marmot]
migrations_dir = \"priv/migrations/curling\"
seeds_dir = \"priv/seeds/curling\"
sql_dir = \"src/sql/curling\"
output = \"src/generated/sql/curling\"

[[tools.marmot.databases]]
name = \"curling\"
"
  let config =
    project.parse_config(toml, ["--database-name", "curling"], option.None)
  let assert Config(
    output: option.Some("src/generated/sql/curling"),
    sql_dir: option.Some("src/sql/curling"),
    migrations_dir: option.Some("priv/migrations/curling"),
    seeds_dir: option.Some("priv/seeds/curling"),
    ..,
  ) = config
}

pub fn parse_config_cli_database_path_keeps_named_dirs_test() {
  let toml =
    "[tools.marmot.databases.curling]
path = \"db/curling.db\"
migrations_dir = \"db/migrations/curling\"
seeds_dir = \"db/seeds/curling\"
"
  let config =
    project.parse_config(
      toml,
      [
        "--database",
        "tmp/test.db",
        "--database-name",
        "curling",
      ],
      option.None,
    )
  let assert Config(
    database: option.Some("tmp/test.db"),
    database_name: option.Some("curling"),
    migrations_dir: option.Some("db/migrations/curling"),
    seeds_dir: option.Some("db/seeds/curling"),
    ..,
  ) = config
}

pub fn parse_config_cli_named_database_wins_over_env_test() {
  let toml =
    "[tools.marmot.databases.curling]
path = \"db/curling.db\"
"
  let config =
    project.parse_config(
      toml,
      ["--database-name", "curling"],
      option.Some("db/env.db"),
    )
  let assert Config(
    database: option.Some("db/curling.db"),
    database_name: option.Some("curling"),
    ..,
  ) = config
}

pub fn parse_config_missing_cli_named_database_does_not_use_database_path_test() {
  let toml =
    "[tools.marmot]
database = \"db/legacy.db\"
"
  let config =
    project.parse_config(toml, ["--database-name", "missing"], option.None)
  let assert Config(
    database: option.None,
    database_name: option.Some("missing"),
    ..,
  ) = config
}

pub fn parse_config_mixed_database_config_sets_error_test() {
  let toml =
    "[tools.marmot]
database = \"db/app.db\"

[tools.marmot.databases.analytics]
path = \"db/analytics.db\"
"
  let config = project.parse_config(toml, [], option.None)
  let assert Config(error: option.Some(project.MixedDatabaseConfig), ..) =
    config
}

pub fn parse_config_malformed_toml_sets_error_test() {
  let toml =
    "[tools.marmot
database = \"dev.sqlite\"
"
  let config = project.parse_config(toml, [], option.None)
  let assert Config(error: option.Some(project.TomlParseError(_)), ..) = config
}

pub fn config_error_toml_parse_to_string_test() {
  let err = project.TomlParseError("Expected ] on line 1, got end of file")
  let msg = project.config_error_to_string(err)
  let assert True = string.contains(msg, "Could not parse gleam.toml")
  let assert True = string.contains(msg, "Expected ] on line 1")
}

pub fn parse_config_empty_toml_is_valid_test() {
  // Empty string is valid TOML — should not produce a parse error
  let config = project.parse_config("", [], option.None)
  let assert Config(error: option.None, ..) = config
}

pub fn list_sql_files_test() {
  use <- with_temp_dir("test_tmp2")

  let assert Ok(_) = simplifile.create_directory_all("test_tmp2/sql")
  let assert Ok(_) = simplifile.write("test_tmp2/sql/find_user.sql", "SELECT 1")
  let assert Ok(_) =
    simplifile.write("test_tmp2/sql/list_posts.sql", "SELECT 1")
  let assert Ok(_) = simplifile.write("test_tmp2/sql/readme.md", "ignore me")

  let files = project.list_sql_files("test_tmp2/sql")
  let assert True = list.contains(files, "test_tmp2/sql/find_user.sql")
  let assert True = list.contains(files, "test_tmp2/sql/list_posts.sql")
  let assert 2 = list.length(files)
  Nil
}

pub fn parse_config_flag_without_value_test() {
  let config =
    project.parse_config("", ["--database", "--output", "src/gen"], option.None)
  let assert Config(
    database: option.None,
    database_name: option.None,
    output: option.Some("src/gen"),
    query_function: option.None,
    sql_dir: option.None,
    migrations_dir: option.None,
    seeds_dir: option.None,
    databases: _,
    error: option.None,
  ) = config
}

pub fn parse_config_database_empty_value_test() {
  let config = project.parse_config("", ["--database", ""], option.None)
  let assert Config(database: option.None, ..) = config
}

pub fn parse_config_database_equals_empty_value_test() {
  let config = project.parse_config("", ["--database="], option.None)
  let assert Config(database: option.None, ..) = config
}

pub fn parse_config_database_equals_value_test() {
  let config = project.parse_config("", ["--database=test.sqlite"], option.None)
  let assert Config(database: option.Some("test.sqlite"), ..) = config
}

pub fn parse_config_output_equals_value_test() {
  let config = project.parse_config("", ["--output=src/gen"], option.None)
  let assert Config(output: option.Some("src/gen"), ..) = config
}

pub fn parse_config_flag_as_last_arg_test() {
  let config = project.parse_config("", ["--database"], option.None)
  let assert Config(
    database: option.None,
    database_name: option.None,
    output: option.None,
    query_function: option.None,
    sql_dir: option.None,
    migrations_dir: option.None,
    seeds_dir: option.None,
    databases: _,
    error: option.None,
  ) = config
}

pub fn output_path_default_test() {
  let assert "src/generated/sql/app_sql.gleam" =
    project.output_path("src/app/sql", option.None)
}

pub fn output_path_default_nested_test() {
  let assert "src/generated/sql/app/users_sql.gleam" =
    project.output_path("src/app/users/sql", option.None)
}

pub fn output_path_common_prefix_test() {
  // output and sql_dir share "src/", so relative is "app/accounts/sql",
  // strip trailing /sql -> "app/accounts"
  let assert "src/generated/app/accounts_sql.gleam" =
    project.output_path("src/app/accounts/sql", option.Some("src/generated"))
}

pub fn output_path_deeper_common_prefix_test() {
  // output and sql_dir share "src/server/", so relative is "accounts/sql",
  // strip trailing /sql -> "accounts"
  let assert "src/server/generated/sql/accounts_sql.gleam" =
    project.output_path(
      "src/server/accounts/sql",
      option.Some("src/server/generated/sql"),
    )
}

pub fn output_path_nested_entity_test() {
  let assert "src/server/generated/sql/admin/orders_sql.gleam" =
    project.output_path(
      "src/server/admin/orders/sql",
      option.Some("src/server/generated/sql"),
    )
}

pub fn output_path_trailing_slash_on_output_test() {
  let assert "src/server/generated/sql/accounts_sql.gleam" =
    project.output_path(
      "src/server/accounts/sql",
      option.Some("src/server/generated/sql/"),
    )
}

pub fn output_path_multi_dir_no_collision_test() {
  let path1 = project.output_path("src/users/sql", option.Some("src/generated"))
  let path2 = project.output_path("src/posts/sql", option.Some("src/generated"))
  let assert True = path1 != path2
  let assert "src/generated/users_sql.gleam" = path1
  let assert "src/generated/posts_sql.gleam" = path2
}

pub fn output_path_single_sql_dir_test() {
  // When there's just src/sql and an output dir, the sql_dir relative to the
  // common prefix is just "sql", which gets stripped, yielding no entity path.
  // File goes inside the output directory, not alongside it.
  let assert "src/generated/sql.gleam" =
    project.output_path("src/sql", option.Some("src/generated"))
}

pub fn output_path_default_root_sql_test() {
  // When sql_dir is "src/sql" (root-level) with default output, both share
  // "src/" prefix, relative is "sql", all segments filtered leaving empty
  // entity path -> file placed inside the output directory as sql.gleam.
  let assert "src/generated/sql/sql.gleam" =
    project.output_path("src/sql", option.None)
}

pub fn output_path_sql_dir_with_subdirs_test() {
  // sql_dir = "src/sql", subdir is "likes" -> strips "sql" from path
  // src/sql/likes relative to src/generated/sql: common prefix is "src/"
  // relative is "sql/likes", strip "sql" segments -> "likes"
  let assert "src/generated/sql/likes_sql.gleam" =
    project.output_path("src/sql/likes", option.Some("src/generated/sql"))
}

pub fn output_path_sql_segment_stripping_test() {
  // likes/sql/queries path should strip the "sql" segment
  let assert "src/generated/sql/likes/queries_sql.gleam" =
    project.output_path(
      "src/likes/sql/queries/sql",
      option.Some("src/generated/sql"),
    )
}

pub fn output_path_sql_dir_root_test() {
  // Files directly in sql_dir root: "sql" segment gets stripped.
  // File goes inside the output directory, not alongside it.
  let assert "src/generated/sql.gleam" =
    project.output_path("src/sql", option.Some("src/generated"))
}

pub fn output_path_from_source_root_for_named_database_test() {
  let assert "src/generated/sql/primary/users_sql.gleam" =
    project.output_path_from_source_root(
      "src/sql/primary/users",
      option.Some("src/generated/sql/primary"),
      option.Some("src/sql/primary"),
    )
}

pub fn output_path_from_source_root_for_named_database_root_test() {
  let assert "src/generated/sql/primary/sql.gleam" =
    project.output_path_from_source_root(
      "src/sql/primary",
      option.Some("src/generated/sql/primary"),
      option.Some("src/sql/primary"),
    )
}

pub fn find_sql_directories_with_sql_dir_nested_test() {
  use <- with_temp_dir("test_tmp_sqldir4")

  let assert Ok(_) =
    simplifile.create_directory_all("test_tmp_sqldir4/src/sql/likes/sql")
  let assert Ok(_) =
    simplifile.write(
      "test_tmp_sqldir4/src/sql/likes/sql/get_likes.sql",
      "SELECT 1",
    )
  let assert Ok(_) =
    simplifile.create_directory_all("test_tmp_sqldir4/src/sql/articles")
  let assert Ok(_) =
    simplifile.write(
      "test_tmp_sqldir4/src/sql/articles/get_articles.sql",
      "SELECT 1",
    )

  let dirs =
    project.find_sql_directories(
      "test_tmp_sqldir4/src",
      option.Some("test_tmp_sqldir4/src/sql"),
    )
  let assert True = list.contains(dirs, "test_tmp_sqldir4/src/sql/likes/sql")
  let assert True = list.contains(dirs, "test_tmp_sqldir4/src/sql/articles")
  let assert 2 = list.length(dirs)
  Nil
}

pub fn find_sql_directories_result_missing_configured_dir_test() {
  let result =
    project.find_sql_directories_result(
      "test_tmp_missing/src",
      option.Some("test_tmp_missing/src/sql"),
    )
  let assert Error(Nil) = result
}

pub fn validate_output_under_src_test() {
  let config =
    Config(
      database: option.None,
      database_name: option.None,
      output: option.Some("src/generated"),
      query_function: option.None,
      sql_dir: option.None,
      migrations_dir: option.None,
      seeds_dir: option.None,
      databases: dict.new(),
      error: option.None,
    )
  let assert Ok(Nil) = project.validate_output(config)
}

pub fn validate_output_not_under_src_test() {
  let config =
    Config(
      database: option.None,
      database_name: option.None,
      output: option.Some("gen/output"),
      query_function: option.None,
      sql_dir: option.None,
      migrations_dir: option.None,
      seeds_dir: option.None,
      databases: dict.new(),
      error: option.None,
    )
  let assert Error(Nil) = project.validate_output(config)
}

pub fn validate_output_none_test() {
  let config =
    Config(
      database: option.None,
      database_name: option.None,
      output: option.None,
      query_function: option.None,
      sql_dir: option.None,
      migrations_dir: option.None,
      seeds_dir: option.None,
      databases: dict.new(),
      error: option.None,
    )
  let assert Ok(Nil) = project.validate_output(config)
}

pub fn validate_output_path_traversal_test() {
  let config =
    Config(
      database: option.None,
      database_name: option.None,
      output: option.Some("src/../../etc/evil"),
      query_function: option.None,
      sql_dir: option.None,
      migrations_dir: option.None,
      seeds_dir: option.None,
      databases: dict.new(),
      error: option.None,
    )
  let assert Error(Nil) = project.validate_output(config)
}

pub fn validate_output_dot_segments_test() {
  let config =
    Config(
      database: option.None,
      database_name: option.None,
      output: option.Some("src/./generated"),
      query_function: option.None,
      sql_dir: option.None,
      migrations_dir: option.None,
      seeds_dir: option.None,
      databases: dict.new(),
      error: option.None,
    )
  let assert Ok(Nil) = project.validate_output(config)
}

pub fn validate_output_double_traversal_test() {
  let config =
    Config(
      database: option.None,
      database_name: option.None,
      output: option.Some("src/a/../../../outside"),
      query_function: option.None,
      sql_dir: option.None,
      migrations_dir: option.None,
      seeds_dir: option.None,
      databases: dict.new(),
      error: option.None,
    )
  let assert Error(Nil) = project.validate_output(config)
}
