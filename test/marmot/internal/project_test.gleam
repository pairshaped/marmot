import gleam/list
import gleam/option
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
    output: option.None,
    query_function: option.None,
    sql_dir: option.None,
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
    output: option.Some("src/app/generated"),
    query_function: option.None,
    sql_dir: option.None,
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
    output: option.None,
    query_function: option.Some("server/db.query"),
    sql_dir: option.None,
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
    output: option.Some("src/other/"),
    query_function: option.None,
    sql_dir: option.None,
  ) = config
}

pub fn parse_config_env_overrides_all_test() {
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

  let assert Ok(_) = simplifile.create_directory_all("test_tmp_sqldir/src/sql/likes")
  let assert Ok(_) = simplifile.write("test_tmp_sqldir/src/sql/likes/get_likes.sql", "SELECT 1")
  let assert Ok(_) = simplifile.create_directory_all("test_tmp_sqldir/src/sql/articles")
  let assert Ok(_) = simplifile.write("test_tmp_sqldir/src/sql/articles/get_articles.sql", "SELECT 1")

  let dirs = project.find_sql_directories("test_tmp_sqldir/src", option.Some("test_tmp_sqldir/src/sql"))
  let assert True = list.contains(dirs, "test_tmp_sqldir/src/sql/likes")
  let assert True = list.contains(dirs, "test_tmp_sqldir/src/sql/articles")
  let assert 2 = list.length(dirs)
  Nil
}

pub fn find_sql_directories_with_sql_dir_root_files_test() {
  use <- with_temp_dir("test_tmp_sqldir2")

  let assert Ok(_) = simplifile.create_directory_all("test_tmp_sqldir2/src/sql/likes")
  let assert Ok(_) = simplifile.write("test_tmp_sqldir2/src/sql/likes/get_likes.sql", "SELECT 1")
  let assert Ok(_) = simplifile.write("test_tmp_sqldir2/src/sql/get_settings.sql", "SELECT 1")

  let dirs = project.find_sql_directories("test_tmp_sqldir2/src", option.Some("test_tmp_sqldir2/src/sql"))
  let assert True = list.contains(dirs, "test_tmp_sqldir2/src/sql")
  let assert True = list.contains(dirs, "test_tmp_sqldir2/src/sql/likes")
  let assert 2 = list.length(dirs)
  Nil
}

pub fn find_sql_directories_with_sql_dir_empty_subdir_test() {
  use <- with_temp_dir("test_tmp_sqldir3")

  let assert Ok(_) = simplifile.create_directory_all("test_tmp_sqldir3/src/sql/empty")
  let assert Ok(_) = simplifile.create_directory_all("test_tmp_sqldir3/src/sql/likes")
  let assert Ok(_) = simplifile.write("test_tmp_sqldir3/src/sql/likes/get_likes.sql", "SELECT 1")

  let dirs = project.find_sql_directories("test_tmp_sqldir3/src", option.Some("test_tmp_sqldir3/src/sql"))
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
    output: option.Some("src/gen"),
    query_function: option.None,
    sql_dir: option.None,
  ) = config
}

pub fn parse_config_flag_as_last_arg_test() {
  let config = project.parse_config("", ["--database"], option.None)
  let assert Config(
    database: option.None,
    output: option.None,
    query_function: option.None,
    sql_dir: option.None,
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
  let assert "src/generated_sql.gleam" =
    project.output_path("src/sql", option.Some("src/generated"))
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
    project.output_path("src/likes/sql/queries/sql", option.Some("src/generated/sql"))
}

pub fn output_path_sql_dir_root_test() {
  // Files directly in sql_dir root: "sql" segment gets stripped
  let assert "src/generated_sql.gleam" =
    project.output_path("src/sql", option.Some("src/generated"))
}

pub fn find_sql_directories_with_sql_dir_nested_test() {
  use <- with_temp_dir("test_tmp_sqldir4")

  let assert Ok(_) = simplifile.create_directory_all("test_tmp_sqldir4/src/sql/likes/sql")
  let assert Ok(_) = simplifile.write("test_tmp_sqldir4/src/sql/likes/sql/get_likes.sql", "SELECT 1")
  let assert Ok(_) = simplifile.create_directory_all("test_tmp_sqldir4/src/sql/articles")
  let assert Ok(_) = simplifile.write("test_tmp_sqldir4/src/sql/articles/get_articles.sql", "SELECT 1")

  let dirs = project.find_sql_directories("test_tmp_sqldir4/src", option.Some("test_tmp_sqldir4/src/sql"))
  let assert True = list.contains(dirs, "test_tmp_sqldir4/src/sql/likes/sql")
  let assert True = list.contains(dirs, "test_tmp_sqldir4/src/sql/articles")
  let assert 2 = list.length(dirs)
  Nil
}

pub fn validate_output_under_src_test() {
  let config =
    Config(
      database: option.None,
      output: option.Some("src/generated"),
      query_function: option.None,
      sql_dir: option.None,
    )
  let assert Ok(Nil) = project.validate_output(config)
}

pub fn validate_output_not_under_src_test() {
  let config =
    Config(
      database: option.None,
      output: option.Some("gen/output"),
      query_function: option.None,
      sql_dir: option.None,
    )
  let assert Error(Nil) = project.validate_output(config)
}

pub fn validate_output_none_test() {
  let config =
    Config(
      database: option.None,
      output: option.None,
      query_function: option.None,
      sql_dir: option.None,
    )
  let assert Ok(Nil) = project.validate_output(config)
}

pub fn validate_output_path_traversal_test() {
  let config =
    Config(
      database: option.None,
      output: option.Some("src/../../etc/evil"),
      query_function: option.None,
      sql_dir: option.None,
    )
  let assert Error(Nil) = project.validate_output(config)
}

pub fn validate_output_dot_segments_test() {
  let config =
    Config(
      database: option.None,
      output: option.Some("src/./generated"),
      query_function: option.None,
      sql_dir: option.None,
    )
  let assert Ok(Nil) = project.validate_output(config)
}

pub fn validate_output_double_traversal_test() {
  let config =
    Config(
      database: option.None,
      output: option.Some("src/a/../../../outside"),
      query_function: option.None,
      sql_dir: option.None,
    )
  let assert Error(Nil) = project.validate_output(config)
}
