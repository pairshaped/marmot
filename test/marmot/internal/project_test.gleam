import gleam/list
import gleam/option
import marmot/internal/project.{Config}
import simplifile

pub fn parse_config_empty_toml_test() {
  let config = project.parse_config("", [], option.None)
  let assert Config(
    database: option.None,
    output: option.None,
    query_function: option.None,
  ) = config
}

pub fn parse_config_from_toml_test() {
  let toml =
    "[marmot]
database = \"dev.sqlite\"
output = \"src/app/generated\"
"
  let config = project.parse_config(toml, [], option.None)
  let assert Config(
    database: option.Some("dev.sqlite"),
    output: option.Some("src/app/generated"),
    query_function: option.None,
  ) = config
}

pub fn should_parse_query_function_from_toml_test() {
  let toml =
    "[marmot]
database = \"dev.sqlite\"
query_function = \"server/db.query\"
"
  let config = project.parse_config(toml, [], option.None)
  let assert Config(
    database: option.Some("dev.sqlite"),
    output: option.None,
    query_function: option.Some("server/db.query"),
  ) = config
}

pub fn should_default_query_function_to_none_test() {
  let toml =
    "[marmot]
database = \"dev.sqlite\"
"
  let config = project.parse_config(toml, [], option.None)
  let assert Config(query_function: option.None, ..) = config
}

pub fn parse_config_cli_overrides_toml_test() {
  let toml =
    "[marmot]
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
  ) = config
}

pub fn parse_config_env_overrides_all_test() {
  let toml =
    "[marmot]
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
  let assert Ok(_) = simplifile.create_directory_all("test_tmp/src/app/sql")
  let assert Ok(_) =
    simplifile.write("test_tmp/src/app/sql/find_user.sql", "SELECT 1")
  let assert Ok(_) = simplifile.create_directory_all("test_tmp/src/other/sql")
  let assert Ok(_) =
    simplifile.write("test_tmp/src/other/sql/list_items.sql", "SELECT 1")

  let dirs = project.find_sql_directories("test_tmp/src")
  let assert True = list.contains(dirs, "test_tmp/src/app/sql")
  let assert True = list.contains(dirs, "test_tmp/src/other/sql")
  let assert 2 = list.length(dirs)

  let assert Ok(_) = simplifile.delete("test_tmp")
}

pub fn list_sql_files_test() {
  let assert Ok(_) = simplifile.create_directory_all("test_tmp2/sql")
  let assert Ok(_) = simplifile.write("test_tmp2/sql/find_user.sql", "SELECT 1")
  let assert Ok(_) =
    simplifile.write("test_tmp2/sql/list_posts.sql", "SELECT 1")
  let assert Ok(_) = simplifile.write("test_tmp2/sql/readme.md", "ignore me")

  let files = project.list_sql_files("test_tmp2/sql")
  let assert True = list.contains(files, "test_tmp2/sql/find_user.sql")
  let assert True = list.contains(files, "test_tmp2/sql/list_posts.sql")
  let assert 2 = list.length(files)

  let assert Ok(_) = simplifile.delete("test_tmp2")
}

pub fn parse_config_flag_without_value_test() {
  // --database with no value followed by another flag should not consume the flag
  let config =
    project.parse_config("", ["--database", "--output", "src/gen"], option.None)
  let assert Config(
    database: option.None,
    output: option.Some("src/gen"),
    query_function: option.None,
  ) = config
}

pub fn output_path_configured_multi_dir_test() {
  // Different sql dirs should produce different output files
  let path1 = project.output_path("src/users/sql", option.Some("src/generated"))
  let path2 = project.output_path("src/posts/sql", option.Some("src/generated"))
  let assert True = path1 != path2
}

pub fn output_path_default_test() {
  let assert "src/app/sql.gleam" =
    project.output_path("src/app/sql", option.None)
}

pub fn output_path_configured_test() {
  let assert "src/generated/src_app_sql.gleam" =
    project.output_path("src/app/sql", option.Some("src/generated"))
}
