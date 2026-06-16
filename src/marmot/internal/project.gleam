//// Project configuration and file discovery.
////
//// Responsibilities:
//// - Parse Config from gleam.toml + CLI args + env vars with correct precedence
//// - Discover SQL directories (default `src/**/sql/` or custom `sql_dir`)
//// - Compute output paths from sql/ directory to generated .gleam file
//// - Validate output is under src/
////
//// What lives elsewhere: .sql file processing -> marmot.gleam; SQL
//// introspection -> sqlite.gleam; code generation -> codegen.gleam.

import gleam/bool
import gleam/dict
import gleam/io
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import simplifile
import tom

/// Marmot configuration from gleam.toml [tools.marmot], CLI flags, and env vars.
///
/// All fields are optional. Precedence is resolved in `parse_config`:
/// database: CLI path > CLI name > env > toml; output: CLI >
/// selected database reference > toml; migration, seed, SQL, and init SQL
/// paths: selected database reference > toml; the rest are toml-only.
pub type Config {
  Config(
    /// Path to the SQLite database file used for introspection.
    database: Option(String),
    /// Named database reference selected from `[tools.marmot.databases]`.
    database_name: Option(String),
    /// Output directory for generated modules (must be under `src/`).
    output: Option(String),
    /// Fully-qualified wrapper function replacing `sqlight.query`.
    query_function: Option(String),
    /// Custom directory for SQL file discovery instead of `src/**/sql/`.
    sql_dir: Option(String),
    /// SQL file run on Marmot's introspection connection before generation.
    init_sql: Option(String),
    /// Custom directory for migration files instead of `db/migrations/`.
    migrations_dir: Option(String),
    /// Custom directory for seed files instead of `db/seeds/`.
    seeds_dir: Option(String),
    /// Named database references from `[tools.marmot.databases]`.
    databases: dict.Dict(String, DatabaseReference),
    /// Config problem found while preserving the old parse_config API.
    error: Option(ConfigError),
  )
}

pub type DatabaseReference {
  DatabaseReference(
    path: Option(String),
    migrations_dir: Option(String),
    seeds_dir: Option(String),
    sql_dir: Option(String),
    init_sql: Option(String),
    output: Option(String),
  )
}

pub type ConfigError {
  MixedDatabaseConfig
  /// Both --database and --database-name were provided when named databases
  /// are configured. With named databases, --database-name selects the ref;
  /// --database would silently override its path, so it's rejected.
  MixedDatabaseCliArgs
  /// --database-name NAME was specified but NAME does not match any configured
  /// named database reference.
  UnknownDatabaseName(name: String)
  TomlParseError(reason: String)
  /// An entry in [[tools.marmot.databases]] is missing or has an empty name.
  MalformedDatabaseArrayEntry
}

type TomlConfig {
  TomlConfig(
    database: Option(String),
    output: Option(String),
    query_function: Option(String),
    sql_dir: Option(String),
    init_sql: Option(String),
    migrations_dir: Option(String),
    seeds_dir: Option(String),
    databases: dict.Dict(String, DatabaseReference),
  )
}

/// Parse configuration from gleam.toml content, CLI args, and env var.
/// Database precedence: CLI flags > env_database > gleam.toml > None
/// Output precedence: CLI flags > gleam.toml > None
/// Query function: gleam.toml only (no CLI flag)
pub fn parse_config(
  toml_content: String,
  args: List(String),
  env_database: Option(String),
) -> Config {
  let #(toml_config, parse_error) = parse_toml_config(toml_content)
  let cli = parse_cli_args(args)
  let database_name = cli.database_name
  let database_ref =
    selected_database_reference(toml_config.databases, database_name)
  let config_error = case parse_error {
    option.Some(_) -> parse_error
    option.None ->
      case cli.database, database_name, database_ref {
        option.Some(_), option.Some(_), option.Some(_) ->
          option.Some(MixedDatabaseCliArgs)
        _, option.Some(name), option.None ->
          case dict.is_empty(toml_config.databases) {
            False -> option.Some(UnknownDatabaseName(name:))
            True -> option.None
          }
        _, _, _ ->
          case toml_config.database, dict.is_empty(toml_config.databases) {
            option.Some(_), False -> option.Some(MixedDatabaseConfig)
            _, _ -> option.None
          }
      }
  }

  let database = case cli.database {
    option.Some(_) -> cli.database
    option.None ->
      case database_name {
        option.Some(name) ->
          case database_ref {
            option.Some(ref) -> option.Some(named_database_path(name, ref))
            option.None -> option.None
          }
        option.None ->
          case dict.is_empty(toml_config.databases) {
            False -> option.None
            True ->
              case env_database {
                option.Some(_) -> env_database
                option.None -> toml_config.database
              }
          }
      }
  }

  let output = case cli.output {
    option.Some(_) -> cli.output
    option.None ->
      case database_ref {
        option.Some(ref) ->
          case database_name {
            option.Some(name) ->
              named_database_output(name, ref, toml_config.output)
            option.None -> toml_config.output
          }
        option.None -> toml_config.output
      }
  }

  let sql_dir = case database_ref {
    option.Some(ref) ->
      case database_name {
        option.Some(name) ->
          named_database_sql_dir(name, ref, toml_config.sql_dir)
        option.None -> toml_config.sql_dir
      }
    option.None -> toml_config.sql_dir
  }

  let init_sql = case database_ref {
    option.Some(ref) ->
      case database_name {
        option.Some(_) -> named_database_init_sql(ref, toml_config.init_sql)
        option.None -> toml_config.init_sql
      }
    option.None -> toml_config.init_sql
  }

  let migrations_dir = case database_ref {
    option.Some(ref) ->
      case database_name {
        option.Some(name) ->
          option.Some(named_database_migrations_dir(
            name,
            ref,
            toml_config.migrations_dir,
          ))
        option.None -> toml_config.migrations_dir
      }
    option.None -> toml_config.migrations_dir
  }

  let seeds_dir = case database_ref {
    option.Some(ref) ->
      case database_name {
        option.Some(name) ->
          option.Some(named_database_seeds_dir(name, ref, toml_config.seeds_dir))
        option.None -> toml_config.seeds_dir
      }
    option.None -> toml_config.seeds_dir
  }

  Config(
    database:,
    database_name:,
    output:,
    query_function: toml_config.query_function,
    sql_dir:,
    init_sql:,
    migrations_dir:,
    seeds_dir:,
    databases: toml_config.databases,
    error: config_error,
  )
}

pub fn config_error_to_string(error: ConfigError) -> String {
  case error {
    MixedDatabaseConfig ->
      "error: Mixed database configuration
  \u{250c}\u{2500} gleam.toml
  \u{2502}
  \u{2502} [tools.marmot].database cannot be used with [tools.marmot.databases].
  \u{2502}
  hint: Use database/migrations_dir/seeds_dir for one database, or define only named databases and pass --database-name when one command should target a single database."

    MixedDatabaseCliArgs ->
      "error: Mixed database configuration
  \u{250c}\u{2500} command line
  \u{2502}
  \u{2502} --database cannot be used with --database-name when named databases are configured.
  \u{2502}
  hint: Use --database for a simple database path, or --database-name to select a named database."

    TomlParseError(reason:) -> "error: Could not parse gleam.toml
  \u{250c}\u{2500} gleam.toml
  \u{2502}
  \u{2502} " <> reason

    MalformedDatabaseArrayEntry ->
      "error: Missing or empty name in [[tools.marmot.databases]]
  \u{250c}\u{2500} gleam.toml
  \u{2502}
  \u{2502} Each database entry must have a name field.
  \u{2502}
  hint: Add name = \"db_name\" to each [[tools.marmot.databases]] entry."

    UnknownDatabaseName(name:) -> "error: Unknown database name
  \u{250c}\u{2500} " <> name <> "
  \u{2502}
  \u{2502} No database named \"" <> name <> "\" is configured in [tools.marmot.databases].
  \u{2502}
  hint: Check the --database-name spelling, or add the database to gleam.toml."
  }
}

fn parse_toml_config(
  toml_content: String,
) -> #(TomlConfig, Option(ConfigError)) {
  case tom.parse(toml_content) {
    Ok(parsed) -> {
      warn_unknown_config_keys(parsed)
      let #(databases, array_error) = parse_database_references(parsed)
      let config_error = case array_error {
        option.Some(_) -> array_error
        option.None -> database_config_error_from_toml(parsed)
      }
      #(
        TomlConfig(
          database: get_toml_string(parsed, ["tools", "marmot", "database"]),
          output: get_toml_string(parsed, ["tools", "marmot", "output"]),
          query_function: get_toml_string(parsed, [
            "tools",
            "marmot",
            "query_function",
          ]),
          sql_dir: get_toml_string(parsed, ["tools", "marmot", "sql_dir"]),
          init_sql: get_toml_string(parsed, ["tools", "marmot", "init_sql"]),
          migrations_dir: get_toml_string(parsed, [
            "tools",
            "marmot",
            "migrations_dir",
          ]),
          seeds_dir: get_toml_string(parsed, ["tools", "marmot", "seeds_dir"]),
          databases: databases,
        ),
        config_error,
      )
    }
    Error(err) -> #(empty_toml_config(), option.Some(toml_parse_error(err)))
  }
}

fn toml_parse_error(err: tom.ParseError) -> ConfigError {
  case err {
    tom.Unexpected(got:, expected:) ->
      TomlParseError(reason: "Expected " <> expected <> ", got " <> got)
    tom.KeyAlreadyInUse(key:) ->
      TomlParseError(reason: "Duplicate key: " <> string.join(key, "."))
  }
}

fn database_config_error_from_toml(
  parsed: dict.Dict(String, tom.Toml),
) -> Option(ConfigError) {
  case tom.get_string(parsed, ["tools", "marmot", "database"]) {
    Ok(_) ->
      case tom.get_table(parsed, ["tools", "marmot", "databases"]) {
        Ok(_) -> option.Some(MixedDatabaseConfig)
        Error(_) ->
          case tom.get_array(parsed, ["tools", "marmot", "databases"]) {
            Ok(_) -> option.Some(MixedDatabaseConfig)
            Error(_) -> option.None
          }
      }
    Error(_) -> option.None
  }
}

fn empty_toml_config() -> TomlConfig {
  TomlConfig(
    database: option.None,
    output: option.None,
    query_function: option.None,
    sql_dir: option.None,
    init_sql: option.None,
    migrations_dir: option.None,
    seeds_dir: option.None,
    databases: dict.new(),
  )
}

fn get_toml_string(
  parsed: dict.Dict(String, tom.Toml),
  key: List(String),
) -> Option(String) {
  tom.get_string(parsed, key)
  |> result.map(option.Some)
  |> result.unwrap(option.None)
}

fn parse_database_references(
  parsed: dict.Dict(String, tom.Toml),
) -> #(dict.Dict(String, DatabaseReference), Option(ConfigError)) {
  case tom.get_table(parsed, ["tools", "marmot", "databases"]) {
    Error(_) -> parse_database_reference_array(parsed)
    Ok(databases) -> #(
      databases
        |> dict.fold(dict.new(), fn(acc, name, value) {
          case tom.as_table(value) {
            Error(_) -> acc
            Ok(table) -> dict.insert(acc, name, parse_database_reference(table))
          }
        }),
      option.None,
    )
  }
}

fn parse_database_reference_array(
  parsed: dict.Dict(String, tom.Toml),
) -> #(dict.Dict(String, DatabaseReference), Option(ConfigError)) {
  case tom.get_array(parsed, ["tools", "marmot", "databases"]) {
    Error(_) -> #(dict.new(), option.None)
    Ok(databases) ->
      databases
      |> list.fold(#(dict.new(), option.None), fn(acc, value) {
        case tom.as_table(value) {
          Error(_) -> acc
          Ok(table) -> fold_array_entry(acc, table)
        }
      })
  }
}

fn fold_array_entry(
  acc: #(dict.Dict(String, DatabaseReference), Option(ConfigError)),
  table: dict.Dict(String, tom.Toml),
) -> #(dict.Dict(String, DatabaseReference), Option(ConfigError)) {
  let #(db_acc, err) = acc
  case get_toml_string(table, ["name"]) {
    option.None -> #(db_acc, option.Some(MalformedDatabaseArrayEntry))
    option.Some(name) ->
      case string.trim(name) == "" {
        True -> #(db_acc, option.Some(MalformedDatabaseArrayEntry))
        False -> #(
          dict.insert(db_acc, name, parse_database_reference(table)),
          err,
        )
      }
  }
}

fn parse_database_reference(
  table: dict.Dict(String, tom.Toml),
) -> DatabaseReference {
  DatabaseReference(
    path: get_toml_string(table, ["path"]),
    migrations_dir: get_toml_string(table, ["migrations_dir"]),
    seeds_dir: get_toml_string(table, ["seeds_dir"]),
    sql_dir: get_toml_string(table, ["sql_dir"]),
    init_sql: get_toml_string(table, ["init_sql"]),
    output: get_toml_string(table, ["output"]),
  )
}

pub fn named_database_init_sql(
  ref: DatabaseReference,
  fallback: Option(String),
) -> Option(String) {
  case ref.init_sql {
    option.Some(path) -> option.Some(path)
    option.None -> fallback
  }
}

pub fn named_database_path(name: String, ref: DatabaseReference) -> String {
  ref.path
  |> option.unwrap("db/" <> name <> ".sqlite")
}

pub fn named_database_migrations_dir(
  name: String,
  ref: DatabaseReference,
  fallback: Option(String),
) -> String {
  case ref.migrations_dir {
    option.Some(dir) -> dir
    option.None ->
      fallback
      |> option.map(fn(dir) { path_join_namespace(dir, name) })
      |> option.unwrap("db/migrations/" <> name)
  }
}

pub fn named_database_seeds_dir(
  name: String,
  ref: DatabaseReference,
  fallback: Option(String),
) -> String {
  case ref.seeds_dir {
    option.Some(dir) -> dir
    option.None ->
      fallback
      |> option.map(fn(dir) { path_join_namespace(dir, name) })
      |> option.unwrap("db/seeds/" <> name)
  }
}

pub fn named_database_sql_dir(
  name: String,
  ref: DatabaseReference,
  fallback: Option(String),
) -> Option(String) {
  case ref.sql_dir {
    option.Some(dir) -> option.Some(dir)
    option.None ->
      fallback
      |> option.map(fn(dir) { path_join_namespace(dir, name) })
      |> option.or(option.Some("src/sql/" <> name))
  }
}

pub fn named_database_output(
  name: String,
  ref: DatabaseReference,
  fallback: Option(String),
) -> Option(String) {
  case ref.output {
    option.Some(dir) -> option.Some(dir)
    option.None ->
      fallback
      |> option.map(fn(dir) { path_join_namespace(dir, name) })
      |> option.or(option.Some("src/generated/sql/" <> name))
  }
}

fn path_join(base: String, segment: String) -> String {
  case string.ends_with(base, "/") {
    True -> base <> segment
    False -> base <> "/" <> segment
  }
}

fn path_join_namespace(base: String, name: String) -> String {
  let trimmed = trim_trailing_slash(base)
  case path_last_segment(trimmed) == name {
    True -> trimmed
    False -> path_join(trimmed, name)
  }
}

fn trim_trailing_slash(path: String) -> String {
  use <- bool.guard(!string.ends_with(path, "/"), path)
  string.drop_end(path, 1)
}

fn path_last_segment(path: String) -> String {
  path
  |> string.split("/")
  |> list.last
  |> result.unwrap("")
}

fn selected_database_reference(
  databases: dict.Dict(String, DatabaseReference),
  database_name: Option(String),
) -> Option(DatabaseReference) {
  use name <- option.then(database_name)
  dict.get(databases, name)
  |> result.map(option.Some)
  |> result.unwrap(option.None)
}

type CliArgs {
  CliArgs(
    database: Option(String),
    database_name: Option(String),
    output: Option(String),
  )
}

fn parse_cli_args(args: List(String)) -> CliArgs {
  parse_cli_args_loop(args, CliArgs(option.None, option.None, option.None))
}

fn parse_cli_args_loop(args: List(String), acc: CliArgs) -> CliArgs {
  case args {
    ["--database", value, ..rest] ->
      case is_missing_flag_value(value) {
        True -> parse_cli_args_loop([value, ..rest], acc)
        False ->
          parse_cli_args_loop(
            rest,
            CliArgs(..acc, database: option.Some(value)),
          )
      }
    ["--database-name", value, ..rest] ->
      case is_missing_flag_value(value) {
        True -> parse_cli_args_loop([value, ..rest], acc)
        False ->
          parse_cli_args_loop(
            rest,
            CliArgs(..acc, database_name: option.Some(value)),
          )
      }
    ["--output", value, ..rest] ->
      case is_missing_flag_value(value) {
        True -> parse_cli_args_loop([value, ..rest], acc)
        False ->
          parse_cli_args_loop(rest, CliArgs(..acc, output: option.Some(value)))
      }
    [arg, ..rest] -> parse_cli_equals_arg(arg, rest, acc)
    [] -> acc
  }
}

fn parse_cli_equals_arg(
  arg: String,
  rest: List(String),
  acc: CliArgs,
) -> CliArgs {
  case string.starts_with(arg, "--database=") {
    True -> {
      let value = string.drop_start(arg, string.length("--database="))
      case is_empty_flag_value(value) {
        True -> parse_cli_args_loop(rest, acc)
        False ->
          parse_cli_args_loop(
            rest,
            CliArgs(..acc, database: option.Some(value)),
          )
      }
    }
    False ->
      case string.starts_with(arg, "--output=") {
        True -> {
          let value = string.drop_start(arg, string.length("--output="))
          case is_empty_flag_value(value) {
            True -> parse_cli_args_loop(rest, acc)
            False ->
              parse_cli_args_loop(
                rest,
                CliArgs(..acc, output: option.Some(value)),
              )
          }
        }
        False -> parse_cli_database_name_equals_arg(arg, rest, acc)
      }
  }
}

fn parse_cli_database_name_equals_arg(
  arg: String,
  rest: List(String),
  acc: CliArgs,
) -> CliArgs {
  case string.starts_with(arg, "--database-name=") {
    True -> {
      let value = string.drop_start(arg, string.length("--database-name="))
      case is_empty_flag_value(value) {
        True -> parse_cli_args_loop(rest, acc)
        False ->
          parse_cli_args_loop(
            rest,
            CliArgs(..acc, database_name: option.Some(value)),
          )
      }
    }
    False -> parse_cli_args_loop(rest, acc)
  }
}

fn is_missing_flag_value(value: String) -> Bool {
  string.starts_with(value, "--") || is_empty_flag_value(value)
}

fn is_empty_flag_value(value: String) -> Bool {
  string.trim(value) == ""
}

/// Find SQL directories to process.
/// When sql_dir is configured, recursively finds all directories containing
/// .sql files under sql_dir.
/// Otherwise, recursively finds directories named "sql" under src/.
pub fn find_sql_directories(
  root: String,
  sql_dir: Option(String),
) -> List(String) {
  find_sql_directories_result(root, sql_dir)
  |> result.unwrap([])
}

@internal
pub fn find_sql_directories_result(
  root: String,
  sql_dir: Option(String),
) -> Result(List(String), Nil) {
  case sql_dir {
    option.Some(dir) -> find_dirs_with_sql_files(dir)
    option.None -> Ok(find_sql_directories_recursive(root))
  }
}

fn find_dirs_with_sql_files(dir: String) -> Result(List(String), Nil) {
  case simplifile.read_directory(at: dir) {
    Ok(entries) -> {
      let has_sql_here =
        list.any(entries, fn(f) { string.ends_with(f, ".sql") })
      let child_dirs =
        entries
        |> list.flat_map(fn(entry) {
          let path = dir <> "/" <> entry
          case simplifile.is_directory(path) {
            Ok(True) ->
              find_dirs_with_sql_files(path)
              |> result.unwrap([])
            _ -> []
          }
        })

      case has_sql_here {
        True -> Ok(list.sort([dir, ..child_dirs], string.compare))
        False -> Ok(list.sort(child_dirs, string.compare))
      }
    }
    Error(_) -> Error(Nil)
  }
}

fn find_sql_directories_recursive(dir: String) -> List(String) {
  case simplifile.read_directory(dir) {
    Ok(entries) -> {
      entries
      |> list.flat_map(fn(entry) {
        find_sql_directory_child(dir: dir, entry: entry)
      })
    }
    Error(_) -> []
  }
}

fn find_sql_directory_child(
  dir dir: String,
  entry entry: String,
) -> List(String) {
  let path = dir <> "/" <> entry

  case simplifile.is_directory(path) {
    Ok(True) -> find_sql_directory_from_child_dir(dir, path, entry)
    _ -> []
  }
}

fn find_sql_directory_from_child_dir(
  parent: String,
  path: String,
  entry: String,
) -> List(String) {
  let is_generated_output = entry == "generated"
  let is_src_root = parent == "src" || string.ends_with(parent, "/src")

  case is_generated_output, is_src_root, entry {
    True, True, _ -> []
    _, _, "sql" -> [path]
    _, _, _ -> find_sql_directories_recursive(path)
  }
}

/// Return sorted `.sql` paths directly inside `dir`; nested directories are
/// discovered before this by `find_sql_directories`.
pub fn list_sql_files(dir: String) -> List(String) {
  case simplifile.read_directory(dir) {
    Ok(entries) ->
      entries
      |> list.filter(fn(e) { string.ends_with(e, ".sql") })
      |> list.map(fn(e) { dir <> "/" <> e })
      |> list.sort(string.compare)
    Error(_) -> []
  }
}

/// Determine the output path for a sql/ directory.
///
/// Defaults to `src/generated/sql` when no output is configured.
/// Finds the longest common directory prefix between the output dir and
/// `sql_dir`, strips it from `sql_dir`, strips trailing `/sql`, then joins
/// with the output dir and appends `_sql.gleam`.
///
/// Examples:
///   - `src/app/users/sql` -> `src/generated/sql/users_sql.gleam`
///   - `src/server/accounts/sql` with output `src/server/generated/sql`
///     -> `src/server/generated/sql/accounts_sql.gleam`
pub fn output_path(
  sql_dir: String,
  configured_output: Option(String),
) -> String {
  let trimmed = output_dir(configured_output)
  let output_parts = string.split(trimmed, "/")
  let sql_parts = string.split(sql_dir, "/")
  let common_len = common_prefix_length(output_parts, sql_parts, 0)
  let relative = list.drop(sql_parts, common_len)
  output_path_from_relative(trimmed, relative)
}

pub fn output_path_from_source_root(
  sql_dir: String,
  configured_output: Option(String),
  source_root: Option(String),
) -> String {
  let trimmed = output_dir(configured_output)
  case source_root {
    option.None -> output_path(sql_dir, configured_output)
    option.Some(root) -> {
      let root_parts = path_parts(root)
      let sql_parts = path_parts(sql_dir)
      case path_has_prefix(sql_parts, root_parts) {
        True ->
          output_path_from_relative(
            trimmed,
            list.drop(sql_parts, list.length(root_parts)),
          )
        False -> output_path(sql_dir, configured_output)
      }
    }
  }
}

pub fn transaction_output_path(configured_output: Option(String)) -> String {
  output_dir(configured_output) <> "/transaction.gleam"
}

fn output_dir(configured_output: Option(String)) -> String {
  let output = case configured_output {
    option.Some(o) -> o
    option.None -> "src/generated/sql"
  }
  case string.ends_with(output, "/") {
    True -> string.drop_end(output, 1)
    False -> output
  }
}

fn output_path_from_relative(output: String, relative: List(String)) -> String {
  let entity_parts = list.filter(relative, fn(seg) { seg != "sql" })
  let entity_path = string.join(entity_parts, "/")
  case entity_path {
    "" -> output <> "/sql.gleam"
    path -> output <> "/" <> path <> "_sql.gleam"
  }
}

fn path_parts(path: String) -> List(String) {
  let trimmed = case string.ends_with(path, "/") {
    True -> string.drop_end(path, 1)
    False -> path
  }
  string.split(trimmed, "/")
}

fn path_has_prefix(path: List(String), prefix: List(String)) -> Bool {
  case path, prefix {
    _, [] -> True
    [part, ..path_rest], [prefix_part, ..prefix_rest] if part == prefix_part ->
      path_has_prefix(path_rest, prefix_rest)
    _, _ -> False
  }
}

fn common_prefix_length(a: List(String), b: List(String), acc: Int) -> Int {
  case a, b {
    [x, ..rest_a], [y, ..rest_b] if x == y ->
      common_prefix_length(rest_a, rest_b, acc + 1)
    _, _ -> acc
  }
}

/// Validate that the configured output directory is under src/.
/// Resolves `.` and `..` segments before checking, so paths like
/// `src/../../etc/foo` are correctly rejected.
pub fn validate_output(config: Config) -> Result(Nil, Nil) {
  case config.output {
    option.None -> Ok(Nil)
    option.Some(output) ->
      case string.starts_with(resolve_path(output), "src/") {
        True -> Ok(Nil)
        False -> Error(Nil)
      }
  }
}

/// Resolve `.` and `..` segments in a path without touching the filesystem.
/// Leading `..` segments that would escape the project root are preserved
/// so that `validate_output` correctly rejects paths like `../src/generated`.
fn resolve_path(path: String) -> String {
  path
  |> string.split("/")
  |> list.fold([], fn(acc, segment) {
    case segment {
      "." | "" -> acc
      ".." ->
        case acc {
          // Don't pop past already-accumulated ".." (we're above root)
          ["..", ..] -> ["..", ..acc]
          [_, ..rest] -> rest
          // Empty stack means we're escaping the project root
          [] -> [".."]
        }
      _ -> [segment, ..acc]
    }
  })
  |> list.reverse
  |> string.join("/")
}

const known_config_keys = [
  "database",
  "databases",
  "output",
  "query_function",
  "sql_dir",
  "init_sql",
  "migrations_dir",
  "seeds_dir",
]

fn warn_unknown_config_keys(parsed: dict.Dict(String, tom.Toml)) -> Nil {
  case tom.get_table(parsed, ["tools", "marmot"]) {
    Ok(table) -> {
      let keys = dict.keys(table)
      let unknown =
        list.filter(keys, fn(k) { !list.contains(known_config_keys, k) })
      case unknown {
        [] -> Nil
        _ ->
          io.println_error(
            "warning: Unrecognized keys in [tools.marmot]: "
            <> string.join(unknown, ", ")
            <> "\n  Known keys: "
            <> string.join(known_config_keys, ", "),
          )
      }
    }
    Error(_) -> Nil
  }
}
