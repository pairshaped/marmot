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
/// database: env > CLI > toml; output: CLI > toml; the rest are toml-only.
pub type Config {
  Config(
    /// Path to the SQLite database file used for introspection.
    database: Option(String),
    /// Output directory for generated modules (must be under `src/`).
    output: Option(String),
    /// Fully-qualified wrapper function replacing `sqlight.query`.
    query_function: Option(String),
    /// Custom directory for SQL file discovery instead of `src/**/sql/`.
    sql_dir: Option(String),
  )
}

/// Parse configuration from gleam.toml content, CLI args, and env var.
/// Database precedence: env_database > CLI flags > gleam.toml > None
/// Output precedence: CLI flags > gleam.toml > None
/// Query function: gleam.toml only (no CLI flag)
pub fn parse_config(
  toml_content: String,
  args: List(String),
  env_database: Option(String),
) -> Config {
  let #(toml_database, toml_output, toml_query_function, toml_sql_dir) = case
    tom.parse(toml_content)
  {
    Ok(parsed) -> {
      warn_unknown_config_keys(parsed)
      #(
        tom.get_string(parsed, ["tools", "marmot", "database"])
          |> result.map(option.Some)
          |> result.unwrap(option.None),
        tom.get_string(parsed, ["tools", "marmot", "output"])
          |> result.map(option.Some)
          |> result.unwrap(option.None),
        tom.get_string(parsed, ["tools", "marmot", "query_function"])
          |> result.map(option.Some)
          |> result.unwrap(option.None),
        tom.get_string(parsed, ["tools", "marmot", "sql_dir"])
          |> result.map(option.Some)
          |> result.unwrap(option.None),
      )
    }
    Error(_) -> #(option.None, option.None, option.None, option.None)
  }

  let cli = parse_cli_args(args)

  let database = case env_database {
    option.Some(_) -> env_database
    option.None ->
      case cli.database {
        option.Some(_) -> cli.database
        option.None -> toml_database
      }
  }

  let output = case cli.output {
    option.Some(_) -> cli.output
    option.None -> toml_output
  }

  Config(
    database:,
    output:,
    query_function: toml_query_function,
    sql_dir: toml_sql_dir,
  )
}

type CliArgs {
  CliArgs(database: Option(String), output: Option(String))
}

fn parse_cli_args(args: List(String)) -> CliArgs {
  parse_cli_args_loop(args, CliArgs(option.None, option.None))
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
        False -> parse_cli_args_loop(rest, acc)
      }
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
    Ok(True) -> find_sql_directory_from_child_dir(path, entry)
    _ -> []
  }
}

fn find_sql_directory_from_child_dir(
  path: String,
  entry: String,
) -> List(String) {
  case entry {
    "sql" -> [path]
    _ -> find_sql_directories_recursive(path)
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
  let output = case configured_output {
    option.Some(o) -> o
    option.None -> "src/generated/sql"
  }
  let trimmed = case string.ends_with(output, "/") {
    True -> string.drop_end(output, 1)
    False -> output
  }
  let output_parts = string.split(trimmed, "/")
  let sql_parts = string.split(sql_dir, "/")
  let common_len = common_prefix_length(output_parts, sql_parts, 0)
  let relative = list.drop(sql_parts, common_len)
  let entity_parts = list.filter(relative, fn(seg) { seg != "sql" })
  let entity_path = string.join(entity_parts, "/")
  case entity_path {
    "" -> trimmed <> "/sql.gleam"
    path -> trimmed <> "/" <> path <> "_sql.gleam"
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

const known_config_keys = ["database", "output", "query_function", "sql_dir"]

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
