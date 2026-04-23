import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import simplifile
import tom

pub type Config {
  Config(
    database: Option(String),
    output: Option(String),
    query_function: Option(String),
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
  // Parse toml values
  let #(toml_database, toml_output, toml_query_function) = case
    tom.parse(toml_content)
  {
    Ok(parsed) -> #(
      tom.get_string(parsed, ["tools", "marmot", "database"])
        |> result.map(option.Some)
        |> result.unwrap(option.None),
      tom.get_string(parsed, ["tools", "marmot", "output"])
        |> result.map(option.Some)
        |> result.unwrap(option.None),
      tom.get_string(parsed, ["tools", "marmot", "query_function"])
        |> result.map(option.Some)
        |> result.unwrap(option.None),
    )
    Error(_) -> #(option.None, option.None, option.None)
  }

  // Parse CLI args
  let cli = parse_cli_args(args)

  // Apply precedence: env > cli > toml
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

  Config(database:, output:, query_function: toml_query_function)
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
      case string.starts_with(value, "--") {
        True -> parse_cli_args_loop([value, ..rest], acc)
        False ->
          parse_cli_args_loop(
            rest,
            CliArgs(..acc, database: option.Some(value)),
          )
      }
    ["--output", value, ..rest] ->
      case string.starts_with(value, "--") {
        True -> parse_cli_args_loop([value, ..rest], acc)
        False ->
          parse_cli_args_loop(rest, CliArgs(..acc, output: option.Some(value)))
      }
    [_, ..rest] -> parse_cli_args_loop(rest, acc)
    [] -> acc
  }
}

/// Recursively find all directories named "sql" under the given root.
pub fn find_sql_directories(root: String) -> List(String) {
  find_sql_directories_recursive(root)
}

fn find_sql_directories_recursive(dir: String) -> List(String) {
  case simplifile.read_directory(dir) {
    Ok(entries) -> {
      entries
      |> list.flat_map(fn(entry) {
        let path = dir <> "/" <> entry
        case simplifile.is_directory(path) {
          Ok(True) ->
            case entry {
              "sql" -> [path]
              _ -> find_sql_directories_recursive(path)
            }
          _ -> []
        }
      })
    }
    Error(_) -> []
  }
}

/// List all .sql files in a directory.
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
pub fn output_path(sql_dir: String, configured_output: Option(String)) -> String {
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
  let entity_parts = case list.last(relative) {
    Ok("sql") -> list.take(relative, list.length(relative) - 1)
    _ -> relative
  }
  let entity_path = string.join(entity_parts, "/")
  case entity_path {
    "" -> trimmed <> "_sql.gleam"
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
      "." -> acc
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
