import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import simplifile
import tom

pub type Config {
  Config(database: Option(String), output: Option(String))
}

/// Parse configuration from gleam.toml content, CLI args, and env var.
/// Database precedence: env_database > CLI flags > gleam.toml > None
/// Output precedence: CLI flags > gleam.toml > None
pub fn parse_config(
  toml_content: String,
  args: List(String),
  env_database: Option(String),
) -> Config {
  // Parse toml values
  let #(toml_database, toml_output) = case tom.parse(toml_content) {
    Ok(parsed) -> #(
      tom.get_string(parsed, ["marmot", "database"])
        |> result.map(option.Some)
        |> result.unwrap(option.None),
      tom.get_string(parsed, ["marmot", "output"])
        |> result.map(option.Some)
        |> result.unwrap(option.None),
    )
    Error(_) -> #(option.None, option.None)
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

  Config(database:, output:)
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
          parse_cli_args_loop(rest, CliArgs(..acc, database: option.Some(value)))
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
/// Default: sibling of the sql/ directory (e.g., src/app/sql -> src/app/sql.gleam)
/// Configured: derives filename from the parent directory to avoid collisions
/// (e.g., src/users/sql with output "src/gen" -> src/gen/users_sql.gleam)
pub fn output_path(sql_dir: String, configured_output: Option(String)) -> String {
  case configured_output {
    option.Some(output) -> {
      let trimmed = case string.ends_with(output, "/") {
        True -> string.drop_end(output, 1)
        False -> output
      }
      // Derive a unique filename from the sql_dir path
      let module_name =
        sql_dir
        |> string.replace("/", "_")
        |> string.replace(".", "_")
      trimmed <> "/" <> module_name <> ".gleam"
    }
    option.None -> sql_dir <> ".gleam"
  }
}
