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
    source_root: Option(String),
    query_function: Option(String),
  )
}

/// Parse configuration from gleam.toml content, CLI args, and env var.
/// Database precedence: env_database > CLI flags > gleam.toml > None
/// Output precedence: CLI flags > gleam.toml > None
/// Source root / query function: gleam.toml only (no CLI flag — structural config)
pub fn parse_config(
  toml_content: String,
  args: List(String),
  env_database: Option(String),
) -> Config {
  // Parse toml values
  let #(toml_database, toml_output, toml_source_root, toml_query_function) = case
    tom.parse(toml_content)
  {
    Ok(parsed) -> #(
      tom.get_string(parsed, ["marmot", "database"])
        |> result.map(option.Some)
        |> result.unwrap(option.None),
      tom.get_string(parsed, ["marmot", "output"])
        |> result.map(option.Some)
        |> result.unwrap(option.None),
      tom.get_string(parsed, ["marmot", "source_root"])
        |> result.map(option.Some)
        |> result.unwrap(option.None),
      tom.get_string(parsed, ["marmot", "query_function"])
        |> result.map(option.Some)
        |> result.unwrap(option.None),
    )
    Error(_) -> #(option.None, option.None, option.None, option.None)
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

  Config(
    database:,
    output:,
    source_root: toml_source_root,
    query_function: toml_query_function,
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
/// - No `output`: sibling of the sql/ directory
///   (e.g., `src/app/sql` -> `src/app/sql.gleam`).
/// - With `output` and `source_root` where `sql_dir` is under `source_root`:
///   strip the `source_root` prefix and trailing `/sql`, then join with
///   `output` (e.g., `src/server/accounts/sql` with root `src/server` and
///   output `src/server/generated/sql` -> `src/server/generated/sql/accounts.gleam`).
/// - With `output` but no `source_root` (or `sql_dir` not under `source_root`):
///   derive a flat mangled filename from the full `sql_dir` path to avoid
///   collisions (e.g., `src/users/sql` -> `src/gen/src_users_sql.gleam`).
pub fn output_path(
  sql_dir: String,
  configured_output: Option(String),
  source_root: Option(String),
) -> String {
  case configured_output {
    option.None -> sql_dir <> ".gleam"
    option.Some(output) -> {
      let trimmed = case string.ends_with(output, "/") {
        True -> string.drop_end(output, 1)
        False -> output
      }
      case source_root {
        option.Some(root) -> {
          let normalized_root = case string.ends_with(root, "/") {
            True -> root
            False -> root <> "/"
          }
          case string.starts_with(sql_dir, normalized_root) {
            True -> {
              let relative =
                string.drop_start(sql_dir, string.length(normalized_root))
              let entity_path = case string.ends_with(relative, "/sql") {
                True -> string.drop_end(relative, 4)
                False -> relative
              }
              trimmed <> "/" <> entity_path <> ".gleam"
            }
            False -> legacy_mangled_path(sql_dir, trimmed)
          }
        }
        option.None -> legacy_mangled_path(sql_dir, trimmed)
      }
    }
  }
}

fn legacy_mangled_path(sql_dir: String, trimmed_output: String) -> String {
  let module_name =
    sql_dir
    |> string.replace("/", "_")
    |> string.replace(".", "_")
  trimmed_output <> "/" <> module_name <> ".gleam"
}
