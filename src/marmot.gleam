//// CLI entry point for Marmot.
////
//// Responsibilities:
//// - Parse config and CLI args, open the SQLite database
//// - Discover SQL directories and .sql files under src/
//// - Orchestrate per-file processing: read -> validate -> introspect -> codegen
//// - Detect output collisions, write generated modules, run gleam format
////
//// Detail work is delegated: config -> project.gleam, introspection ->
//// sqlite.gleam, code generation -> codegen.gleam. This module only handles
//// CLI I/O (args, env, file reading/writing, stderr, exit codes).

import argv
import gleam/dict
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option}
import gleam/order
import gleam/result
import gleam/string
import marmot/internal/codegen
import marmot/internal/error
import marmot/internal/project
import marmot/internal/query
import marmot/internal/sqlite
import marmot/internal/sqlite/tokenize
import marmot/migrations
import marmot/seeds
import simplifile
import sqlight

pub fn main() -> Nil {
  let args = argv.load().arguments
  case args {
    [] -> run_generate(args)
    ["help", ..] | ["--help", ..] | ["-h", ..] -> run_help()
    ["migrate", ..rest] -> run_migrate(rest)
    ["seed", ..rest] -> run_seed(rest)
    ["reset", ..rest] -> run_reset(rest)
    [command, ..] ->
      case string.starts_with(command, "-") {
        True -> run_generate(args)
        False -> run_unknown_command(command)
      }
  }
}

pub fn help_text() -> String {
  "Marmot

Usage:
  gleam run -m marmot [-- --database PATH]
  gleam run -m marmot migrate [--database PATH | --database-name NAME]
  gleam run -m marmot seed [--database PATH | --database-name NAME]
  gleam run -m marmot reset [--database PATH | --database-name NAME]
  gleam run -m marmot help

Commands:
  generate  Generate type-safe Gleam modules from src/**/sql/*.sql
  migrate   Run db/migrations/NNN_description.sql files once
  seed      Run every db/seeds/NNN_description.sql file
  reset     Delete the SQLite database, then migrate and seed
  help      Print this help text

Database:
  Configure one database with [tools.marmot].database, or define named refs in
  [tools.marmot.databases]. Use --database-name NAME to run one named ref.

Migration and seed directories:
  Defaults are db/migrations and db/seeds. Configure them with
  [tools.marmot].migrations_dir, [tools.marmot].seeds_dir, or named database
  refs. Without --database-name, migrate, seed, reset, and generate run all named
  database refs when named refs are configured."
}

fn run_help() -> Nil {
  io.println(help_text())
}

fn run_unknown_command(command: String) -> Nil {
  io.println_error(
    "error: Unknown command " <> command <> "\n\n" <> help_text(),
  )
  halt(1)
}

fn run_generate(args: List(String)) -> Nil {
  with_database_targets(args, fn(config, targets) {
    list.each(targets, fn(target) {
      print_database_target(target)
      let target_config = config_for_target(config, target)
      case project.validate_output(target_config) {
        Error(_) -> {
          let output = option.unwrap(target_config.output, "")
          io.println_error(error.to_string(error.OutputNotUnderSrc(output:)))
          halt(1)
        }
        Ok(_) -> Nil
      }
      with_open_database(target.path, fn(db) { generate_all(db, target_config) })
    })
  })
}

pub fn migrate(
  database_path: String,
) -> Result(List(String), migrations.MigrationError) {
  migrations.migrate(database_path)
}

pub fn migrate_from(
  database_path: String,
  migrations_dir: String,
) -> Result(List(String), migrations.MigrationError) {
  migrations.migrate_from(database_path, migrations_dir)
}

pub fn seed(database_path: String) -> Result(List(String), seeds.SeedError) {
  seeds.seed(database_path)
}

pub fn seed_from(
  database_path: String,
  seeds_dir: String,
) -> Result(List(String), seeds.SeedError) {
  seeds.seed_from(database_path, seeds_dir)
}

pub type ResetError {
  ResetDatabasePathIsDirectory(path: String)
  ResetDatabaseDeleteError(path: String, message: String)
  ResetMigrationError(error: migrations.MigrationError)
  ResetSeedError(error: seeds.SeedError)
}

type DatabaseTarget {
  DatabaseTarget(
    name: Option(String),
    path: String,
    migrations_dir: String,
    seeds_dir: String,
    sql_dir: Option(String),
    output: Option(String),
  )
}

type DatabaseTargetError {
  TargetDatabaseNotConfigured
  TargetConfigError(error: project.ConfigError)
  TargetUnknownDatabaseName(name: String)
}

pub fn reset(
  database_path: String,
) -> Result(#(List(String), List(String)), ResetError) {
  reset_from(database_path, migrations.migration_dir, seeds.seed_dir)
}

pub fn reset_from(
  database_path: String,
  migrations_dir: String,
  seeds_dir: String,
) -> Result(#(List(String), List(String)), ResetError) {
  use _ <- result.try(drop_database(database_path))
  use applied_migrations <- result.try(
    migrations.migrate_from(database_path, migrations_dir)
    |> result.map_error(ResetMigrationError),
  )
  use applied_seeds <- result.try(
    seeds.seed_from(database_path, seeds_dir)
    |> result.map_error(ResetSeedError),
  )
  Ok(#(applied_migrations, applied_seeds))
}

fn run_migrate(args: List(String)) -> Nil {
  run_sql_files_command(
    args,
    fn(target) { migrations.migrate_from(target.path, target.migrations_dir) },
    migrations.to_string,
    "Applied",
    "No migrations to apply",
    "migration",
  )
}

fn run_seed(args: List(String)) -> Nil {
  run_sql_files_command(
    args,
    fn(target) { seeds.seed_from(target.path, target.seeds_dir) },
    seeds.to_string,
    "Ran",
    "No seed files to run",
    "seed file",
  )
}

fn run_reset(args: List(String)) -> Nil {
  with_database_targets(args, fn(_config, targets) {
    list.each(targets, fn(target) {
      print_database_target(target)
      case reset_from(target.path, target.migrations_dir, target.seeds_dir) {
        Error(err) -> {
          io.println_error(reset_error_to_string(err))
          halt(1)
        }
        Ok(#(applied_migrations, applied_seeds)) -> {
          io.println("Dropped " <> target.path)
          print_sql_files_summary(
            applied_migrations,
            "Applied",
            "No migrations to apply",
            "migration",
          )
          print_sql_files_summary(
            applied_seeds,
            "Ran",
            "No seed files to run",
            "seed file",
          )
        }
      }
    })
  })
}

fn run_sql_files_command(
  args: List(String),
  run: fn(DatabaseTarget) -> Result(List(String), error_type),
  error_to_string: fn(error_type) -> String,
  action: String,
  empty_message: String,
  summary_noun: String,
) -> Nil {
  with_database_targets(args, fn(_config, targets) {
    list.each(targets, fn(target) {
      print_database_target(target)
      case run(target) {
        Error(err) -> {
          io.println_error(error_to_string(err))
          halt(1)
        }
        Ok(applied) ->
          print_sql_files_summary(applied, action, empty_message, summary_noun)
      }
    })
  })
}

fn migrations_directory(config: project.Config) -> String {
  option.unwrap(config.migrations_dir, migrations.migration_dir)
}

fn seeds_directory(config: project.Config) -> String {
  option.unwrap(config.seeds_dir, seeds.seed_dir)
}

fn with_database_targets(
  args: List(String),
  callback: fn(project.Config, List(DatabaseTarget)) -> Nil,
) -> Nil {
  let env_database = get_env("DATABASE_URL")
  let toml_content = case simplifile.read("gleam.toml") {
    Ok(content) -> content
    Error(_) -> {
      io.println_error(
        "error: Could not read gleam.toml
  Marmot must be run from a Gleam project root with a gleam.toml file.",
      )
      halt(1)
      ""
    }
  }
  let config = project.parse_config(toml_content, args, env_database)

  case database_targets(config) {
    Error(err) -> {
      io.println_error(database_target_error_to_string(err))
      halt(1)
    }
    Ok(targets) -> callback(config, targets)
  }
}

fn database_targets(
  config: project.Config,
) -> Result(List(DatabaseTarget), DatabaseTargetError) {
  use _ <- result.try(case config.error {
    option.Some(err) -> Error(TargetConfigError(err))
    option.None -> Ok(Nil)
  })

  case config.database_name {
    option.Some(name) ->
      case dict.get(config.databases, name) {
        Ok(ref) -> {
          use target <- result.try(named_database_target(name, ref, config))
          // Named ref's own path wins — --database flag cannot override it
          Ok([target])
        }
        Error(_) ->
          case dict.is_empty(config.databases) {
            // No named databases in config — --database-name is not meaningful
            // here but don't punish the user; use --database path if available
            True ->
              case config.database {
                option.Some(path) ->
                  Ok([
                    DatabaseTarget(
                      name: config.database_name,
                      path:,
                      migrations_dir: migrations_directory(config),
                      seeds_dir: seeds_directory(config),
                      sql_dir: config.sql_dir,
                      output: config.output,
                    ),
                  ])
                option.None -> Error(TargetUnknownDatabaseName(name:))
              }
            // Named databases ARE configured, so --database-name must resolve
            False -> Error(TargetUnknownDatabaseName(name:))
          }
      }
    option.None ->
      case config.database {
        option.Some(path) ->
          Ok([
            DatabaseTarget(
              name: config.database_name,
              path:,
              migrations_dir: migrations_directory(config),
              seeds_dir: seeds_directory(config),
              sql_dir: config.sql_dir,
              output: config.output,
            ),
          ])
        option.None ->
          case dict.is_empty(config.databases) {
            True -> Error(TargetDatabaseNotConfigured)
            False -> named_database_targets(config)
          }
      }
  }
}

fn named_database_targets(
  config: project.Config,
) -> Result(List(DatabaseTarget), DatabaseTargetError) {
  config.databases
  |> dict.to_list
  |> list.sort(compare_named_database)
  |> list.try_map(fn(entry) {
    let #(name, ref) = entry
    named_database_target(name, ref, config)
  })
}

fn compare_named_database(
  a: #(String, project.DatabaseReference),
  b: #(String, project.DatabaseReference),
) -> order.Order {
  let #(a_name, _) = a
  let #(b_name, _) = b
  string.compare(a_name, b_name)
}

fn named_database_target(
  name: String,
  ref: project.DatabaseReference,
  config: project.Config,
) -> Result(DatabaseTarget, DatabaseTargetError) {
  Ok(DatabaseTarget(
    name: option.Some(name),
    path: project.named_database_path(name, ref),
    migrations_dir: project.named_database_migrations_dir(
      name,
      ref,
      config.migrations_dir,
    ),
    seeds_dir: project.named_database_seeds_dir(name, ref, config.seeds_dir),
    sql_dir: project.named_database_sql_dir(name, ref, config.sql_dir),
    output: project.named_database_output(name, ref, config.output),
  ))
}

fn config_for_target(
  config: project.Config,
  target: DatabaseTarget,
) -> project.Config {
  project.Config(
    ..config,
    database: option.Some(target.path),
    database_name: target.name,
    migrations_dir: option.Some(target.migrations_dir),
    seeds_dir: option.Some(target.seeds_dir),
    sql_dir: target.sql_dir,
    output: target.output,
  )
}

fn with_open_database(
  db_path: String,
  callback: fn(sqlight.Connection) -> Nil,
) -> Nil {
  case sqlight.open(db_path) {
    Ok(db) -> {
      callback(db)
      let _close_result = sqlight.close(db)
      Nil
    }
    Error(err) -> {
      io.println_error(
        error.to_string(error.DatabaseOpenError(
          path: db_path,
          message: err.message,
        )),
      )
      halt(1)
    }
  }
}

fn print_database_target(target: DatabaseTarget) -> Nil {
  case target.name {
    option.None -> Nil
    option.Some(name) -> io.println("Database " <> name <> ": " <> target.path)
  }
}

fn database_target_error_to_string(error: DatabaseTargetError) -> String {
  case error {
    TargetDatabaseNotConfigured -> error.to_string(error.DatabaseNotConfigured)
    TargetConfigError(error:) -> project.config_error_to_string(error)
    TargetUnknownDatabaseName(name:) -> "error: Unknown database name
  \u{250c}\u{2500} " <> name <> "
  \u{2502}
  \u{2502} No matching [tools.marmot.databases." <> name <> "] entry was found.
  \u{2502}
  hint: Add the named database to gleam.toml or choose another --database-name."
  }
}

fn print_sql_files_summary(
  applied: List(String),
  action: String,
  empty_message: String,
  summary_noun: String,
) -> Nil {
  list.each(applied, fn(version) { io.println(action <> " " <> version) })
  case applied {
    [] -> io.println(empty_message)
    _ ->
      io.println(
        action
        <> " "
        <> int.to_string(list.length(applied))
        <> " "
        <> summary_noun
        <> "(s)",
      )
  }
}

fn drop_database(database_path: String) -> Result(Nil, ResetError) {
  case simplifile.is_directory(database_path) {
    Ok(True) -> Error(ResetDatabasePathIsDirectory(path: database_path))
    Ok(False) -> drop_database_files(database_path)
    Error(err) ->
      Error(ResetDatabaseDeleteError(
        path: database_path,
        message: string.inspect(err),
      ))
  }
}

fn drop_database_files(database_path: String) -> Result(Nil, ResetError) {
  [
    database_path,
    database_path <> "-wal",
    database_path <> "-shm",
    database_path <> "-journal",
  ]
  |> list.try_each(delete_file_if_present)
}

fn delete_file_if_present(path: String) -> Result(Nil, ResetError) {
  case simplifile.is_file(path) {
    Ok(False) -> Ok(Nil)
    Ok(True) ->
      simplifile.delete(path)
      |> result.map_error(fn(err) {
        ResetDatabaseDeleteError(path: path, message: string.inspect(err))
      })
    Error(err) ->
      Error(ResetDatabaseDeleteError(path: path, message: string.inspect(err)))
  }
}

fn reset_error_to_string(error: ResetError) -> String {
  case error {
    ResetDatabasePathIsDirectory(path:) -> "error: Database path is a directory
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} Reset deletes the configured SQLite database file before rebuilding it.
  \u{2502}
  hint: Set --database to a SQLite file path."

    ResetDatabaseDeleteError(path:, message:) ->
      "error: Could not delete SQLite database
  \u{250c}\u{2500} " <> path <> "
  \u{2502}
  \u{2502} " <> message

    ResetMigrationError(error:) -> migrations.to_string(error)
    ResetSeedError(error:) -> seeds.to_string(error)
  }
}

fn generate_all(db: sqlight.Connection, config: project.Config) -> Nil {
  // Reject malformed query_function early rather than silently falling back
  // to sqlight.query, which would generate code using the wrong call path.
  case config.query_function {
    option.Some(value) ->
      case codegen.parse_query_function(config.query_function) {
        option.None -> {
          io.println_error(
            "error: query_function \""
            <> value
            <> "\" is malformed, expected \"module/path.function\" format.",
          )
          halt(1)
        }
        option.Some(_) -> Nil
      }
    option.None -> Nil
  }
  let sql_dirs = case
    project.find_sql_directories_result("src", config.sql_dir)
  {
    Ok(dirs) -> dirs
    Error(_) -> {
      let sql_dir = option.unwrap(config.sql_dir, "src")
      io.println_error("error: Could not read SQL directory " <> sql_dir)
      halt(1)
      []
    }
  }
  case sql_dirs {
    [] -> io.println("No sql/ directories found under src/")
    dirs -> {
      let outputs =
        list.map(dirs, fn(dir) {
          project.output_path_from_source_root(
            dir,
            config.output,
            config.sql_dir,
          )
        })
      case list.length(list.unique(outputs)) != list.length(outputs) {
        True -> {
          io.println_error(
            "error: Multiple sql/ directories would write to the same output file.\n  Remove the output configuration or restructure your sql/ directories.",
          )
          halt(1)
        }
        False -> Nil
      }

      let success_count =
        list.fold(dirs, 0, fn(count, dir) {
          case generate_for_directory(db, dir, config) {
            True -> count + 1
            False -> count
          }
        })
      case success_count != list.length(dirs) {
        True -> {
          io.println_error("error: Some files could not be written")
          halt(1)
        }
        False -> Nil
      }

      io.println(
        "Generated " <> int.to_string(list.length(dirs)) <> " module(s)",
      )
    }
  }
}

fn generate_for_directory(
  db: sqlight.Connection,
  sql_dir: String,
  config: project.Config,
) -> Bool {
  let sql_files = project.list_sql_files(sql_dir)
  case config.sql_dir {
    option.None -> warn_subdirectories(sql_dir)
    option.Some(_) -> Nil
  }
  let queries =
    list.filter_map(sql_files, fn(file_path) { process_sql_file(db, file_path) })

  case queries {
    [] ->
      // If there were SQL files but none produced queries, that means they all
      // had errors. Only return True (success) when the directory was empty.
      list.is_empty(sql_files)
    _ -> {
      let output =
        project.output_path_from_source_root(
          sql_dir,
          config.output,
          config.sql_dir,
        )
      case codegen.generate_module_with_config(queries, config.query_function) {
        Error(err) -> {
          io.println_error(error.to_string(err))
          False
        }
        Ok(raw_content) -> {
          let module_content = format_gleam(raw_content)
          write_module_file(output, module_content)
        }
      }
    }
  }
}

fn warn_subdirectories(sql_dir: String) -> Nil {
  case simplifile.read_directory(sql_dir) {
    Ok(entries) -> {
      let subdirs =
        list.filter(entries, fn(e) {
          case simplifile.is_directory(sql_dir <> "/" <> e) {
            Ok(True) -> True
            _ -> False
          }
        })
      case subdirs {
        [] -> Nil
        dirs ->
          io.println_error(
            "warning: "
            <> sql_dir
            <> " contains subdirectories that will be ignored: "
            <> string.join(dirs, ", ")
            <> "\n  hint: Marmot only reads .sql files directly inside sql/ directories",
          )
      }
    }
    Error(_) ->
      io.println_error("warning: Could not read directory " <> sql_dir)
  }
}

fn write_module_file(output: String, module_content: String) -> Bool {
  case ensure_parent_dir(output) {
    Error(MakeDirError(msg)) -> {
      io.println_error(
        "error: Could not create parent directory for "
        <> output
        <> "\n  "
        <> msg,
      )
      False
    }
    Ok(_) ->
      case simplifile.write(output, module_content) {
        Ok(_) -> {
          io.println("  wrote " <> output)
          True
        }
        Error(_) -> {
          io.println_error("error: Could not write to " <> output)
          False
        }
      }
  }
}

@internal
pub fn ensure_parent_dir(path: String) -> Result(Nil, MakeDirError) {
  let parent =
    path
    |> string.split("/")
    |> list.reverse
    |> list.rest
    |> result.unwrap([])
    |> list.reverse
    |> string.join("/")

  case parent {
    "" -> Ok(Nil)
    dir ->
      case simplifile.create_directory_all(dir) {
        Ok(_) -> Ok(Nil)
        Error(err) -> Error(MakeDirError(simplifile.describe_error(err)))
      }
  }
}

fn process_sql_file(
  db: sqlight.Connection,
  file_path: String,
) -> Result(query.Query, Nil) {
  use content <- result.try(case simplifile.read(file_path) {
    Ok(content) -> Ok(content)
    Error(read_err) -> {
      io.println_error(
        error.to_string(error.FileReadError(
          path: file_path,
          message: "Could not read file: " <> string.inspect(read_err),
        )),
      )
      Error(Nil)
    }
  })

  let filename =
    file_path
    |> string.split("/")
    |> list.last
    |> result.unwrap("query.sql")
  use name <- result.try(case query.function_name(filename) {
    Ok(n) -> Ok(n)
    Error(_) -> {
      io.println_error(error.to_string(error.InvalidFilename(path: file_path)))
      Error(Nil)
    }
  })

  let trimmed = string.trim(content)
  use sql <- result.try(validate_sql(trimmed, file_path))
  use custom_type_name <- result.try(
    sqlite.parse_returns_annotation(sql)
    |> result.map_error(fn(err) {
      let sqlite.InvalidReturnsTypeName(name: n, reason: r) = err
      io.println_error(
        error.to_string(error.InvalidReturnsAnnotation(
          path: file_path,
          name: n,
          reason: r,
        )),
      )
      Nil
    }),
  )
  use query_info <- result.try(
    sqlite.introspect_query(db, file_path, sql)
    |> result.map_error(fn(err) {
      io.println_error(error.to_string(err))
      Nil
    }),
  )
  use _ <- result.try(check_duplicate_columns(query_info.columns, file_path))
  use _ <- result.try(check_generated_column_names(
    query_info.columns,
    file_path,
  ))
  use _ <- result.try(check_generated_parameter_names(
    query_info.parameters,
    file_path,
  ))
  Ok(query.Query(
    name: name,
    sql: sqlite.strip_nullability_suffixes(sql),
    path: file_path,
    parameters: query_info.parameters,
    columns: query_info.columns,
    custom_type_name: custom_type_name,
  ))
}

pub fn validate_sql(trimmed: String, file_path: String) -> Result(String, Nil) {
  let sql_without_comments =
    trimmed
    |> query.strip_comments
    |> string.trim

  case sql_without_comments {
    "" -> {
      io.println_error(error.to_string(error.EmptySqlFile(path: file_path)))
      Error(Nil)
    }
    _ -> {
      let semicolon_count = count_semicolons(trimmed)
      case has_multiple_queries(sql_without_comments, semicolon_count) {
        True -> {
          io.println_error(
            error.to_string(error.MultipleQueries(path: file_path)),
          )
          Error(Nil)
        }
        False -> Ok(remove_trailing_semicolon(trimmed, semicolon_count))
      }
    }
  }
}

fn has_multiple_queries(
  sql_without_comments: String,
  semicolon_count: Int,
) -> Bool {
  semicolon_count > 1
  || { semicolon_count == 1 && !string.ends_with(sql_without_comments, ";") }
}

fn count_semicolons(sql: String) -> Int {
  tokenize.tokenize(sql)
  |> list.fold(0, fn(count, token) {
    case token {
      tokenize.Semicolon -> count + 1
      _ -> count
    }
  })
}

fn remove_trailing_semicolon(sql: String, semicolon_count: Int) -> String {
  case semicolon_count {
    0 -> sql
    _ ->
      sql
      |> trim_trailing_comment_lines
      |> string.trim
      |> string.drop_end(1)
      |> string.trim
  }
}

fn trim_trailing_comment_lines(sql: String) -> String {
  let lines =
    sql
    |> string.split("\n")
    |> list.reverse
    |> list.drop_while(fn(line) {
      line
      |> query.strip_comments
      |> string.trim
      == ""
    })

  case lines {
    [] -> ""
    [line, ..rest] ->
      [query.strip_comments(line) |> string.trim, ..rest]
      |> list.reverse
      |> string.join("\n")
  }
}

pub fn check_duplicate_columns(
  columns: List(query.Column),
  file_path: String,
) -> Result(Nil, Nil) {
  let names = list.map(columns, fn(c) { c.name })
  let dupes = query.find_duplicates(names)
  case dupes {
    [] -> Ok(Nil)
    _ -> {
      io.println_error(
        error.to_string(error.DuplicateColumns(path: file_path, columns: dupes)),
      )
      Error(Nil)
    }
  }
}

/// Check for semicolons outside of quoted SQL contexts, line comments, and
/// block comments. Delegates to the tokenizer which already handles all
/// quoting and comment styles correctly.
pub fn contains_semicolon_outside_strings(sql: String) -> Bool {
  count_semicolons(sql) > 0
}

pub fn check_generated_column_names(
  columns: List(query.Column),
  file_path: String,
) -> Result(Nil, Nil) {
  let generated =
    columns
    |> list.map(fn(c) { #(c.name, generated_name(c.name)) })
  check_generated_name_pairs(generated, file_path)
}

pub fn check_generated_parameter_names(
  parameters: List(query.Parameter),
  file_path: String,
) -> Result(Nil, Nil) {
  let generated =
    parameters
    |> list.map(fn(p) { #(p.name, generated_name(p.name)) })
  check_generated_name_pairs(generated, file_path)
}

fn generated_name(name: String) -> String {
  name
  |> query.sanitize_identifier
  |> query.safe_name
}

fn check_generated_name_pairs(
  pairs: List(#(String, String)),
  file_path: String,
) -> Result(Nil, Nil) {
  let generated_names = list.map(pairs, fn(pair) { pair.1 })
  let dupes = query.find_duplicates(generated_names)

  case dupes {
    [] -> Ok(Nil)
    _ -> {
      let conflicts =
        pairs
        |> list.filter(fn(pair) { list.contains(dupes, pair.1) })
      io.println_error(
        error.to_string(error.GeneratedNameCollision(
          path: file_path,
          names: conflicts,
        )),
      )
      Error(Nil)
    }
  }
}

/// Run `gleam format` on generated code. Falls back to the original string
/// if formatting fails (e.g., gleam not on PATH) and prints a warning.
@internal
pub fn format_gleam(code: String) -> String {
  let tmp_dir = get_tmp_dir()
  case make_tmp_file(tmp_dir, code) {
    Error(_) -> {
      io.println_error(
        "warning: Could not write temp file for formatting, skipping gleam format",
      )
      code
    }
    Ok(tmp) -> {
      let exit_code = run_executable("gleam", ["format", tmp])
      let formatted = format_gleam_after_run(code, tmp, exit_code)
      let _delete_result = simplifile.delete(tmp)
      formatted
    }
  }
}

@internal
pub fn format_gleam_after_run(
  code: String,
  tmp: String,
  exit_code: Int,
) -> String {
  case exit_code {
    0 ->
      case simplifile.read(tmp) {
        Ok(result) -> result
        Error(_) -> {
          io.println_error(
            "warning: Could not read formatted file, using unformatted output",
          )
          code
        }
      }
    -1 -> {
      io.println_error("warning: gleam not found on PATH, skipping format")
      code
    }
    -2 -> {
      io.println_error(
        "warning: gleam format timed out, using unformatted output",
      )
      code
    }
    _ -> {
      io.println_error(
        "warning: gleam format failed (exit code "
        <> int.to_string(exit_code)
        <> "), using unformatted output",
      )
      code
    }
  }
}

fn get_tmp_dir() -> String {
  get_env("TMPDIR")
  |> option.lazy_or(fn() { get_env("TMP") })
  |> option.lazy_or(fn() { get_env("TEMP") })
  |> option.unwrap("/tmp")
}

/// Run an executable with arguments, bypassing the shell.
/// Uses erlang:open_port with spawn_executable to avoid shell injection.
/// Returns the exit status code, or -1 if the executable was not found.
@internal
pub fn run_executable(executable: String, args: List(String)) -> Int {
  case find_executable(executable) {
    option.None -> -1
    option.Some(path) -> run_executable_ffi(path, args)
  }
}

@external(erlang, "marmot_ffi", "run_executable")
fn run_executable_ffi(path: String, args: List(String)) -> Int

/// Run an executable with arguments in a specific working directory.
/// Same semantics as run_executable but sets CWD before spawning.
@internal
pub fn run_executable_in(
  executable: String,
  args: List(String),
  cwd: String,
) -> Int {
  case find_executable(executable) {
    option.None -> -1
    option.Some(path) -> run_executable_in_ffi(path, args, cwd)
  }
}

@external(erlang, "marmot_ffi", "run_executable_in")
fn run_executable_in_ffi(path: String, args: List(String), cwd: String) -> Int

/// Run an executable with a custom timeout (in milliseconds).
@internal
pub fn run_executable_in_timeout(
  executable: String,
  args: List(String),
  cwd: String,
  timeout_ms: Int,
) -> Int {
  case find_executable(executable) {
    option.None -> -1
    option.Some(path) ->
      run_executable_in_timeout_ffi(path, args, cwd, timeout_ms)
  }
}

@external(erlang, "marmot_ffi", "run_executable_in_timeout")
fn run_executable_in_timeout_ffi(
  path: String,
  args: List(String),
  cwd: String,
  timeout_ms: Int,
) -> Int

@external(erlang, "marmot_ffi", "find_executable")
fn find_executable(name: String) -> Option(String)

fn halt(code: Int) -> Nil {
  // init:stop/1 performs a graceful shutdown that flushes I/O buffers,
  // unlike erlang:halt/1 which exits immediately and may lose stderr output
  init_stop(code)
  // init:stop is async; give it one scheduler tick before forcing exit.
  timer_sleep(100)
  erlang_halt(code)
}

@external(erlang, "init", "stop")
fn init_stop(code: Int) -> Nil

@external(erlang, "erlang", "halt")
fn erlang_halt(code: Int) -> Nil

@external(erlang, "timer", "sleep")
fn timer_sleep(ms: Int) -> Nil

@internal
pub type MakeTmpFileError {
  MakeTmpFileError(String)
}

/// Error from ensure_parent_dir when the parent directory cannot be created.
@internal
pub type MakeDirError {
  MakeDirError(String)
}

/// Create a temp file with a cryptographically random name and write content
/// atomically using exclusive mode (prevents symlink races).
@internal
pub fn make_tmp_file(
  dir: String,
  content: String,
) -> Result(String, MakeTmpFileError) {
  make_tmp_file_raw(dir, content)
}

@external(erlang, "marmot_ffi", "make_tmp_file")
fn make_tmp_file_raw(
  dir: String,
  content: String,
) -> Result(String, MakeTmpFileError)

/// Look up a single environment variable by name. The FFI handles
/// binary-to-charlist conversion for OTP 27+ compatibility.
@external(erlang, "marmot_ffi", "get_env")
fn get_env(name: String) -> Option(String)
