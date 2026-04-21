import argv
import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import marmot/internal/codegen
import marmot/internal/error
import marmot/internal/project
import marmot/internal/query
import marmot/internal/sqlite
import simplifile
import sqlight

pub fn main() -> Nil {
  let args = argv.load().arguments
  run_generate(args)
}

fn with_database(
  args: List(String),
  callback: fn(sqlight.Connection, project.Config) -> Nil,
) -> Nil {
  let env_database = get_env("DATABASE_URL")
  let toml_content =
    simplifile.read("gleam.toml")
    |> result.unwrap("")
  let config = project.parse_config(toml_content, args, env_database)

  case project.validate_output(config) {
    Error(output) -> {
      io.println_error(error.to_string(error.OutputNotUnderSrc(output:)))
      halt(1)
    }
    Ok(_) -> Nil
  }

  case config.database {
    option.None -> {
      io.println_error(error.to_string(error.DatabaseNotConfigured))
      halt(1)
    }
    option.Some(db_path) ->
      case sqlight.open(db_path) {
        Ok(db) -> {
          callback(db, config)
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
}

fn run_generate(args: List(String)) -> Nil {
  with_database(args, fn(db, config) { generate_all(db, config) })
}

fn generate_all(db: sqlight.Connection, config: project.Config) -> Nil {
  let sql_dirs = project.find_sql_directories("src")
  case sql_dirs {
    [] -> io.println("No sql/ directories found under src/")
    dirs -> {
      // Detect output path collisions when using configured output directory
      let outputs =
        list.map(dirs, fn(dir) { project.output_path(dir, config.output) })
      case list.length(list.unique(outputs)) == list.length(outputs) {
        False -> {
          io.println_error(
            "error: Multiple sql/ directories would write to the same output file."
            <> "\n  Remove the output configuration or restructure your sql/ directories.",
          )
          halt(1)
        }
        True -> {
          let success_count =
            list.fold(dirs, 0, fn(count, dir) {
              case generate_for_directory(db, dir, config) {
                True -> count + 1
                False -> count
              }
            })
          case success_count == list.length(dirs) {
            True ->
              io.println(
                "Generated " <> int.to_string(list.length(dirs)) <> " module(s)",
              )
            False -> {
              io.println_error("error: Some files could not be written")
              halt(1)
            }
          }
        }
      }
    }
  }
}

fn generate_for_directory(
  db: sqlight.Connection,
  sql_dir: String,
  config: project.Config,
) -> Bool {
  let sql_files = project.list_sql_files(sql_dir)
  warn_subdirectories(sql_dir)
  let queries =
    list.filter_map(sql_files, fn(file_path) { process_sql_file(db, file_path) })

  case queries {
    [] ->
      // If there were SQL files but none produced queries, that means they all
      // had errors. Only return True (success) when the directory was empty.
      list.is_empty(sql_files)
    _ -> {
      let output = project.output_path(sql_dir, config.output)
      let module_content =
        codegen.generate_module_with_config(queries, config.query_function)
        |> format_gleam
      case ensure_parent_dir(output) {
        Error(msg) -> {
          io.println_error("error: " <> msg)
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
    Error(_) -> Nil
  }
}

fn ensure_parent_dir(path: String) -> Result(Nil, String) {
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
        Error(_) -> Error("Could not create directory: " <> dir)
      }
  }
}

fn process_sql_file(
  db: sqlight.Connection,
  file_path: String,
) -> Result(query.Query, Nil) {
  use content <- result.try(
    simplifile.read(file_path)
    |> result.map_error(fn(_) {
      io.println_error(
        error.to_string(error.FileReadError(
          path: file_path,
          message: "Could not read file",
        )),
      )
      Nil
    }),
  )

  let filename =
    file_path
    |> string.split("/")
    |> list.last
    |> result.unwrap("query.sql")
  use name <- result.try(
    query.function_name(filename)
    |> result.map_error(fn(_) {
      io.println_error(error.to_string(error.InvalidFilename(path: file_path)))
      Nil
    }),
  )

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
    sqlite.introspect_query(db, sql)
    |> result.map_error(fn(err) {
      io.println_error(
        error.to_string(error.SqlError(path: file_path, message: err.message)),
      )
      Nil
    }),
  )
  use _ <- result.try(check_duplicate_columns(query_info.columns, file_path))
  Ok(query.Query(
    name: name,
    sql: sqlite.strip_nullability_suffixes(sql),
    path: file_path,
    parameters: query_info.parameters,
    columns: query_info.columns,
    custom_type_name: custom_type_name,
  ))
}

fn validate_sql(trimmed: String, file_path: String) -> Result(String, Nil) {
  case trimmed {
    "" -> {
      io.println_error(error.to_string(error.EmptySqlFile(path: file_path)))
      Error(Nil)
    }
    sql -> {
      let stripped = case string.ends_with(sql, ";") {
        True ->
          sql
          |> string.drop_end(1)
          |> string.trim
        False -> sql
      }
      case contains_semicolon_outside_strings(stripped) {
        True -> {
          io.println_error(
            error.to_string(error.MultipleQueries(path: file_path)),
          )
          Error(Nil)
        }
        False -> Ok(stripped)
      }
    }
  }
}

fn check_duplicate_columns(
  columns: List(query.Column),
  file_path: String,
) -> Result(Nil, Nil) {
  let names = list.map(columns, fn(c) { c.name })
  let dupes = find_duplicates(names)
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

fn find_duplicates(names: List(String)) -> List(String) {
  do_find_duplicates(names, [], [])
}

fn do_find_duplicates(
  remaining: List(String),
  seen: List(String),
  dupes: List(String),
) -> List(String) {
  case remaining {
    [] -> list.reverse(dupes)
    [name, ..rest] ->
      case list.contains(seen, name) {
        True ->
          case list.contains(dupes, name) {
            True -> do_find_duplicates(rest, seen, dupes)
            False -> do_find_duplicates(rest, seen, [name, ..dupes])
          }
        False -> do_find_duplicates(rest, [name, ..seen], dupes)
      }
  }
}

/// Check for semicolons outside of quoted SQL contexts and line comments.
/// Handles single-quoted strings (with '' escapes), double-quoted identifiers,
/// and `-- line comments` (which extend to end of line).
fn contains_semicolon_outside_strings(sql: String) -> Bool {
  do_check_semicolon(sql, False, False, False)
}

fn do_check_semicolon(
  s: String,
  in_single_quote: Bool,
  in_double_quote: Bool,
  in_line_comment: Bool,
) -> Bool {
  case string.pop_grapheme(s) {
    Error(_) -> False
    // Newline ends any active line comment
    Ok(#("\n", rest)) ->
      do_check_semicolon(rest, in_single_quote, in_double_quote, False)
    // Everything else inside a line comment is ignored
    Ok(#(_, rest)) if in_line_comment ->
      do_check_semicolon(rest, in_single_quote, in_double_quote, True)
    Ok(#("'", rest)) ->
      case in_double_quote {
        True -> do_check_semicolon(rest, in_single_quote, True, False)
        False ->
          case in_single_quote {
            True ->
              // Check for escaped quote ''
              case string.pop_grapheme(rest) {
                Ok(#("'", rest2)) ->
                  do_check_semicolon(rest2, True, False, False)
                _ -> do_check_semicolon(rest, False, False, False)
              }
            False -> do_check_semicolon(rest, True, False, False)
          }
      }
    Ok(#("\"", rest)) ->
      case in_single_quote {
        True -> do_check_semicolon(rest, True, in_double_quote, False)
        // No explicit "" escape handling needed: toggling twice is a no-op
        // that leaves in_double_quote in the correct state.
        False -> do_check_semicolon(rest, False, !in_double_quote, False)
      }
    // `--` starts a line comment, but only outside quoted contexts
    Ok(#("-", rest)) ->
      case in_single_quote || in_double_quote {
        True ->
          do_check_semicolon(rest, in_single_quote, in_double_quote, False)
        False ->
          case string.pop_grapheme(rest) {
            Ok(#("-", rest2)) -> do_check_semicolon(rest2, False, False, True)
            _ ->
              do_check_semicolon(rest, in_single_quote, in_double_quote, False)
          }
      }
    Ok(#(";", rest)) ->
      case in_single_quote || in_double_quote {
        True ->
          do_check_semicolon(rest, in_single_quote, in_double_quote, False)
        False -> True
      }
    Ok(#(_, rest)) ->
      do_check_semicolon(rest, in_single_quote, in_double_quote, False)
  }
}

/// Run `gleam format` on generated code. Falls back to the original string
/// if formatting fails (e.g., gleam not on PATH) and prints a warning.
fn format_gleam(code: String) -> String {
  let suffix = int.to_string(int.absolute_value(unique_integer()))
  let tmp_dir = get_tmp_dir()
  let tmp = tmp_dir <> "/marmot_fmt_" <> suffix <> ".gleam"
  case simplifile.write(tmp, code) {
    Error(_) -> {
      io.println_error(
        "warning: Could not write temp file for formatting, skipping gleam format",
      )
      code
    }
    Ok(_) -> {
      run_os_cmd("gleam format " <> tmp)
      let formatted = case simplifile.read(tmp) {
        Ok(result) -> result
        Error(_) -> {
          io.println_error(
            "warning: gleam format failed, using unformatted output",
          )
          code
        }
      }
      let _ = simplifile.delete(tmp)
      formatted
    }
  }
}

fn get_tmp_dir() -> String {
  get_env("TMPDIR")
  |> option.lazy_or(fn() { get_env("TMP") })
  |> option.lazy_or(fn() { get_env("TEMP") })
  |> option.unwrap("/tmp")
}

@external(erlang, "os", "cmd")
fn os_cmd_ffi(command: List(Int)) -> List(Int)

fn run_os_cmd(command: String) -> Nil {
  command
  |> string.to_utf_codepoints
  |> list.map(string.utf_codepoint_to_int)
  |> os_cmd_ffi
  Nil
}

fn halt(code: Int) -> Nil {
  // init:stop/1 performs a graceful shutdown that flushes I/O buffers,
  // unlike erlang:halt/1 which exits immediately and may lose stderr output
  init_stop(code)
  // init:stop is async; wait for graceful shutdown, then force exit
  timer_sleep(5000)
  erlang_halt(code)
}

@external(erlang, "init", "stop")
fn init_stop(code: Int) -> Nil

@external(erlang, "erlang", "halt")
fn erlang_halt(code: Int) -> Nil

@external(erlang, "erlang", "unique_integer")
fn unique_integer() -> Int

@external(erlang, "timer", "sleep")
fn timer_sleep(ms: Int) -> Nil

// Uses os:getenv/0 + linear scan rather than os:getenv/1, because on OTP 27
// the /1 variant requires a charlist argument and crashes with badarg when
// passed a Gleam String (binary).
@external(erlang, "os", "getenv")
fn getenv_list_ffi() -> Dynamic

fn get_env(name: String) -> Option(String) {
  let raw = getenv_list_ffi()
  case decode.run(raw, decode.list(decode.string)) {
    Ok(entries) -> find_env(entries, name <> "=")
    Error(_) -> option.None
  }
}

fn find_env(entries: List(String), prefix: String) -> Option(String) {
  case entries {
    [] -> option.None
    [entry, ..rest] ->
      case string.starts_with(entry, prefix) {
        True -> option.Some(string.drop_start(entry, string.length(prefix)))
        False -> find_env(rest, prefix)
      }
  }
}
