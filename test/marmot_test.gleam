import argv
import birdie
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import marmot
import marmot/internal/codegen
import marmot/internal/query
import marmot/internal/sqlite
import simplifile
import sqlight

@external(erlang, "marmot_test_ffi", "rescue")
fn rescue(body: fn() -> Nil) -> Result(Nil, String)

pub fn main() -> Nil {
  let args = argv.load().arguments
  let suite = parse_suite(args)
  let workers = parse_workers(args)
  let options = [
    Verbose,
    NoTty,
    Report(#(GleeunitProgress, [Colored(True)])),
    ScaleTimeouts(10),
    Parallel(workers),
  ]

  let result =
    find_files(matching: "**/*.{erl,gleam}", in: "test")
    |> list.filter(include_file(_, suite))
    |> list.map(gleam_to_erlang_module_name)
    |> list.map(dangerously_convert_string_to_atom(_, Utf8))
    |> run_eunit(options)

  let code = case result {
    Ok(_) -> 0
    Error(_) -> 1
  }
  halt(code)
}

type Suite {
  Fast
  Slow
  All
}

const slow_test_file = "marmot/e2e_slow_test.gleam"

fn parse_suite(args: List(String)) -> Suite {
  case args {
    ["--suite", "fast", ..] -> Fast
    ["--suite", "slow", ..] -> Slow
    ["--suite", "all", ..] -> All
    ["--suite", value, ..] -> {
      io.println_error(
        "Unknown test suite " <> value <> ". Expected fast, slow, or all.",
      )
      halt(1)
      All
    }
    [_, ..rest] -> parse_suite(rest)
    [] -> All
  }
}

fn parse_workers(args: List(String)) -> Int {
  case args {
    ["--workers", value, ..] ->
      case int.parse(value) {
        Ok(n) ->
          case n > 0 {
            True -> n
            False -> 1
          }
        Error(_) -> 1
      }
    [_, ..rest] -> parse_workers(rest)
    [] -> 1
  }
}

fn include_file(file: String, suite: Suite) -> Bool {
  case suite {
    Fast -> file != slow_test_file
    Slow -> file == slow_test_file
    All -> True
  }
}

fn gleam_to_erlang_module_name(path: String) -> String {
  case string.ends_with(path, ".gleam") {
    True ->
      path
      |> string.replace(".gleam", "")
      |> string.replace("/", "@")

    False ->
      path
      |> string.split("/")
      |> list.last
      |> result.unwrap(path)
      |> string.replace(".erl", "")
  }
}

@external(erlang, "gleeunit_ffi", "find_files")
fn find_files(matching matching: String, in in: String) -> List(String)

type Atom

type Encoding {
  Utf8
}

@external(erlang, "erlang", "binary_to_atom")
fn dangerously_convert_string_to_atom(a: String, b: Encoding) -> Atom

type ReportModuleName {
  GleeunitProgress
}

type GleeunitProgressOption {
  Colored(Bool)
}

type EunitOption {
  Verbose
  NoTty
  Report(#(ReportModuleName, List(GleeunitProgressOption)))
  ScaleTimeouts(Int)
  Parallel(Int)
}

@external(erlang, "gleeunit_ffi", "run_eunit")
fn run_eunit(a: List(Atom), b: List(EunitOption)) -> Result(Nil, a)

@external(erlang, "erlang", "halt")
fn halt(code: Int) -> Nil

pub fn end_to_end_shared_row_types_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orgs (id INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
      on: db,
    )

  let sql_1 = "-- returns: OrgRow\nSELECT id, name FROM orgs WHERE id = @id"
  let sql_2 = "-- returns: OrgRow\nSELECT id, name FROM orgs"
  let sql_3 = "SELECT id FROM orgs"

  let assert Ok(info_1) = sqlite.introspect_query(db, sql_1)
  let assert Ok(info_2) = sqlite.introspect_query(db, sql_2)
  let assert Ok(info_3) = sqlite.introspect_query(db, sql_3)

  let assert Ok(option.Some("OrgRow")) = sqlite.parse_returns_annotation(sql_1)
  let assert Ok(option.Some("OrgRow")) = sqlite.parse_returns_annotation(sql_2)
  let assert Ok(option.None) = sqlite.parse_returns_annotation(sql_3)

  let queries = [
    query.Query(
      name: "get_org",
      sql: sql_1,
      path: "a.sql",
      parameters: info_1.parameters,
      columns: info_1.columns,
      custom_type_name: option.Some("OrgRow"),
    ),
    query.Query(
      name: "list_orgs",
      sql: sql_2,
      path: "b.sql",
      parameters: info_2.parameters,
      columns: info_2.columns,
      custom_type_name: option.Some("OrgRow"),
    ),
    query.Query(
      name: "count_orgs",
      sql: sql_3,
      path: "c.sql",
      parameters: info_3.parameters,
      columns: info_3.columns,
      custom_type_name: option.None,
    ),
  ]

  let assert Ok(output) =
    codegen.generate_module_with_config(queries, option.None)

  // Shared type emitted exactly once.
  let assert 1 = count_substring(output, "pub type OrgRow {")
  // Shared decoder emitted exactly once.
  let assert 1 = count_substring(output, "fn org_row_decoder()")
  // Unannotated query keeps its per-query type.
  let assert 1 = count_substring(output, "pub type CountOrgsRow {")
  // Annotated queries return the shared type.
  let assert True =
    string.contains(output, "Result(List(OrgRow), sqlight.Error)")
  // Annotated query functions reference the shared decoder.
  let assert True = string.contains(output, "expecting: org_row_decoder()")
}

fn count_substring(haystack: String, needle: String) -> Int {
  string.split(haystack, needle)
  |> list.length
  |> int.subtract(1)
}

pub fn snapshot_shared_row_types_output_test() {
  use db <- sqlight.with_connection(":memory:")
  let assert Ok(_) =
    sqlight.exec(
      "CREATE TABLE orgs (
        id INTEGER NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        bio TEXT
      )",
      on: db,
    )

  let sql_1 =
    "-- returns: OrgRow\nSELECT id, name, bio FROM orgs WHERE id = @id"
  let sql_2 = "-- returns: OrgRow\nSELECT id, name, bio FROM orgs"

  let assert Ok(info_1) = sqlite.introspect_query(db, sql_1)
  let assert Ok(info_2) = sqlite.introspect_query(db, sql_2)

  let queries = [
    query.Query(
      name: "get_org",
      sql: sql_1,
      path: "a.sql",
      parameters: info_1.parameters,
      columns: info_1.columns,
      custom_type_name: option.Some("OrgRow"),
    ),
    query.Query(
      name: "list_orgs",
      sql: sql_2,
      path: "b.sql",
      parameters: info_2.parameters,
      columns: info_2.columns,
      custom_type_name: option.Some("OrgRow"),
    ),
  ]

  let assert Ok(output) =
    codegen.generate_module_with_config(queries, option.None)
  output
  |> birdie.snap(title: "shared_row_types_two_queries")
}

// ---- marmot module unit tests ----

pub fn validate_sql_empty_test() {
  let assert Error(Nil) = marmot.validate_sql("", "test.sql")
}

pub fn validate_sql_valid_test() {
  let assert Ok("SELECT 1") = marmot.validate_sql("SELECT 1", "test.sql")
}

pub fn validate_sql_trailing_semicolon_test() {
  let assert Ok("SELECT 1") = marmot.validate_sql("SELECT 1;", "test.sql")
}

pub fn validate_sql_multiple_statements_test() {
  let assert Error(Nil) = marmot.validate_sql("SELECT 1; SELECT 2", "test.sql")
}

pub fn validate_sql_semicolon_in_string_test() {
  // Semicolons inside string literals should not trigger the multiple-queries check
  let assert Ok("SELECT 'hello;world'") =
    marmot.validate_sql("SELECT 'hello;world'", "test.sql")
}

pub fn validate_sql_string_literal_semicolon_with_trailing_semicolon_test() {
  let assert Ok("SELECT ';'") = marmot.validate_sql("SELECT ';';", "test.sql")
}

pub fn validate_sql_semicolon_in_comment_test() {
  let assert Ok("SELECT 1 -- comment; still a comment") =
    marmot.validate_sql("SELECT 1 -- comment; still a comment", "test.sql")
}

pub fn validate_sql_trailing_semicolon_with_line_comment_test() {
  let assert Ok("SELECT 1") =
    marmot.validate_sql("SELECT 1; -- comment", "test.sql")
}

pub fn validate_sql_trailing_semicolon_with_block_comment_test() {
  let assert Ok("SELECT 1") =
    marmot.validate_sql("SELECT 1; /* comment */", "test.sql")
}

pub fn validate_sql_preserves_leading_annotations_test() {
  let assert Ok("-- returns: Foo\nSELECT 1") =
    marmot.validate_sql("-- returns: Foo\nSELECT 1; -- comment", "test.sql")
}

pub fn validate_sql_preserves_annotations_trailing_comment_line_test() {
  let assert Ok("-- returns: Foo\nSELECT 1") =
    marmot.validate_sql("-- returns: Foo\nSELECT 1;\n-- comment", "test.sql")
}

pub fn validate_sql_comment_only_line_test() {
  let assert Error(Nil) = marmot.validate_sql("-- only comment", "test.sql")
}

pub fn validate_sql_comment_only_block_test() {
  let assert Error(Nil) = marmot.validate_sql("/* only comment */", "test.sql")
}

pub fn contains_semicolon_outside_strings_none_test() {
  let assert False = marmot.contains_semicolon_outside_strings("SELECT 1")
}

pub fn contains_semicolon_outside_strings_simple_test() {
  let assert True =
    marmot.contains_semicolon_outside_strings("SELECT 1; SELECT 2")
}

pub fn contains_semicolon_outside_strings_in_string_test() {
  let assert False =
    marmot.contains_semicolon_outside_strings("SELECT 'hello;world'")
}

pub fn contains_semicolon_outside_strings_in_double_quoted_test() {
  let assert False =
    marmot.contains_semicolon_outside_strings("SELECT \"hello;world\"")
}

pub fn contains_semicolon_outside_strings_in_line_comment_test() {
  let assert False =
    marmot.contains_semicolon_outside_strings("SELECT 1 -- a; comment")
}

pub fn contains_semicolon_outside_strings_in_block_comment_test() {
  let assert False =
    marmot.contains_semicolon_outside_strings("SELECT /* a; comment */ 1")
}

pub fn check_duplicate_columns_no_duplicates_test() {
  let cols = [
    query.Column(name: "id", column_type: query.IntType, nullable: False),
    query.Column(name: "name", column_type: query.StringType, nullable: True),
  ]
  let assert Ok(Nil) = marmot.check_duplicate_columns(cols, "test.sql")
}

pub fn check_duplicate_columns_has_duplicates_test() {
  let cols = [
    query.Column(name: "id", column_type: query.IntType, nullable: False),
    query.Column(name: "name", column_type: query.StringType, nullable: True),
    query.Column(name: "id", column_type: query.IntType, nullable: False),
  ]
  let assert Error(Nil) = marmot.check_duplicate_columns(cols, "test.sql")
}

pub fn check_duplicate_columns_empty_test() {
  let assert Ok(Nil) = marmot.check_duplicate_columns([], "test.sql")
}

pub fn check_duplicate_columns_single_test() {
  let cols = [
    query.Column(name: "id", column_type: query.IntType, nullable: False),
  ]
  let assert Ok(Nil) = marmot.check_duplicate_columns(cols, "test.sql")
}

pub fn check_generated_column_names_collide_test() {
  let cols = [
    query.Column(name: "foo-bar", column_type: query.StringType, nullable: False),
    query.Column(name: "foo_bar", column_type: query.StringType, nullable: False),
  ]

  let assert Error(Nil) =
    marmot.check_generated_column_names(cols, "test.sql")
}

pub fn check_generated_column_names_no_collision_test() {
  let cols = [
    query.Column(name: "foo-bar", column_type: query.StringType, nullable: False),
    query.Column(name: "bar_baz", column_type: query.StringType, nullable: False),
  ]

  let assert Ok(Nil) =
    marmot.check_generated_column_names(cols, "test.sql")
}

// ---- FFI failure paths ----

pub fn run_executable_not_found_test() {
  let assert -1 = marmot.run_executable("zzz_nonexistent_binary_xyz", [])
}

pub fn run_executable_success_test() {
  // gleam --version should always work in the dev environment
  let assert 0 = marmot.run_executable("gleam", ["--version"])
}

pub fn make_tmp_file_success_test() {
  let result = marmot.make_tmp_file("/tmp", "test content")
  case result {
    Ok(path) -> {
      let assert Ok(True) = simplifile.is_file(path)
      let _ = simplifile.delete(path)
    }
    Error(_) -> panic as "expected Ok, got Error"
  }
}

pub fn make_tmp_file_error_test() {
  let result = marmot.make_tmp_file("/nonexistent_dir_xyz_test", "test")
  case result {
    Ok(_) -> panic as "expected Error, got Ok"
    Error(_) -> Nil
  }
}

pub fn format_gleam_happy_path_test() {
  let input = "pub fn main() {\n  1 + 1\n}\n"
  let output = marmot.format_gleam(input)
  // Output should be valid Gleam: gleam format on it should not change it
  let result = marmot.make_tmp_file("/tmp", output)
  case result {
    Ok(path) -> {
      let exit_code =
        marmot.run_executable("gleam", ["format", "--check", path])
      let _ = simplifile.delete(path)
      let assert 0 = exit_code
    }
    Error(_) -> panic as "could not create temp file"
  }
}

pub fn format_gleam_not_found_falls_back_test() {
  let input = "pub fn main(){1 + 1}"
  let assert "pub fn main(){1 + 1}" =
    marmot.format_gleam_after_run(input, "/tmp/missing", -1)
}

pub fn format_gleam_timeout_falls_back_test() {
  let input = "pub fn main(){1 + 1}"
  let assert "pub fn main(){1 + 1}" =
    marmot.format_gleam_after_run(input, "/tmp/missing", -2)
}

pub fn format_gleam_nonzero_exit_falls_back_test() {
  let input = "pub fn main(){1 + 1}"
  let assert "pub fn main(){1 + 1}" =
    marmot.format_gleam_after_run(input, "/tmp/missing", 2)
}

pub fn ensure_parent_dir_file_blocks_directory_test() {
  let base = "test_parent_dir_blocked"
  let result =
    rescue(fn() {
      let assert Ok(_) = simplifile.write(base, "blocking file")
      let assert Error(Nil) = marmot.ensure_parent_dir(base <> "/out.gleam")
      Nil
    })
  let _ = simplifile.delete(base)
  case result {
    Ok(Nil) -> Nil
    Error(msg) -> panic as msg
  }
}
