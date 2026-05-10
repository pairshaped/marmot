import gleam/list
import marmot/internal/error.{type MarmotError}
import marmot/internal/sqlite
import simplifile
import sqlight

type Fixture {
  Fixture(
    file: String,
    schema_setup: String,
    expected_predicate: fn(MarmotError) -> Bool,
    description: String,
  )
}

fn fixtures() -> List(Fixture) {
  [
    Fixture(
      file: "test/fixtures/sql_failures/value_count_mismatch.sql",
      schema_setup: "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER);",
      expected_predicate: fn(err) {
        case err {
          error.InsertValuesCountMismatch(_, _, _, _) -> True
          _ -> False
        }
      },
      description: "INSERT VALUES with 1 expression against a 3-column table",
    ),
  ]
}

pub fn sql_failure_fixtures_test() {
  list.each(fixtures(), fn(fixture) {
    let assert Ok(sql) = simplifile.read(fixture.file)
    let assert Ok(conn) = sqlight.open(":memory:")
    let assert Ok(_) = sqlight.exec(fixture.schema_setup, conn)
    let result = sqlite.introspect_query(conn, fixture.file, sql)
    case result {
      Error(err) ->
        case fixture.expected_predicate(err) {
          True -> Nil
          False ->
            panic as {
              "Fixture "
              <> fixture.file
              <> " ("
              <> fixture.description
              <> ") produced unexpected MarmotError variant"
            }
        }
      Ok(_) ->
        panic as {
          "Fixture "
          <> fixture.file
          <> " ("
          <> fixture.description
          <> ") was expected to fail but introspect_query returned Ok"
        }
    }
  })
}
