//// SQLite table metadata loading, PK lookup, rootpage mapping.

import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/io
import gleam/list
import gleam/set
import gleam/string
import marmot/internal/query.{type Column, Column, StringType}
import marmot/internal/sqlite/parse
import sqlight.{type Connection}

/// Get all table metadata in a single pass: schemas, primary keys, and rootpage
/// mappings.
pub fn get_table_metadata(
  db: Connection,
) -> #(Dict(String, List(Column)), Dict(String, String), Dict(Int, String)) {
  let master_decoder = {
    use name <- decode.field(0, decode.string)
    use rootpage <- decode.field(1, decode.int)
    decode.success(#(name, rootpage))
  }

  let tables = case
    sqlight.query(
      "SELECT name, rootpage FROM sqlite_master WHERE type='table'",
      on: db,
      with: [],
      expecting: master_decoder,
    )
  {
    Ok(rows) -> rows
    Error(err) -> {
      io.println_error(
        "warning: Could not read table metadata: " <> err.message,
      )
      []
    }
  }

  let index_parent_decoder = {
    use rootpage <- decode.field(0, decode.int)
    use tbl_name <- decode.field(1, decode.string)
    decode.success(#(rootpage, tbl_name))
  }
  let indexes = case
    sqlight.query(
      "SELECT rootpage, tbl_name FROM sqlite_master WHERE type='index'",
      on: db,
      with: [],
      expecting: index_parent_decoder,
    )
  {
    Ok(rows) -> rows
    Error(_) -> []
  }

  let pragma_decoder = {
    use col_name <- decode.field(1, decode.string)
    use type_str <- decode.field(2, decode.string)
    use notnull <- decode.field(3, decode.int)
    use pk <- decode.field(5, decode.int)
    decode.success(#(col_name, type_str, notnull, pk))
  }

  list.fold(tables, #(dict.new(), dict.new(), dict.new()), fn(acc, table) {
    let #(schemas, pks, rootpages) = acc
    let #(table_name, rootpage) = table
    let rootpages = dict.insert(rootpages, rootpage, table_name)

    let pragma_sql =
      "PRAGMA table_info(\"" <> parse.quote_identifier(table_name) <> "\")"
    case
      sqlight.query(pragma_sql, on: db, with: [], expecting: pragma_decoder)
    {
      Ok(rows) -> {
        let columns =
          list.map(rows, fn(row) {
            let #(col_name, type_str, notnull, pk) = row
            let column_type = case query.parse_sqlite_type(type_str) {
              Ok(t) -> t
              Error(_) -> StringType
            }
            // SQLite quirk: INTEGER PRIMARY KEY columns have notnull=0 in
            // PRAGMA output because they're rowid aliases (which auto-assign
            // on INSERT), but they're always NOT NULL at read time. Force
            // nullable=False for single-column integer primary keys.
            let nullable = case pk > 0, column_type {
              True, query.IntType -> False
              _, _ -> notnull == 0
            }
            Column(name: col_name, column_type: column_type, nullable: nullable)
          })
        let schemas = dict.insert(schemas, table_name, columns)
        let pks = case list.find(rows, fn(row) { row.3 > 0 }) {
          Ok(#(pk_name, _, _, _)) -> dict.insert(pks, table_name, pk_name)
          Error(_) -> pks
        }
        #(schemas, pks, rootpages)
      }
      Error(err) -> {
        io.println_error(
          "warning: Could not read schema for table "
          <> table_name
          <> ": "
          <> err.message,
        )
        #(schemas, pks, rootpages)
      }
    }
  })
  |> add_index_rootpages(indexes)
}

fn add_index_rootpages(
  acc: #(Dict(String, List(Column)), Dict(String, String), Dict(Int, String)),
  indexes: List(#(Int, String)),
) -> #(Dict(String, List(Column)), Dict(String, String), Dict(Int, String)) {
  let #(schemas, pks, rootpages) = acc
  let rootpages =
    list.fold(indexes, rootpages, fn(acc, entry) {
      let #(rootpage, tbl_name) = entry
      dict.insert(acc, rootpage, tbl_name)
    })
  #(schemas, pks, rootpages)
}

pub type ColumnMetadata {
  ColumnMetadata(
    column: Column,
    is_rowid_alias: Bool,
    is_generated: Bool,
    // Raw flag from PRAGMA table_xinfo: 0=normal, 1=hidden, 2=virtual generated, 3=stored generated
    hidden: Int,
  )
}

pub type TableMetadataV2 {
  TableMetadataV2(
    columns: Dict(String, List(ColumnMetadata)),
    pks: Dict(String, String),
    rootpages: Dict(Int, String),
  )
}

pub fn get_table_metadata_v2(db: Connection) -> TableMetadataV2 {
  let xinfo_decoder = {
    use col_name <- decode.field(1, decode.string)
    use type_str <- decode.field(2, decode.string)
    use notnull <- decode.field(3, decode.int)
    // field 4 (dflt_value) intentionally skipped - not used here
    use pk <- decode.field(5, decode.int)
    use hidden <- decode.field(6, decode.int)
    decode.success(#(col_name, type_str, notnull, pk, hidden))
  }

  let master_decoder = {
    use name <- decode.field(0, decode.string)
    use rootpage <- decode.field(1, decode.int)
    decode.success(#(name, rootpage))
  }
  let tables = case
    sqlight.query(
      "SELECT name, rootpage FROM sqlite_master WHERE type='table'",
      on: db,
      with: [],
      expecting: master_decoder,
    )
  {
    Ok(rows) -> rows
    Error(err) -> {
      io.println_error(
        "warning: Could not read table metadata: " <> err.message,
      )
      []
    }
  }

  let index_parent_decoder = {
    use rootpage <- decode.field(0, decode.int)
    use tbl_name <- decode.field(1, decode.string)
    decode.success(#(rootpage, tbl_name))
  }
  let indexes = case
    sqlight.query(
      "SELECT rootpage, tbl_name FROM sqlite_master WHERE type='index'",
      on: db,
      with: [],
      expecting: index_parent_decoder,
    )
  {
    Ok(rows) -> rows
    Error(_) -> []
  }

  // Build a set of WITHOUT ROWID table names via pragma_table_list.
  // Field 0 = name, field 1 = wr (1 if WITHOUT ROWID, 0 otherwise).
  let table_list_decoder = {
    use name <- decode.field(0, decode.string)
    use wr <- decode.field(1, decode.int)
    decode.success(#(name, wr))
  }
  let without_rowid_set = case
    sqlight.query(
      "SELECT name, wr FROM pragma_table_list WHERE type='table'",
      on: db,
      with: [],
      expecting: table_list_decoder,
    )
  {
    Ok(rows) ->
      rows
      |> list.filter_map(fn(row) {
        let #(name, wr) = row
        case wr == 1 {
          True -> Ok(name)
          False -> Error(Nil)
        }
      })
      |> set.from_list
    Error(_) -> set.new()
  }

  list.fold(
    tables,
    TableMetadataV2(columns: dict.new(), pks: dict.new(), rootpages: dict.new()),
    fn(acc, table) {
      let #(table_name, rootpage) = table
      let rootpages = dict.insert(acc.rootpages, rootpage, table_name)
      let is_without_rowid = set.contains(without_rowid_set, table_name)
      let pragma_sql =
        "PRAGMA table_xinfo(\"" <> parse.quote_identifier(table_name) <> "\")"
      case
        sqlight.query(pragma_sql, on: db, with: [], expecting: xinfo_decoder)
      {
        Ok(rows) -> {
          // Count how many columns are part of the primary key.
          let pk_count = list.count(rows, fn(row) { row.3 > 0 })
          let metadatas =
            list.map(rows, fn(row) {
              let #(col_name, type_str, notnull, pk, hidden) = row
              let column_type = case query.parse_sqlite_type(type_str) {
                Ok(t) -> t
                Error(_) -> StringType
              }
              // A column is a rowid alias when it is INTEGER PRIMARY KEY on a
              // normal (rowid) table with a single-column PK. WITHOUT ROWID
              // tables expose the same declaration without the auto-assign
              // behavior, so they are not aliases.
              //
              // Note: INTEGER PRIMARY KEY DESC is technically NOT a rowid alias
              // per SQLite, but PRAGMA table_xinfo does not expose sort
              // direction. We treat it as a rowid alias here. Detecting DESC
              // requires parsing the CREATE TABLE statement from sqlite_master,
              // which is out of scope.
              let is_rowid_alias =
                pk > 0
                && pk_count == 1
                && string.uppercase(type_str) == "INTEGER"
                && !is_without_rowid
              // Rowid alias columns are always non-null at read time even though
              // PRAGMA reports notnull=0 (they auto-assign on INSERT). WITHOUT
              // ROWID PK columns and explicitly NOT NULL columns are handled
              // correctly by PRAGMA (notnull=1), so no override is needed for
              // those. Composite PK columns on ordinary rowid tables are NOT
              // forced non-null: SQLite allows NULLs there as a legacy quirk.
              let pk_implies_non_null = is_rowid_alias
              let nullable = !pk_implies_non_null && notnull == 0
              ColumnMetadata(
                column: Column(
                  name: col_name,
                  column_type: column_type,
                  nullable: nullable,
                ),
                is_rowid_alias: is_rowid_alias,
                is_generated: hidden == 2 || hidden == 3,
                hidden: hidden,
              )
            })
          let columns = dict.insert(acc.columns, table_name, metadatas)
          let pks = case
            list.find(rows, fn(row) {
              let #(_, _, _, pk, _) = row
              pk > 0
            })
          {
            Ok(#(pk_name, _, _, _, _)) ->
              dict.insert(acc.pks, table_name, pk_name)
            Error(_) -> acc.pks
          }
          TableMetadataV2(columns: columns, pks: pks, rootpages: rootpages)
        }
        Error(err) -> {
          io.println_error(
            "warning: Could not read schema for table "
            <> table_name
            <> ": "
            <> err.message,
          )
          TableMetadataV2(
            columns: acc.columns,
            pks: acc.pks,
            rootpages: rootpages,
          )
        }
      }
    },
  )
  |> add_index_rootpages_v2(indexes)
}

fn add_index_rootpages_v2(
  acc: TableMetadataV2,
  indexes: List(#(Int, String)),
) -> TableMetadataV2 {
  let rootpages =
    list.fold(indexes, acc.rootpages, fn(rp, entry) {
      let #(rootpage, tbl_name) = entry
      dict.insert(rp, rootpage, tbl_name)
    })
  TableMetadataV2(columns: acc.columns, pks: acc.pks, rootpages: rootpages)
}
