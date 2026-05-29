//// SQLite table metadata loading, PK lookup, rootpage mapping.

import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/int
import gleam/io
import gleam/list
import gleam/result
import gleam/set.{type Set}
import gleam/string
import marmot/internal/query.{type Column, Column, StringType}
import marmot/internal/sqlite/parse
import marmot/internal/sqlite/parse/util
import sqlight.{type Connection}

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
    /// Index columns keyed by index rootpage. Each entry is the list of Columns
    /// that form the index key (key=1 in PRAGMA index_xinfo), in index column order.
    index_columns: Dict(Int, List(Column)),
  )
}

pub fn get_table_metadata_v2(db: Connection) -> TableMetadataV2 {
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

  let index_decoder = {
    use name <- decode.field(0, decode.string)
    use rootpage <- decode.field(1, decode.int)
    use tbl_name <- decode.field(2, decode.string)
    decode.success(#(name, rootpage, tbl_name))
  }
  let indexes = case
    sqlight.query(
      "SELECT name, rootpage, tbl_name FROM sqlite_master WHERE type='index'",
      on: db,
      with: [],
      expecting: index_decoder,
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

  let base =
    list.fold(
      tables,
      TableMetadataV2(
        columns: dict.new(),
        pks: dict.new(),
        rootpages: dict.new(),
        index_columns: dict.new(),
      ),
      fn(acc, table) {
        add_table_metadata_v2(acc, table, db, without_rowid_set)
      },
    )
  let index_columns = build_index_columns_v2(db, indexes, base.columns)
  let with_index_rp = add_index_rootpages_v2(base, indexes)
  TableMetadataV2(
    columns: with_index_rp.columns,
    pks: with_index_rp.pks,
    rootpages: with_index_rp.rootpages,
    index_columns: index_columns,
  )
}

fn add_table_metadata_v2(
  acc: TableMetadataV2,
  table: #(String, Int),
  db: Connection,
  without_rowid_set: Set(String),
) -> TableMetadataV2 {
  let #(table_name, rootpage) = table
  let rootpages = dict.insert(acc.rootpages, rootpage, table_name)
  let is_without_rowid = set.contains(without_rowid_set, table_name)

  case query_table_xinfo(db, table_name) {
    Ok(rows) -> {
      let pk_count = list.count(rows, table_xinfo_row_has_pk)
      let metadatas =
        list.map(rows, fn(row) {
          metadata_from_xinfo_row(row, pk_count, is_without_rowid)
        })
      let columns = dict.insert(acc.columns, table_name, metadatas)
      let pks = add_table_xinfo_pk(acc.pks, table_name, rows)
      TableMetadataV2(
        columns: columns,
        pks: pks,
        rootpages: rootpages,
        index_columns: acc.index_columns,
      )
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
        index_columns: acc.index_columns,
      )
    }
  }
}

fn query_table_xinfo(
  db: Connection,
  table_name: String,
) -> Result(List(#(String, String, Int, Int, Int)), sqlight.Error) {
  let xinfo_decoder = {
    use col_name <- decode.field(1, decode.string)
    use type_str <- decode.field(2, decode.string)
    use notnull <- decode.field(3, decode.int)
    // field 4 (dflt_value) intentionally skipped - not used here
    use pk <- decode.field(5, decode.int)
    use hidden <- decode.field(6, decode.int)
    decode.success(#(col_name, type_str, notnull, pk, hidden))
  }

  let pragma_sql =
    "PRAGMA table_xinfo(\"" <> parse.quote_identifier(table_name) <> "\")"
  sqlight.query(pragma_sql, on: db, with: [], expecting: xinfo_decoder)
}

fn metadata_from_xinfo_row(
  row: #(String, String, Int, Int, Int),
  pk_count: Int,
  is_without_rowid: Bool,
) -> ColumnMetadata {
  let #(col_name, type_str, notnull, pk, hidden) = row
  let column_type = case query.parse_sqlite_type(type_str) {
    Ok(t) -> t
    Error(_) -> StringType
  }
  let is_rowid_alias =
    pk > 0
    && pk_count == 1
    && string.uppercase(type_str) == "INTEGER"
    && !is_without_rowid
  let nullable = !is_rowid_alias && notnull == 0
  ColumnMetadata(
    column: Column(name: col_name, column_type: column_type, nullable: nullable),
    is_rowid_alias: is_rowid_alias,
    is_generated: hidden == 2 || hidden == 3,
    hidden: hidden,
  )
}

fn add_table_xinfo_pk(
  pks: Dict(String, String),
  table_name: String,
  rows: List(#(String, String, Int, Int, Int)),
) -> Dict(String, String) {
  case list.find(rows, table_xinfo_row_has_pk) {
    Ok(#(pk_name, _, _, _, _)) -> dict.insert(pks, table_name, pk_name)
    Error(_) -> pks
  }
}

fn table_xinfo_row_has_pk(row: #(String, String, Int, Int, Int)) -> Bool {
  row.3 > 0
}

fn add_index_rootpages_v2(
  acc: TableMetadataV2,
  indexes: List(#(String, Int, String)),
) -> TableMetadataV2 {
  let rootpages =
    list.fold(indexes, acc.rootpages, fn(rp, entry) {
      let #(_, rootpage, tbl_name) = entry
      dict.insert(rp, rootpage, tbl_name)
    })
  TableMetadataV2(
    columns: acc.columns,
    pks: acc.pks,
    rootpages: rootpages,
    index_columns: acc.index_columns,
  )
}

fn build_index_columns_v2(
  db: Connection,
  indexes: List(#(String, Int, String)),
  table_columns: Dict(String, List(ColumnMetadata)),
) -> Dict(Int, List(Column)) {
  list.fold(indexes, dict.new(), fn(acc, entry) {
    let #(index_name, rootpage, tbl_name) = entry
    case dict.get(table_columns, tbl_name) {
      Error(_) -> acc
      Ok(cols) -> {
        let pragma_sql =
          "PRAGMA index_xinfo(\"" <> parse.quote_identifier(index_name) <> "\")"
        let xinfo_decoder = {
          use seqno <- decode.field(0, decode.int)
          use cid <- decode.field(1, decode.int)
          use key <- decode.field(5, decode.int)
          decode.success(#(seqno, cid, key))
        }
        case
          sqlight.query(pragma_sql, on: db, with: [], expecting: xinfo_decoder)
        {
          Error(_) -> acc
          Ok(rows) -> {
            let key_cols =
              rows
              |> list.filter(fn(row) { row.2 == 1 })
              |> list.sort(fn(a, b) { int.compare(a.0, b.0) })
            let indexed_columns =
              list.filter_map(key_cols, fn(row) {
                let #(_, cid, _) = row
                util.list_at(cols, cid)
                |> result.map(fn(cm) { cm.column })
              })
            dict.insert(acc, rootpage, indexed_columns)
          }
        }
      }
    }
  })
}
