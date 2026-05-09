//// EXPLAIN opcode analysis: type definitions, cursor-to-table mapping, join
//// nullability computation, autoindex tracing, and parameter type resolution.
////
//// Opcodes are the decoded rows from SQLite's `EXPLAIN` output. This module turns
//// them into structural facts: which cursors map to which tables, which columns
//// may be NULL due to outer joins, which autoindex cursors shadow real tables,
//// and what type each `?` parameter should have based on comparison context.
////
//// What lives elsewhere: the EXPLAIN query itself -> sqlite.gleam; result column
//// extraction -> results.gleam; SQL text parsing -> parse/*.gleam.

import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{type Option}
import marmot/internal/query.{type Column, type Parameter, Column, Parameter}
import marmot/internal/sqlite/parse/util

/// Opcode from EXPLAIN output
pub type Opcode {
  Opcode(
    addr: Int,
    opcode: String,
    p1: Int,
    p2: Int,
    p3: Int,
    p4: String,
    p5: Int,
  )
}

/// Nullable-cursor set + the set of tables those cursors read from.
/// We track both because text-based column resolution (resolve_select_item)
/// goes by table name, while opcode-based resolution goes by cursor id.
pub type JoinNullability {
  JoinNullability(
    nullable_cursors: Dict(Int, Nil),
    nullable_tables: Dict(String, Nil),
  )
}

/// Find cursor IDs that may produce NULL rows due to LEFT JOIN semantics.
/// SQLite emits `NullRow` on cursor P1 when an outer join has no matching
/// inner row. After this, any `Column` read from that cursor returns NULL.
/// `IfNullRow` tests this state. Any cursor that is the target of either
/// opcode is a "nullable cursor": columns resolved against it must be
/// marked nullable in the generated type.
fn find_nullable_cursors(opcodes: List(Opcode)) -> Dict(Int, Nil) {
  list.fold(opcodes, dict.new(), fn(acc, op) {
    case op.opcode {
      "NullRow" | "IfNullRow" -> dict.insert(acc, op.p1, Nil)
      _ -> acc
    }
  })
}

/// Build a mapping from each autoindex/sorter cursor to its source table cursor.
/// SQLite builds transient autoindexes when no suitable index exists for a JOIN
/// (e.g., LEFT JOIN on an unindexed TEXT column). The pattern: `OpenAutoindex
/// p1=auto_cursor` followed by a loop that reads from the source cursor via
/// Column/Rowid opcodes and inserts into the autoindex via IdxInsert.
///
/// This mapping is load-bearing for nullability: when a LEFT JOIN has no match,
/// SQLite targets the autoindex cursor with NullRow (not the original table
/// cursor). Without propagating nullable-cursor flags through this mapping,
/// columns from the right side of an unindexed LEFT JOIN would incorrectly
/// appear non-nullable. See `compute_join_nullability` below and the test
/// "left join on unindexed column marks right side nullable" in sqlite_test.
///
/// By scanning for `IdxInsert p1=auto_cursor` and looking back at the nearest
/// preceding `Column` or `Rowid` opcode that reads from a real-table cursor,
/// we can map auto_cursor -> source_cursor (and thus -> table_name).
fn build_autoindex_source(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
) -> Dict(Int, Int) {
  let autoindex_cursors =
    list.filter_map(opcodes, fn(op) {
      case op.opcode {
        "OpenAutoindex" -> Ok(#(op.p1, op.addr))
        _ -> Error(Nil)
      }
    })

  list.fold(autoindex_cursors, dict.new(), fn(acc, entry) {
    let #(auto_cursor, open_addr) = entry
    let idx_insert =
      list.find(opcodes, fn(op) {
        op.opcode == "IdxInsert" && op.p1 == auto_cursor && op.addr > open_addr
      })
    case idx_insert {
      Error(_) -> acc
      Ok(insert_op) -> {
        let source =
          list.find_map(
            list.reverse(
              list.filter(opcodes, fn(op) {
                op.addr < insert_op.addr && op.addr > open_addr
              }),
            ),
            fn(op) {
              case op.opcode {
                "Column" | "Rowid" | "IdxRowid" ->
                  case dict.has_key(cursor_table, op.p1) {
                    True -> Ok(op.p1)
                    False -> Error(Nil)
                  }
                _ -> Error(Nil)
              }
            },
          )
        case source {
          Ok(src_cursor) -> dict.insert(acc, auto_cursor, src_cursor)
          Error(_) -> acc
        }
      }
    }
  })
}

/// Compute join nullability info from opcodes and cursor-to-table mapping.
pub fn compute_join_nullability(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
) -> JoinNullability {
  let nullable_cursors = find_nullable_cursors(opcodes)
  let autoindex_source = build_autoindex_source(opcodes, cursor_table)
  let nullable_tables =
    dict.fold(nullable_cursors, dict.new(), fn(acc, cursor_id, _) {
      case dict.get(cursor_table, cursor_id) {
        Ok(table_name) -> dict.insert(acc, table_name, Nil)
        Error(_) ->
          case dict.get(autoindex_source, cursor_id) {
            Ok(source_cursor) ->
              case dict.get(cursor_table, source_cursor) {
                Ok(table_name) -> dict.insert(acc, table_name, Nil)
                Error(_) -> acc
              }
            Error(_) -> acc
          }
      }
    })
  JoinNullability(
    nullable_cursors: nullable_cursors,
    nullable_tables: nullable_tables,
  )
}

/// If the Column opcode that produced this result register reads from a
/// nullable cursor (LEFT-JOIN right side), mark the column nullable. The
/// `Column` opcode format is `Column P1=cursor P2=col_idx P3=dest_reg`.
pub fn apply_cursor_nullability(
  base: Result(Column, Nil),
  dest_reg: Int,
  opcodes: List(Opcode),
  join_nullability: JoinNullability,
) -> Result(Column, Nil) {
  case base {
    Error(_) -> Error(Nil)
    Ok(col) -> {
      let producer =
        list.find(opcodes, fn(op) { op.opcode == "Column" && op.p3 == dest_reg })
      case producer {
        Ok(op) ->
          case dict.has_key(join_nullability.nullable_cursors, op.p1) {
            True -> Ok(Column(..col, nullable: True))
            False -> Ok(col)
          }
        Error(_) -> Ok(col)
      }
    }
  }
}

/// Find the Column/Rowid opcode that writes to a given register and resolve type
pub fn find_column_for_register(
  reg: Int,
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
) -> Result(Column, Nil) {
  // Rowid-like opcodes write the rowid (PK) of cursor p1 into register p2.
  //   Rowid       - direct table cursor
  //   IdxRowid    - index cursor (resolves via parent table)
  //   SeekRowid   - seeks by rowid, writes result to p2
  let rowid_op =
    list.find(opcodes, fn(op) {
      case op.opcode {
        "Rowid" | "IdxRowid" | "SeekRowid" -> op.p2 == reg
        _ -> False
      }
    })

  case rowid_op {
    Ok(op) ->
      case dict.get(cursor_table, op.p1) {
        Ok(table) -> Ok(resolve_rowid_column(table, table_schemas, pk_columns))
        Error(_) ->
          Ok(Column(name: "rowid", column_type: query.IntType, nullable: False))
      }
    Error(_) -> {
      // Find the ResultRow address so we can prefer Column opcodes in the
      // output phase (after sorting/ephemeral fill) over those in the fill
      // phase. When ORDER BY is used, the same register may be written to
      // twice: once during the sorter-fill phase (writing a different value)
      // and once during the output phase (writing the actual result). We want
      // the output-phase write, which is the LAST Column writing to this
      // register before ResultRow.
      let result_row_addr =
        list.fold(opcodes, 0, fn(acc, op) {
          case op.opcode == "ResultRow" {
            True -> op.addr
            False -> acc
          }
        })

      let column_op =
        list.find(
          list.reverse(
            list.filter(opcodes, fn(op) { op.addr < result_row_addr }),
          ),
          fn(op) { op.opcode == "Column" && op.p3 == reg },
        )

      case column_op {
        Ok(op) -> resolve_column(op.p1, op.p2, cursor_table, table_schemas)
        Error(_) -> Error(Nil)
      }
    }
  }
}

/// Resolve the rowid (INTEGER PRIMARY KEY) column for a table.
/// Uses the pk field from PRAGMA table_info to correctly identify the PK column.
fn resolve_rowid_column(
  table: String,
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
) -> Column {
  case dict.get(pk_columns, table) {
    Ok(pk_name) ->
      case query.find_column(table_schemas, table, pk_name) {
        Ok(col) -> col
        Error(_) ->
          Column(name: pk_name, column_type: query.IntType, nullable: False)
      }
    Error(_) ->
      Column(name: "rowid", column_type: query.IntType, nullable: False)
  }
}

@internal
pub fn debug_warning(message: String) -> Nil {
  // Keep regular generation quiet. These warnings are useful while working on
  // inference, but noisy during normal use and tests.
  case marmot_debug_warnings_enabled() {
    True -> io.println_error(message)
    False -> Nil
  }
}

@internal
pub fn marmot_debug_warnings_enabled() -> Bool {
  case get_env("MARMOT_DEBUG_WARNINGS") {
    option.Some(_) -> True
    option.None -> False
  }
}

@external(erlang, "marmot_ffi", "get_env")
fn get_env(name: String) -> Option(String)

/// Look up a column by cursor and index.
/// Returns Error(Nil) when column resolution fails, after logging a warning
/// so users can diagnose type inference gaps.
pub fn resolve_column(
  cursor: Int,
  col_idx: Int,
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
) -> Result(Column, Nil) {
  case dict.get(cursor_table, cursor) {
    Ok(table_name) ->
      case dict.get(table_schemas, table_name) {
        Ok(table_cols) ->
          case util.list_at(table_cols, col_idx) {
            Ok(col) -> Ok(col)
            Error(_) -> {
              debug_warning(
                "warning: column at index "
                <> int.to_string(col_idx)
                <> " not found in table "
                <> table_name,
              )
              Error(Nil)
            }
          }
        Error(_) -> {
          debug_warning(
            "warning: table " <> table_name <> " not found in schema",
          )
          Error(Nil)
        }
      }
    Error(_) -> {
      debug_warning(
        "warning: cursor "
        <> int.to_string(cursor)
        <> " not found in cursor-to-table map",
      )
      Error(Nil)
    }
  }
}

/// Infer parameter type from comparison context.
/// Key insight: when a register is reused, find the Column opcode closest
/// to (but before) the comparison that uses it.
pub fn infer_parameter_type(
  var_op: Opcode,
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
) -> Result(Parameter, Nil) {
  let var_reg = var_op.p2
  // Eq/Ne/Lt/Le/Gt/Ge: p1 and p3 are both registers being compared.
  // SeekGE/SeekGT/SeekLE/SeekLT: p1 is a CURSOR; p3 is the key register.
  let eq_ops = ["Eq", "Ne", "Lt", "Le", "Gt", "Ge"]
  let seek_ops = ["SeekGE", "SeekGT", "SeekLE", "SeekLT"]

  let comparison =
    list.find(opcodes, fn(op) {
      case list.contains(eq_ops, op.opcode) {
        True -> op.p1 == var_reg || op.p3 == var_reg
        False ->
          case list.contains(seek_ops, op.opcode) {
            True -> op.p3 == var_reg
            False -> False
          }
      }
    })

  case comparison {
    Ok(cmp) -> {
      let other_reg = case list.contains(seek_ops, cmp.opcode) {
        // For Seek opcodes (SeekGE, SeekGT, SeekLT, SeekLE), the comparison
        // register holds the parameter value and the "other side" is the
        // indexed column. resolve_seek_cursor_key resolves this by taking the
        // FIRST column of the parent table's schema (not the index definition).
        // This is only correct when the indexed column happens to be the table's
        // first column. For indexes on any other column (including a
        // single-column index on email when the table's first column is id),
        // the parameter will be attributed to the wrong table column.
        // See opcode_test "seek parameter resolution" tests.
        True -> -1
        False ->
          case cmp.p1 == var_reg {
            True -> cmp.p3
            False -> cmp.p1
          }
      }
      case other_reg {
        -1 -> resolve_seek_cursor_key(cmp, cursor_table, table_schemas)
        _ ->
          find_nearest_column_source(
            other_reg,
            cmp.addr,
            opcodes,
            cursor_table,
            table_schemas,
          )
      }
    }
    Error(_) -> {
      // Check for SeekRowid - Variable used as rowid
      let seek_rowid =
        list.find(opcodes, fn(op) {
          op.opcode == "SeekRowid" && op.p3 == var_reg
        })
      case seek_rowid {
        Ok(sr) ->
          case dict.get(cursor_table, sr.p1) {
            Ok(table) ->
              case dict.get(pk_columns, table) {
                Ok(pk_name) ->
                  Ok(Parameter(
                    name: pk_name,
                    column_type: query.IntType,
                    nullable: False,
                  ))
                Error(_) ->
                  Ok(Parameter(
                    name: "id",
                    column_type: query.IntType,
                    nullable: False,
                  ))
              }
            Error(_) ->
              Ok(Parameter(
                name: "id",
                column_type: query.IntType,
                nullable: False,
              ))
          }
        Error(_) -> {
          debug_warning(
            "warning: no comparison context for Variable p1="
            <> int.to_string(var_op.p1)
            <> " register p2="
            <> int.to_string(var_op.p2),
          )
          Error(Nil)
        }
      }
    }
  }
}

/// Find the Column opcode that writes to target_reg, closest to but before cmp_addr
fn find_nearest_column_source(
  target_reg: Int,
  cmp_addr: Int,
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
) -> Result(Parameter, Nil) {
  let candidates =
    list.filter(opcodes, fn(op) {
      op.opcode == "Column" && op.p3 == target_reg && op.addr < cmp_addr
    })

  let best: Result(Opcode, Nil) =
    list.fold(candidates, Error(Nil), fn(acc: Result(Opcode, Nil), op) {
      case acc {
        Error(_) -> Ok(op)
        Ok(prev) ->
          case op.addr > prev.addr {
            True -> Ok(op)
            False -> Ok(prev)
          }
      }
    })

  case best {
    Ok(cop) ->
      resolve_column_to_parameter(cop.p1, cop.p2, cursor_table, table_schemas)
    Error(_) -> {
      debug_warning(
        "warning: no Column opcode found writing to register "
        <> int.to_string(target_reg),
      )
      Error(Nil)
    }
  }
}

/// Resolve a Seek parameter's target column by taking the first column of the
/// cursor's underlying table. This is only correct when the indexed column
/// happens to be the table's first column; otherwise the parameter is
/// attributed to the wrong column.
fn resolve_seek_cursor_key(
  seek_op: Opcode,
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
) -> Result(Parameter, Nil) {
  let cursor = seek_op.p1
  case dict.get(cursor_table, cursor) {
    Ok(table_name) ->
      case dict.get(table_schemas, table_name) {
        Ok(table_cols) ->
          case list.first(table_cols) {
            Ok(col) ->
              Ok(Parameter(
                name: col.name,
                column_type: col.column_type,
                nullable: col.nullable,
              ))
            Error(_) -> Error(Nil)
          }
        Error(_) -> Error(Nil)
      }
    Error(_) -> Error(Nil)
  }
}

/// Resolve a cursor + column index to a Parameter
fn resolve_column_to_parameter(
  cursor: Int,
  col_idx: Int,
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
) -> Result(Parameter, Nil) {
  case dict.get(cursor_table, cursor) {
    Ok(table_name) ->
      case dict.get(table_schemas, table_name) {
        Ok(table_cols) ->
          case util.list_at(table_cols, col_idx) {
            Ok(col) ->
              Ok(Parameter(
                name: col.name,
                column_type: col.column_type,
                nullable: col.nullable,
              ))
            Error(_) -> {
              debug_warning(
                "warning: parameter cursor "
                <> int.to_string(cursor)
                <> " col_idx "
                <> int.to_string(col_idx)
                <> " not found in table "
                <> table_name,
              )
              Error(Nil)
            }
          }
        Error(_) -> {
          debug_warning(
            "warning: table "
            <> table_name
            <> " not found in schema for parameter resolution",
          )
          Error(Nil)
        }
      }
    Error(_) -> {
      debug_warning(
        "warning: cursor "
        <> int.to_string(cursor)
        <> " not found in cursor-to-table map for parameter",
      )
      Error(Nil)
    }
  }
}

/// Deduplicate Variable opcodes by p1 (parameter number). SQLite sometimes
/// emits multiple Variable opcodes for a single `?` when the parameter is
/// used in multiple contexts. We want one Parameter per `?`.
pub fn dedupe_variables_by_p1(ops: List(Opcode)) -> List(Opcode) {
  do_dedupe_variables(ops, -1, [])
}

fn do_dedupe_variables(
  remaining: List(Opcode),
  last_p1: Int,
  acc: List(Opcode),
) -> List(Opcode) {
  case remaining {
    [] -> list.reverse(acc)
    [op, ..rest] ->
      case op.p1 == last_p1 {
        True -> do_dedupe_variables(rest, last_p1, acc)
        False -> do_dedupe_variables(rest, op.p1, [op, ..acc])
      }
  }
}

/// A decoder that handles both string and non-string p4 values from EXPLAIN
pub fn flexible_string_decoder() -> decode.Decoder(String) {
  decode.one_of(decode.string, [
    decode.map(decode.int, int.to_string),
    // EXPLAIN may return float p4 values for opcodes like RealAffinity;
    // the numeric value is not meaningful for our type inference, so discard it.
    decode.map(decode.float, fn(_) { "" }),
    decode.success(""),
  ])
}
