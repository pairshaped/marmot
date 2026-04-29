import gleam/dict
import marmot/internal/query.{
  type Column, Column, IntType, Parameter, StringType,
}
import marmot/internal/sqlite/opcode.{type Opcode, Opcode, find_column_for_register, infer_parameter_type}

// ---- Helpers ----

fn op(addr, opcode, p1, p2, p3) -> Opcode {
  Opcode(addr: addr, opcode: opcode, p1: p1, p2: p2, p3: p3, p4: "", p5: 0)
}

fn col(name: String, column_type) -> Column {
  Column(name: name, column_type: column_type, nullable: False)
}

fn schema(cols: List(Column)) -> dict.Dict(String, List(Column)) {
  dict.from_list([#("t", cols)])
}

fn cursor_map(pairs: List(#(Int, String))) -> dict.Dict(Int, String) {
  dict.from_list(pairs)
}

fn pk_map(pairs: List(#(String, String))) -> dict.Dict(String, String) {
  dict.from_list(pairs)
}

// ---- find_column_for_register ---

pub fn find_column_for_register_column_opcode_test() {
  // A Column opcode writes to register 1 from cursor 0, column index 0.
  // Cursor 0 maps to table "t", which has [Column("id", IntType)].
  // ResultRow is at address 10, so the Column at addr 8 qualifies.
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(8, "Column", 0, 0, 1), // cursor=0, col_idx=0, reg=1
    op(10, "ResultRow", 1, 2, 0),
  ]
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType)])
  let pk = pk_map([#("t", "id")])

  let result =
    find_column_for_register(1, opcodes, cursor_table, schemas, pk)
  let assert Column(name: "id", column_type: IntType, nullable: False) = result
}

pub fn find_column_for_register_rowid_opcode_test() {
  // A Rowid opcode writes to register 2 from cursor 0.
  // Resolves via PK column of the table.
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(7, "Rowid", 0, 2, 0), // cursor=0, writes rowid to reg=2
    op(10, "ResultRow", 1, 2, 0),
  ]
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType)])
  let pk = pk_map([#("t", "id")])

  let result =
    find_column_for_register(2, opcodes, cursor_table, schemas, pk)
  let assert Column(name: "id", column_type: IntType, nullable: False) = result
}

pub fn find_column_for_register_seek_rowid_test() {
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(7, "SeekRowid", 0, 2, 0), // cursor=0, writes to reg=2
    op(10, "ResultRow", 1, 2, 0),
  ]
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType)])
  let pk = pk_map([#("t", "id")])

  let result =
    find_column_for_register(2, opcodes, cursor_table, schemas, pk)
  let assert Column(name: "id", column_type: IntType, nullable: False) = result
}

pub fn find_column_for_register_idx_rowid_test() {
  // IdxRowid also writes rowid. Tests the opcode pattern match.
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(7, "IdxRowid", 0, 3, 0),
    op(10, "ResultRow", 1, 2, 0),
  ]
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("pk_col", IntType)])
  let pk = pk_map([#("t", "pk_col")])

  let result =
    find_column_for_register(3, opcodes, cursor_table, schemas, pk)
  let assert Column(name: "pk_col", column_type: IntType, nullable: False) = result
}

pub fn find_column_for_register_not_found_test() {
  // No opcode writes to register 99. Returns unknown_column.
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(8, "Column", 0, 0, 1),
    op(10, "ResultRow", 1, 2, 0),
  ]
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType)])
  let pk = pk_map([#("t", "id")])

  let result =
    find_column_for_register(99, opcodes, cursor_table, schemas, pk)
  let assert "unknown" = result.name
  let assert StringType = result.column_type
  let assert True = result.nullable
}

pub fn find_column_for_register_sorter_aware_test() {
  // When ORDER BY is used, a Column opcode may write to register 1
  // during the sorter-fill phase AND during the output phase.
  // The function should pick the LAST Column writing to the register
  // before ResultRow (the output-phase write).
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    // Sorter-fill phase: Column writes to reg 1 with wrong col_idx
    op(6, "Column", 0, 1, 1), // cursor=0, col_idx=1 (wrong!), reg=1
    op(7, "SorterSort", 0, 0, 0),
    // Output phase: Column writes to reg 1 with correct col_idx
    op(8, "Column", 0, 0, 1), // cursor=0, col_idx=0 (correct), reg=1
    op(10, "ResultRow", 1, 2, 0),
  ]
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType), col("name", StringType)])
  let pk = pk_map([#("t", "id")])

  let result =
    find_column_for_register(1, opcodes, cursor_table, schemas, pk)
  // Should pick the output-phase Column (col_idx=0 -> "id"), not the fill-phase
  let assert Column(name: "id", column_type: IntType, nullable: False) = result
}

pub fn find_column_for_register_multiple_cursors_test() {
  // Two cursors, each with its own table. Register 2 maps to
  // cursor 1, column index 0.
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(6, "OpenRead", 1, 2, 0),
    op(8, "Column", 1, 0, 2), // cursor=1, col_idx=0, reg=2
    op(10, "ResultRow", 3, 2, 0),
  ]
  let cursor_table = cursor_map([#(0, "t1"), #(1, "t2")])
  let schemas = dict.from_list([
    #("t1", [col("a", IntType)]),
    #("t2", [col("b", StringType)]),
  ])
  let pk = pk_map([])

  let result =
    find_column_for_register(2, opcodes, cursor_table, schemas, pk)
  let assert Column(name: "b", column_type: StringType, nullable: False) = result
}

pub fn find_column_for_register_rowid_no_pk_test() {
  // Rowid opcode, but the table has no PK in pk_columns.
  // Falls back to default "rowid" column.
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(7, "Rowid", 0, 2, 0),
    op(10, "ResultRow", 1, 2, 0),
  ]
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType)])
  let pk = pk_map([])

  let result =
    find_column_for_register(2, opcodes, cursor_table, schemas, pk)
  let assert Column(name: "rowid", column_type: IntType, nullable: False) = result
}

// ---- infer_parameter_type ---

pub fn infer_parameter_eq_comparison_test() {
  // Variable op writes to reg 1. Eq compares reg 1 (p1) with reg 2 (p3).
  // A Column opcode writes to reg 2 from cursor 0, col_idx 0.
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(6, "Variable", 1, 1, 0), // param_num=1, writes to reg=1
    op(7, "Column", 0, 0, 2), // cursor=0, col_idx=0, writes to reg=2
    op(8, "Eq", 1, 0, 2), // compares reg 1 and reg 2, addr=8
    op(10, "ResultRow", 1, 2, 0),
  ]
  let var_op = op(6, "Variable", 1, 1, 0)
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType)])
  let pk = pk_map([#("t", "id")])

  let result =
    infer_parameter_type(var_op, opcodes, cursor_table, schemas, pk)
  let assert Parameter(name: "id", column_type: IntType, nullable: False) = result
}

pub fn infer_parameter_ne_comparison_test() {
  // Ne comparison works the same as Eq for parameter inference
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(6, "Variable", 1, 1, 0),
    op(7, "Column", 0, 1, 2), // col_idx=1 -> "name"
    op(8, "Ne", 1, 0, 2),
    op(10, "ResultRow", 1, 2, 0),
  ]
  let var_op = op(6, "Variable", 1, 1, 0)
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType), col("name", StringType)])
  let pk = pk_map([#("t", "id")])

  let result =
    infer_parameter_type(var_op, opcodes, cursor_table, schemas, pk)
  let assert Parameter(name: "name", column_type: StringType, nullable: False) =
    result
}

pub fn infer_parameter_variable_is_p3_test() {
  // Variable writes to reg 2. Eq compares reg 1 (p1) with reg 2 (p3).
  // The Variable is on the p3 side.
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(6, "Column", 0, 0, 1),
    op(7, "Variable", 1, 2, 0),
    op(8, "Eq", 1, 0, 2),
    op(10, "ResultRow", 1, 2, 0),
  ]
  let var_op = op(7, "Variable", 1, 2, 0)
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType)])
  let pk = pk_map([#("t", "id")])

  let result =
    infer_parameter_type(var_op, opcodes, cursor_table, schemas, pk)
  let assert Parameter(name: "id", column_type: IntType, nullable: False) = result
}

pub fn infer_parameter_seek_ge_test() {
  // SeekGE uses p3 as the key register (the Variable's register).
  // The other side is the first column of the index at cursor p1.
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(6, "Variable", 1, 3, 0), // writes to reg=3
    op(7, "SeekGE", 0, 0, 3), // cursor=0, key_reg=3
    op(10, "ResultRow", 1, 2, 0),
  ]
  let var_op = op(6, "Variable", 1, 3, 0)
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType), col("val", StringType)])
  let pk = pk_map([#("t", "id")])

  let result =
    infer_parameter_type(var_op, opcodes, cursor_table, schemas, pk)
  // SeekGE resolves via cursor -> table -> first column
  let assert Parameter(name: "id", column_type: IntType, nullable: False) = result
}

pub fn infer_parameter_seek_lt_test() {
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(6, "Variable", 1, 3, 0),
    op(7, "SeekLT", 0, 0, 3),
    op(10, "ResultRow", 1, 2, 0),
  ]
  let var_op = op(6, "Variable", 1, 3, 0)
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("name", StringType)])
  let pk = pk_map([#("t", "name")])

  let result =
    infer_parameter_type(var_op, opcodes, cursor_table, schemas, pk)
  let assert Parameter(name: "name", column_type: StringType, nullable: False) =
    result
}

pub fn infer_parameter_seek_rowid_test() {
  // SeekRowid uses p3 as the value register (the Variable's register).
  // Infers from PK column of the target table.
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(6, "Variable", 1, 3, 0),
    op(7, "SeekRowid", 0, 0, 3), // cursor=0, value_reg=3
    op(10, "ResultRow", 1, 2, 0),
  ]
  let var_op = op(6, "Variable", 1, 3, 0)
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("user_id", IntType)])
  let pk = pk_map([#("t", "user_id")])

  let result =
    infer_parameter_type(var_op, opcodes, cursor_table, schemas, pk)
  let assert Parameter(name: "user_id", column_type: IntType, nullable: False) =
    result
}

pub fn infer_parameter_no_comparison_test() {
  // Variable op without any matching comparison returns unknown_param
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(6, "Variable", 1, 1, 0),
    op(7, "Column", 0, 0, 2),
    op(10, "ResultRow", 1, 2, 0),
  ]
  let var_op = op(6, "Variable", 1, 1, 0)
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType)])
  let pk = pk_map([#("t", "id")])

  let result =
    infer_parameter_type(var_op, opcodes, cursor_table, schemas, pk)
  let assert "param" = result.name
  let assert StringType = result.column_type
}

pub fn infer_parameter_seek_rowid_no_pk_test() {
  // SeekRowid but no PK info available — defaults to "id" + IntType
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(6, "Variable", 1, 3, 0),
    op(7, "SeekRowid", 0, 0, 3),
    op(10, "ResultRow", 1, 2, 0),
  ]
  let var_op = op(6, "Variable", 1, 3, 0)
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType)])
  let pk = pk_map([])

  let result =
    infer_parameter_type(var_op, opcodes, cursor_table, schemas, pk)
  let assert Parameter(name: "id", column_type: IntType, nullable: False) = result
}

pub fn infer_parameter_seek_rowid_unknown_cursor_test() {
  // SeekRowid cursor not in cursor_table — defaults to "id" + IntType
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(6, "Variable", 1, 3, 0),
    op(7, "SeekRowid", 99, 0, 3),
    op(10, "ResultRow", 1, 2, 0),
  ]
  let var_op = op(6, "Variable", 1, 3, 0)
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([col("id", IntType)])
  let pk = pk_map([])

  let result =
    infer_parameter_type(var_op, opcodes, cursor_table, schemas, pk)
  let assert Parameter(name: "id", column_type: IntType, nullable: False) = result
}

pub fn infer_parameter_seek_ge_empty_schema_test() {
  // SeekGE on a cursor with an empty table schema — defaults to IntType param
  let opcodes = [
    op(5, "OpenRead", 0, 1, 0),
    op(6, "Variable", 1, 3, 0),
    op(7, "SeekGE", 0, 0, 3),
    op(10, "ResultRow", 1, 2, 0),
  ]
  let var_op = op(6, "Variable", 1, 3, 0)
  let cursor_table = cursor_map([#(0, "t")])
  let schemas = schema([])
  let pk = pk_map([])

  let result =
    infer_parameter_type(var_op, opcodes, cursor_table, schemas, pk)
  let assert "param" = result.name
  let assert IntType = result.column_type
  let assert False = result.nullable
}
