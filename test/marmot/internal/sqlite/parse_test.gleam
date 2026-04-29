import gleam/list
import gleam/option
import marmot/internal/query.{BitArrayType, Column, FloatType, IntType, StringType}
import marmot/internal/sqlite/tokenize.{
  type Token, CloseParen, Comma, Gt, Minus, Number, OpenParen, Star,
  StringLit, Word,
}
import marmot/internal/sqlite/parse.{
  type SelectItem, OverrideNone, SelectItem, infer_expression_type,
}

// ---- Helpers ----

fn item(alias: String, tokens: List(Token)) -> SelectItem {
  SelectItem(
    alias: alias,
    tokens: tokens,
    bare_column: option.None,
    override: OverrideNone,
  )
}

fn cast_item(alias: String, rest: List(Token)) -> SelectItem {
  item(alias, list.append([Word("CAST"), OpenParen], rest))
}

fn coalesce_item(alias: String, rest: List(Token)) -> SelectItem {
  item(alias, list.append([Word("COALESCE"), OpenParen], rest))
}

fn case_item(alias: String, tokens: List(Token)) -> SelectItem {
  item(alias, tokens)
}

// ---- infer_expression_type: CAST ----

pub fn infer_cast_integer_test() {
  let result =
    infer_expression_type(cast_item("id", [Word("id"), Word("AS"), Word("INTEGER"), CloseParen]))
  let assert Column(name: "id", column_type: IntType, nullable: False) = result
}

pub fn infer_cast_int_alias_test() {
  let result =
    infer_expression_type(cast_item("id", [Word("id"), Word("AS"), Word("INT"), CloseParen]))
  let assert Column(name: "id", column_type: IntType, nullable: False) = result
}

pub fn infer_cast_bigint_test() {
  let result =
    infer_expression_type(cast_item("id", [Word("id"), Word("AS"), Word("BIGINT"), CloseParen]))
  let assert Column(name: "id", column_type: IntType, nullable: False) = result
}

pub fn infer_cast_real_test() {
  let result =
    infer_expression_type(cast_item("val", [Word("val"), Word("AS"), Word("REAL"), CloseParen]))
  let assert Column(name: "val", column_type: FloatType, nullable: False) = result
}

pub fn infer_cast_float_test() {
  let result = infer_expression_type(cast_item("val", [
    Word("val"), Word("AS"), Word("FLOAT"), CloseParen,
  ]))
  let assert Column(name: "val", column_type: FloatType, nullable: False) = result
}

pub fn infer_cast_double_test() {
  let result = infer_expression_type(cast_item("val", [
    Word("val"), Word("AS"), Word("DOUBLE"), CloseParen,
  ]))
  let assert Column(name: "val", column_type: FloatType, nullable: False) = result
}

pub fn infer_cast_text_test() {
  let result = infer_expression_type(cast_item("name", [
    Word("name"), Word("AS"), Word("TEXT"), CloseParen,
  ]))
  let assert Column(name: "name", column_type: StringType, nullable: False) = result
}

pub fn infer_cast_varchar_test() {
  let result = infer_expression_type(cast_item("name", [
    Word("name"), Word("AS"), Word("VARCHAR"), CloseParen,
  ]))
  let assert Column(name: "name", column_type: StringType, nullable: False) = result
}

pub fn infer_cast_char_test() {
  let result = infer_expression_type(cast_item("name", [
    Word("name"), Word("AS"), Word("CHAR"), CloseParen,
  ]))
  let assert Column(name: "name", column_type: StringType, nullable: False) = result
}

pub fn infer_cast_blob_test() {
  let result = infer_expression_type(cast_item("data", [
    Word("data"), Word("AS"), Word("BLOB"), CloseParen,
  ]))
  let assert Column(name: "data", column_type: BitArrayType, nullable: False) = result
}

pub fn infer_cast_unknown_type_test() {
  let result = infer_expression_type(cast_item("x", [
    Word("x"), Word("AS"), Word("UNKNOWN"), CloseParen,
  ]))
  let assert Column(name: "x", column_type: StringType, nullable: True) = result
}

pub fn infer_cast_no_as_keyword_test() {
  let result = infer_expression_type(cast_item("x", [Word("x"), CloseParen]))
  let assert Column(name: "x", column_type: StringType, nullable: True) = result
}

pub fn infer_cast_with_trailing_expression_test() {
  let result = infer_expression_type(cast_item("val", [
    Word("val"), Word("AS"), Word("INTEGER"), CloseParen, Word("extra"),
  ]))
  let assert Column(name: "val", column_type: IntType, nullable: False) = result
}

pub fn infer_cast_nullable_override_test() {
  let result = infer_expression_type(cast_item("name", [
    Word("name"), Word("AS"), Word("TEXT"), CloseParen,
  ]))
  let assert Column(name: "name", column_type: StringType, nullable: False) = result
}

// ---- infer_expression_type: COALESCE ----

pub fn infer_coalesce_string_literal_test() {
  let result = infer_expression_type(coalesce_item("name", [Word("name"), Comma, StringLit("unknown")]))
  let assert Column(name: "name", column_type: StringType, nullable: False) = result
}

pub fn infer_coalesce_integer_literal_test() {
  let result = infer_expression_type(coalesce_item("cnt", [Word("count"), Comma, Number("0")]))
  let assert Column(name: "cnt", column_type: IntType, nullable: False) = result
}

pub fn infer_coalesce_float_literal_test() {
  let result = infer_expression_type(coalesce_item("val", [Word("val"), Comma, Number("3.14")]))
  let assert Column(name: "val", column_type: FloatType, nullable: False) = result
}

pub fn infer_coalesce_last_arg_null_test() {
  let result = infer_expression_type(coalesce_item("val", [Word("val"), Comma, Word("NULL")]))
  let assert Column(name: "val", column_type: StringType, nullable: True) = result
}

pub fn infer_coalesce_column_ref_last_test() {
  let result = infer_expression_type(coalesce_item("name", [Word("a"), Comma, Word("b")]))
  let assert Column(name: "name", column_type: StringType, nullable: True) = result
}

pub fn infer_coalesce_multiple_args_test() {
  let result = infer_expression_type(coalesce_item("x", [
    Word("a"), Comma, Word("b"), Comma, Word("c"), Comma, Number("42"),
  ]))
  let assert Column(name: "x", column_type: IntType, nullable: False) = result
}

pub fn infer_coalesce_single_arg_numeric_test() {
  let result = infer_expression_type(coalesce_item("x", [Number("42")]))
  let assert Column(name: "x", column_type: IntType, nullable: False) = result
}

pub fn infer_coalesce_empty_test() {
  let result = infer_expression_type(coalesce_item("empty", []))
  let assert Column(name: "empty", column_type: StringType, nullable: True) = result
}

pub fn infer_coalesce_with_trailing_expression_test() {
  let result = infer_expression_type(coalesce_item("name", [
    Word("name"), Comma, StringLit("default"), CloseParen, Word("extra"),
  ]))
  let assert Column(name: "name", column_type: StringType, nullable: False) = result
}

// ---- infer_expression_type: CASE ----

pub fn infer_case_simple_consistent_int_test() {
  let result = infer_expression_type(case_item("val", [
    Word("CASE"), Word("x"), Word("WHEN"), Number("1"), Word("THEN"),
    Number("10"), Word("WHEN"), Number("2"), Word("THEN"), Number("20"),
    Word("ELSE"), Number("0"), Word("END"),
  ]))
  let assert Column(name: "val", column_type: IntType, nullable: False) = result
}

pub fn infer_case_searched_consistent_string_test() {
  let result = infer_expression_type(case_item("label", [
    Word("CASE"), Word("WHEN"), Word("x"), Gt, Number("0"), Word("THEN"),
    StringLit("pos"), Word("ELSE"), StringLit("zero"), Word("END"),
  ]))
  let assert Column(name: "label", column_type: StringType, nullable: False) = result
}

pub fn infer_case_with_null_branch_test() {
  let result = infer_expression_type(case_item("val", [
    Word("CASE"), Word("WHEN"), Word("x"), Gt, Number("0"), Word("THEN"),
    Number("1"), Word("ELSE"), Word("NULL"), Word("END"),
  ]))
  let assert Column(name: "val", column_type: IntType, nullable: True) = result
}

pub fn infer_case_no_else_test() {
  let result = infer_expression_type(case_item("val", [
    Word("CASE"), Word("WHEN"), Word("x"), Gt, Number("0"), Word("THEN"),
    Number("1"), Word("END"),
  ]))
  let assert Column(name: "val", column_type: IntType, nullable: True) = result
}

pub fn infer_case_mixed_types_test() {
  let result = infer_expression_type(case_item("val", [
    Word("CASE"), Word("WHEN"), Word("x"), Gt, Number("0"), Word("THEN"),
    StringLit("str"), Word("ELSE"), Number("42"), Word("END"),
  ]))
  let assert Column(name: "val", column_type: StringType, nullable: True) = result
}

pub fn infer_case_all_null_branches_test() {
  let result = infer_expression_type(case_item("val", [
    Word("CASE"), Word("WHEN"), Word("x"), Gt, Number("0"), Word("THEN"),
    Word("NULL"), Word("ELSE"), Word("NULL"), Word("END"),
  ]))
  let assert Column(name: "val", column_type: StringType, nullable: True) = result
}

pub fn infer_case_empty_test() {
  let result = infer_expression_type(case_item("empty", []))
  let assert Column(name: "empty", column_type: StringType, nullable: True) = result
}

pub fn infer_case_consistent_float_test() {
  let result = infer_expression_type(case_item("val", [
    Word("CASE"), Word("WHEN"), Word("x"), Gt, Number("0"), Word("THEN"),
    Number("1.5"), Word("ELSE"), Number("2.5"), Word("END"),
  ]))
  let assert Column(name: "val", column_type: FloatType, nullable: False) = result
}

// ---- infer_expression_type: other functions, literals, dispatch ----

pub fn infer_expr_count_star_test() {
  let item = item("cnt", [Word("COUNT"), OpenParen, Star, CloseParen])
  let assert Column(name: "cnt", column_type: IntType, nullable: False) =
    infer_expression_type(item)
}

pub fn infer_expr_exists_test() {
  let item = item("ex", [
    Word("EXISTS"), OpenParen, Word("SELECT"), Star, Word("FROM"), Word("t"),
    CloseParen,
  ])
  let assert Column(name: "ex", column_type: IntType, nullable: False) =
    infer_expression_type(item)
}

pub fn infer_expr_sum_test() {
  let item = item("total", [Word("SUM"), OpenParen, Word("amount"), CloseParen])
  let assert Column(name: "total", column_type: FloatType, nullable: True) =
    infer_expression_type(item)
}

pub fn infer_expr_avg_test() {
  let item = item("avg_val", [Word("AVG"), OpenParen, Word("val"), CloseParen])
  let assert Column(name: "avg_val", column_type: FloatType, nullable: True) =
    infer_expression_type(item)
}

pub fn infer_expr_max_test() {
  let item = item("max_val", [Word("MAX"), OpenParen, Word("val"), CloseParen])
  let assert Column(name: "max_val", column_type: IntType, nullable: True) =
    infer_expression_type(item)
}

pub fn infer_expr_min_test() {
  let item = item("min_val", [Word("MIN"), OpenParen, Word("val"), CloseParen])
  let assert Column(name: "min_val", column_type: IntType, nullable: True) =
    infer_expression_type(item)
}

pub fn infer_expr_row_number_over_test() {
  let item = item("rn", [
    Word("ROW_NUMBER"), OpenParen, CloseParen, Word("OVER"), OpenParen,
    Word("ORDER"), Word("BY"), Word("id"), CloseParen,
  ])
  let assert Column(name: "rn", column_type: IntType, nullable: False) =
    infer_expression_type(item)
}

pub fn infer_expr_window_without_over_test() {
  let item = item("rn", [Word("ROW_NUMBER"), OpenParen, CloseParen])
  let assert Column(name: "rn", column_type: StringType, nullable: True) =
    infer_expression_type(item)
}

pub fn infer_expr_unknown_function_test() {
  let item = item("foo", [Word("UNKNOWN_FUNC"), OpenParen, Word("arg"), CloseParen])
  let assert Column(name: "foo", column_type: StringType, nullable: True) =
    infer_expression_type(item)
}

pub fn infer_expr_integer_literal_test() {
  let item = item("num", [Number("42")])
  let assert Column(name: "num", column_type: IntType, nullable: False) =
    infer_expression_type(item)
}

pub fn infer_expr_float_literal_test() {
  let item = item("num", [Number("3.14")])
  let assert Column(name: "num", column_type: FloatType, nullable: False) =
    infer_expression_type(item)
}

pub fn infer_expr_negative_int_literal_test() {
  let item = item("num", [Minus, Number("5")])
  let assert Column(name: "num", column_type: IntType, nullable: False) =
    infer_expression_type(item)
}

pub fn infer_expr_negative_float_literal_test() {
  let item = item("num", [Minus, Number("3.14")])
  let assert Column(name: "num", column_type: FloatType, nullable: False) =
    infer_expression_type(item)
}

pub fn infer_expr_string_literal_test() {
  let item = item("greeting", [StringLit("hello")])
  let assert Column(name: "greeting", column_type: StringType, nullable: False) =
    infer_expression_type(item)
}

pub fn infer_expr_column_ref_falls_back_test() {
  let item = item("id", [Word("id")])
  let assert Column(name: "id", column_type: StringType, nullable: True) =
    infer_expression_type(item)
}
