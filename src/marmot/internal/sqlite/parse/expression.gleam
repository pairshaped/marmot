//// Expression type inference from SQL token structure.

import gleam/list
import gleam/option.{type Option}
import gleam/string
import marmot/internal/query.{type Column, type ColumnType, Column, StringType}
import marmot/internal/sqlite/parse.{
  type NullabilityOverride, type SelectItem, OverrideNonNull, OverrideNone,
  OverrideNullable,
}
import marmot/internal/sqlite/tokenize.{
  type Token, CloseParen, Dot, Minus, Number, OpenParen, Plus, Slash, Star,
  StringLit, Word,
}

/// Infer the type of a SELECT expression from its token structure.
pub fn infer_expression_type(item: SelectItem) -> Column {
  case item.tokens {
    // Literal values
    [Number(n)] ->
      case string.contains(n, ".") {
        True ->
          Column(
            name: item.alias,
            column_type: query.FloatType,
            nullable: False,
          )
        False ->
          Column(name: item.alias, column_type: query.IntType, nullable: False)
      }
    [StringLit(_)] ->
      Column(name: item.alias, column_type: StringType, nullable: False)
    [Minus, Number(n)] ->
      case string.contains(n, ".") {
        True ->
          Column(
            name: item.alias,
            column_type: query.FloatType,
            nullable: False,
          )
        False ->
          Column(name: item.alias, column_type: query.IntType, nullable: False)
      }

    // Function calls: Word(name) OpenParen ...
    [Word(func_name), OpenParen, ..rest] -> {
      let upper = string.uppercase(func_name)
      case upper {
        "COUNT" ->
          Column(name: item.alias, column_type: query.IntType, nullable: False)
        "EXISTS" ->
          Column(name: item.alias, column_type: query.IntType, nullable: False)
        "CAST" -> infer_cast_type(item.alias, rest)
        "COALESCE" -> infer_coalesce_type(item.alias, rest)
        "SUM" ->
          // Design decision: SUM() is always mapped to FloatType.
          // SQLite returns integer for SUM of integer columns,
          // but Float is a safe superset that avoids silent
          // truncation. Nullable because SUM returns NULL for
          // empty result sets.
          Column(name: item.alias, column_type: query.FloatType, nullable: True)
        "AVG" ->
          // AVG always returns REAL in SQLite. Nullable because AVG
          // returns NULL for an empty result set.
          Column(name: item.alias, column_type: query.FloatType, nullable: True)
        "MAX" | "MIN" ->
          // MAX/MIN return the same type as their argument. Without opcode
          // analysis we can't determine the argument type, so we default to
          // the most common case (numeric). The opcode fallback will refine
          // this for cases where the argument type is knowable.
          Column(name: item.alias, column_type: query.IntType, nullable: True)
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" ->
          case tokenize.has_keyword(item.tokens, "OVER") {
            True ->
              Column(
                name: item.alias,
                column_type: query.IntType,
                nullable: False,
              )
            False ->
              Column(name: item.alias, column_type: StringType, nullable: True)
          }
        _ -> Column(name: item.alias, column_type: StringType, nullable: True)
      }
    }

    // CASE expression
    [Word(w), ..] ->
      case string.uppercase(w) == "CASE" {
        True -> infer_case_type(item.alias, item.tokens)
        False ->
          case infer_arithmetic_type(item.tokens) {
            option.Some(t) ->
              Column(name: item.alias, column_type: t, nullable: True)
            option.None ->
              Column(name: item.alias, column_type: StringType, nullable: True)
          }
      }

    _ ->
      case infer_arithmetic_type(item.tokens) {
        option.Some(t) ->
          Column(name: item.alias, column_type: t, nullable: True)
        option.None ->
          Column(name: item.alias, column_type: StringType, nullable: True)
      }
  }
}

/// Check if tokens contain arithmetic operators and infer FloatType as a safe
/// default (division always produces Float; mixed-type arithmetic produces
/// Float; Int-only arithmetic is the sole case where Int would be correct).
fn infer_arithmetic_type(tokens: List(Token)) -> Option(query.ColumnType) {
  // Detect binary arithmetic by scanning adjacent token pairs. A Star
  // preceded by OpenParen or Dot is a wildcard, not multiplication.
  let #(has_op, _) =
    list.fold(tokens, #(False, option.None), fn(acc, t) {
      let found = acc.0
      let prev = acc.1
      let is_op = case prev, t {
        _, Plus -> True
        _, Slash -> True
        option.Some(OpenParen), Star -> False
        // COUNT(*) or SELECT *
        option.Some(Dot), Star -> False
        // table.*
        _, Star -> True
        option.Some(Number(_)), Minus -> True
        // binary subtraction
        option.Some(Word(_)), Minus -> True
        // column - x
        option.Some(CloseParen), Minus -> True
        // ) - 1
        _, Minus -> False
        // unary negation
        _, _ -> False
      }
      #(found || is_op, option.Some(t))
    })
  case has_op {
    False -> option.None
    True -> option.Some(query.FloatType)
  }
}

/// Infer type from CAST(... AS type). `rest` is tokens after the opening paren.
fn infer_cast_type(alias: String, rest: List(Token)) -> Column {
  // Use proper paren-matching to extract only the tokens inside CAST(),
  // ignoring any trailing expression after the closing paren.
  let #(inner, _remaining) = tokenize.collect_inside_parens(rest)
  case tokenize.split_at_last_keyword(inner, "AS") {
    Error(_) -> Column(name: alias, column_type: StringType, nullable: True)
    Ok(#(_, type_tokens)) ->
      case type_tokens {
        [Word(type_name), ..] ->
          case string.uppercase(type_name) {
            "INTEGER" | "INT" | "BIGINT" ->
              Column(name: alias, column_type: query.IntType, nullable: False)
            "REAL" | "FLOAT" | "DOUBLE" ->
              Column(name: alias, column_type: query.FloatType, nullable: False)
            "TEXT" | "VARCHAR" | "CHAR" ->
              Column(name: alias, column_type: StringType, nullable: False)
            "BLOB" ->
              Column(
                name: alias,
                column_type: query.BitArrayType,
                nullable: False,
              )
            _ -> Column(name: alias, column_type: StringType, nullable: True)
          }
        _ -> Column(name: alias, column_type: StringType, nullable: True)
      }
  }
}

/// Infer COALESCE type from its last argument.
/// `rest` is everything after the opening paren of COALESCE(. Use proper
/// paren-matching to extract only the tokens inside the COALESCE call,
/// ignoring any trailing expression (e.g., `+ 1` in `COALESCE(...) + 1`).
fn infer_coalesce_type(alias: String, rest: List(Token)) -> Column {
  let #(inner, _remaining) = tokenize.collect_inside_parens(rest)
  let args = tokenize.split_on_commas(inner)
  case list.last(args) {
    Error(_) -> Column(name: alias, column_type: StringType, nullable: True)
    Ok(last_arg) ->
      case infer_literal_token_type(last_arg) {
        option.Some(t) -> Column(name: alias, column_type: t, nullable: False)
        option.None ->
          Column(name: alias, column_type: StringType, nullable: True)
      }
  }
}

/// Infer CASE expression type from THEN/ELSE branches.
fn infer_case_type(alias: String, tokens: List(Token)) -> Column {
  let #(branches, has_else) = extract_case_branches(tokens)
  case branches {
    [] -> Column(name: alias, column_type: StringType, nullable: True)
    _ -> {
      let types =
        list.filter_map(branches, fn(branch) {
          case branch {
            [Word(w)] ->
              case string.uppercase(w) == "NULL" {
                True -> Error(Nil)
                False -> Error(Nil)
              }
            _ ->
              case infer_literal_token_type(branch) {
                option.Some(t) -> Ok(t)
                option.None -> Error(Nil)
              }
          }
        })
      let has_null =
        list.any(branches, fn(b) {
          case b {
            [Word(w)] -> string.uppercase(w) == "NULL"
            _ -> False
          }
        })
      let null_count =
        list.count(branches, fn(b) {
          case b {
            [Word(w)] -> string.uppercase(w) == "NULL"
            _ -> False
          }
        })
      let has_unresolved =
        list.length(types) + null_count != list.length(branches)
      case has_unresolved {
        True -> Column(name: alias, column_type: StringType, nullable: True)
        False ->
          case types {
            [] -> Column(name: alias, column_type: StringType, nullable: True)
            [first, ..rest_types] ->
              case list.all(rest_types, fn(t) { t == first }) {
                True -> {
                  let nullable = has_null || !has_else
                  Column(name: alias, column_type: first, nullable: nullable)
                }
                False ->
                  Column(name: alias, column_type: StringType, nullable: True)
              }
          }
      }
    }
  }
}

/// Extract THEN and ELSE operand token lists from a CASE expression.
fn extract_case_branches(tokens: List(Token)) -> #(List(List(Token)), Bool) {
  // Skip initial CASE keyword
  let body = case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == "CASE" {
        True -> rest
        False -> tokens
      }
    _ -> tokens
  }
  // Strip trailing END
  let body = strip_trailing_end(body)
  do_extract_case_branches(body, [], [], False, 0, InNone)
}

type CaseScanState {
  InNone
  InWhen
  InThen
  InElse
}

fn do_extract_case_branches(
  tokens: List(Token),
  current: List(Token),
  acc: List(List(Token)),
  has_else: Bool,
  depth: Int,
  state: CaseScanState,
) -> #(List(List(Token)), Bool) {
  case tokens {
    [] -> {
      let final_acc = case state {
        InThen | InElse ->
          case current {
            [] -> acc
            _ -> [list.reverse(current), ..acc]
          }
        _ -> acc
      }
      #(list.reverse(final_acc), has_else)
    }
    [OpenParen, ..rest] ->
      do_extract_case_branches(
        rest,
        [OpenParen, ..current],
        acc,
        has_else,
        depth + 1,
        state,
      )
    [CloseParen, ..rest] ->
      do_extract_case_branches(
        rest,
        [CloseParen, ..current],
        acc,
        has_else,
        depth - 1,
        state,
      )
    [Word(w), ..rest] -> {
      let upper = string.uppercase(w)
      case depth == 0 {
        False ->
          // Nested CASE ... END tracking
          case upper == "CASE" {
            True ->
              do_extract_case_branches(
                rest,
                [Word(w), ..current],
                acc,
                has_else,
                depth + 1,
                state,
              )
            False ->
              case upper == "END" {
                True ->
                  do_extract_case_branches(
                    rest,
                    [Word(w), ..current],
                    acc,
                    has_else,
                    depth - 1,
                    state,
                  )
                False ->
                  do_extract_case_branches(
                    rest,
                    [Word(w), ..current],
                    acc,
                    has_else,
                    depth,
                    state,
                  )
              }
          }
        True ->
          case upper {
            "WHEN" -> {
              let new_acc = case state {
                InThen | InElse ->
                  case current {
                    [] -> acc
                    _ -> [list.reverse(current), ..acc]
                  }
                _ -> acc
              }
              do_extract_case_branches(rest, [], new_acc, has_else, 0, InWhen)
            }
            "THEN" ->
              do_extract_case_branches(rest, [], acc, has_else, 0, InThen)
            "ELSE" -> {
              let new_acc = case state {
                InThen ->
                  case current {
                    [] -> acc
                    _ -> [list.reverse(current), ..acc]
                  }
                _ -> acc
              }
              do_extract_case_branches(rest, [], new_acc, True, 0, InElse)
            }
            "CASE" ->
              do_extract_case_branches(
                rest,
                [Word(w), ..current],
                acc,
                has_else,
                depth + 1,
                state,
              )
            _ ->
              do_extract_case_branches(
                rest,
                [Word(w), ..current],
                acc,
                has_else,
                depth,
                state,
              )
          }
      }
    }
    [token, ..rest] ->
      do_extract_case_branches(
        rest,
        [token, ..current],
        acc,
        has_else,
        depth,
        state,
      )
  }
}

fn strip_trailing_end(tokens: List(Token)) -> List(Token) {
  case list.reverse(tokens) {
    [Word(w), ..rest] ->
      case string.uppercase(w) == "END" {
        True -> list.reverse(rest)
        False -> tokens
      }
    _ -> tokens
  }
}

fn infer_literal_token_type(tokens: List(Token)) -> Option(ColumnType) {
  case tokens {
    [Number(n)] ->
      case string.contains(n, ".") {
        True -> option.Some(query.FloatType)
        False -> option.Some(query.IntType)
      }
    [Minus, Number(n)] ->
      case string.contains(n, ".") {
        True -> option.Some(query.FloatType)
        False -> option.Some(query.IntType)
      }
    [StringLit(_)] -> option.Some(StringType)
    _ -> option.None
  }
}

// ---- Override application ----

pub fn apply_override(col: Column, override: NullabilityOverride) -> Column {
  case override {
    OverrideNonNull -> Column(..col, nullable: False)
    OverrideNullable -> Column(..col, nullable: True)
    OverrideNone -> col
  }
}
