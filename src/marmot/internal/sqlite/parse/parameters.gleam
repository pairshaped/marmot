//// WHERE/SET/VALUES parameter binding and binder discovery.

import gleam/bool
import gleam/list
import gleam/option.{type Option}
import gleam/string
import marmot/internal/query
import marmot/internal/sqlite/parse/util.{token_list_to_name}
import marmot/internal/sqlite/tokenize.{
  type Token, CloseParen, Dot, Eq, Ge, Gt, Le, Lt, Minus, Ne, OpenParen,
  ParamAnon, ParamNamed, Plus, QuotedId, Slash, Star, Word,
}

pub type Binder {
  Binder(name: String, binder_column: Option(String))
}

// ---- INSERT column parsing ----

/// Parse INSERT column names from tokens.
/// Finds the first (...) after the table name.
pub fn parse_insert_columns(tokens: List(Token)) -> List(String) {
  case find_first_paren_group(tokens) {
    option.None -> []
    option.Some(inner_tokens) ->
      tokenize.split_on_commas(inner_tokens)
      |> list.filter_map(fn(group) {
        case group {
          [Word(name)] -> Ok(name)
          [QuotedId(name)] -> Ok(name)
          _ -> Error(Nil)
        }
      })
  }
}

/// Find the first parenthesized group and return its inner tokens.
fn find_first_paren_group(tokens: List(Token)) -> Option(List(Token)) {
  case tokens {
    [] -> option.None
    [OpenParen, ..rest] -> {
      let #(inner, _remaining) = tokenize.collect_inside_parens(rest)
      option.Some(inner)
    }
    [_, ..rest] -> find_first_paren_group(rest)
  }
}

// ---- VALUES placeholder parsing ----

pub fn parse_values_placeholder_positions(tokens: List(Token)) -> List(Int) {
  case tokenize.split_at_keyword(tokens, "VALUES") {
    Error(_) -> []
    Ok(#(_, after)) ->
      case after {
        [OpenParen, ..rest] -> {
          let #(inner, _) = tokenize.collect_inside_parens(rest)
          let parts = tokenize.split_on_commas(inner)
          list.index_map(parts, fn(part, idx) { #(part, idx) })
          |> list.filter_map(fn(pair) {
            let #(part, idx) = pair
            case part {
              [ParamAnon] -> Ok(idx)
              [ParamNamed(_)] -> Ok(idx)
              _ -> Error(Nil)
            }
          })
        }
        _ -> []
      }
  }
}

// ---- UPDATE SET parsing ----

pub fn parse_update_set_columns(
  tokens: List(Token),
) -> List(#(String, String)) {
  case tokenize.split_at_keyword(tokens, "SET") {
    Error(_) -> []
    Ok(#(_, after_set)) -> {
      let set_part =
        tokenize.take_until_keywords(after_set, ["WHERE", "RETURNING"])
      tokenize.split_on_commas(set_part)
      |> list.filter_map(fn(assignment) {
        // Split on = to get col = rhs
        case split_tokens_on_eq(assignment) {
          Error(_) -> Error(Nil)
          Ok(#(lhs_tokens, rhs_tokens)) -> {
            let col = token_list_to_name(lhs_tokens)
            case has_param_token(rhs_tokens) {
              True ->
                case find_named_param_in_tokens(rhs_tokens) {
                  option.Some(param_name) -> Ok(#(param_name, col))
                  option.None -> Ok(#(col, col))
                }
              False -> Error(Nil)
            }
          }
        }
      })
    }
  }
}

fn split_tokens_on_eq(
  tokens: List(Token),
) -> Result(#(List(Token), List(Token)), Nil) {
  do_split_on_eq(tokens, [])
}

fn do_split_on_eq(
  tokens: List(Token),
  before: List(Token),
) -> Result(#(List(Token), List(Token)), Nil) {
  case tokens {
    [] -> Error(Nil)
    [Eq, ..rest] -> Ok(#(list.reverse(before), rest))
    [token, ..rest] -> do_split_on_eq(rest, [token, ..before])
  }
}

fn has_param_token(tokens: List(Token)) -> Bool {
  list.any(tokens, fn(t) {
    case t {
      ParamAnon | ParamNamed(_) -> True
      _ -> False
    }
  })
}

fn find_named_param_in_tokens(tokens: List(Token)) -> Option(String) {
  list.find_map(tokens, fn(t) {
    case t {
      ParamNamed(name) -> Ok(name)
      _ -> Error(Nil)
    }
  })
  |> option.from_result
}

// ---- WHERE parsing ----

pub fn parse_where_columns(tokens: List(Token)) -> List(#(String, String)) {
  case tokenize.split_at_keyword(tokens, "WHERE") {
    Error(_) -> []
    Ok(#(_, where_tokens)) -> {
      let where_part = tokenize.take_until_keywords(where_tokens, ["RETURNING"])
      tokenize.split_on_and_or(where_part)
      |> list.flat_map(parse_where_condition)
    }
  }
}

fn parse_where_condition(tokens: List(Token)) -> List(#(String, String)) {
  case has_param_token(tokens) {
    False -> []
    True ->
      case has_subquery(tokens) {
        True ->
          extract_all_named_params_from_tokens(tokens)
          |> list.map(fn(name) { #(name, name) })
        False -> parse_simple_where_condition(tokens)
      }
  }
}

fn has_subquery(tokens: List(Token)) -> Bool {
  do_has_subquery(tokens)
}

fn do_has_subquery(tokens: List(Token)) -> Bool {
  case tokens {
    [] -> False
    // EXISTS (SELECT ...) or NOT EXISTS (SELECT ...)
    [Word(w), OpenParen, Word(s), ..] -> {
      let uw = string.uppercase(w)
      let us = string.uppercase(s)
      case { uw == "IN" || uw == "EXISTS" } && us == "SELECT" {
        True -> True
        False ->
          case uw == "NOT" {
            True ->
              // Peek at the next word inside parens: NOT EXISTS (SELECT ...)
              case list.drop(tokens, 2) {
                [Word(next), ..] ->
                  case string.uppercase(next) == "EXISTS" {
                    True -> True
                    False -> do_has_subquery(list.drop(tokens, 1))
                  }
                _ -> do_has_subquery(list.drop(tokens, 1))
              }
            False -> do_has_subquery(list.drop(tokens, 1))
          }
      }
    }
    // = (SELECT ...) or < (SELECT ...) or > (SELECT ...) etc.
    [Eq, OpenParen, Word(w), ..]
    | [Ne, OpenParen, Word(w), ..]
    | [Lt, OpenParen, Word(w), ..]
    | [Gt, OpenParen, Word(w), ..]
    | [Le, OpenParen, Word(w), ..]
    | [Ge, OpenParen, Word(w), ..] ->
      case string.uppercase(w) == "SELECT" {
        True -> True
        False -> do_has_subquery(list.drop(tokens, 1))
      }
    [_, ..rest] -> do_has_subquery(rest)
  }
}

fn parse_simple_where_condition(
  tokens: List(Token),
) -> List(#(String, String)) {
  // Find the operator and extract LHS column
  let lhs_result = extract_lhs_column(tokens)
  case lhs_result {
    option.None -> []
    option.Some(lhs) ->
      // Check if LHS contains a named param (arithmetic expression)
      case find_named_param_in_name(lhs) {
        option.Some(param_name) -> {
          let lookup = extract_column_before_param(lhs)
          [#(param_name, normalize_column_ref(lookup))]
        }
        option.None -> {
          let col_name = normalize_column_ref(lhs)
          // Extract all named params from the RHS and map each to the LHS column.
          // Handles BETWEEN @from AND @to and single-param cases like col = @val.
          let rhs_params = extract_all_named_params_from_tokens(tokens)
          case rhs_params {
            [] -> [#(col_name, col_name)]
            params -> list.map(params, fn(p) { #(p, col_name) })
          }
        }
      }
  }
}

fn extract_lhs_column(tokens: List(Token)) -> Option(String) {
  // Walk tokens to find the comparison operator, collect LHS
  do_extract_lhs(tokens, [])
}

fn do_extract_lhs(tokens: List(Token), acc: List(Token)) -> Option(String) {
  case tokens {
    [] -> option.None
    // Comparison operators mark the boundary
    [Eq, ..] | [Ne, ..] | [Lt, ..] | [Gt, ..] | [Le, ..] | [Ge, ..] ->
      case acc {
        [] -> option.None
        _ -> option.Some(tokens_to_column_name(list.reverse(acc)))
      }
    // Keyword operators. NOT before LIKE/IN/BETWEEN is treated as part of the
    // operator boundary (e.g., NOT LIKE, NOT IN, NOT BETWEEN).
    [Word(w), ..] -> {
      let upper = string.uppercase(w)
      case upper == "NOT" {
        True ->
          case list.drop(tokens, 1) {
            [Word(next), ..] -> {
              let next_upper = string.uppercase(next)
              case
                next_upper == "LIKE"
                || next_upper == "IN"
                || next_upper == "BETWEEN"
              {
                True ->
                  case acc {
                    [] -> option.None
                    _ -> option.Some(tokens_to_column_name(list.reverse(acc)))
                  }
                False -> do_extract_lhs(list.drop(tokens, 1), [Word(w), ..acc])
              }
            }
            _ -> do_extract_lhs(list.drop(tokens, 1), [Word(w), ..acc])
          }
        False ->
          case
            upper == "LIKE"
            || upper == "IN"
            || upper == "IS"
            || upper == "BETWEEN"
          {
            True ->
              case acc {
                [] -> option.None
                _ -> option.Some(tokens_to_column_name(list.reverse(acc)))
              }
            False -> do_extract_lhs(list.drop(tokens, 1), [Word(w), ..acc])
          }
      }
    }
    [token, ..rest] -> do_extract_lhs(rest, [token, ..acc])
  }
}

fn tokens_to_column_name(tokens: List(Token)) -> String {
  tokens
  |> list.filter_map(fn(t) {
    case t {
      Word(text) -> Ok(text)
      QuotedId(text) -> Ok(text)
      Dot -> Ok(".")
      Plus -> Ok("+")
      Minus -> Ok("-")
      Star -> Ok("*")
      Slash -> Ok("/")
      ParamNamed(name) -> Ok("@" <> name)
      _ -> Error(Nil)
    }
  })
  |> string.join("")
}

fn find_named_param_in_name(s: String) -> Option(String) {
  case string.split_once(s, "@") {
    Ok(#(_, after)) -> option.Some(take_ident_chars(after))
    Error(_) ->
      case string.split_once(s, ":") {
        Ok(#(_, after)) -> option.Some(take_ident_chars(after))
        Error(_) ->
          case string.split_once(s, "$") {
            Ok(#(_, after)) -> option.Some(take_ident_chars(after))
            Error(_) -> option.None
          }
      }
  }
}

fn take_ident_chars(s: String) -> String {
  string.to_graphemes(s)
  |> list.take_while(is_ident_char)
  |> string.join("")
}

fn is_ident_char(c: String) -> Bool {
  query.is_sql_ident_char(c)
}

fn extract_column_before_param(s: String) -> String {
  // Strip from @ or : or $ onward, trim trailing arithmetic op
  let before = case string.split_once(s, "@") {
    Ok(#(b, _)) -> b
    Error(_) ->
      case string.split_once(s, ":") {
        Ok(#(b, _)) -> b
        Error(_) ->
          case string.split_once(s, "$") {
            Ok(#(b, _)) -> b
            Error(_) -> s
          }
      }
  }
  let trimmed = string.trim(before)
  strip_trailing_arithmetic_op(trimmed)
}

fn strip_trailing_arithmetic_op(s: String) -> String {
  use <- bool.guard(
    when: !string.ends_with(s, "+")
      && !string.ends_with(s, "-")
      && !string.ends_with(s, "*")
      && !string.ends_with(s, "/"),
    return: s,
  )
  string.drop_end(s, 1) |> string.trim
}

fn extract_all_named_params_from_tokens(tokens: List(Token)) -> List(String) {
  list.filter_map(tokens, fn(t) {
    case t {
      ParamNamed(name) -> Ok(name)
      _ -> Error(Nil)
    }
  })
}

// ---- Parameter binder scanning ----

/// Find parameter binders in a token list.
pub fn find_param_binders(tokens: List(Token)) -> List(Binder) {
  do_find_param_binders(tokens, [], [])
}

fn do_find_param_binders(
  tokens: List(Token),
  prev: List(Token),
  acc: List(Binder),
) -> List(Binder) {
  case tokens {
    [] -> list.reverse(acc)
    [ParamAnon, ..rest] -> {
      let col = extract_column_from_prev(prev)
      case col {
        option.None -> do_find_param_binders(rest, [ParamAnon, ..prev], acc)
        option.Some(name) -> {
          let bare = strip_table_prefix(name)
          do_find_param_binders(rest, [ParamAnon, ..prev], [
            Binder(name: bare, binder_column: option.Some(name)),
            ..acc
          ])
        }
      }
    }
    [ParamNamed(name), ..rest] ->
      case list.find(acc, fn(b) { b.name == name }) {
        Ok(_) -> do_find_param_binders(rest, [ParamNamed(name), ..prev], acc)
        Error(_) -> {
          let column = extract_column_from_prev(prev)
          do_find_param_binders(rest, [ParamNamed(name), ..prev], [
            Binder(name: name, binder_column: column),
            ..acc
          ])
        }
      }
    [token, ..rest] -> do_find_param_binders(rest, [token, ..prev], acc)
  }
}

/// Look backward in the reversed previous-token list for a column
/// before a comparison operator.
fn extract_column_from_prev(prev: List(Token)) -> Option(String) {
  // prev is reversed: for "col = ?", prev is [Eq, Word("col")].
  // For "col BETWEEN ?", prev is [Word("BETWEEN"), Word("col")].
  // Always skip the operator first, then extract the column after it.
  case skip_operator_in_prev(prev) {
    option.Some(after_op) -> extract_column_word(after_op)
    option.None ->
      // No recognized operator; check for quoted identifier
      case prev {
        [QuotedId(col), ..] -> option.Some(col)
        _ -> option.None
      }
  }
}

fn skip_operator_in_prev(prev: List(Token)) -> Option(List(Token)) {
  case prev {
    // Symbolic operators
    [Eq, ..rest] -> option.Some(rest)
    [Ne, ..rest] -> option.Some(rest)
    [Lt, ..rest] -> option.Some(rest)
    [Gt, ..rest] -> option.Some(rest)
    [Le, ..rest] -> option.Some(rest)
    [Ge, ..rest] -> option.Some(rest)
    // Keyword operators (reversed, so just the word)
    [Word(w), ..rest] -> {
      let upper = string.uppercase(w)
      case
        upper == "LIKE"
        || upper == "IS"
        || upper == "BETWEEN"
        || upper == "AND"
        || upper == "NOT"
      {
        True ->
          // IS NOT: skip both
          case upper == "NOT" {
            True ->
              case rest {
                [Word(w2), ..rest2] ->
                  case string.uppercase(w2) == "IS" {
                    True -> option.Some(rest2)
                    False -> option.Some(rest)
                  }
                _ -> option.Some(rest)
              }
            False ->
              // AND after BETWEEN: skip AND, then look for BETWEEN
              case upper == "AND" {
                True -> skip_between_and(rest)
                False -> option.Some(rest)
              }
          }
        False -> option.None
      }
    }
    _ -> option.None
  }
}

fn skip_between_and(prev: List(Token)) -> Option(List(Token)) {
  // After AND in "col BETWEEN ? AND ?", we need to find BETWEEN
  // and extract the column before it.
  // prev at this point is everything before "AND"
  // Walk backward looking for BETWEEN keyword
  do_skip_between(prev, 0)
}

fn do_skip_between(prev: List(Token), skipped: Int) -> Option(List(Token)) {
  case prev {
    [] -> option.None
    [Word(w), ..rest] ->
      case string.uppercase(w) == "BETWEEN" {
        True -> option.Some(rest)
        False -> do_skip_between(rest, skipped + 1)
      }
    [_, ..rest] -> do_skip_between(rest, skipped + 1)
  }
}

fn extract_column_word(prev: List(Token)) -> Option(String) {
  case prev {
    // Skip NOT that precedes a keyword operator (e.g., NOT LIKE, NOT IN,
    // NOT BETWEEN). prev is reversed, so NOT is the first word after the op.
    [Word("NOT"), Word(col), Dot, Word(table), ..] ->
      option.Some(table <> "." <> col)
    [Word("NOT"), Word(col), ..] -> option.Some(col)
    [Word("NOT"), QuotedId(col), ..] -> option.Some(col)
    [Word("NOT"), CloseParen, ..rest] -> {
      let inner = collect_inside_reversed_parens(rest, 1, [])
      case inner {
        option.Some(tokens) -> extract_column_word(list.reverse(tokens))
        option.None -> option.None
      }
    }
    // table.col (reversed prev: [Word(col), Dot, Word(table), ...])
    [Word(col), Dot, Word(table), ..] -> option.Some(table <> "." <> col)
    // Simple column
    [Word(col), ..] -> option.Some(col)
    [QuotedId(col), ..] -> option.Some(col)
    // Function-wrapped: CloseParen is first in reversed prev
    [CloseParen, ..rest] -> {
      // Walk backward through parens to find inner column
      let inner = collect_inside_reversed_parens(rest, 1, [])
      case inner {
        option.Some(tokens) -> extract_column_word(list.reverse(tokens))
        option.None -> option.None
      }
    }
    _ -> option.None
  }
}

fn collect_inside_reversed_parens(
  prev: List(Token),
  depth: Int,
  acc: List(Token),
) -> Option(List(Token)) {
  case depth {
    0 -> option.Some(acc)
    _ ->
      case prev {
        [] -> option.None
        [OpenParen, ..rest] ->
          collect_inside_reversed_parens(rest, depth - 1, acc)
        [CloseParen, ..rest] ->
          collect_inside_reversed_parens(rest, depth + 1, acc)
        [token, ..rest] ->
          collect_inside_reversed_parens(rest, depth, [token, ..acc])
      }
  }
}

fn strip_table_prefix(name: String) -> String {
  case string.split_once(name, ".") {
    Ok(#(_, col)) -> col
    Error(_) -> name
  }
}

// ---- Subquery table extraction ----

/// Find all tables referenced after FROM keywords anywhere in the tokens.
pub fn find_all_subquery_tables(tokens: List(Token)) -> List(String) {
  do_find_subquery_tables(tokens, [])
}

fn do_find_subquery_tables(
  tokens: List(Token),
  acc: List(String),
) -> List(String) {
  case tokens {
    [] -> list.reverse(acc)
    [Word(w), ..rest] ->
      case string.uppercase(w) == "FROM" {
        True -> {
          let table = tokenize.first_word(rest)
          case table {
            "" -> do_find_subquery_tables(rest, acc)
            name -> do_find_subquery_tables(rest, [name, ..acc])
          }
        }
        False -> do_find_subquery_tables(rest, acc)
      }
    [_, ..rest] -> do_find_subquery_tables(rest, acc)
  }
}

// ---- Column reference normalization ----

/// Strip table prefix and function wrappers from a column reference.
pub fn normalize_column_ref(raw: String) -> String {
  let name = case string.split_once(raw, ".") {
    Ok(#(_, col)) -> col
    Error(_) -> raw
  }
  do_normalize_column_ref(name)
}

fn do_normalize_column_ref(name: String) -> String {
  case string.split_once(name, "(") {
    Ok(#(_, rest)) ->
      case string.split_once(rest, ")") {
        Ok(#(inner, _)) -> do_normalize_column_ref(string.trim(inner))
        Error(_) -> name
      }
    Error(_) -> name
  }
}

// ---- Utility ----

/// Check if a token is a named parameter prefix.
pub fn starts_with_param_prefix(s: String) -> Bool {
  string.starts_with(s, "@")
  || string.starts_with(s, ":")
  || string.starts_with(s, "$")
}
