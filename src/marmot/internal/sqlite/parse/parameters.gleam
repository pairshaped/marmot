//// WHERE/SET/VALUES parameter binding and binder discovery.

import gleam/bool
import gleam/list
import gleam/option.{type Option}
import gleam/result
import gleam/string
import marmot/internal/query
import marmot/internal/sqlite/parse/util
import marmot/internal/sqlite/tokenize.{
  type Token, Dot, Eq, Ge, Gt, Le, Lt, Minus, Ne, OpenParen, ParamAnon,
  ParamNamed, Plus, QuotedId, Slash, Star, Word,
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

/// Walk VALUES(...) tokens to find the positional index of each `?` or `@name`
/// placeholder. Splits on top-level commas inside the VALUES parentheses
/// (paren depth is tracked, so commas inside function calls and subqueries
/// are handled correctly). Returns the 0-based position of each parameter token.
/// Only detects parameters when the entire VALUES item is exactly a param token;
/// params nested inside expressions within a VALUES item are missed.
pub fn parse_values_placeholder_positions(tokens: List(Token)) -> List(Int) {
  case tokenize.split_at_keyword(tokens, "VALUES") {
    Error(_) -> []
    Ok(#(_, after)) -> parse_values_placeholders_after_keyword(after)
  }
}

fn parse_values_placeholders_after_keyword(tokens: List(Token)) -> List(Int) {
  case tokens {
    [OpenParen, ..rest] -> {
      let #(inner, _) = tokenize.collect_inside_parens(rest)
      tokenize.split_on_commas(inner)
      |> list.index_map(fn(part, idx) { #(part, idx) })
      |> list.filter_map(values_placeholder_position)
    }
    _ -> []
  }
}

fn values_placeholder_position(pair: #(List(Token), Int)) -> Result(Int, Nil) {
  let #(part, idx) = pair
  case part {
    [ParamAnon] | [ParamNamed(_)] -> Ok(idx)
    _ -> Error(Nil)
  }
}

// ---- UPDATE SET parsing ----

/// Parse a SET body slice (no SET keyword, no following WHERE/RETURNING).
/// The whole-statement helper `parse_update_set_columns/1` is now a shim.
pub fn parse_update_set_body(
  set_tokens: List(Token),
) -> List(#(String, String)) {
  tokenize.split_on_commas(set_tokens)
  |> list.filter_map(parse_update_set_assignment)
}

fn parse_update_set_assignment(
  assignment: List(Token),
) -> Result(#(String, String), Nil) {
  use split <- result.try(split_tokens_on_eq(assignment))
  let #(lhs_tokens, rhs_tokens) = split
  use <- bool.guard(!has_param_token(rhs_tokens), Error(Nil))

  let col = util.token_list_to_name(lhs_tokens)
  case find_named_param_in_tokens(rhs_tokens) {
    option.Some(param_name) -> Ok(#(param_name, col))
    option.None -> Ok(#(col, col))
  }
}

pub fn parse_update_set_columns(
  tokens: List(Token),
) -> List(#(String, String)) {
  case tokenize.split_at_keyword(tokens, "SET") {
    Error(_) -> []
    Ok(#(_, after_set)) -> {
      let set_part =
        tokenize.take_until_keywords(after_set, ["WHERE", "RETURNING"])
      parse_update_set_body(set_part)
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

/// Parse a WHERE body slice (no WHERE keyword, no RETURNING/etc beyond).
/// The whole-statement helper `parse_where_columns/1` is now a shim.
pub fn parse_where_body(where_tokens: List(Token)) -> List(#(String, String)) {
  tokenize.split_on_and_or(where_tokens)
  |> list.flat_map(parse_where_condition)
}

pub fn parse_where_columns(tokens: List(Token)) -> List(#(String, String)) {
  case tokenize.split_at_keyword(tokens, "WHERE") {
    Error(_) -> []
    Ok(#(_, where_tokens)) -> {
      let where_part = tokenize.take_until_keywords(where_tokens, ["RETURNING"])
      parse_where_body(where_part)
    }
  }
}

fn parse_where_condition(tokens: List(Token)) -> List(#(String, String)) {
  use <- bool.guard(!has_param_token(tokens), [])

  case has_subquery(tokens) {
    True ->
      extract_all_named_params_from_tokens(tokens)
      |> list.map(fn(name) { #(name, name) })
    False -> parse_simple_where_condition(tokens)
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
      has_subquery_after_word_paren(tokens, uw, us)
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

fn has_subquery_after_word_paren(
  tokens: List(Token),
  upper_word: String,
  upper_second: String,
) -> Bool {
  use <- bool.guard(
    { upper_word == "IN" || upper_word == "EXISTS" } && upper_second == "SELECT",
    True,
  )
  has_not_exists_subquery(tokens, upper_word)
}

fn has_not_exists_subquery(tokens: List(Token), upper_word: String) -> Bool {
  use <- bool.guard(upper_word != "NOT", do_has_subquery(list.drop(tokens, 1)))

  // Peek at the next word inside parens: NOT EXISTS (SELECT ...)
  case list.drop(tokens, 2) {
    [Word(next), ..] ->
      case string.uppercase(next) == "EXISTS" {
        True -> True
        False -> do_has_subquery(list.drop(tokens, 1))
      }
    _ -> do_has_subquery(list.drop(tokens, 1))
  }
}

fn parse_simple_where_condition(
  tokens: List(Token),
) -> List(#(String, String)) {
  let lhs_result = extract_lhs_column(tokens)
  case lhs_result {
    option.None -> []
    option.Some(lhs) -> parse_simple_where_lhs(lhs, tokens)
  }
}

fn parse_simple_where_lhs(
  lhs: String,
  tokens: List(Token),
) -> List(#(String, String)) {
  // Check if LHS contains a named param (arithmetic expression)
  case find_named_param_in_name(lhs) {
    option.Some(param_name) -> {
      let lookup = extract_column_before_param(lhs)
      [#(param_name, normalize_column_ref(lookup))]
    }
    option.None -> parse_simple_where_lhs_column(lhs, tokens)
  }
}

fn parse_simple_where_lhs_column(
  lhs: String,
  tokens: List(Token),
) -> List(#(String, String)) {
  let col_name = normalize_column_ref(lhs)
  // Extract all named params from the RHS and map each to the LHS column.
  // Handles BETWEEN @from AND @to and single-param cases like col = @val.
  let rhs_params = extract_all_named_params_from_tokens(tokens)
  case rhs_params {
    [] -> [#(col_name, col_name)]
    params -> list.map(params, fn(p) { #(p, col_name) })
  }
}

fn extract_lhs_column(tokens: List(Token)) -> Option(String) {
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
      extract_lhs_from_word(tokens, w, upper, acc)
    }
    [token, ..rest] -> do_extract_lhs(rest, [token, ..acc])
  }
}

fn extract_lhs_from_word(
  tokens: List(Token),
  word: String,
  upper: String,
  acc: List(Token),
) -> Option(String) {
  case upper == "NOT" {
    True -> extract_lhs_from_not_operator(tokens, word, acc)
    False -> extract_lhs_from_keyword_operator(tokens, word, upper, acc)
  }
}

fn extract_lhs_from_not_operator(
  tokens: List(Token),
  word: String,
  acc: List(Token),
) -> Option(String) {
  case list.drop(tokens, 1) {
    [Word(next), ..] -> {
      let next_upper = string.uppercase(next)
      case is_not_keyword_operator(next_upper) {
        True -> column_name_from_lhs_acc(acc)
        False -> do_extract_lhs(list.drop(tokens, 1), [Word(word), ..acc])
      }
    }
    _ -> do_extract_lhs(list.drop(tokens, 1), [Word(word), ..acc])
  }
}

fn extract_lhs_from_keyword_operator(
  tokens: List(Token),
  word: String,
  upper: String,
  acc: List(Token),
) -> Option(String) {
  case is_keyword_operator(upper) {
    True -> column_name_from_lhs_acc(acc)
    False -> do_extract_lhs(list.drop(tokens, 1), [Word(word), ..acc])
  }
}

fn is_not_keyword_operator(upper: String) -> Bool {
  upper == "LIKE" || upper == "IN" || upper == "BETWEEN"
}

fn is_keyword_operator(upper: String) -> Bool {
  is_not_keyword_operator(upper) || upper == "IS"
}

fn column_name_from_lhs_acc(acc: List(Token)) -> Option(String) {
  case acc {
    [] -> option.None
    _ -> option.Some(tokens_to_column_name(list.reverse(acc)))
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
