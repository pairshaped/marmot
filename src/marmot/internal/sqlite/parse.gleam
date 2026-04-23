import gleam/list
import gleam/option.{type Option}
import gleam/string
import marmot/internal/query.{type Column, type ColumnType, Column, StringType}
import marmot/internal/sqlite/tokenize.{
  type Token, CloseParen, Comma, Dot, Eq, Ge, Gt, Le, Lt, Minus, Ne,
  NullOverride, NullableOverride, Number, OpenParen, ParamAnon, ParamNamed, Plus,
  QuotedId, Slash, Star, StringLit, Word,
}

// ---- Types ----

pub type StatementType {
  Select
  Insert
  Update
  Delete
  Other
}

pub type NullabilityOverride {
  OverrideNonNull
  OverrideNullable
  OverrideNone
}

pub type SelectItem {
  SelectItem(
    alias: String,
    tokens: List(Token),
    bare_column: Option(String),
    override: NullabilityOverride,
  )
}

pub type Binder {
  Binder(name: String, binder_column: Option(String))
}

// ---- String operations (pre-tokenization) ----

/// Normalize SQL whitespace: strip line comments, convert newlines/tabs to
/// spaces, collapse runs, and trim.
pub fn normalize_sql_whitespace(sql: String) -> String {
  sql
  |> query.strip_comments
  |> string.replace("\r\n", " ")
  |> string.replace("\r", " ")
  |> string.replace("\n", " ")
  |> string.replace("\t", " ")
  |> query.collapse_spaces
  |> string.trim
}

/// Strip Marmot-specific `!`/`?` nullability suffixes from alias names
/// before sending SQL to SQLite's EXPLAIN. Kept as a string operation
/// because its output is sent to SQLite, not analyzed by Marmot.
pub fn strip_nullability_suffixes(sql: String) -> String {
  let graphemes = string.to_graphemes(sql)
  do_strip_suffixes(graphemes, [], "", False, False)
  |> list.reverse
  |> string.join("")
}

fn do_strip_suffixes(
  chars: List(String),
  acc: List(String),
  prev: String,
  in_single: Bool,
  in_double: Bool,
) -> List(String) {
  case chars {
    [] -> acc
    ["'", ..rest] ->
      case in_double {
        True -> do_strip_suffixes(rest, ["'", ..acc], "'", in_single, True)
        False ->
          case in_single {
            True ->
              case rest {
                ["'", ..rest2] ->
                  do_strip_suffixes(rest2, ["'", "'", ..acc], "'", True, False)
                _ -> do_strip_suffixes(rest, ["'", ..acc], "'", False, False)
              }
            False -> do_strip_suffixes(rest, ["'", ..acc], "'", True, False)
          }
      }
    ["\"", ..rest] ->
      case in_single {
        True -> do_strip_suffixes(rest, ["\"", ..acc], "\"", True, in_double)
        False -> do_strip_suffixes(rest, ["\"", ..acc], "\"", False, !in_double)
      }
    [ch, ..rest] ->
      case in_single || in_double {
        True -> do_strip_suffixes(rest, [ch, ..acc], ch, in_single, in_double)
        False ->
          case { ch == "!" || ch == "?" } && is_ident_char(prev) {
            True -> {
              let next_ok = case rest {
                [] -> True
                [c, ..] ->
                  c == " " || c == "," || c == ")" || c == "\t" || c == "\n"
              }
              case next_ok {
                True -> do_strip_suffixes(rest, acc, prev, in_single, in_double)
                False ->
                  do_strip_suffixes(rest, [ch, ..acc], ch, in_single, in_double)
              }
            }
            False ->
              do_strip_suffixes(rest, [ch, ..acc], ch, in_single, in_double)
          }
      }
  }
}

fn is_ident_char(c: String) -> Bool {
  query.is_sql_ident_char(c)
}

/// Escape double quotes in an identifier to prevent SQL injection.
pub fn quote_identifier(name: String) -> String {
  string.replace(name, "\"", "\"\"")
}

// ---- Statement classification ----

pub fn classify_statement(tokens: List(Token)) -> StatementType {
  case tokens {
    [Word(w), ..] ->
      case string.uppercase(w) {
        "SELECT" -> Select
        "INSERT" -> Insert
        "UPDATE" -> Update
        "DELETE" -> Delete
        _ -> Other
      }
    _ -> Other
  }
}

// ---- SELECT list parsing ----

/// Parse the SELECT list from tokens.
pub fn parse_select_items(tokens: List(Token)) -> List(SelectItem) {
  let main_tokens = skip_with_prefix(tokens)
  let after_select = skip_select_keyword(main_tokens)
  case after_select {
    [] -> []
    _ -> {
      let select_tokens =
        tokenize.take_until_keywords(after_select, [
          "FROM", "WHERE", "GROUP", "ORDER", "LIMIT",
        ])
      tokenize.split_on_commas(select_tokens)
      |> list.map(parse_select_item)
    }
  }
}

fn skip_select_keyword(tokens: List(Token)) -> List(Token) {
  case tokens {
    [Word(w), Word(d), ..rest] -> {
      let uw = string.uppercase(w)
      let ud = string.uppercase(d)
      case uw == "SELECT" && ud == "DISTINCT" {
        True -> rest
        False ->
          case uw == "SELECT" {
            True -> [Word(d), ..rest]
            False -> tokens
          }
      }
    }
    [Word(w), ..rest] ->
      case string.uppercase(w) == "SELECT" {
        True -> rest
        False -> tokens
      }
    _ -> tokens
  }
}

/// Parse a single SELECT-list item from tokens.
pub fn parse_select_item(tokens: List(Token)) -> SelectItem {
  let #(expr_tokens, alias_tokens, has_as) = case
    tokenize.split_at_last_keyword(tokens, "AS")
  {
    Ok(#(before, after)) -> #(before, after, True)
    Error(_) -> #(tokens, tokens, False)
  }

  let alias_text = case has_as {
    True -> token_list_to_name(alias_tokens)
    False ->
      // No explicit AS. For "table.col", use "col" as alias.
      case list.reverse(tokens) {
        [Word(name), Dot, _, ..] -> name
        _ -> token_list_to_name(tokens)
      }
  }

  let bare_column = detect_bare_column(expr_tokens)

  // Check for nullability override at end of alias tokens
  let #(final_alias, override) = case list.last(alias_tokens) {
    Ok(NullOverride) -> #(
      strip_override_from_alias(alias_text),
      OverrideNonNull,
    )
    Ok(NullableOverride) -> #(
      strip_override_from_alias(alias_text),
      OverrideNullable,
    )
    _ ->
      // Also check if the non-AS alias ends with override
      case has_as {
        False ->
          case list.last(tokens) {
            Ok(NullOverride) -> #(
              strip_override_from_alias(alias_text),
              OverrideNonNull,
            )
            Ok(NullableOverride) -> #(
              strip_override_from_alias(alias_text),
              OverrideNullable,
            )
            _ -> #(alias_text, OverrideNone)
          }
        True -> #(alias_text, OverrideNone)
      }
  }

  SelectItem(
    alias: final_alias,
    tokens: expr_tokens,
    bare_column: bare_column,
    override: override,
  )
}

fn strip_override_from_alias(alias: String) -> String {
  // Override tokens are separate, so the alias text from tokens
  // shouldn't include them. But if it does (edge case), strip it.
  case string.ends_with(alias, "!") || string.ends_with(alias, "?") {
    True -> string.drop_end(alias, 1)
    False -> alias
  }
}

fn detect_bare_column(tokens: List(Token)) -> Option(String) {
  case tokens {
    [Word(name)] -> option.Some(name)
    [Word(_), Dot, Word(name)] -> option.Some(name)
    _ -> option.None
  }
}

/// Get a clean name from a token list (first Word or QuotedId).
fn token_list_to_name(tokens: List(Token)) -> String {
  case tokens {
    [] -> ""
    [Word(t), ..] -> t
    [QuotedId(t), ..] -> t
    [NullOverride, ..] -> ""
    [NullableOverride, ..] -> ""
    _ ->
      tokens
      |> list.filter(fn(t) {
        case t {
          NullOverride | NullableOverride -> False
          _ -> True
        }
      })
      |> list.map(tokenize.token_text)
      |> string.join("")
  }
}

// ---- CTE prefix skipping ----

fn skip_with_prefix(tokens: List(Token)) -> List(Token) {
  case tokens {
    [Word(w), Word(r), ..rest] -> {
      let uw = string.uppercase(w)
      let ur = string.uppercase(r)
      case uw == "WITH" && ur == "RECURSIVE" {
        True -> skip_cte_definitions(rest)
        False ->
          case uw == "WITH" {
            True -> skip_cte_definitions([Word(r), ..rest])
            False -> tokens
          }
      }
    }
    [Word(w), ..rest] ->
      case string.uppercase(w) == "WITH" {
        True -> skip_cte_definitions(rest)
        False -> tokens
      }
    _ -> tokens
  }
}

fn skip_cte_definitions(tokens: List(Token)) -> List(Token) {
  case tokenize.find_keyword(tokens, "AS") {
    option.None -> tokens
    option.Some(as_idx) -> {
      let after_as = list.drop(tokens, as_idx + 1)
      case after_as {
        [OpenParen, ..rest] -> {
          let remaining = tokenize.skip_matching_paren(rest, 1)
          case remaining {
            [Comma, ..rest2] -> skip_cte_definitions(rest2)
            _ -> remaining
          }
        }
        _ -> tokens
      }
    }
  }
}

// ---- FROM clause parsing ----

/// Parse the FROM clause to get table names.
pub fn parse_from_tables(tokens: List(Token)) -> List(String) {
  let main_tokens = skip_with_prefix(tokens)
  let after_select = skip_select_keyword(main_tokens)
  case tokenize.split_at_keyword(after_select, "FROM") {
    Error(_) -> []
    Ok(#(_, from_tokens)) -> {
      let from_part =
        tokenize.take_until_keywords(from_tokens, [
          "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "RETURNING",
        ])
      extract_table_names_from_from(from_part)
    }
  }
}

fn extract_table_names_from_from(tokens: List(Token)) -> List(String) {
  split_on_joins(tokens)
  |> list.filter_map(fn(segment) {
    // Strip ON clause: take tokens until ON keyword
    let before_on = tokenize.take_until_keywords(segment, ["ON"])
    case before_on {
      [Word(name), ..] -> Ok(name)
      [QuotedId(name), ..] -> Ok(name)
      _ -> Error(Nil)
    }
  })
}

fn split_on_joins(tokens: List(Token)) -> List(List(Token)) {
  do_split_on_joins(tokens, [], [])
}

fn do_split_on_joins(
  tokens: List(Token),
  current: List(Token),
  acc: List(List(Token)),
) -> List(List(Token)) {
  case tokens {
    [] ->
      case current {
        [] -> list.reverse(acc)
        _ -> list.reverse([list.reverse(current), ..acc])
      }
    [Word(w), ..rest] -> {
      let upper = string.uppercase(w)
      case upper == "JOIN" {
        True -> {
          let cleaned = strip_trailing_join_modifiers(current)
          case cleaned {
            [] -> do_split_on_joins(rest, [], acc)
            _ -> do_split_on_joins(rest, [], [list.reverse(cleaned), ..acc])
          }
        }
        False -> do_split_on_joins(rest, [Word(w), ..current], acc)
      }
    }
    [token, ..rest] -> do_split_on_joins(rest, [token, ..current], acc)
  }
}

fn strip_trailing_join_modifiers(reversed_tokens: List(Token)) -> List(Token) {
  case reversed_tokens {
    [Word(w), ..rest] -> {
      let upper = string.uppercase(w)
      case
        upper == "LEFT"
        || upper == "RIGHT"
        || upper == "INNER"
        || upper == "CROSS"
        || upper == "OUTER"
      {
        True -> strip_trailing_join_modifiers(rest)
        False -> reversed_tokens
      }
    }
    _ -> reversed_tokens
  }
}

// ---- Table name extraction ----

/// Extract table name after a keyword (INTO, UPDATE, FROM for DELETE).
fn extract_name_after_keyword(tokens: List(Token), keyword: String) -> String {
  case tokenize.split_at_keyword(tokens, keyword) {
    Ok(#(_, after)) -> tokenize.first_word(after)
    Error(_) -> ""
  }
}

pub fn parse_insert_table_name(tokens: List(Token)) -> String {
  extract_name_after_keyword(tokens, "INTO")
}

pub fn parse_update_table_name(tokens: List(Token)) -> String {
  extract_name_after_keyword(tokens, "UPDATE")
}

pub fn parse_delete_table_name(tokens: List(Token)) -> String {
  extract_name_after_keyword(tokens, "FROM")
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

// ---- RETURNING column parsing ----

pub fn parse_returning_columns(tokens: List(Token)) -> List(String) {
  case tokenize.split_at_keyword(tokens, "RETURNING") {
    Error(_) -> []
    Ok(#(_, after)) ->
      tokenize.split_on_commas(after)
      |> list.map(fn(group) {
        // Handle "expr AS alias"
        case tokenize.split_at_last_keyword(group, "AS") {
          Ok(#(_, alias_tokens)) -> token_list_to_name(alias_tokens)
          Error(_) -> token_list_to_name(group)
        }
      })
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

pub fn parse_update_set_columns(tokens: List(Token)) -> List(#(String, String)) {
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
    [Word(w1), OpenParen, Word(w2), ..] -> {
      let u1 = string.uppercase(w1)
      let u2 = string.uppercase(w2)
      case { u1 == "IN" || u1 == "=" } && u2 == "SELECT" {
        True -> True
        False -> do_has_subquery(list.drop(tokens, 1))
      }
    }
    // Also check: IN ( SELECT or = ( SELECT  (with space after paren)
    [Word(w), OpenParen, ..rest] -> {
      let u = string.uppercase(w)
      case u == "IN" {
        True ->
          case rest {
            [Word(s), ..] ->
              case string.uppercase(s) == "SELECT" {
                True -> True
                False -> do_has_subquery(list.drop(tokens, 1))
              }
            _ -> do_has_subquery(list.drop(tokens, 1))
          }
        False -> do_has_subquery(list.drop(tokens, 1))
      }
    }
    [Eq, OpenParen, Word(w), ..] ->
      case string.uppercase(w) == "SELECT" {
        True -> True
        False -> do_has_subquery(list.drop(tokens, 1))
      }
    [_, ..rest] -> do_has_subquery(rest)
  }
}

fn parse_simple_where_condition(tokens: List(Token)) -> List(#(String, String)) {
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
          [#(col_name, col_name)]
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
    // Keyword operators
    [Word(w), ..] -> {
      let upper = string.uppercase(w)
      case
        upper == "LIKE" || upper == "IN" || upper == "IS" || upper == "BETWEEN"
      {
        True ->
          case acc {
            [] -> option.None
            _ -> option.Some(tokens_to_column_name(list.reverse(acc)))
          }
        False -> do_extract_lhs(list.drop(tokens, 1), [Word(w), ..acc])
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
  case
    string.ends_with(s, "+")
    || string.ends_with(s, "-")
    || string.ends_with(s, "*")
    || string.ends_with(s, "/")
  {
    True -> string.drop_end(s, 1) |> string.trim
    False -> s
  }
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
        option.None ->
          do_find_param_binders(rest, [ParamAnon, ..prev], acc)
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
  case prev {
    // col OP ? -- prev is reversed: [OP, col, ...]
    // Simple: operator then word
    [Word(col), ..] -> option.Some(col)
    [QuotedId(col), ..] -> option.Some(col)
    // table.col OP ? -- prev: [OP, Word(col), Dot, Word(table), ...]
    // But OP was already consumed as current token... wait, prev includes
    // everything before the param. The operator IS in prev.
    // Actually, prev is: [Eq, Word(col), ...] or [Word("LIKE"), Word(col), ...]
    // Wait no, we push tokens onto prev in order, so prev is reversed.
    // If SQL is: col = ?, tokens are [Word("col"), Eq, ParamAnon]
    // When we hit ParamAnon, prev is [Eq, Word("col")]
    // So prev[0] = Eq (the operator), prev[1] = Word("col")
    _ ->
      case skip_operator_in_prev(prev) {
        option.Some(after_op) -> extract_column_word(after_op)
        option.None -> option.None
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

// ---- Expression type inference ----

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
          Column(name: item.alias, column_type: StringType, nullable: True)
      }

    _ -> Column(name: item.alias, column_type: StringType, nullable: True)
  }
}

/// Infer type from CAST(... AS type). `rest` is tokens after the opening paren.
fn infer_cast_type(alias: String, rest: List(Token)) -> Column {
  // Find AS keyword inside the CAST parens, take the type after it
  let inner = case list.reverse(rest) {
    [CloseParen, ..rev_rest] -> list.reverse(rev_rest)
    _ -> rest
  }
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
fn infer_coalesce_type(alias: String, rest: List(Token)) -> Column {
  let inner = case list.reverse(rest) {
    [CloseParen, ..rev_rest] -> list.reverse(rev_rest)
    _ -> rest
  }
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

// ---- Utility ----

/// Check if a token is a named parameter prefix.
pub fn starts_with_param_prefix(s: String) -> Bool {
  string.starts_with(s, "@")
  || string.starts_with(s, ":")
  || string.starts_with(s, "$")
}

pub fn list_at(lst: List(a), idx: Int) -> Result(a, Nil) {
  lst |> list.drop(idx) |> list.first
}

pub fn make_range(start: Int, count: Int) -> List(Int) {
  case count <= 0 {
    True -> []
    False -> do_make_range(start, start + count, [])
  }
}

fn do_make_range(current: Int, end: Int, acc: List(Int)) -> List(Int) {
  case current >= end {
    True -> list.reverse(acc)
    False -> do_make_range(current + 1, end, [current, ..acc])
  }
}
