//// SELECT list parsing, CTE skipping, FROM table extraction, RETURNING parsing.

import gleam/bool
import gleam/list
import gleam/option.{type Option}
import gleam/string
import marmot/internal/sqlite/parse/util
import marmot/internal/sqlite/tokenize.{
  type Token, Comma, Dot, NullOverride, NullableOverride, OpenParen, QuotedId,
  Word,
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

// ---- SELECT list parsing ----

/// Parse the SELECT list from tokens.
///
/// This is a heuristic token walker, not a SQL grammar. It handles common
/// shapes: plain columns, aliased columns (`col AS name`), qualified columns
/// (`t.col`), and simple expressions with column references.
/// Fallback: when it can't resolve a SELECT item to a known column, it returns
/// the item with StringType and names derived from aliases or expression text.
/// Known blind spots: subquery columns in the SELECT list, CTE references, and
/// deeply nested expressions with function calls.
/// Parse a select-list slice (no SELECT keyword, no FROM and beyond).
/// Body-level entry point. The whole-statement helper `parse_select_items/1`
/// is now a shim that slices and delegates.
pub fn parse_select_item_list(select_list_tokens: List(Token)) -> List(SelectItem) {
  tokenize.split_on_commas(select_list_tokens)
  |> list.map(parse_select_item)
}

pub fn parse_select_items(tokens: List(Token)) -> List(SelectItem) {
  let main_tokens = skip_with_prefix(tokens)
  let after_select = skip_select_keyword(main_tokens)
  case after_select {
    [] -> []
    _ -> {
      let select_tokens =
        tokenize.take_until_keywords(after_select, [
          "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION",
          "INTERSECT", "EXCEPT",
        ])
      parse_select_item_list(select_tokens)
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
    True -> util.token_list_to_name(alias_tokens)
    False ->
      // No explicit AS. For "table.col", use "col" as alias.
      case list.reverse(tokens) {
        [Word(name), Dot, _, ..] -> name
        _ -> util.token_list_to_name(tokens)
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
  use <- bool.guard(
    when: !string.ends_with(alias, "!") && !string.ends_with(alias, "?"),
    return: alias,
  )
  string.drop_end(alias, 1)
}

fn detect_bare_column(tokens: List(Token)) -> Option(String) {
  case tokens {
    [Word(name)] -> option.Some(name)
    [QuotedId(name)] -> option.Some(name)
    [Word(_), Dot, Word(name)] -> option.Some(name)
    _ -> option.None
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
          "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "RETURNING", "UNION",
          "INTERSECT", "EXCEPT",
        ])
      extract_table_names_from_from(from_part)
    }
  }
}

fn extract_table_names_from_from(tokens: List(Token)) -> List(String) {
  split_on_joins(tokens)
  |> list.filter_map(fn(segment) {
    let before_on = tokenize.take_until_keywords(segment, ["ON", "USING"])
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
        || upper == "NATURAL"
      {
        True -> strip_trailing_join_modifiers(rest)
        False -> reversed_tokens
      }
    }
    _ -> reversed_tokens
  }
}

// ---- RETURNING column parsing ----

/// Parse a RETURNING body slice into column names. The whole-statement helper
/// `parse_returning_columns/1` is now a shim that slices and delegates.
pub fn parse_returning_body(returning_tokens: List(Token)) -> List(String) {
  tokenize.split_on_commas(returning_tokens)
  |> list.map(fn(group) {
    case tokenize.split_at_last_keyword(group, "AS") {
      Ok(#(_, alias_tokens)) -> util.token_list_to_name(alias_tokens)
      Error(_) -> util.token_list_to_name(group)
    }
  })
}

pub fn parse_returning_columns(tokens: List(Token)) -> List(String) {
  case tokenize.split_at_keyword(tokens, "RETURNING") {
    Error(_) -> []
    Ok(#(_, after)) -> parse_returning_body(after)
  }
}
