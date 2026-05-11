//// Parameter binder discovery: find column references near parameter tokens.

import gleam/list
import gleam/option.{type Option}
import gleam/string
import marmot/internal/sqlite/tokenize.{
  type Token, CloseParen, Dot, Eq, Ge, Gt, Le, Lt, Minus, Ne, OpenParen,
  ParamAnon, ParamNamed, Percent, Plus, QuotedId, Slash, Star, Word,
}

pub type Binder {
  Binder(name: String, binder_column: Option(String))
}

pub type BinderOccurrence {
  BinderOccurrence(binder: Binder, depth: Int, anonymous: Bool)
}

/// Find parameter binders in a token list.
///
/// Heuristic walker: scans for `?` or `@name` tokens and looks backward through
/// the preceding tokens to find a column name (`col = ?`, `col > ?`, etc.).
/// Handles simple comparison patterns. Falls back to StringType when the
/// preceding context is too complex: IN-list params (`col IN (?, ?, ?)`),
/// nested expressions, function calls, subqueries. Known blind spots: parameters
/// in ON clauses of nested joins, parameters inside CASE expressions.
pub fn find_param_binders(tokens: List(Token)) -> List(Binder) {
  find_param_binder_occurrences(tokens)
  |> dedupe_named_binders([])
}

pub fn find_param_binder_occurrences(
  tokens: List(Token),
) -> List(BinderOccurrence) {
  do_find_param_binder_occurrences(tokens, [], 0, [])
}

fn do_find_param_binder_occurrences(
  tokens: List(Token),
  prev: List(Token),
  depth: Int,
  acc: List(BinderOccurrence),
) -> List(BinderOccurrence) {
  case tokens {
    [] -> list.reverse(acc)
    [OpenParen, ..rest] ->
      do_find_param_binder_occurrences(
        rest,
        [OpenParen, ..prev],
        depth + 1,
        acc,
      )
    [CloseParen, ..rest] ->
      do_find_param_binder_occurrences(
        rest,
        [CloseParen, ..prev],
        depth - 1,
        acc,
      )
    [ParamAnon, ..rest] -> {
      let col = extract_column_from_prev(prev)
      case col {
        option.None ->
          do_find_param_binder_occurrences(
            rest,
            [ParamAnon, ..prev],
            depth,
            acc,
          )
        option.Some(name) -> {
          let bare = strip_table_prefix(name)
          do_find_param_binder_occurrences(rest, [ParamAnon, ..prev], depth, [
            BinderOccurrence(
              binder: Binder(name: bare, binder_column: option.Some(name)),
              depth: depth,
              anonymous: True,
            ),
            ..acc
          ])
        }
      }
    }
    [ParamNamed(name), ..rest] -> {
      let column = extract_column_from_prev(prev)
      do_find_param_binder_occurrences(rest, [ParamNamed(name), ..prev], depth, [
        BinderOccurrence(
          binder: Binder(name: name, binder_column: column),
          depth: depth,
          anonymous: False,
        ),
        ..acc
      ])
    }
    [token, ..rest] ->
      do_find_param_binder_occurrences(rest, [token, ..prev], depth, acc)
  }
}

fn dedupe_named_binders(
  occurrences: List(BinderOccurrence),
  acc: List(Binder),
) -> List(Binder) {
  case occurrences {
    [] -> list.reverse(acc)
    [BinderOccurrence(binder: b, anonymous: anonymous, ..), ..rest] -> {
      case anonymous {
        True -> dedupe_named_binders(rest, [b, ..acc])
        False ->
          case list.find(acc, fn(existing) { existing.name == b.name }) {
            Ok(existing) ->
              case existing.binder_column, b.binder_column {
                option.None, option.Some(_) -> {
                  let new_acc =
                    list.map(acc, fn(existing) {
                      case existing.name == b.name {
                        True -> b
                        False -> existing
                      }
                    })
                  dedupe_named_binders(rest, new_acc)
                }
                _, _ -> dedupe_named_binders(rest, acc)
              }
            Error(_) -> dedupe_named_binders(rest, [b, ..acc])
          }
      }
    }
  }
}

/// Look backward in the reversed previous-token list for a column
/// before a comparison operator.
fn extract_column_from_prev(prev: List(Token)) -> Option(String) {
  // prev is reversed: for "col = ?", prev is [Eq, Word("col")].
  // For "col BETWEEN ?", prev is [Word("BETWEEN"), Word("col")].
  // Always skip the operator first, then extract the column after it.
  let effective_prev = skip_cast_wrapper(prev)
  case skip_operator_in_prev(effective_prev) {
    option.Some(after_op) -> extract_column_word(after_op)
    option.None ->
      case effective_prev {
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
    // Arithmetic operators (col + @p, col * @p, etc.)
    [Plus, ..rest] -> option.Some(rest)
    [Minus, ..rest] -> option.Some(rest)
    [Star, ..rest] -> option.Some(rest)
    [Slash, ..rest] -> option.Some(rest)
    [Percent, ..rest] -> option.Some(rest)
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

fn skip_cast_wrapper(prev: List(Token)) -> List(Token) {
  case prev {
    [OpenParen, Word(w), ..rest] ->
      case string.uppercase(w) == "CAST" {
        True -> rest
        False -> prev
      }
    _ -> prev
  }
}

fn strip_table_prefix(name: String) -> String {
  case string.split_once(name, ".") {
    Ok(#(_, col)) -> col
    Error(_) -> name
  }
}
