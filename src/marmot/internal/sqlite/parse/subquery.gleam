//// Subquery table discovery: find all FROM-referenced tables in token streams.

import gleam/list
import gleam/string
import marmot/internal/sqlite/tokenize.{type Token, Word}

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
