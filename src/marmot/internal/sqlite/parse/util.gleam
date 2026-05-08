//// Small shared helpers used across parser modules.

import gleam/list
import gleam/string
import marmot/internal/sqlite/tokenize.{
  type Token, NullOverride, NullableOverride, QuotedId, Word,
}

/// Get a clean name from a token list (first Word or QuotedId).
pub fn token_list_to_name(tokens: List(Token)) -> String {
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
