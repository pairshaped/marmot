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
