import gleam/list
import gleam/option.{type Option}
import gleam/string
import marmot/internal/query

// ---- Token type ----

pub type Token {
  // Identifiers and keywords (distinguished by text, not token type)
  Word(text: String)
  // Literals
  StringLit(text: String)
  QuotedId(text: String)
  Number(text: String)
  // Punctuation
  OpenParen
  CloseParen
  Comma
  Semicolon
  Dot
  // Parameters
  ParamAnon
  ParamNamed(name: String)
  // Operators
  Eq
  Ne
  Lt
  Gt
  Le
  Ge
  Plus
  Minus
  Star
  Slash
  // Marmot-specific nullability overrides
  NullOverride
  NullableOverride
}

// ---- Tokenizer ----

/// Tokenize a SQL string into a list of tokens.
/// Whitespace and comments are consumed but not emitted.
/// Operates on grapheme list for O(1) head/tail (fixes O(n^2) scanning).
pub fn tokenize(sql: String) -> List(Token) {
  string.to_graphemes(sql)
  |> do_tokenize([])
  |> list.reverse
}

fn do_tokenize(chars: List(String), acc: List(Token)) -> List(Token) {
  case chars {
    [] -> acc

    // Whitespace
    [" ", ..rest] -> do_tokenize(rest, acc)
    ["\t", ..rest] -> do_tokenize(rest, acc)
    ["\n", ..rest] -> do_tokenize(rest, acc)
    ["\r", ..rest] -> do_tokenize(rest, acc)

    // Line comment --
    ["-", "-", ..rest] -> skip_line_comment(rest, acc)

    // Block comment /*
    ["/", "*", ..rest] -> skip_block_comment(rest, acc)

    // String literal '...'
    ["'", ..rest] -> {
      let #(text, remaining) = consume_single_quoted(rest, [])
      do_tokenize(remaining, [StringLit(text), ..acc])
    }

    // Quoted identifier "..."
    ["\"", ..rest] -> {
      let #(text, remaining) = consume_double_quoted(rest, [])
      do_tokenize(remaining, [QuotedId(text), ..acc])
    }

    // Backtick identifier `...`
    ["`", ..rest] -> {
      let #(text, remaining) = consume_backtick(rest, [])
      do_tokenize(remaining, [QuotedId(text), ..acc])
    }

    // Multi-char operators (must come before single-char)
    ["<", "=", ..rest] -> do_tokenize(rest, [Le, ..acc])
    ["<", ">", ..rest] -> do_tokenize(rest, [Ne, ..acc])
    [">", "=", ..rest] -> do_tokenize(rest, [Ge, ..acc])
    ["!", "=", ..rest] -> do_tokenize(rest, [Ne, ..acc])

    // Single-char operators
    ["<", ..rest] -> do_tokenize(rest, [Lt, ..acc])
    [">", ..rest] -> do_tokenize(rest, [Gt, ..acc])
    ["=", ..rest] -> do_tokenize(rest, [Eq, ..acc])
    ["+", ..rest] -> do_tokenize(rest, [Plus, ..acc])
    ["-", ..rest] -> do_tokenize(rest, [Minus, ..acc])
    ["*", ..rest] -> do_tokenize(rest, [Star, ..acc])
    ["/", ..rest] -> do_tokenize(rest, [Slash, ..acc])

    // Punctuation
    ["(", ..rest] -> do_tokenize(rest, [OpenParen, ..acc])
    [")", ..rest] -> do_tokenize(rest, [CloseParen, ..acc])
    [",", ..rest] -> do_tokenize(rest, [Comma, ..acc])
    [";", ..rest] -> do_tokenize(rest, [Semicolon, ..acc])
    [".", ..rest] -> do_tokenize(rest, [Dot, ..acc])

    // Parameters and overrides
    ["?", ..rest] -> handle_question_mark(rest, acc)
    ["@", ..rest] -> consume_named_param(rest, acc)
    [":", ..rest] -> consume_named_param(rest, acc)
    ["$", ..rest] -> consume_named_param(rest, acc)
    ["!", ..rest] -> handle_exclamation(rest, acc)

    // Numbers (starts with digit)
    [c, ..] -> {
      case is_digit(c) {
        True -> {
          let #(num, remaining) = consume_number(chars, [])
          do_tokenize(remaining, [Number(num), ..acc])
        }
        False ->
          case is_alpha_or_underscore(c) {
            True -> {
              let #(word, remaining) = consume_word(chars, [])
              do_tokenize(remaining, [Word(word), ..acc])
            }
            // Skip unknown characters
            False -> {
              let rest = case chars {
                [_, ..r] -> r
                _ -> []
              }
              do_tokenize(rest, acc)
            }
          }
      }
    }
  }
}

// ---- Comment skipping ----

fn skip_line_comment(chars: List(String), acc: List(Token)) -> List(Token) {
  case chars {
    [] -> acc
    ["\n", ..rest] -> do_tokenize(rest, acc)
    [_, ..rest] -> skip_line_comment(rest, acc)
  }
}

fn skip_block_comment(chars: List(String), acc: List(Token)) -> List(Token) {
  case chars {
    [] -> acc
    ["*", "/", ..rest] -> do_tokenize(rest, acc)
    [_, ..rest] -> skip_block_comment(rest, acc)
  }
}

// ---- String/identifier consumers ----

fn consume_single_quoted(
  chars: List(String),
  acc: List(String),
) -> #(String, List(String)) {
  case chars {
    [] -> #(join_rev(acc), [])
    ["'", "'", ..rest] -> consume_single_quoted(rest, ["'", ..acc])
    ["'", ..rest] -> #(join_rev(acc), rest)
    [c, ..rest] -> consume_single_quoted(rest, [c, ..acc])
  }
}

fn consume_double_quoted(
  chars: List(String),
  acc: List(String),
) -> #(String, List(String)) {
  case chars {
    [] -> #(join_rev(acc), [])
    ["\"", "\"", ..rest] -> consume_double_quoted(rest, ["\"", ..acc])
    ["\"", ..rest] -> #(join_rev(acc), rest)
    [c, ..rest] -> consume_double_quoted(rest, [c, ..acc])
  }
}

fn consume_backtick(
  chars: List(String),
  acc: List(String),
) -> #(String, List(String)) {
  case chars {
    [] -> #(join_rev(acc), [])
    ["`", ..rest] -> #(join_rev(acc), rest)
    [c, ..rest] -> consume_backtick(rest, [c, ..acc])
  }
}

// ---- Word and number consumers ----

fn consume_word(
  chars: List(String),
  acc: List(String),
) -> #(String, List(String)) {
  case chars {
    [c, ..rest] ->
      case is_word_char(c) {
        True -> consume_word(rest, [c, ..acc])
        False -> #(join_rev(acc), chars)
      }
    [] -> #(join_rev(acc), [])
  }
}

fn consume_number(
  chars: List(String),
  acc: List(String),
) -> #(String, List(String)) {
  case chars {
    [".", c, ..rest] ->
      case is_digit(c) {
        True -> consume_number(rest, [c, ".", ..acc])
        False -> #(join_rev(acc), chars)
      }
    [c, ..rest] ->
      case is_digit(c) {
        True -> consume_number(rest, [c, ..acc])
        False -> #(join_rev(acc), chars)
      }
    [] -> #(join_rev(acc), [])
  }
}

// ---- Parameter and override handling ----

/// Disambiguate `?` as NullableOverride vs ParamAnon.
/// After a Word, followed by boundary char: NullableOverride.
/// Otherwise: ParamAnon.
fn handle_question_mark(rest: List(String), acc: List(Token)) -> List(Token) {
  let prev_is_word = case acc {
    [Word(_), ..] -> True
    _ -> False
  }
  let next_is_boundary = case rest {
    [] -> True
    [c, ..] -> c == " " || c == "," || c == ")" || c == "\t" || c == "\n"
  }
  case prev_is_word && next_is_boundary {
    True -> do_tokenize(rest, [NullableOverride, ..acc])
    False -> do_tokenize(rest, [ParamAnon, ..acc])
  }
}

/// Disambiguate `!` as NullOverride vs unknown.
/// `!=` is handled in the main match before this is reached.
fn handle_exclamation(rest: List(String), acc: List(Token)) -> List(Token) {
  let prev_is_word = case acc {
    [Word(_), ..] -> True
    _ -> False
  }
  let next_is_boundary = case rest {
    [] -> True
    [c, ..] -> c == " " || c == "," || c == ")" || c == "\t" || c == "\n"
  }
  case prev_is_word && next_is_boundary {
    True -> do_tokenize(rest, [NullOverride, ..acc])
    // Unknown standalone ! (not !=, not override), skip it
    False -> do_tokenize(rest, acc)
  }
}

/// Consume a named parameter (@name, :name, $name).
/// The prefix character has already been consumed.
fn consume_named_param(chars: List(String), acc: List(Token)) -> List(Token) {
  let #(name, remaining) = consume_word(chars, [])
  case name {
    "" -> do_tokenize(remaining, acc)
    n -> do_tokenize(remaining, [ParamNamed(n), ..acc])
  }
}

// ---- Character classification ----

fn is_digit(c: String) -> Bool {
  case c {
    "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" -> True
    _ -> False
  }
}

fn is_alpha_or_underscore(c: String) -> Bool {
  c == "_"
  || {
    let code = query.char_code(c)
    { code >= 65 && code <= 90 } || { code >= 97 && code <= 122 }
  }
}

fn is_word_char(c: String) -> Bool {
  is_alpha_or_underscore(c) || is_digit(c)
}

fn join_rev(chars: List(String)) -> String {
  chars |> list.reverse |> string.join("")
}

// ---- Token walking helpers ----

type StepResult(acc) {
  Continue(acc)
  Stop(acc)
}

/// Walk tokens left-to-right, automatically tracking paren depth.
/// Calls `step` for each token with the current state and depth.
/// If `step` returns `Stop(state)`, walking halts and returns
/// `#(state, remaining_tokens)`.
fn walk_tokens(
  tokens: List(Token),
  from state: a,
  at_depth depth: Int,
  with step: fn(a, Token, Int) -> StepResult(a),
) -> #(a, List(Token)) {
  case tokens {
    [] -> #(state, [])
    [token, ..rest] -> {
      case step(state, token, depth) {
        Continue(next_state) -> {
          let next_depth = case token {
            OpenParen -> depth + 1
            CloseParen -> depth - 1
            _ -> depth
          }
          walk_tokens(rest, next_state, next_depth, step)
        }
        Stop(next_state) -> #(next_state, rest)
      }
    }
  }
}

/// Fold over all tokens, automatically tracking paren depth.
fn fold_tokens(
  tokens: List(Token),
  from state: a,
  at_depth depth: Int,
  with fold: fn(a, Token, Int) -> a,
) -> a {
  case tokens {
    [] -> state
    [token, ..rest] -> {
      let next_state = fold(state, token, depth)
      let next_depth = case token {
        OpenParen -> depth + 1
        CloseParen -> depth - 1
        _ -> depth
      }
      fold_tokens(rest, next_state, next_depth, fold)
    }
  }
}

// ---- Token utilities ----

/// Get text representation of a token.
pub fn token_text(token: Token) -> String {
  case token {
    Word(t) -> t
    StringLit(t) -> "'" <> t <> "'"
    QuotedId(t) -> "\"" <> t <> "\""
    Number(t) -> t
    OpenParen -> "("
    CloseParen -> ")"
    Comma -> ","
    Semicolon -> ";"
    Dot -> "."
    ParamAnon -> "?"
    ParamNamed(n) -> "@" <> n
    Eq -> "="
    Ne -> "!="
    Lt -> "<"
    Gt -> ">"
    Le -> "<="
    Ge -> ">="
    Plus -> "+"
    Minus -> "-"
    Star -> "*"
    Slash -> "/"
    NullOverride -> "!"
    NullableOverride -> "?"
  }
}

/// Join tokens into a space-separated text string.
pub fn tokens_to_text(tokens: List(Token)) -> String {
  tokens
  |> list.map(token_text)
  |> string.join(" ")
}

/// Check if a token is a specific keyword (case-insensitive).
pub fn is_keyword(token: Token, keyword: String) -> Bool {
  case token {
    Word(text) -> string.uppercase(text) == string.uppercase(keyword)
    _ -> False
  }
}

/// Find the index of a keyword at paren depth 0.
pub fn find_keyword(tokens: List(Token), keyword: String) -> Option(Int) {
  let upper = string.uppercase(keyword)
  do_find_keyword(tokens, upper, 0, 0)
}

fn do_find_keyword(
  tokens: List(Token),
  keyword: String,
  idx: Int,
  depth: Int,
) -> Option(Int) {
  let #(state, _) =
    walk_tokens(tokens, #(idx, option.None), at_depth: depth, with: fn(
      state,
      token,
      depth,
    ) {
      let #(idx, found) = state
      case token {
        Word(text) if depth == 0 ->
          case string.uppercase(text) == keyword {
            True -> Stop(#(idx + 1, option.Some(idx)))
            False -> Continue(#(idx + 1, found))
          }
        _ -> Continue(#(idx + 1, found))
      }
    })
  state.1
}

/// Find the index of the LAST occurrence of a keyword at paren depth 0.
pub fn find_last_keyword(tokens: List(Token), keyword: String) -> Option(Int) {
  let upper = string.uppercase(keyword)
  do_find_last_keyword(tokens, upper, 0, 0, option.None)
}

fn do_find_last_keyword(
  tokens: List(Token),
  keyword: String,
  idx: Int,
  depth: Int,
  last: Option(Int),
) -> Option(Int) {
  fold_tokens(tokens, #(idx, last), at_depth: depth, with: fn(
    state,
    token,
    depth,
  ) {
    let #(idx, last) = state
    case token {
      Word(text) if depth == 0 ->
        case string.uppercase(text) == keyword {
          True -> #(idx + 1, option.Some(idx))
          False -> #(idx + 1, last)
        }
      _ -> #(idx + 1, last)
    }
  }).1
}

/// Check if a keyword exists at paren depth 0.
pub fn has_keyword(tokens: List(Token), keyword: String) -> Bool {
  option.is_some(find_keyword(tokens, keyword))
}

/// Split tokens at the first occurrence of a keyword at depth 0.
/// Returns (before_keyword, after_keyword) excluding the keyword itself.
pub fn split_at_keyword(
  tokens: List(Token),
  keyword: String,
) -> Result(#(List(Token), List(Token)), Nil) {
  case find_keyword(tokens, keyword) {
    option.Some(idx) -> {
      let before = list.take(tokens, idx)
      let after = list.drop(tokens, idx + 1)
      Ok(#(before, after))
    }
    option.None -> Error(Nil)
  }
}

/// Split tokens at the LAST occurrence of a keyword at depth 0.
pub fn split_at_last_keyword(
  tokens: List(Token),
  keyword: String,
) -> Result(#(List(Token), List(Token)), Nil) {
  case find_last_keyword(tokens, keyword) {
    option.Some(idx) -> {
      let before = list.take(tokens, idx)
      let after = list.drop(tokens, idx + 1)
      Ok(#(before, after))
    }
    option.None -> Error(Nil)
  }
}

fn do_take_until_keywords(
  tokens: List(Token),
  keywords: List(String),
) -> List(Token) {
  let #(acc, _) =
    walk_tokens(tokens, [], at_depth: 0, with: fn(acc, token, depth) {
      case token {
        Word(w) if depth == 0 ->
          case list.contains(keywords, string.uppercase(w)) {
            True -> Stop(acc)
            False -> Continue([Word(w), ..acc])
          }
        _ -> Continue([token, ..acc])
      }
    })
  list.reverse(acc)
}

/// Take tokens until one of the given keywords appears at depth 0.
pub fn take_until_keywords(
  tokens: List(Token),
  keywords: List(String),
) -> List(Token) {
  let uppers = list.map(keywords, string.uppercase)
  do_take_until_keywords(tokens, uppers)
}

/// Drop tokens until one of the given keywords appears at depth 0.
/// Returns tokens starting FROM the keyword (inclusive).
pub fn drop_until_keyword(tokens: List(Token), keyword: String) -> List(Token) {
  case find_keyword(tokens, keyword) {
    option.Some(idx) -> list.drop(tokens, idx)
    option.None -> []
  }
}

/// Split token list on Comma tokens at paren depth 0.
pub fn split_on_commas(tokens: List(Token)) -> List(List(Token)) {
  let #(current, groups) =
    fold_tokens(tokens, #([], []), at_depth: 0, with: fn(state, token, depth) {
      let #(current, groups) = state
      case token {
        Comma if depth == 0 -> #([], [list.reverse(current), ..groups])
        _ -> #([token, ..current], groups)
      }
    })
  case current {
    [] -> list.reverse(groups)
    _ -> list.reverse([list.reverse(current), ..groups])
  }
}

/// Split token list on AND/OR keywords at paren depth 0.
pub fn split_on_and_or(tokens: List(Token)) -> List(List(Token)) {
  let #(inner, _) =
    fold_tokens(tokens, #(#([], []), False), at_depth: 0, with: fn(
      state,
      token,
      depth,
    ) {
      let #(#(current, groups), in_between) = state
      case token {
        Word(w) ->
          case string.uppercase(w) {
            "BETWEEN" -> #(#([Word(w), ..current], groups), True)
            "AND" if depth == 0 && in_between ->
              #(#([Word(w), ..current], groups), False)
            "AND" | "OR" if depth == 0 ->
              case current {
                [] -> #(#([], groups), False)
                _ ->
                  #(#([], [list.reverse(current), ..groups]), False)
              }
            _ -> #(#([Word(w), ..current], groups), in_between)
          }
        _ -> #(#([token, ..current], groups), in_between)
      }
    })
  let #(current, groups) = inner
  case current {
    [] -> list.reverse(groups)
    _ -> list.reverse([list.reverse(current), ..groups])
  }
}

/// Skip past matching parens. Starts at depth 1 (opening paren consumed).
/// Returns remaining tokens after the matching close paren.
pub fn skip_matching_paren(tokens: List(Token), depth: Int) -> List(Token) {
  case depth {
    0 -> tokens
    _ -> {
      let #(_, remaining) =
        walk_tokens(tokens, [], at_depth: depth, with: fn(_, token, d) {
          case token {
            CloseParen if d == 1 -> Stop([])
            _ -> Continue([])
          }
        })
      remaining
    }
  }
}

/// Collect tokens inside matching parens (depth 1). Opening paren consumed.
/// Returns (inner_tokens, remaining_after_close_paren).
pub fn collect_inside_parens(tokens: List(Token)) -> #(List(Token), List(Token)) {
  let #(acc, remaining) =
    walk_tokens(tokens, [], at_depth: 1, with: fn(acc, token, depth) {
      case token {
        CloseParen if depth == 1 -> Stop(acc)
        _ -> Continue([token, ..acc])
      }
    })
  #(list.reverse(acc), remaining)
}

/// Get the first Word text from a token list, or empty string.
pub fn first_word(tokens: List(Token)) -> String {
  case tokens {
    [] -> ""
    [Word(t), ..] -> t
    [QuotedId(t), ..] -> t
    [_, ..rest] -> first_word(rest)
  }
}
