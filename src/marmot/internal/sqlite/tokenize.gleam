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
  case tokens {
    [] -> option.None
    [OpenParen, ..rest] -> do_find_keyword(rest, keyword, idx + 1, depth + 1)
    [CloseParen, ..rest] -> do_find_keyword(rest, keyword, idx + 1, depth - 1)
    [Word(text), ..rest] ->
      case depth == 0 && string.uppercase(text) == keyword {
        True -> option.Some(idx)
        False -> do_find_keyword(rest, keyword, idx + 1, depth)
      }
    [_, ..rest] -> do_find_keyword(rest, keyword, idx + 1, depth)
  }
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
  case tokens {
    [] -> last
    [OpenParen, ..rest] ->
      do_find_last_keyword(rest, keyword, idx + 1, depth + 1, last)
    [CloseParen, ..rest] ->
      do_find_last_keyword(rest, keyword, idx + 1, depth - 1, last)
    [Word(text), ..rest] ->
      case depth == 0 && string.uppercase(text) == keyword {
        True ->
          do_find_last_keyword(rest, keyword, idx + 1, depth, option.Some(idx))
        False -> do_find_last_keyword(rest, keyword, idx + 1, depth, last)
      }
    [_, ..rest] -> do_find_last_keyword(rest, keyword, idx + 1, depth, last)
  }
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

/// Take tokens until one of the given keywords appears at depth 0.
pub fn take_until_keywords(
  tokens: List(Token),
  keywords: List(String),
) -> List(Token) {
  let uppers = list.map(keywords, string.uppercase)
  do_take_until_keywords(tokens, uppers, [], 0)
}

fn do_take_until_keywords(
  tokens: List(Token),
  keywords: List(String),
  acc: List(Token),
  depth: Int,
) -> List(Token) {
  case tokens {
    [] -> list.reverse(acc)
    [OpenParen, ..rest] ->
      do_take_until_keywords(rest, keywords, [OpenParen, ..acc], depth + 1)
    [CloseParen, ..rest] ->
      do_take_until_keywords(rest, keywords, [CloseParen, ..acc], depth - 1)
    [Word(w), ..rest] -> {
      let upper = string.uppercase(w)
      case depth == 0 && list.contains(keywords, upper) {
        True -> list.reverse(acc)
        False -> do_take_until_keywords(rest, keywords, [Word(w), ..acc], depth)
      }
    }
    [token, ..rest] ->
      do_take_until_keywords(rest, keywords, [token, ..acc], depth)
  }
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
  do_split_on_commas(tokens, [], [], 0)
}

fn do_split_on_commas(
  tokens: List(Token),
  current: List(Token),
  acc: List(List(Token)),
  depth: Int,
) -> List(List(Token)) {
  case tokens {
    [] ->
      case current {
        [] -> list.reverse(acc)
        _ -> list.reverse([list.reverse(current), ..acc])
      }
    [OpenParen, ..rest] ->
      do_split_on_commas(rest, [OpenParen, ..current], acc, depth + 1)
    [CloseParen, ..rest] ->
      do_split_on_commas(rest, [CloseParen, ..current], acc, depth - 1)
    [Comma, ..rest] ->
      case depth {
        0 -> do_split_on_commas(rest, [], [list.reverse(current), ..acc], 0)
        _ -> do_split_on_commas(rest, [Comma, ..current], acc, depth)
      }
    [token, ..rest] -> do_split_on_commas(rest, [token, ..current], acc, depth)
  }
}

/// Split token list on AND/OR keywords at paren depth 0.
pub fn split_on_and_or(tokens: List(Token)) -> List(List(Token)) {
  do_split_on_and_or(tokens, [], [], 0, False)
}

fn do_split_on_and_or(
  tokens: List(Token),
  current: List(Token),
  acc: List(List(Token)),
  depth: Int,
  in_between: Bool,
) -> List(List(Token)) {
  case tokens {
    [] ->
      case current {
        [] -> list.reverse(acc)
        _ -> list.reverse([list.reverse(current), ..acc])
      }
    [OpenParen, ..rest] ->
      do_split_on_and_or(
        rest,
        [OpenParen, ..current],
        acc,
        depth + 1,
        in_between,
      )
    [CloseParen, ..rest] ->
      do_split_on_and_or(
        rest,
        [CloseParen, ..current],
        acc,
        depth - 1,
        in_between,
      )
    [Word(w), ..rest] -> {
      let upper = string.uppercase(w)
      case upper {
        // Track BETWEEN so the next AND is consumed as part of the expression
        "BETWEEN" ->
          do_split_on_and_or(rest, [Word(w), ..current], acc, depth, True)
        // AND after BETWEEN is part of the BETWEEN expression, not a separator
        "AND" if depth == 0 && in_between ->
          do_split_on_and_or(rest, [Word(w), ..current], acc, depth, False)
        "AND" | "OR" if depth == 0 ->
          case current {
            [] -> do_split_on_and_or(rest, [], acc, 0, False)
            _ ->
              do_split_on_and_or(
                rest,
                [],
                [list.reverse(current), ..acc],
                0,
                False,
              )
          }
        _ ->
          do_split_on_and_or(rest, [Word(w), ..current], acc, depth, in_between)
      }
    }
    [token, ..rest] ->
      do_split_on_and_or(rest, [token, ..current], acc, depth, in_between)
  }
}

/// Skip past matching parens. Starts at depth 1 (opening paren consumed).
/// Returns remaining tokens after the matching close paren.
pub fn skip_matching_paren(tokens: List(Token), depth: Int) -> List(Token) {
  case depth {
    0 -> tokens
    _ ->
      case tokens {
        [] -> []
        [OpenParen, ..rest] -> skip_matching_paren(rest, depth + 1)
        [CloseParen, ..rest] -> skip_matching_paren(rest, depth - 1)
        [_, ..rest] -> skip_matching_paren(rest, depth)
      }
  }
}

/// Collect tokens inside matching parens (depth 1). Opening paren consumed.
/// Returns (inner_tokens, remaining_after_close_paren).
pub fn collect_inside_parens(tokens: List(Token)) -> #(List(Token), List(Token)) {
  do_collect_inside_parens(tokens, [], 1)
}

fn do_collect_inside_parens(
  tokens: List(Token),
  acc: List(Token),
  depth: Int,
) -> #(List(Token), List(Token)) {
  case tokens {
    [] -> #(list.reverse(acc), [])
    [CloseParen, ..rest] ->
      case depth {
        1 -> #(list.reverse(acc), rest)
        _ -> do_collect_inside_parens(rest, [CloseParen, ..acc], depth - 1)
      }
    [OpenParen, ..rest] ->
      do_collect_inside_parens(rest, [OpenParen, ..acc], depth + 1)
    [token, ..rest] -> do_collect_inside_parens(rest, [token, ..acc], depth)
  }
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
