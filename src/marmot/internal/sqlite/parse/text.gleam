//// Text-level SQL helpers: normalization, nullability stripping, quoting.

import gleam/list
import gleam/string
import marmot/internal/query

/// Normalize SQL whitespace: strip comments, replace newlines/tabs with spaces
/// (preserving string literals), collapse runs, and trim.
pub fn normalize_sql_whitespace(sql: String) -> String {
  sql
  |> query.strip_comments
  |> query.normalize_whitespace
  |> string.trim
}

/// Strip Marmot-specific `!`/`?` nullability suffixes from alias names
/// before sending SQL to SQLite's EXPLAIN. Kept as a string operation
/// because its output is sent to SQLite, not analyzed by Marmot.
///
/// Note: this does not track comment state (line or block comments) because
/// comments are already stripped by `query.strip_comments` before this
/// function is called. Suffixes inside string literals are protected by
/// the in_single/in_double tracking.
///
/// The grapheme-level `!`/`?` detection is safe because the input has already
/// been whitespace-normalized: `?` placeholders always follow an operator
/// or keyword (non-ident-char preceding context), so they pass through
/// unchanged. Nullability suffixes follow an ident char and a boundary,
/// which is the only case this function strips.
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
          strip_suffix_or_keep_char(ch, rest, acc, prev, in_single, in_double)
      }
  }
}

fn strip_suffix_or_keep_char(
  ch: String,
  rest: List(String),
  acc: List(String),
  prev: String,
  in_single: Bool,
  in_double: Bool,
) -> List(String) {
  case is_nullability_suffix(ch, prev) && suffix_has_valid_next(rest) {
    True -> do_strip_suffixes(rest, acc, prev, in_single, in_double)
    False -> do_strip_suffixes(rest, [ch, ..acc], ch, in_single, in_double)
  }
}

fn is_nullability_suffix(ch: String, prev: String) -> Bool {
  { ch == "!" || ch == "?" } && is_ident_char(prev)
}

fn suffix_has_valid_next(rest: List(String)) -> Bool {
  case rest {
    [] -> True
    [c, ..] -> c == " " || c == "," || c == ")" || c == "\t" || c == "\n"
  }
}

/// Delegates to query.is_sql_ident_char. This is intentionally separate from
/// tokenize.is_word_char: both match [a-zA-Z0-9_] but serve different roles
/// (suffix-stripping context vs. token boundary detection).
fn is_ident_char(c: String) -> Bool {
  query.is_sql_ident_char(c)
}

/// Escape double quotes in an identifier to prevent SQL injection.
pub fn quote_identifier(name: String) -> String {
  string.replace(name, "\"", "\"\"")
}
