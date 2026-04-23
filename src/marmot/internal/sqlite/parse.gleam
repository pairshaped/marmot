import gleam/bool
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import marmot/internal/query.{
  type Column, type ColumnType, Column, StringType,
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
    /// The display/field name (from AS alias or the expression itself)
    alias: String,
    /// The raw expression (left side of AS, or the whole item)
    expression: String,
    /// If the expression is a bare column reference (possibly "table.col"),
    /// this is the column name (without table prefix). None for expressions
    /// like COUNT(*), COALESCE(...), subqueries, etc.
    bare_column: option.Option(String),
    /// Nullability override from alias suffix (`name!` / `name?`).
    override: NullabilityOverride,
  )
}

pub type Binder {
  Binder(name: String, binder_column: option.Option(String))
}

type Placeholder {
  PlaceholderNone
  PlaceholderAnon(pos: Int, after: Int)
  PlaceholderNamed(name: String, after: Int)
}

/// Parsing state for CASE expression branch extraction
type CaseScanState {
  InNone
  InWhen
  InThen
  InElse
}

// ---- Statement classification ----

pub fn classify_statement(sql: String) -> StatementType {
  let upper = string.uppercase(string.trim(sql))
  case string.starts_with(upper, "SELECT") {
    True -> Select
    False ->
      case string.starts_with(upper, "INSERT") {
        True -> Insert
        False ->
          case string.starts_with(upper, "UPDATE") {
            True -> Update
            False ->
              case string.starts_with(upper, "DELETE") {
                True -> Delete
                False -> Other
              }
          }
      }
  }
}

// ---- Whitespace normalization ----

/// Normalize SQL whitespace: strip line comments, convert newlines/tabs to
/// spaces, collapse runs, and trim. Safe on SQL because string literals
/// aren't split across lines and whitespace inside identifiers isn't allowed.
pub fn normalize_sql_whitespace(sql: String) -> String {
  sql
  |> query.strip_line_comments
  |> string.replace("\r\n", " ")
  |> string.replace("\r", " ")
  |> string.replace("\n", " ")
  |> string.replace("\t", " ")
  |> query.collapse_spaces
  |> string.trim
}

// ---- Nullability suffix stripping ----

/// Strip the Marmot-specific nullability suffixes `!` / `?` from alias names
/// before sending SQL to SQLite's EXPLAIN. We only strip when the suffix
/// appears directly after an identifier character and is followed by
/// whitespace, comma, end-of-string, or closing paren. This avoids
/// mangling legitimate SQL like `WHERE x != y` or `?` placeholders.
pub fn strip_nullability_suffixes(sql: String) -> String {
  do_strip_nullability_suffixes(sql, "", "", False, False)
}

fn do_strip_nullability_suffixes(
  remaining: String,
  acc: String,
  prev_char: String,
  in_single: Bool,
  in_double: Bool,
) -> String {
  case string.pop_grapheme(remaining) {
    Error(_) -> acc
    Ok(#("'", rest)) ->
      case in_double {
        True ->
          do_strip_nullability_suffixes(rest, acc <> "'", "'", in_single, True)
        False ->
          case in_single {
            True ->
              case string.pop_grapheme(rest) {
                Ok(#("'", rest2)) ->
                  do_strip_nullability_suffixes(
                    rest2,
                    acc <> "''",
                    "'",
                    True,
                    False,
                  )
                _ ->
                  do_strip_nullability_suffixes(
                    rest,
                    acc <> "'",
                    "'",
                    False,
                    False,
                  )
              }
            False ->
              do_strip_nullability_suffixes(rest, acc <> "'", "'", True, False)
          }
      }
    Ok(#("\"", rest)) ->
      case in_single {
        True ->
          do_strip_nullability_suffixes(
            rest,
            acc <> "\"",
            "\"",
            True,
            in_double,
          )
        False ->
          do_strip_nullability_suffixes(
            rest,
            acc <> "\"",
            "\"",
            False,
            !in_double,
          )
      }
    Ok(#(ch, rest)) ->
      case in_single || in_double {
        True ->
          do_strip_nullability_suffixes(
            rest,
            acc <> ch,
            ch,
            in_single,
            in_double,
          )
        False ->
          case ch == "!" || ch == "?" {
            True -> {
              let prev_ok = case prev_char {
                "" -> False
                p -> is_ident_char(p)
              }
              let next_char = case string.pop_grapheme(rest) {
                Ok(#(c, _)) -> c
                Error(_) -> " "
              }
              let next_ok = case next_char {
                " " | "," | ")" | "\t" | "\n" -> True
                _ -> False
              }
              case prev_ok && next_ok {
                True ->
                  do_strip_nullability_suffixes(
                    rest,
                    acc,
                    prev_char,
                    in_single,
                    in_double,
                  )
                False ->
                  do_strip_nullability_suffixes(
                    rest,
                    acc <> ch,
                    ch,
                    in_single,
                    in_double,
                  )
              }
            }
            False ->
              do_strip_nullability_suffixes(
                rest,
                acc <> ch,
                ch,
                in_single,
                in_double,
              )
          }
      }
  }
}

fn is_ident_char(c: String) -> Bool {
  query.is_sql_ident_char(c)
}

// ---- String masking ----

/// Replace the contents of string literals ('...' and "...") with spaces,
/// preserving character positions. Keyword-scanning functions call this so
/// they don't match keywords that appear inside string literals.
pub fn mask_string_contents(sql: String) -> String {
  do_mask_string_contents(sql, "", False, False)
}

fn do_mask_string_contents(
  remaining: String,
  acc: String,
  in_single: Bool,
  in_double: Bool,
) -> String {
  case string.pop_grapheme(remaining) {
    Error(_) -> acc
    Ok(#("'", rest)) ->
      case in_double {
        True -> do_mask_string_contents(rest, acc <> " ", False, True)
        False ->
          case in_single {
            True ->
              case string.pop_grapheme(rest) {
                Ok(#("'", rest2)) ->
                  do_mask_string_contents(rest2, acc <> "  ", True, False)
                _ -> do_mask_string_contents(rest, acc <> "'", False, False)
              }
            False -> do_mask_string_contents(rest, acc <> "'", True, False)
          }
      }
    Ok(#("\"", rest)) ->
      case in_single {
        True -> do_mask_string_contents(rest, acc <> " ", True, False)
        False ->
          case in_double {
            True ->
              case string.pop_grapheme(rest) {
                Ok(#("\"", rest2)) ->
                  do_mask_string_contents(rest2, acc <> "  ", False, True)
                _ -> do_mask_string_contents(rest, acc <> "\"", False, False)
              }
            False -> do_mask_string_contents(rest, acc <> "\"", False, True)
          }
      }
    Ok(#(ch, rest)) ->
      case in_single || in_double {
        True -> do_mask_string_contents(rest, acc <> " ", in_single, in_double)
        False -> do_mask_string_contents(rest, acc <> ch, in_single, in_double)
      }
  }
}

// ---- Keyword scanning ----

/// Find a SQL keyword at parenthesis depth 0 on pre-masked input.
pub fn find_top_level_keyword_on_masked(
  masked: String,
  keyword: String,
) -> option.Option(Int) {
  do_find_top_level_keyword(masked, keyword, 0, 0)
}

fn do_find_top_level_keyword(
  s: String,
  keyword: String,
  idx: Int,
  depth: Int,
) -> option.Option(Int) {
  let keyword_len = string.length(keyword)
  case string.length(s) < keyword_len {
    True -> option.None
    False -> {
      let head_char = string.slice(s, 0, 1)
      case head_char {
        "(" ->
          do_find_top_level_keyword(
            string.drop_start(s, 1),
            keyword,
            idx + 1,
            depth + 1,
          )
        ")" ->
          do_find_top_level_keyword(
            string.drop_start(s, 1),
            keyword,
            idx + 1,
            depth - 1,
          )
        _ ->
          case depth == 0 {
            True -> {
              let head = string.slice(s, 0, keyword_len)
              case head == keyword {
                True -> option.Some(idx)
                False ->
                  do_find_top_level_keyword(
                    string.drop_start(s, 1),
                    keyword,
                    idx + 1,
                    depth,
                  )
              }
            }
            False ->
              do_find_top_level_keyword(
                string.drop_start(s, 1),
                keyword,
                idx + 1,
                depth,
              )
          }
      }
    }
  }
}

/// Find a SQL keyword at parenthesis depth 0, respecting string literals.
/// Accepts pre-masked input.
pub fn find_keyword_idx_on_masked(
  masked_upper: String,
  keyword: String,
) -> option.Option(Int) {
  let target = " " <> keyword <> " "
  let end_target = " " <> keyword
  do_find_keyword_idx(masked_upper, target, end_target, 0, 0)
}

fn do_find_keyword_idx(
  s: String,
  target: String,
  end_target: String,
  idx: Int,
  depth: Int,
) -> option.Option(Int) {
  case string.pop_grapheme(s) {
    Error(_) -> option.None
    Ok(#("(", rest)) ->
      do_find_keyword_idx(rest, target, end_target, idx + 1, depth + 1)
    Ok(#(")", rest)) ->
      do_find_keyword_idx(rest, target, end_target, idx + 1, depth - 1)
    Ok(#(_, rest)) ->
      case depth == 0 {
        True -> {
          let target_len = string.length(target)
          let head = string.slice(s, 0, target_len)
          case head == target {
            True -> option.Some(idx)
            False ->
              case s == end_target {
                True -> option.Some(idx)
                False ->
                  do_find_keyword_idx(rest, target, end_target, idx + 1, depth)
              }
          }
        }
        False -> do_find_keyword_idx(rest, target, end_target, idx + 1, depth)
      }
  }
}

fn find_top_level_keyword_offset(
  s: String,
  keyword: String,
) -> option.Option(Int) {
  find_top_level_keyword_on_masked(mask_string_contents(s), keyword)
}

/// Find the index of the top-level `FROM` keyword.
/// Respects nested parentheses, subqueries, and string literals.
pub fn find_top_level_from(s: String) -> option.Option(Int) {
  find_top_level_keyword_on_masked(
    mask_string_contents(string.uppercase(s)),
    " FROM ",
  )
}

/// Like `find_top_level_from` but the caller has already masked string literals.
pub fn find_top_level_from_on_masked(masked: String) -> option.Option(Int) {
  find_top_level_keyword_on_masked(masked, " FROM ")
}

/// Check whether a SQL keyword appears at top-level in the SQL string.
pub fn contains_keyword(sql: String, keyword: String) -> Bool {
  let masked = mask_string_contents(string.uppercase(sql))
  let spaced = " " <> keyword <> " "
  case find_top_level_keyword_on_masked(masked, spaced) {
    option.Some(_) -> True
    option.None -> {
      let end_target = " " <> keyword
      case string.ends_with(masked, end_target) {
        True -> True
        False -> False
      }
    }
  }
}

/// Split a string on a top-level SQL keyword (outside parens and string
/// literals). The keyword should include surrounding spaces (e.g., " SET ").
pub fn split_on_keyword(
  haystack: String,
  keyword: String,
) -> Result(#(String, String), Nil) {
  let masked = mask_string_contents(haystack)
  case find_top_level_keyword_on_masked(masked, keyword) {
    option.Some(idx) -> {
      let before = string.slice(haystack, 0, idx)
      let after = string.drop_start(haystack, idx + string.length(keyword))
      Ok(#(before, after))
    }
    option.None -> {
      let trimmed_keyword = string.trim_end(keyword)
      let end_target = trimmed_keyword
      case find_keyword_idx_on_masked(masked, string.trim(trimmed_keyword)) {
        option.Some(idx) -> {
          let before = string.slice(haystack, 0, idx)
          let after =
            string.drop_start(haystack, idx + string.length(end_target))
          Ok(#(before, after))
        }
        option.None -> Error(Nil)
      }
    }
  }
}

// ---- Comma and condition splitting ----

/// Split a string on top-level commas (ignoring commas inside parens,
/// single-quoted string literals, or double-quoted identifiers).
pub fn split_top_level_commas(s: String) -> List(String) {
  do_split_top_level_commas(s, "", [], 0, False, False)
}

fn do_split_top_level_commas(
  remaining: String,
  current: String,
  acc: List(String),
  depth: Int,
  in_single: Bool,
  in_double: Bool,
) -> List(String) {
  case string.pop_grapheme(remaining) {
    Error(_) ->
      case current {
        "" -> list.reverse(acc)
        _ -> list.reverse([string.trim(current), ..acc])
      }
    Ok(#("'", rest)) ->
      case in_double {
        True ->
          do_split_top_level_commas(
            rest,
            current <> "'",
            acc,
            depth,
            in_single,
            True,
          )
        False ->
          case in_single {
            True ->
              case string.pop_grapheme(rest) {
                Ok(#("'", rest2)) ->
                  do_split_top_level_commas(
                    rest2,
                    current <> "''",
                    acc,
                    depth,
                    True,
                    False,
                  )
                _ ->
                  do_split_top_level_commas(
                    rest,
                    current <> "'",
                    acc,
                    depth,
                    False,
                    False,
                  )
              }
            False ->
              do_split_top_level_commas(
                rest,
                current <> "'",
                acc,
                depth,
                True,
                False,
              )
          }
      }
    Ok(#("\"", rest)) ->
      case in_single {
        True ->
          do_split_top_level_commas(
            rest,
            current <> "\"",
            acc,
            depth,
            True,
            in_double,
          )
        False ->
          do_split_top_level_commas(
            rest,
            current <> "\"",
            acc,
            depth,
            False,
            !in_double,
          )
      }
    Ok(#(char, rest)) if in_single || in_double ->
      do_split_top_level_commas(
        rest,
        current <> char,
        acc,
        depth,
        in_single,
        in_double,
      )
    Ok(#("(", rest)) ->
      do_split_top_level_commas(
        rest,
        current <> "(",
        acc,
        depth + 1,
        False,
        False,
      )
    Ok(#(")", rest)) ->
      do_split_top_level_commas(
        rest,
        current <> ")",
        acc,
        depth - 1,
        False,
        False,
      )
    Ok(#(",", rest)) ->
      case depth {
        0 ->
          do_split_top_level_commas(
            rest,
            "",
            [string.trim(current), ..acc],
            0,
            False,
            False,
          )
        _ ->
          do_split_top_level_commas(
            rest,
            current <> ",",
            acc,
            depth,
            False,
            False,
          )
      }
    Ok(#(char, rest)) ->
      do_split_top_level_commas(rest, current <> char, acc, depth, False, False)
  }
}

/// Split WHERE conditions on top-level AND/OR keywords (case-insensitive).
/// Respects parenthesis depth, single-quoted string literals, and
/// double-quoted identifiers.
pub fn split_where_conditions(where_part: String) -> List(String) {
  do_split_on_and_or(where_part, "", [], 0, False, False)
}

fn do_split_on_and_or(
  remaining: String,
  current: String,
  acc: List(String),
  depth: Int,
  in_single: Bool,
  in_double: Bool,
) -> List(String) {
  case string.pop_grapheme(remaining) {
    Error(_) ->
      case string.trim(current) {
        "" -> list.reverse(acc)
        trimmed -> list.reverse([trimmed, ..acc])
      }
    Ok(#("'", rest)) ->
      case in_double {
        True ->
          do_split_on_and_or(rest, current <> "'", acc, depth, in_single, True)
        False ->
          case in_single {
            True ->
              case string.pop_grapheme(rest) {
                Ok(#("'", rest2)) ->
                  do_split_on_and_or(
                    rest2,
                    current <> "''",
                    acc,
                    depth,
                    True,
                    False,
                  )
                _ ->
                  do_split_on_and_or(
                    rest,
                    current <> "'",
                    acc,
                    depth,
                    False,
                    False,
                  )
              }
            False ->
              do_split_on_and_or(rest, current <> "'", acc, depth, True, False)
          }
      }
    Ok(#("\"", rest)) ->
      case in_single {
        True ->
          do_split_on_and_or(rest, current <> "\"", acc, depth, True, in_double)
        False ->
          do_split_on_and_or(
            rest,
            current <> "\"",
            acc,
            depth,
            False,
            !in_double,
          )
      }
    Ok(#(char, rest)) if in_single || in_double ->
      do_split_on_and_or(
        rest,
        current <> char,
        acc,
        depth,
        in_single,
        in_double,
      )
    Ok(#("(", rest)) ->
      do_split_on_and_or(rest, current <> "(", acc, depth + 1, False, False)
    Ok(#(")", rest)) ->
      do_split_on_and_or(rest, current <> ")", acc, depth - 1, False, False)
    Ok(#(" ", rest)) ->
      case depth == 0 {
        True -> {
          let peek4 = string.uppercase(string.slice(rest, 0, 4))
          case peek4 {
            "AND " -> {
              let trimmed = string.trim(current)
              let after = string.drop_start(rest, 4)
              case trimmed {
                "" -> do_split_on_and_or(after, "", acc, 0, False, False)
                _ ->
                  do_split_on_and_or(
                    after,
                    "",
                    [trimmed, ..acc],
                    0,
                    False,
                    False,
                  )
              }
            }
            _ -> {
              let peek3 = string.uppercase(string.slice(rest, 0, 3))
              case peek3 {
                "OR " -> {
                  let trimmed = string.trim(current)
                  let after = string.drop_start(rest, 3)
                  case trimmed {
                    "" -> do_split_on_and_or(after, "", acc, 0, False, False)
                    _ ->
                      do_split_on_and_or(
                        after,
                        "",
                        [trimmed, ..acc],
                        0,
                        False,
                        False,
                      )
                  }
                }
                _ ->
                  do_split_on_and_or(rest, current <> " ", acc, 0, False, False)
              }
            }
          }
        }
        False ->
          do_split_on_and_or(rest, current <> " ", acc, depth, False, False)
      }
    Ok(#(char, rest)) ->
      do_split_on_and_or(rest, current <> char, acc, depth, False, False)
  }
}

// ---- Paren matching ----

pub fn walk_matching_paren(s: String, depth: Int) -> String {
  walk_matching_paren_impl(s, depth, False)
}

fn walk_matching_paren_impl(s: String, depth: Int, in_string: Bool) -> String {
  case depth {
    0 -> s
    _ ->
      case string.pop_grapheme(s) {
        Error(_) -> s
        Ok(#("'", rest)) ->
          case in_string {
            True ->
              case string.pop_grapheme(rest) {
                Ok(#("'", rest2)) ->
                  walk_matching_paren_impl(rest2, depth, True)
                _ -> walk_matching_paren_impl(rest, depth, False)
              }
            False -> walk_matching_paren_impl(rest, depth, True)
          }
        Ok(#(_, rest)) if in_string ->
          walk_matching_paren_impl(rest, depth, in_string)
        Ok(#("(", rest)) -> walk_matching_paren_impl(rest, depth + 1, False)
        Ok(#(")", rest)) -> walk_matching_paren_impl(rest, depth - 1, False)
        Ok(#(_, rest)) -> walk_matching_paren_impl(rest, depth, False)
      }
  }
}

/// Walk through a string tracking parenthesis depth to find the matching
/// close paren for an already-opened opening paren (depth starts at 1).
pub fn find_matching_close_paren(
  s: String,
  depth: Int,
  acc: String,
) -> Result(String, Nil) {
  case string.pop_grapheme(s) {
    Error(_) -> Error(Nil)
    Ok(#("(", rest)) -> find_matching_close_paren(rest, depth + 1, acc <> "(")
    Ok(#(")", rest)) ->
      case depth == 1 {
        True -> Ok(acc)
        False -> find_matching_close_paren(rest, depth - 1, acc <> ")")
      }
    Ok(#(char, rest)) -> find_matching_close_paren(rest, depth, acc <> char)
  }
}

// ---- SELECT list parsing ----

/// Parse the SELECT list from a normalized SQL string.
pub fn parse_select_items(sql: String) -> List(SelectItem) {
  let main_sql = skip_with_prefix(sql)
  let upper = string.uppercase(main_sql)
  let start_prefix = case string.starts_with(upper, "SELECT DISTINCT ") {
    True -> "SELECT DISTINCT "
    False ->
      case string.starts_with(upper, "SELECT ") {
        True -> "SELECT "
        False -> ""
      }
  }
  case start_prefix {
    "" -> []
    prefix -> {
      let after_select = string.drop_start(main_sql, string.length(prefix))
      let masked_after = mask_string_contents(after_select)
      let end_idx = case find_top_level_from_on_masked(masked_after) {
        option.Some(idx) -> idx
        option.None -> {
          [
            find_top_level_keyword_on_masked(masked_after, " WHERE "),
            find_top_level_keyword_on_masked(masked_after, " GROUP BY "),
            find_top_level_keyword_on_masked(masked_after, " ORDER BY "),
            find_top_level_keyword_on_masked(masked_after, " LIMIT "),
          ]
          |> list.filter_map(fn(x) {
            case x {
              option.Some(i) -> Ok(i)
              option.None -> Error(Nil)
            }
          })
          |> list.sort(int.compare)
          |> list.first
          |> result.unwrap(string.length(after_select))
        }
      }
      let select_list = string.slice(after_select, 0, end_idx)
      split_top_level_commas(select_list)
      |> list.map(parse_select_item)
    }
  }
}

/// Parse a single SELECT-list item: `expr [AS alias]`.
pub fn parse_select_item(raw: String) -> SelectItem {
  let trimmed = string.trim(raw)
  let #(expr, alias) = case rsplit_on_as(trimmed) {
    option.Some(#(e, a)) -> #(string.trim(e), string.trim(a))
    option.None -> #(trimmed, trimmed)
  }
  let clean_alias = case alias == expr {
    True -> {
      case string.split_once(alias, ".") {
        Ok(#(_, after)) ->
          case is_simple_identifier(after) {
            True -> after
            False -> alias
          }
        Error(_) -> alias
      }
    }
    False -> alias
  }
  let bare_column = case is_simple_identifier(expr) {
    True -> option.Some(expr)
    False ->
      case string.split_once(expr, ".") {
        Ok(#(_, after)) ->
          case is_simple_identifier(after) {
            True -> option.Some(after)
            False -> option.None
          }
        Error(_) -> option.None
      }
  }
  let #(final_alias, override) = extract_nullability_override(clean_alias)
  SelectItem(
    alias: final_alias,
    expression: expr,
    bare_column: bare_column,
    override: override,
  )
}

fn extract_nullability_override(alias: String) -> #(String, NullabilityOverride) {
  use <- bool.guard(string.ends_with(alias, "!"), #(
    string.drop_end(alias, 1),
    OverrideNonNull,
  ))
  use <- bool.guard(string.ends_with(alias, "?"), #(
    string.drop_end(alias, 1),
    OverrideNullable,
  ))
  #(alias, OverrideNone)
}

/// Split on the LAST top-level " AS " (case-insensitive).
fn rsplit_on_as(s: String) -> option.Option(#(String, String)) {
  let upper = string.uppercase(s)
  let positions = find_all_top_level_as(mask_string_contents(upper), 0, 0, [])
  case list.last(positions) {
    Error(_) -> option.None
    Ok(pos) -> {
      let before = string.slice(s, 0, pos)
      let after = string.drop_start(s, pos + 4)
      option.Some(#(before, after))
    }
  }
}

fn find_all_top_level_as(
  s: String,
  idx: Int,
  depth: Int,
  acc: List(Int),
) -> List(Int) {
  case string.length(s) < 4 {
    True -> list.reverse(acc)
    False -> {
      let head = string.slice(s, 0, 1)
      case head {
        "(" ->
          find_all_top_level_as(
            string.drop_start(s, 1),
            idx + 1,
            depth + 1,
            acc,
          )
        ")" ->
          find_all_top_level_as(
            string.drop_start(s, 1),
            idx + 1,
            depth - 1,
            acc,
          )
        _ ->
          case depth == 0 && string.starts_with(s, " AS ") {
            True ->
              find_all_top_level_as(string.drop_start(s, 4), idx + 4, depth, [
                idx,
                ..acc
              ])
            False ->
              find_all_top_level_as(
                string.drop_start(s, 1),
                idx + 1,
                depth,
                acc,
              )
          }
      }
    }
  }
}

/// A "simple identifier" is an alpha/underscore-start followed by word chars.
pub fn is_simple_identifier(s: String) -> Bool {
  let trimmed = string.trim(s)
  case string.length(trimmed) {
    0 -> False
    _ -> {
      let graphemes = string.to_graphemes(trimmed)
      list.all(graphemes, is_identifier_char)
    }
  }
}

fn is_identifier_char(c: String) -> Bool {
  query.is_sql_ident_char(c)
}

// ---- CTE prefix skipping ----

/// Skip a WITH [RECURSIVE] prefix to find the main SELECT.
fn skip_with_prefix(sql: String) -> String {
  let upper = string.uppercase(sql)
  case
    string.starts_with(upper, "WITH RECURSIVE "),
    string.starts_with(upper, "WITH ")
  {
    True, _ -> skip_cte_definitions(string.drop_start(sql, 15))
    _, True -> skip_cte_definitions(string.drop_start(sql, 5))
    _, _ -> sql
  }
}

fn skip_cte_definitions(s: String) -> String {
  let trimmed = string.trim_start(s)
  case find_matching_paren_after_as(trimmed) {
    option.None -> trimmed
    option.Some(after_close) -> {
      let rest = string.trim_start(after_close)
      case string.starts_with(rest, ",") {
        True -> skip_cte_definitions(string.drop_start(rest, 1))
        False -> rest
      }
    }
  }
}

fn find_matching_paren_after_as(s: String) -> option.Option(String) {
  let upper = string.uppercase(s)
  case find_top_level_keyword_offset(upper, " AS ") {
    option.None -> option.None
    option.Some(idx) -> {
      let after_as = string.drop_start(s, idx + 4) |> string.trim_start
      case string.pop_grapheme(after_as) {
        Ok(#("(", rest)) -> option.Some(walk_matching_paren(rest, 1))
        _ -> option.None
      }
    }
  }
}

// ---- FROM clause parsing ----

/// Parse the FROM clause to get the list of tables.
pub fn parse_from_tables(sql: String) -> List(String) {
  let main_sql = skip_with_prefix(sql)
  let upper = string.uppercase(main_sql)
  let after_select = case string.starts_with(upper, "SELECT DISTINCT ") {
    True -> string.drop_start(main_sql, 16)
    False ->
      case string.starts_with(upper, "SELECT ") {
        True -> string.drop_start(main_sql, 7)
        False -> main_sql
      }
  }
  case find_top_level_from(after_select) {
    option.None -> []
    option.Some(from_idx) -> {
      let rest =
        after_select
        |> string.drop_start(from_idx + 5)
        |> string.trim
      let rest_upper = string.uppercase(rest)
      let masked_rest = mask_string_contents(rest_upper)
      let end_idx =
        [
          find_keyword_idx_on_masked(masked_rest, "WHERE"),
          find_keyword_idx_on_masked(masked_rest, "GROUP BY"),
          find_keyword_idx_on_masked(masked_rest, "HAVING"),
          find_keyword_idx_on_masked(masked_rest, "ORDER BY"),
          find_keyword_idx_on_masked(masked_rest, "LIMIT"),
          find_keyword_idx_on_masked(masked_rest, "RETURNING"),
        ]
        |> list.filter_map(fn(x) {
          case x {
            option.Some(i) -> Ok(i)
            option.None -> Error(Nil)
          }
        })
        |> list.sort(int.compare)
        |> list.first
      let from_part = case end_idx {
        Ok(i) -> string.slice(rest, 0, i)
        Error(_) -> rest
      }
      extract_table_names_from_from(from_part)
    }
  }
}

/// Extract table names from a FROM clause text, handling JOINs.
fn extract_table_names_from_from(from_part: String) -> List(String) {
  let normalized =
    from_part
    |> replace_ignore_case(" LEFT JOIN ", "|||")
    |> replace_ignore_case(" LEFT OUTER JOIN ", "|||")
    |> replace_ignore_case(" RIGHT JOIN ", "|||")
    |> replace_ignore_case(" INNER JOIN ", "|||")
    |> replace_ignore_case(" CROSS JOIN ", "|||")
    |> replace_ignore_case(" JOIN ", "|||")
  let parts = string.split(normalized, "|||")
  list.filter_map(parts, fn(part) {
    let upper = string.uppercase(part)
    let no_on = case string.split_once(upper, " ON ") {
      Ok(_) -> {
        let idx = case string.split_once(upper, " ON ") {
          Ok(#(before, _)) -> string.length(before)
          Error(_) -> string.length(part)
        }
        string.slice(part, 0, idx)
      }
      Error(_) -> part
    }
    case string.split(string.trim(no_on), " ") {
      [table, ..] -> {
        let cleaned = string.trim(table)
        case cleaned {
          "" -> Error(Nil)
          _ -> Ok(cleaned)
        }
      }
      [] -> Error(Nil)
    }
  })
}

fn replace_ignore_case(
  haystack: String,
  needle: String,
  replacement: String,
) -> String {
  let upper_needle = string.uppercase(needle)
  let upper_haystack = string.uppercase(haystack)
  let needle_len = string.length(upper_needle)
  do_replace_ignore_case(
    haystack,
    upper_haystack,
    upper_needle,
    needle_len,
    replacement,
    "",
  )
}

fn do_replace_ignore_case(
  original: String,
  upper: String,
  upper_needle: String,
  needle_len: Int,
  replacement: String,
  acc: String,
) -> String {
  case string.length(upper) < needle_len {
    True -> acc <> original
    False ->
      case string.starts_with(upper, upper_needle) {
        True ->
          do_replace_ignore_case(
            string.drop_start(original, needle_len),
            string.drop_start(upper, needle_len),
            upper_needle,
            needle_len,
            replacement,
            acc <> replacement,
          )
        False -> {
          let first = string.slice(original, 0, 1)
          do_replace_ignore_case(
            string.drop_start(original, 1),
            string.drop_start(upper, 1),
            upper_needle,
            needle_len,
            replacement,
            acc <> first,
          )
        }
      }
  }
}

// ---- Table name extraction ----

/// Extract the first word after a keyword in SQL (case-insensitive).
/// Handles quoted identifiers (double quotes and backticks).
pub fn extract_word_after_keyword(sql: String, keyword: String) -> String {
  let upper = string.uppercase(string.trim(sql))
  let upper_keyword = string.uppercase(keyword)
  case string.split_once(upper, upper_keyword) {
    Ok(#(before, _)) -> {
      let offset = string.length(before) + string.length(upper_keyword)
      let rest = string.drop_start(string.trim(sql), offset) |> string.trim
      case string.first(rest) {
        Ok("\"") -> {
          let inner = string.drop_start(rest, 1)
          case string.split_once(inner, "\"") {
            Ok(#(name, _)) -> name
            Error(_) -> rest
          }
        }
        Ok("`") -> {
          let inner = string.drop_start(rest, 1)
          case string.split_once(inner, "`") {
            Ok(#(name, _)) -> name
            Error(_) -> rest
          }
        }
        _ ->
          case string.split_once(rest, " ") {
            Ok(#(word, _)) -> word
            Error(_) ->
              case string.split_once(rest, "(") {
                Ok(#(word, _)) -> string.trim(word)
                Error(_) -> rest
              }
          }
      }
    }
    Error(_) -> ""
  }
}

pub fn parse_insert_table_name(sql: String) -> String {
  extract_word_after_keyword(sql, "INTO")
}

pub fn parse_update_table_name(sql: String) -> String {
  extract_word_after_keyword(sql, "UPDATE")
}

pub fn parse_delete_table_name(sql: String) -> String {
  extract_word_after_keyword(sql, "FROM")
}

// ---- INSERT column parsing ----

/// Parse INSERT column names from SQL.
/// Uses depth-aware parenthesis matching to handle nested expressions.
pub fn parse_insert_columns(sql: String) -> List(String) {
  // Find the first `(` that appears after the table name, not inside quotes.
  // This handles edge cases like INSERT INTO "table(x)" (col1, col2) VALUES ...
  case string.split_once(sql, "(") {
    Ok(#(_, rest)) ->
      case find_matching_close_paren(rest, 1, "") {
        Ok(cols_str) ->
          cols_str
          |> string.split(",")
          |> list.map(string.trim)
        Error(_) -> []
      }
    Error(_) -> []
  }
}

// ---- RETURNING column parsing ----

/// Parse RETURNING column names from SQL, handling aliases
pub fn parse_returning_columns(sql: String) -> List(String) {
  let upper = string.uppercase(sql)
  case split_on_keyword(upper, " RETURNING ") {
    Ok(#(before, _)) -> {
      let offset = string.length(before) + string.length(" RETURNING ")
      let original_rest = string.drop_start(sql, offset)
      original_rest
      |> string.trim
      |> split_top_level_commas
      |> list.map(fn(col) {
        let trimmed = string.trim(col)
        case rsplit_on_as(trimmed) {
          option.Some(#(_, alias)) -> string.trim(alias)
          option.None -> trimmed
        }
      })
    }
    Error(_) -> []
  }
}

// ---- VALUES placeholder parsing ----

/// Parse the VALUES(...) clause and return the 0-based positions where
/// a `?` placeholder appears.
pub fn parse_values_placeholder_positions(sql: String) -> List(Int) {
  let upper = string.uppercase(sql)
  case string.split_once(upper, " VALUES ") {
    Error(_) -> []
    Ok(#(before, _)) -> {
      let offset = string.length(before) + 8
      let rest = string.drop_start(sql, offset) |> string.trim_start
      case string.starts_with(rest, "(") {
        False -> []
        True -> {
          let inner_and_rest = string.drop_start(rest, 1)
          let inner = walk_matching_paren(inner_and_rest, 1)
          let inner_part =
            string.slice(
              inner_and_rest,
              0,
              string.length(inner_and_rest) - string.length(inner) - 1,
            )
          let parts = split_top_level_commas(inner_part)
          list.index_map(parts, fn(part, idx) { #(part, idx) })
          |> list.filter_map(fn(pair) {
            let #(part, idx) = pair
            let trimmed = string.trim(part)
            case trimmed {
              "?" -> Ok(idx)
              _ ->
                case starts_with_param_prefix(trimmed) {
                  True -> Ok(idx)
                  False -> Error(Nil)
                }
            }
          })
        }
      }
    }
  }
}

// ---- UPDATE SET parsing ----

/// Parse SET assignments from an UPDATE statement.
/// Returns a list of #(param_name, lookup_col) tuples.
pub fn parse_update_set_columns(sql: String) -> List(#(String, String)) {
  let upper = string.uppercase(sql)
  case split_on_keyword(upper, " SET ") {
    Ok(#(_, rest_upper)) -> {
      let offset = string.length(sql) - string.length(rest_upper)
      let rest = string.drop_start(sql, offset)
      let masked_rest = mask_string_contents(rest)
      let set_part = case
        find_top_level_keyword_on_masked(masked_rest, " WHERE ")
      {
        option.Some(idx) -> string.slice(rest, 0, idx)
        option.None ->
          case find_top_level_keyword_on_masked(masked_rest, " RETURNING ") {
            option.Some(idx) -> string.slice(rest, 0, idx)
            option.None -> rest
          }
      }
      set_part
      |> split_top_level_commas
      |> list.filter_map(fn(part) {
        case string.split_once(string.trim(part), "=") {
          Ok(#(col_name, rhs)) -> {
            let col = string.trim(col_name)
            let rhs_trimmed = string.trim(rhs)
            case string.contains(rhs_trimmed, "?") {
              True -> Ok(#(col, col))
              False ->
                case extract_named_param_from_rhs(rhs_trimmed) {
                  Ok(param_name) -> Ok(#(param_name, col))
                  Error(_) -> Error(Nil)
                }
            }
          }
          Error(_) -> Error(Nil)
        }
      })
    }
    Error(_) -> []
  }
}

// ---- WHERE parsing ----

/// Parse WHERE column names from UPDATE/DELETE statement.
pub fn parse_where_columns(sql: String) -> List(#(String, String)) {
  let masked_sql = mask_string_contents(sql)
  case find_top_level_keyword_on_masked(masked_sql, " WHERE ") {
    option.None -> []
    option.Some(where_offset) -> {
      let offset = where_offset + string.length(" WHERE ")
      let rest = string.drop_start(sql, offset)
      let masked_rest = string.drop_start(masked_sql, offset)
      let where_part = case
        find_top_level_keyword_on_masked(masked_rest, " RETURNING ")
      {
        option.Some(ret_offset) -> string.slice(rest, 0, ret_offset)
        option.None -> rest
      }
      split_where_conditions(where_part)
      |> list.flat_map(parse_where_condition)
    }
  }
}

fn parse_where_condition(part: String) -> List(#(String, String)) {
  let trimmed = string.trim(part)
  case contains_param_marker(trimmed) {
    False -> []
    True -> {
      let upper = string.uppercase(trimmed)
      let has_subquery =
        string.contains(upper, " IN (SELECT")
        || string.contains(upper, " IN ( SELECT")
        || string.contains(upper, "= (SELECT")
        || string.contains(upper, "= ( SELECT")
      case has_subquery {
        True ->
          extract_all_named_params(trimmed)
          |> list.map(fn(name) { #(name, name) })
        False -> parse_simple_where_condition(trimmed)
      }
    }
  }
}

fn parse_simple_where_condition(trimmed: String) -> List(#(String, String)) {
  let lhs = extract_column_before_operator(trimmed)
  case lhs {
    "" -> []
    _ ->
      case
        string.contains(lhs, "@")
        || string.contains(lhs, ":")
        || string.contains(lhs, "$")
      {
        True ->
          case extract_named_param_from_rhs(lhs) {
            Ok(param_name) -> {
              let lookup_col = case find_first_param_prefix(lhs) {
                Ok(#(before_prefix, _)) ->
                  before_prefix
                  |> string.trim
                  |> strip_trailing_arithmetic_op
                Error(_) -> param_name
              }
              [#(param_name, normalize_column_ref(lookup_col))]
            }
            Error(_) -> [
              #(normalize_column_ref(lhs), normalize_column_ref(lhs)),
            ]
          }
        False -> {
          let col_name = normalize_column_ref(lhs)
          [#(col_name, col_name)]
        }
      }
  }
}

fn strip_trailing_arithmetic_op(s: String) -> String {
  let has_op =
    string.ends_with(s, "+")
    || string.ends_with(s, "-")
    || string.ends_with(s, "*")
    || string.ends_with(s, "/")
  use <- bool.guard(!has_op, s)
  string.drop_end(s, 1) |> string.trim
}

fn extract_column_before_operator(condition: String) -> String {
  let operators = [">=", "<=", "!=", "<>", "=", ">", "<", " LIKE ", " IN "]
  extract_column_with_operators(string.trim(condition), operators)
}

fn extract_column_with_operators(
  condition: String,
  operators: List(String),
) -> String {
  case operators {
    [] ->
      case string.split_once(condition, " ") {
        Ok(#(col, _)) -> string.trim(col)
        Error(_) -> ""
      }
    [op, ..rest] -> {
      let check = case string.contains(op, " ") {
        True -> string.split_once(string.uppercase(condition), op)
        False -> string.split_once(condition, op)
      }
      case check {
        Ok(#(col, _)) -> string.trim(col)
        Error(_) -> extract_column_with_operators(condition, rest)
      }
    }
  }
}

// ---- Parameter binder scanning ----

/// Walk through the SQL finding each `?` or named parameter and resolve
/// the column it binds to.
pub fn find_param_binders(sql: String, idx: Int, acc: List(Binder)) -> List(Binder) {
  let found = find_next_placeholder(sql, idx)
  case found {
    PlaceholderNone -> list.reverse(acc)
    PlaceholderAnon(pos, after) -> {
      let before = string.slice(sql, 0, pos)
      case extract_column_binder(before) {
        option.None -> []
        option.Some(col) -> {
          let bare = case string.split_once(col, ".") {
            Ok(#(_, after_dot)) -> after_dot
            Error(_) -> col
          }
          find_param_binders(sql, after, [
            Binder(name: bare, binder_column: option.Some(col)),
            ..acc
          ])
        }
      }
    }
    PlaceholderNamed(name, after) ->
      case list.find(acc, fn(b) { b.name == name }) {
        Ok(_) -> find_param_binders(sql, after, acc)
        Error(_) -> {
          let before = string.slice(sql, 0, after - string.length(name) - 1)
          let column = extract_column_binder(before)
          find_param_binders(sql, after, [
            Binder(name: name, binder_column: column),
            ..acc
          ])
        }
      }
  }
}

fn find_next_placeholder(sql: String, from_idx: Int) -> Placeholder {
  let rest = string.drop_start(sql, from_idx)
  do_find_placeholder(rest, from_idx, False, False)
}

fn do_find_placeholder(
  s: String,
  idx: Int,
  in_single: Bool,
  in_double: Bool,
) -> Placeholder {
  let in_quoted = in_single || in_double
  case string.pop_grapheme(s) {
    Error(_) -> PlaceholderNone
    Ok(#("'", rest)) ->
      case in_double {
        True -> do_find_placeholder(rest, idx + 1, in_single, True)
        False ->
          case in_single {
            True ->
              case string.pop_grapheme(rest) {
                Ok(#("'", rest2)) ->
                  do_find_placeholder(rest2, idx + 2, True, False)
                _ -> do_find_placeholder(rest, idx + 1, False, False)
              }
            False -> do_find_placeholder(rest, idx + 1, True, False)
          }
      }
    Ok(#("\"", rest)) ->
      case in_single {
        True -> do_find_placeholder(rest, idx + 1, True, in_double)
        False -> do_find_placeholder(rest, idx + 1, False, !in_double)
      }
    Ok(#("?", rest)) ->
      case in_quoted {
        True -> do_find_placeholder(rest, idx + 1, in_single, in_double)
        False -> PlaceholderAnon(idx, idx + 1)
      }
    Ok(#("@", rest)) ->
      case in_quoted {
        True -> do_find_placeholder(rest, idx + 1, in_single, in_double)
        False -> read_named_placeholder(rest, idx + 1)
      }
    Ok(#(":", rest)) ->
      case in_quoted {
        True -> do_find_placeholder(rest, idx + 1, in_single, in_double)
        False -> read_named_placeholder(rest, idx + 1)
      }
    Ok(#("$", rest)) ->
      case in_quoted {
        True -> do_find_placeholder(rest, idx + 1, in_single, in_double)
        False -> read_named_placeholder(rest, idx + 1)
      }
    Ok(#(_, rest)) -> do_find_placeholder(rest, idx + 1, in_single, in_double)
  }
}

fn read_named_placeholder(s: String, start_idx: Int) -> Placeholder {
  do_read_named(s, start_idx, "")
}

fn do_read_named(s: String, idx: Int, acc: String) -> Placeholder {
  case string.pop_grapheme(s) {
    Error(_) ->
      case acc {
        "" -> PlaceholderNone
        n -> PlaceholderNamed(n, idx)
      }
    Ok(#(char, rest)) ->
      case is_alphanumeric_char(char) {
        True -> do_read_named(rest, idx + 1, acc <> char)
        False ->
          case acc {
            "" -> do_find_placeholder(s, idx, False, False)
            n -> PlaceholderNamed(n, idx)
          }
      }
  }
}

/// Given the SQL text before a `?`, extract the column name being compared
/// against it.
fn extract_column_binder(before: String) -> option.Option(String) {
  let trimmed = string.trim_end(before)
  let upper = string.uppercase(trimmed)
  case extract_between_column(trimmed, upper) {
    option.Some(col) -> option.Some(col)
    option.None -> {
      let sym_operators = [">=", "<=", "!=", "<>", "=", ">", "<"]
      let without_sym = strip_trailing_operator(trimmed, sym_operators)
      let without_op = case without_sym == trimmed {
        True -> strip_trailing_keyword_operator(trimmed)
        False -> without_sym
      }
      let trimmed2 = string.trim_end(without_op)
      let id = case take_trailing_identifier(trimmed2) {
        "" -> extract_identifier_from_trailing_parens(trimmed2)
        name -> name
      }
      case id {
        "" -> option.None
        name ->
          case is_simple_column_ref(name) {
            True -> option.Some(name)
            False -> option.None
          }
      }
    }
  }
}

fn extract_between_column(
  trimmed: String,
  upper: String,
) -> option.Option(String) {
  case string.ends_with(upper, " BETWEEN") {
    True -> {
      let without_kw = string.drop_end(trimmed, 8)
      let name = take_trailing_identifier(string.trim_end(without_kw))
      case is_simple_column_ref(name) {
        True -> option.Some(name)
        False -> option.None
      }
    }
    False ->
      case string.ends_with(upper, " AND") {
        True -> {
          let upper_before_and = string.drop_end(upper, 4)
          case string.split_once(upper_before_and, " BETWEEN ") {
            Ok(#(before_between_upper, _)) -> {
              let prefix_len = string.length(before_between_upper)
              let before_between = string.slice(trimmed, 0, prefix_len)
              let trimmed_before_between = string.trim_end(before_between)
              let name = take_trailing_identifier(trimmed_before_between)
              case is_simple_column_ref(name) {
                True -> option.Some(name)
                False -> option.None
              }
            }
            Error(_) -> option.None
          }
        }
        False -> option.None
      }
  }
}

fn extract_identifier_from_trailing_parens(s: String) -> String {
  let trimmed = string.trim_end(s)
  case string.ends_with(trimmed, ")") {
    False -> ""
    True -> {
      let without_close = string.slice(trimmed, 0, string.length(trimmed) - 1)
      let inside = find_matching_paren_content(without_close, 1, "")
      take_trailing_identifier(string.trim_end(inside))
    }
  }
}

fn find_matching_paren_content(s: String, depth: Int, acc: String) -> String {
  let reversed = string.to_graphemes(s) |> list.reverse
  do_find_matching_paren(reversed, depth, acc)
}

fn do_find_matching_paren(
  chars: List(String),
  depth: Int,
  acc: String,
) -> String {
  case depth {
    0 -> acc
    _ ->
      case chars {
        [] -> acc
        [")", ..rest] -> do_find_matching_paren(rest, depth + 1, ")" <> acc)
        ["(", ..rest] ->
          case depth {
            1 -> acc
            _ -> do_find_matching_paren(rest, depth - 1, "(" <> acc)
          }
        [char, ..rest] -> do_find_matching_paren(rest, depth, char <> acc)
      }
  }
}

fn strip_trailing_keyword_operator(s: String) -> String {
  let upper = string.uppercase(s)
  let keywords = [" LIKE", " IS NOT", " IS"]
  do_strip_trailing_keyword(s, upper, keywords)
}

fn do_strip_trailing_keyword(
  s: String,
  upper: String,
  keywords: List(String),
) -> String {
  case keywords {
    [] -> s
    [kw, ..rest] -> {
      case string.ends_with(upper, kw) {
        True -> string.slice(s, 0, string.length(s) - string.length(kw))
        False -> do_strip_trailing_keyword(s, upper, rest)
      }
    }
  }
}

fn strip_trailing_operator(s: String, operators: List(String)) -> String {
  case operators {
    [] -> s
    [op, ..rest] -> {
      let len = string.length(op)
      case string.length(s) >= len {
        True -> {
          let tail = string.slice(s, string.length(s) - len, len)
          case string.uppercase(tail) == op {
            True -> string.slice(s, 0, string.length(s) - len)
            False -> strip_trailing_operator(s, rest)
          }
        }
        False -> strip_trailing_operator(s, rest)
      }
    }
  }
}

fn take_trailing_identifier(s: String) -> String {
  let graphemes = string.to_graphemes(s)
  take_trailing_id_chars(list.reverse(graphemes), [])
  |> string.join("")
}

fn take_trailing_id_chars(
  reversed: List(String),
  acc: List(String),
) -> List(String) {
  case reversed {
    [] -> acc
    [char, ..rest] ->
      case is_ident_or_dot(char) {
        True -> take_trailing_id_chars(rest, [char, ..acc])
        False -> acc
      }
  }
}

fn is_ident_or_dot(c: String) -> Bool {
  c == "." || is_alphanumeric_char(c) || c == "_"
}

fn is_simple_column_ref(s: String) -> Bool {
  case string.split_once(s, ".") {
    Ok(#(table, col)) ->
      is_simple_identifier(table) && is_simple_identifier(col)
    Error(_) -> is_simple_identifier(s)
  }
}

// ---- Named parameter extraction ----

/// Check if a string starts with a named parameter prefix (@, :, or $).
pub fn starts_with_param_prefix(s: String) -> Bool {
  string.starts_with(s, "@")
  || string.starts_with(s, ":")
  || string.starts_with(s, "$")
}

fn contains_param_marker(s: String) -> Bool {
  string.contains(s, "?")
  || string.contains(s, "@")
  || string.contains(s, ":")
  || string.contains(s, "$")
}

fn find_first_param_prefix(s: String) -> Result(#(String, String), Nil) {
  do_find_first_param_prefix(s, "")
}

fn do_find_first_param_prefix(
  remaining: String,
  before: String,
) -> Result(#(String, String), Nil) {
  case string.pop_grapheme(remaining) {
    Error(_) -> Error(Nil)
    Ok(#(c, rest)) ->
      case c == "@" || c == ":" || c == "$" {
        True -> Ok(#(before, rest))
        False -> do_find_first_param_prefix(rest, before <> c)
      }
  }
}

fn extract_named_param_from_rhs(rhs: String) -> Result(String, Nil) {
  case find_first_param_prefix(rhs) {
    Error(_) -> Error(Nil)
    Ok(#(_, after)) -> Ok(take_identifier_chars(after, ""))
  }
}

/// Extract every @name, :name, or $name identifier from a string.
pub fn extract_all_named_params(s: String) -> List(String) {
  do_extract_all_named_params(s, [])
}

fn do_extract_all_named_params(s: String, acc: List(String)) -> List(String) {
  case find_first_param_prefix(s) {
    Error(_) -> list.reverse(acc)
    Ok(#(_, after)) -> {
      let name = take_identifier_chars(after, "")
      let rest = string.drop_start(after, string.length(name))
      case name {
        "" -> do_extract_all_named_params(rest, acc)
        _ -> do_extract_all_named_params(rest, [name, ..acc])
      }
    }
  }
}

fn take_identifier_chars(s: String, acc: String) -> String {
  case string.pop_grapheme(s) {
    Error(_) -> acc
    Ok(#(char, rest)) -> {
      case is_identifier_char(char) {
        True -> take_identifier_chars(rest, acc <> char)
        False -> acc
      }
    }
  }
}

// ---- Subquery table extraction ----

pub fn find_all_subquery_tables(sql: String) -> List(String) {
  do_find_subquery_tables(sql, [])
}

fn do_find_subquery_tables(sql: String, acc: List(String)) -> List(String) {
  case find_from_outside_strings(sql, 0, False, False) {
    option.None -> list.reverse(acc)
    option.Some(offset) -> {
      let rest = string.drop_start(sql, offset + 5) |> string.trim_start
      let table =
        rest
        |> string.to_graphemes
        |> list.take_while(fn(c) { c == "_" || is_alphanumeric_char(c) })
        |> string.join("")
      let new_acc = case table {
        "" -> acc
        name -> [name, ..acc]
      }
      do_find_subquery_tables(rest, new_acc)
    }
  }
}

fn find_from_outside_strings(
  s: String,
  idx: Int,
  in_single: Bool,
  in_double: Bool,
) -> option.Option(Int) {
  case string.pop_grapheme(s) {
    Error(_) -> option.None
    Ok(#("'", rest)) ->
      case in_double {
        True -> find_from_outside_strings(rest, idx + 1, in_single, True)
        False ->
          case in_single {
            True ->
              case string.pop_grapheme(rest) {
                Ok(#("'", rest2)) ->
                  find_from_outside_strings(rest2, idx + 2, True, False)
                _ -> find_from_outside_strings(rest, idx + 1, False, False)
              }
            False -> find_from_outside_strings(rest, idx + 1, True, False)
          }
      }
    Ok(#("\"", rest)) ->
      case in_single {
        True -> find_from_outside_strings(rest, idx + 1, True, in_double)
        False -> find_from_outside_strings(rest, idx + 1, False, !in_double)
      }
    Ok(#(_, rest)) if in_single || in_double ->
      find_from_outside_strings(rest, idx + 1, in_single, in_double)
    Ok(#(" ", rest)) -> {
      let head = string.slice(s, 0, 6)
      case string.uppercase(head) == " FROM " {
        True -> option.Some(idx)
        False -> find_from_outside_strings(rest, idx + 1, False, False)
      }
    }
    Ok(#(_, rest)) -> find_from_outside_strings(rest, idx + 1, False, False)
  }
}

fn is_alphanumeric_char(c: String) -> Bool {
  query.is_sql_ident_char(c)
}

// ---- Column reference normalization ----

/// Strip table prefixes ("t.name" -> "name") and function wrappers
/// ("LOWER(TRIM(name))" -> "name") from a column reference.
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

/// When an expression-based SELECT item can't be resolved to a table column,
/// infer its type from the expression shape. Handles common aggregates and
/// explicit CASTs. Falls back to StringType nullable.
pub fn infer_expression_type(item: SelectItem) -> Column {
  let upper = string.uppercase(item.expression)
  case infer_literal_type(string.trim(item.expression)) {
    option.Some(t) -> Column(name: item.alias, column_type: t, nullable: False)
    option.None ->
      case string.starts_with(upper, "COUNT(") {
        True ->
          Column(name: item.alias, column_type: query.IntType, nullable: False)
        False ->
          case string.starts_with(upper, "EXISTS(") {
            True ->
              Column(
                name: item.alias,
                column_type: query.IntType,
                nullable: False,
              )
            False ->
              case string.starts_with(upper, "CAST(") {
                True -> infer_cast_type(item.alias, upper)
                False ->
                  case string.starts_with(upper, "COALESCE(") {
                    True -> infer_coalesce_type(item.alias, item.expression)
                    False ->
                      case string.starts_with(upper, "SUM(") {
                        True ->
                          // Design decision: SUM() is always mapped to FloatType.
                          // SQLite returns integer for SUM of integer columns,
                          // but Float is a safe superset that avoids silent
                          // truncation. Nullable because SUM returns NULL for
                          // empty result sets.
                          Column(
                            name: item.alias,
                            column_type: query.FloatType,
                            nullable: True,
                          )
                        False ->
                          case is_integer_window_function(upper) {
                            True ->
                              Column(
                                name: item.alias,
                                column_type: query.IntType,
                                nullable: False,
                              )
                            False ->
                              case string.starts_with(upper, "CASE ") {
                                True ->
                                  infer_case_type(item.alias, item.expression)
                                False ->
                                  Column(
                                    name: item.alias,
                                    column_type: StringType,
                                    nullable: True,
                                  )
                              }
                          }
                      }
                  }
              }
          }
      }
  }
}

fn infer_coalesce_type(alias: String, expr: String) -> Column {
  let inner = case string.split_once(expr, "(") {
    Ok(#(_, after)) -> after
    Error(_) -> expr
  }
  let inner = case string.ends_with(inner, ")") {
    True -> string.drop_end(inner, 1)
    False -> inner
  }
  let args = split_top_level_commas(inner)
  case list.last(args) {
    Error(_) -> Column(name: alias, column_type: StringType, nullable: True)
    Ok(last_arg) -> {
      let trimmed = string.trim(last_arg)
      case infer_literal_type(trimmed) {
        option.Some(t) -> Column(name: alias, column_type: t, nullable: False)
        option.None ->
          Column(name: alias, column_type: StringType, nullable: True)
      }
    }
  }
}

fn infer_case_type(alias: String, expr: String) -> Column {
  let #(branches, has_else) = extract_case_branches(expr)
  case branches {
    [] -> Column(name: alias, column_type: StringType, nullable: True)
    _ -> {
      let types =
        list.filter_map(branches, fn(branch) {
          let trimmed = string.trim(branch)
          case string.uppercase(trimmed) {
            "NULL" -> Error(Nil)
            _ ->
              case infer_literal_type(trimmed) {
                option.Some(t) -> Ok(t)
                option.None -> Error(Nil)
              }
          }
        })
      let has_null =
        list.any(branches, fn(b) { string.uppercase(string.trim(b)) == "NULL" })
      let has_unresolved =
        list.length(types)
        + {
          case has_null {
            True ->
              list.count(branches, fn(b) {
                string.uppercase(string.trim(b)) == "NULL"
              })
            False -> 0
          }
        }
        != list.length(branches)
      case has_unresolved {
        True -> Column(name: alias, column_type: StringType, nullable: True)
        False ->
          case types {
            [] -> Column(name: alias, column_type: StringType, nullable: True)
            [first, ..rest] ->
              case list.all(rest, fn(t) { t == first }) {
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

fn extract_case_branches(expr: String) -> #(List(String), Bool) {
  let body = string.drop_start(expr, 4)
  let upper_body = string.uppercase(body)
  let body = case find_top_level_end(upper_body) {
    option.Some(idx) -> string.slice(body, 0, idx)
    option.None -> body
  }
  do_extract_branches(body, string.uppercase(body), "", [], False, 0, InNone)
}

fn do_extract_branches(
  original: String,
  upper: String,
  current: String,
  acc: List(String),
  has_else: Bool,
  depth: Int,
  state: CaseScanState,
) -> #(List(String), Bool) {
  case string.length(upper) {
    0 -> {
      let final_acc = case state {
        InThen | InElse ->
          case string.trim(current) {
            "" -> acc
            trimmed -> [trimmed, ..acc]
          }
        _ -> acc
      }
      #(list.reverse(final_acc), has_else)
    }
    _ -> {
      let head = string.slice(upper, 0, 1)
      case head {
        "'" -> {
          let #(literal, rest_orig, rest_upper) =
            consume_string_literal(original, upper)
          do_extract_branches(
            rest_orig,
            rest_upper,
            current <> literal,
            acc,
            has_else,
            depth,
            state,
          )
        }
        "(" ->
          do_extract_branches(
            string.drop_start(original, 1),
            string.drop_start(upper, 1),
            current <> string.slice(original, 0, 1),
            acc,
            has_else,
            depth + 1,
            state,
          )
        ")" ->
          do_extract_branches(
            string.drop_start(original, 1),
            string.drop_start(upper, 1),
            current <> string.slice(original, 0, 1),
            acc,
            has_else,
            depth - 1,
            state,
          )
        _ ->
          case depth == 0 {
            False ->
              do_extract_branches(
                string.drop_start(original, 1),
                string.drop_start(upper, 1),
                current <> string.slice(original, 0, 1),
                acc,
                has_else,
                depth,
                state,
              )
            True ->
              case
                string.starts_with(upper, "CASE ")
                || string.starts_with(upper, "CASE\t")
              {
                True ->
                  do_extract_branches(
                    string.drop_start(original, 4),
                    string.drop_start(upper, 4),
                    current <> string.slice(original, 0, 4),
                    acc,
                    has_else,
                    depth + 1,
                    state,
                  )
                False ->
                  case
                    string.starts_with(upper, "END ")
                    || string.starts_with(upper, "END\t")
                    || upper == "END"
                  {
                    True -> {
                      let end_len = case upper == "END" {
                        True -> 3
                        False -> 3
                      }
                      do_extract_branches(
                        string.drop_start(original, end_len),
                        string.drop_start(upper, end_len),
                        current <> string.slice(original, 0, end_len),
                        acc,
                        has_else,
                        depth - 1,
                        state,
                      )
                    }
                    False ->
                      case string.starts_with(upper, " WHEN ") {
                        True -> {
                          let new_acc = case state {
                            InThen | InElse ->
                              case string.trim(current) {
                                "" -> acc
                                trimmed -> [trimmed, ..acc]
                              }
                            _ -> acc
                          }
                          do_extract_branches(
                            string.drop_start(original, 6),
                            string.drop_start(upper, 6),
                            "",
                            new_acc,
                            has_else,
                            0,
                            InWhen,
                          )
                        }
                        False ->
                          case string.starts_with(upper, " THEN ") {
                            True ->
                              do_extract_branches(
                                string.drop_start(original, 6),
                                string.drop_start(upper, 6),
                                "",
                                acc,
                                has_else,
                                0,
                                InThen,
                              )
                            False ->
                              case string.starts_with(upper, " ELSE ") {
                                True -> {
                                  let new_acc = case state {
                                    InThen ->
                                      case string.trim(current) {
                                        "" -> acc
                                        trimmed -> [trimmed, ..acc]
                                      }
                                    _ -> acc
                                  }
                                  do_extract_branches(
                                    string.drop_start(original, 6),
                                    string.drop_start(upper, 6),
                                    "",
                                    new_acc,
                                    True,
                                    0,
                                    InElse,
                                  )
                                }
                                False ->
                                  do_extract_branches(
                                    string.drop_start(original, 1),
                                    string.drop_start(upper, 1),
                                    current <> string.slice(original, 0, 1),
                                    acc,
                                    has_else,
                                    depth,
                                    state,
                                  )
                              }
                          }
                      }
                  }
              }
          }
      }
    }
  }
}

fn find_top_level_end(upper: String) -> option.Option(Int) {
  do_find_top_level_end(mask_string_contents(upper), 0, 0)
}

fn do_find_top_level_end(
  remaining: String,
  idx: Int,
  depth: Int,
) -> option.Option(Int) {
  case string.length(remaining) {
    0 -> option.None
    _ -> {
      let head = string.slice(remaining, 0, 1)
      case head {
        "'" -> skip_string_literal_end(remaining, idx, depth)
        "(" ->
          do_find_top_level_end(
            string.drop_start(remaining, 1),
            idx + 1,
            depth + 1,
          )
        ")" ->
          do_find_top_level_end(
            string.drop_start(remaining, 1),
            idx + 1,
            depth - 1,
          )
        _ ->
          case depth == 0 {
            False ->
              do_find_top_level_end(
                string.drop_start(remaining, 1),
                idx + 1,
                depth,
              )
            True ->
              case string.starts_with(remaining, "CASE ") {
                True ->
                  do_find_top_level_end(
                    string.drop_start(remaining, 4),
                    idx + 4,
                    depth + 1,
                  )
                False ->
                  case
                    remaining == "END"
                    || string.starts_with(remaining, "END ")
                    || string.starts_with(remaining, "END\t")
                  {
                    True -> option.Some(idx)
                    False ->
                      do_find_top_level_end(
                        string.drop_start(remaining, 1),
                        idx + 1,
                        depth,
                      )
                  }
              }
          }
      }
    }
  }
}

fn skip_string_literal_end(
  remaining: String,
  idx: Int,
  depth: Int,
) -> option.Option(Int) {
  let rest = string.drop_start(remaining, 1)
  do_skip_string_literal_end(rest, idx + 1, depth)
}

fn do_skip_string_literal_end(
  remaining: String,
  idx: Int,
  depth: Int,
) -> option.Option(Int) {
  case string.pop_grapheme(remaining) {
    Error(_) -> option.None
    Ok(#("'", rest)) ->
      case string.pop_grapheme(rest) {
        Ok(#("'", rest2)) -> do_skip_string_literal_end(rest2, idx + 2, depth)
        _ -> do_find_top_level_end(rest, idx + 1, depth)
      }
    Ok(#(_, rest)) -> do_skip_string_literal_end(rest, idx + 1, depth)
  }
}

fn consume_string_literal(
  original: String,
  upper: String,
) -> #(String, String, String) {
  let orig_rest = string.drop_start(original, 1)
  let upper_rest = string.drop_start(upper, 1)
  do_consume_string_literal(orig_rest, upper_rest, "'")
}

fn do_consume_string_literal(
  original: String,
  upper: String,
  acc: String,
) -> #(String, String, String) {
  case string.pop_grapheme(original) {
    Error(_) -> #(acc, "", "")
    Ok(#("'", orig_rest)) ->
      case string.pop_grapheme(orig_rest) {
        Ok(#("'", orig_rest2)) ->
          do_consume_string_literal(
            orig_rest2,
            string.drop_start(upper, 2),
            acc <> "''",
          )
        _ -> #(acc <> "'", orig_rest, string.drop_start(upper, 1))
      }
    Ok(#(ch, orig_rest)) ->
      do_consume_string_literal(
        orig_rest,
        string.drop_start(upper, 1),
        acc <> ch,
      )
  }
}

fn is_integer_window_function(upper: String) -> Bool {
  let starts_with_fn =
    string.starts_with(upper, "ROW_NUMBER(")
    || string.starts_with(upper, "RANK(")
    || string.starts_with(upper, "DENSE_RANK(")
    || string.starts_with(upper, "NTILE(")
  starts_with_fn && string.contains(upper, ") OVER")
}

fn infer_literal_type(s: String) -> option.Option(ColumnType) {
  case string.first(s) {
    Error(_) -> option.None
    Ok("'") -> option.Some(StringType)
    Ok(c) ->
      case is_digit_char(c) || c == "-" {
        True ->
          case string.contains(s, ".") {
            True -> option.Some(query.FloatType)
            False -> option.Some(query.IntType)
          }
        False -> option.None
      }
  }
}

fn is_digit_char(c: String) -> Bool {
  case c {
    "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" -> True
    _ -> False
  }
}

fn infer_cast_type(name: String, upper_expr: String) -> Column {
  let inner = case string.starts_with(upper_expr, "CAST(") {
    True -> {
      let after = string.drop_start(upper_expr, 5)
      case string.ends_with(after, ")") {
        True -> string.drop_end(after, 1)
        False -> after
      }
    }
    False -> upper_expr
  }
  case find_last_top_level_as(inner) {
    option.None -> Column(name: name, column_type: StringType, nullable: True)
    option.Some(as_idx) -> {
      let after_as = string.drop_start(inner, as_idx + 4)
      let target = string.trim(after_as)
      let target = case string.split_once(target, ")") {
        Ok(#(t, _)) -> string.trim(t)
        Error(_) -> target
      }
      case target {
        "INTEGER" | "INT" | "BIGINT" ->
          Column(name: name, column_type: query.IntType, nullable: False)
        "REAL" | "FLOAT" | "DOUBLE" ->
          Column(name: name, column_type: query.FloatType, nullable: False)
        "TEXT" | "VARCHAR" | "CHAR" ->
          Column(name: name, column_type: StringType, nullable: False)
        "BLOB" ->
          Column(name: name, column_type: query.BitArrayType, nullable: False)
        _ -> Column(name: name, column_type: StringType, nullable: True)
      }
    }
  }
}

fn find_last_top_level_as(s: String) -> option.Option(Int) {
  do_find_last_top_level_as(mask_string_contents(s), 0, 0, option.None)
}

fn do_find_last_top_level_as(
  s: String,
  idx: Int,
  depth: Int,
  last: option.Option(Int),
) -> option.Option(Int) {
  use <- bool.guard(string.length(s) < 4, last)
  let ch = string.slice(s, 0, 1)
  case ch {
    "(" ->
      do_find_last_top_level_as(
        string.drop_start(s, 1),
        idx + 1,
        depth + 1,
        last,
      )
    ")" ->
      do_find_last_top_level_as(
        string.drop_start(s, 1),
        idx + 1,
        depth - 1,
        last,
      )
    _ ->
      case depth == 0 {
        True -> {
          let head = string.slice(s, 0, 4)
          case string.uppercase(head) == " AS " {
            True ->
              do_find_last_top_level_as(
                string.drop_start(s, 1),
                idx + 1,
                depth,
                option.Some(idx),
              )
            False ->
              do_find_last_top_level_as(
                string.drop_start(s, 1),
                idx + 1,
                depth,
                last,
              )
          }
        }
        False ->
          do_find_last_top_level_as(
            string.drop_start(s, 1),
            idx + 1,
            depth,
            last,
          )
      }
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

// ---- Utility functions ----

/// Escape double quotes in an identifier to prevent SQL injection.
pub fn quote_identifier(name: String) -> String {
  string.replace(name, "\"", "\"\"")
}

/// Get element at index from a list
pub fn list_at(lst: List(a), idx: Int) -> Result(a, Nil) {
  lst |> list.drop(idx) |> list.first
}

/// Create a list of integers from start to start+count-1
pub fn make_range(start: Int, count: Int) -> List(Int) {
  case count <= 0 {
    True -> []
    False ->
      int.range(from: start, to: start + count, with: [], run: fn(acc, i) {
        [i, ..acc]
      })
      |> list.reverse
  }
}
