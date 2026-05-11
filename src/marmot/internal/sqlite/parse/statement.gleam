//// Statement classification and table name extraction.

import gleam/string
import marmot/internal/sqlite/tokenize.{type Token, Word}

pub type StatementType {
  Select
  Insert
  Update
  Delete
  Replace
  Other
}

pub fn classify_statement(tokens: List(Token)) -> StatementType {
  case tokens {
    [Word(w), ..] ->
      case string.uppercase(w) {
        "SELECT" -> Select
        "INSERT" -> Insert
        "UPDATE" -> Update
        "DELETE" -> Delete
        "REPLACE" -> Replace
        _ -> Other
      }
    _ -> Other
  }
}

fn extract_name_after_keyword(tokens: List(Token), keyword: String) -> String {
  case tokenize.split_at_keyword(tokens, keyword) {
    Ok(#(_, after)) -> tokenize.first_word(after)
    Error(_) -> ""
  }
}

pub fn parse_insert_table_name(tokens: List(Token)) -> String {
  case tokens {
    [Word(w), ..rest] -> {
      let upper = string.uppercase(w)
      case upper == "INSERT" || upper == "REPLACE" {
        True -> parse_insert_table_name_after_verb(rest)
        False -> extract_name_after_keyword(tokens, "INTO")
      }
    }
    _ -> ""
  }
}

fn parse_insert_table_name_after_verb(tokens: List(Token)) -> String {
  case tokens {
    [Word(or_kw), Word(next), ..rest] ->
      parse_insert_table_name_after_two_words(or_kw, next, rest, tokens)
    [Word(into), ..rest] ->
      case string.uppercase(into) == "INTO" {
        True -> tokenize.first_word(rest)
        False -> tokenize.first_word(tokens)
      }
    _ -> tokenize.first_word(tokens)
  }
}

fn parse_insert_table_name_after_two_words(
  first: String,
  second: String,
  rest: List(Token),
  original: List(Token),
) -> String {
  case string.uppercase(first) {
    "OR" -> parse_insert_table_name_after_conflict(rest)
    "INTO" -> tokenize.first_word([Word(second), ..rest])
    _ -> tokenize.first_word(original)
  }
}

fn parse_insert_table_name_after_conflict(tokens: List(Token)) -> String {
  case tokens {
    [Word(into), ..rest] ->
      case string.uppercase(into) == "INTO" {
        True -> tokenize.first_word(rest)
        False -> tokenize.first_word(tokens)
      }
    _ -> tokenize.first_word(tokens)
  }
}

pub fn parse_update_table_name(tokens: List(Token)) -> String {
  extract_name_after_keyword(tokens, "UPDATE")
}

pub fn parse_delete_table_name(tokens: List(Token)) -> String {
  extract_name_after_keyword(tokens, "FROM")
}
