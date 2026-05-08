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

/// Extract table name after a keyword (INTO, UPDATE, FROM for DELETE).
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
        True ->
          case rest {
            [Word(or_kw), Word(next), ..rest2] -> {
              case string.uppercase(or_kw) == "OR" {
                True ->
                  case rest2 {
                    [Word(into), ..rest3] -> {
                      case string.uppercase(into) == "INTO" {
                        True -> tokenize.first_word(rest3)
                        False -> tokenize.first_word(rest2)
                      }
                    }
                    _ -> tokenize.first_word(rest2)
                  }
                False ->
                  case string.uppercase(or_kw) == "INTO" {
                    True -> tokenize.first_word([Word(next), ..rest2])
                    False -> tokenize.first_word(rest)
                  }
              }
            }
            [Word(into), ..rest2] ->
              case string.uppercase(into) == "INTO" {
                True -> tokenize.first_word(rest2)
                False -> tokenize.first_word(rest)
              }
            _ -> tokenize.first_word(rest)
          }
        False -> extract_name_after_keyword(tokens, "INTO")
      }
    }
    _ -> ""
  }
}

pub fn parse_update_table_name(tokens: List(Token)) -> String {
  extract_name_after_keyword(tokens, "UPDATE")
}

pub fn parse_delete_table_name(tokens: List(Token)) -> String {
  extract_name_after_keyword(tokens, "FROM")
}
