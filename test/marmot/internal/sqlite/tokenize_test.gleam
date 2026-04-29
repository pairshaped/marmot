import gleam/list
import gleam/option
import gleam/string
import marmot/internal/sqlite/tokenize.{
  CloseParen, Comma, Dot, Eq, Ge, Gt, Le, Lt, Minus, Ne, NullOverride,
  NullableOverride, Number, OpenParen, ParamAnon, ParamNamed, Plus, QuotedId,
  Semicolon, Slash, Star, StringLit, Word,
}

// ---- Basic tokenization ----

pub fn tokenize_simple_select_test() {
  let tokens = tokenize.tokenize("SELECT id, name FROM users")
  let assert [
    Word("SELECT"),
    Word("id"),
    Comma,
    Word("name"),
    Word("FROM"),
    Word("users"),
  ] = tokens
}

pub fn tokenize_select_star_test() {
  let tokens = tokenize.tokenize("SELECT * FROM users")
  let assert [Word("SELECT"), Star, Word("FROM"), Word("users")] = tokens
}

pub fn tokenize_whitespace_variations_test() {
  let tokens = tokenize.tokenize("SELECT  id \t FROM \n users")
  let assert [Word("SELECT"), Word("id"), Word("FROM"), Word("users")] = tokens
}

// ---- Operators ----

pub fn tokenize_comparison_operators_test() {
  let tokens = tokenize.tokenize("a = b")
  let assert [Word("a"), Eq, Word("b")] = tokens

  let tokens2 = tokenize.tokenize("a != b")
  let assert [Word("a"), Ne, Word("b")] = tokens2

  let tokens3 = tokenize.tokenize("a <> b")
  let assert [Word("a"), Ne, Word("b")] = tokens3

  let tokens4 = tokenize.tokenize("a >= b")
  let assert [Word("a"), Ge, Word("b")] = tokens4

  let tokens5 = tokenize.tokenize("a <= b")
  let assert [Word("a"), Le, Word("b")] = tokens5

  let tokens6 = tokenize.tokenize("a > b")
  let assert [Word("a"), Gt, Word("b")] = tokens6

  let tokens7 = tokenize.tokenize("a < b")
  let assert [Word("a"), Lt, Word("b")] = tokens7
}

pub fn tokenize_arithmetic_operators_test() {
  let tokens = tokenize.tokenize("a + b - c * d / e")
  let assert [
    Word("a"),
    Plus,
    Word("b"),
    Minus,
    Word("c"),
    Star,
    Word("d"),
    Slash,
    Word("e"),
  ] = tokens
}

// ---- String literals ----

pub fn tokenize_string_literal_test() {
  let tokens = tokenize.tokenize("'hello world'")
  let assert [StringLit("hello world")] = tokens
}

pub fn tokenize_string_with_escaped_quotes_test() {
  let tokens = tokenize.tokenize("'it''s fine'")
  let assert [StringLit("it's fine")] = tokens
}

pub fn tokenize_string_in_where_test() {
  let tokens = tokenize.tokenize("WHERE name = 'test'")
  let assert [Word("WHERE"), Word("name"), Eq, StringLit("test")] = tokens
}

// ---- Quoted identifiers ----

pub fn tokenize_double_quoted_identifier_test() {
  let tokens = tokenize.tokenize("SELECT \"column name\" FROM t")
  let assert [Word("SELECT"), QuotedId("column name"), Word("FROM"), Word("t")] =
    tokens
}

pub fn tokenize_backtick_identifier_test() {
  let tokens = tokenize.tokenize("SELECT `col` FROM t")
  let assert [Word("SELECT"), QuotedId("col"), Word("FROM"), Word("t")] = tokens
}

// ---- Numbers ----

pub fn tokenize_integer_test() {
  let tokens = tokenize.tokenize("42")
  let assert [Number("42")] = tokens
}

pub fn tokenize_float_test() {
  let tokens = tokenize.tokenize("3.14")
  let assert [Number("3.14")] = tokens
}

pub fn tokenize_negative_number_test() {
  let tokens = tokenize.tokenize("-5")
  let assert [Minus, Number("5")] = tokens
}

// ---- Parameters ----

pub fn tokenize_anonymous_param_test() {
  let tokens = tokenize.tokenize("WHERE id = ?")
  let assert [Word("WHERE"), Word("id"), Eq, ParamAnon] = tokens
}

pub fn tokenize_named_param_at_test() {
  let tokens = tokenize.tokenize("WHERE id = @user_id")
  let assert [Word("WHERE"), Word("id"), Eq, ParamNamed("user_id")] = tokens
}

pub fn tokenize_named_param_colon_test() {
  let tokens = tokenize.tokenize("WHERE id = :id")
  let assert [Word("WHERE"), Word("id"), Eq, ParamNamed("id")] = tokens
}

pub fn tokenize_named_param_dollar_test() {
  let tokens = tokenize.tokenize("WHERE id = $id")
  let assert [Word("WHERE"), Word("id"), Eq, ParamNamed("id")] = tokens
}

pub fn tokenize_multiple_params_test() {
  let tokens = tokenize.tokenize("WHERE a = ? AND b = ?")
  let assert [
    Word("WHERE"),
    Word("a"),
    Eq,
    ParamAnon,
    Word("AND"),
    Word("b"),
    Eq,
    ParamAnon,
  ] = tokens
}

// ---- Nullability overrides ----

pub fn tokenize_nullable_override_test() {
  let tokens = tokenize.tokenize("SELECT name? FROM users")
  let assert [
    Word("SELECT"),
    Word("name"),
    NullableOverride,
    Word("FROM"),
    Word("users"),
  ] = tokens
}

pub fn tokenize_non_null_override_test() {
  let tokens = tokenize.tokenize("SELECT name! FROM users")
  let assert [
    Word("SELECT"),
    Word("name"),
    NullOverride,
    Word("FROM"),
    Word("users"),
  ] = tokens
}

pub fn tokenize_override_not_confused_with_ne_test() {
  let tokens = tokenize.tokenize("WHERE x != y")
  let assert [Word("WHERE"), Word("x"), Ne, Word("y")] = tokens
}

pub fn tokenize_override_not_confused_with_param_test() {
  let tokens = tokenize.tokenize("WHERE id = ?")
  let assert [Word("WHERE"), Word("id"), Eq, ParamAnon] = tokens
}

pub fn tokenize_override_in_comma_list_test() {
  let tokens = tokenize.tokenize("SELECT a?, b! FROM t")
  let assert [
    Word("SELECT"),
    Word("a"),
    NullableOverride,
    Comma,
    Word("b"),
    NullOverride,
    Word("FROM"),
    Word("t"),
  ] = tokens
}

pub fn tokenize_override_before_close_paren_test() {
  let tokens = tokenize.tokenize("(name?)")
  let assert [OpenParen, Word("name"), NullableOverride, CloseParen] = tokens
}

// ---- Comments ----

pub fn tokenize_line_comment_test() {
  let tokens = tokenize.tokenize("SELECT id -- this is a comment\nFROM users")
  let assert [Word("SELECT"), Word("id"), Word("FROM"), Word("users")] = tokens
}

pub fn tokenize_block_comment_test() {
  let tokens = tokenize.tokenize("SELECT /* comment */ id FROM users")
  let assert [Word("SELECT"), Word("id"), Word("FROM"), Word("users")] = tokens
}

pub fn tokenize_block_comment_multiline_test() {
  let tokens =
    tokenize.tokenize("SELECT /* multi\nline\ncomment */ id FROM users")
  let assert [Word("SELECT"), Word("id"), Word("FROM"), Word("users")] = tokens
}

pub fn tokenize_semicolon_in_block_comment_test() {
  let tokens = tokenize.tokenize("SELECT 1 /* ; not a separator */")
  // Semicolon inside block comment should not produce a Semicolon token
  let has_semicolon = list.any(tokens, fn(t) { t == Semicolon })
  let assert False = has_semicolon
}

// ---- Punctuation ----

pub fn tokenize_parens_test() {
  let tokens = tokenize.tokenize("COUNT(*)")
  let assert [Word("COUNT"), OpenParen, Star, CloseParen] = tokens
}

pub fn tokenize_dot_test() {
  let tokens = tokenize.tokenize("u.name")
  let assert [Word("u"), Dot, Word("name")] = tokens
}

pub fn tokenize_semicolon_test() {
  let tokens = tokenize.tokenize("SELECT 1; SELECT 2")
  let assert [
    Word("SELECT"),
    Number("1"),
    Semicolon,
    Word("SELECT"),
    Number("2"),
  ] = tokens
}

// ---- Complex SQL ----

pub fn tokenize_insert_test() {
  let tokens =
    tokenize.tokenize("INSERT INTO users (name, email) VALUES (?, ?)")
  let assert [
    Word("INSERT"),
    Word("INTO"),
    Word("users"),
    OpenParen,
    Word("name"),
    Comma,
    Word("email"),
    CloseParen,
    Word("VALUES"),
    OpenParen,
    ParamAnon,
    Comma,
    ParamAnon,
    CloseParen,
  ] = tokens
}

pub fn tokenize_update_test() {
  let tokens = tokenize.tokenize("UPDATE users SET name = ? WHERE id = ?")
  let assert [
    Word("UPDATE"),
    Word("users"),
    Word("SET"),
    Word("name"),
    Eq,
    ParamAnon,
    Word("WHERE"),
    Word("id"),
    Eq,
    ParamAnon,
  ] = tokens
}

pub fn tokenize_join_test() {
  let tokens =
    tokenize.tokenize(
      "SELECT u.id FROM users u LEFT JOIN emails e ON e.user_id = u.id",
    )
  let assert True = list.contains(tokens, Word("LEFT"))
  let assert True = list.contains(tokens, Word("JOIN"))
  let assert True = list.contains(tokens, Word("ON"))
}

pub fn tokenize_subquery_test() {
  let tokens =
    tokenize.tokenize(
      "SELECT * FROM users WHERE id IN (SELECT user_id FROM active)",
    )
  let assert True = list.contains(tokens, Word("IN"))
  let assert True = list.contains(tokens, Word("SELECT"))
}

pub fn tokenize_coalesce_test() {
  let tokens = tokenize.tokenize("COALESCE(name, 'unknown')")
  let assert [
    Word("COALESCE"),
    OpenParen,
    Word("name"),
    Comma,
    StringLit("unknown"),
    CloseParen,
  ] = tokens
}

pub fn tokenize_cast_test() {
  let tokens = tokenize.tokenize("CAST(id AS INTEGER)")
  let assert [
    Word("CAST"),
    OpenParen,
    Word("id"),
    Word("AS"),
    Word("INTEGER"),
    CloseParen,
  ] = tokens
}

pub fn tokenize_case_expression_test() {
  let tokens = tokenize.tokenize("CASE WHEN x > 0 THEN 1 ELSE 0 END")
  let assert [
    Word("CASE"),
    Word("WHEN"),
    Word("x"),
    Gt,
    Number("0"),
    Word("THEN"),
    Number("1"),
    Word("ELSE"),
    Number("0"),
    Word("END"),
  ] = tokens
}

pub fn tokenize_cte_test() {
  let tokens =
    tokenize.tokenize(
      "WITH active AS (SELECT id FROM users) SELECT * FROM active",
    )
  let assert True = list.contains(tokens, Word("WITH"))
  let first_word = case tokens {
    [Word(w), ..] -> w
    _ -> ""
  }
  let assert "WITH" = first_word
}

// ---- Token utilities ----

pub fn find_keyword_test() {
  let tokens = tokenize.tokenize("SELECT id FROM users WHERE id = ?")
  // [SELECT(0), id(1), FROM(2), users(3), WHERE(4), id(5), Eq(6), ParamAnon(7)]
  let assert option.Some(4) = tokenize.find_keyword(tokens, "WHERE")
  let assert option.Some(2) = tokenize.find_keyword(tokens, "FROM")
  let assert option.None = tokenize.find_keyword(tokens, "HAVING")
}

pub fn find_keyword_skips_nested_test() {
  let tokens =
    tokenize.tokenize("SELECT * FROM users WHERE id IN (SELECT id FROM other)")
  // The second FROM is inside parens, should not be found at depth 0
  let assert option.Some(2) = tokenize.find_keyword(tokens, "FROM")
}

pub fn has_keyword_test() {
  let tokens = tokenize.tokenize("SELECT id FROM users")
  let assert True = tokenize.has_keyword(tokens, "FROM")
  let assert True = tokenize.has_keyword(tokens, "SELECT")
  let assert False = tokenize.has_keyword(tokens, "WHERE")
}

pub fn has_keyword_case_insensitive_test() {
  let tokens = tokenize.tokenize("select id from users")
  let assert True = tokenize.has_keyword(tokens, "FROM")
  let assert True = tokenize.has_keyword(tokens, "SELECT")
}

pub fn split_at_keyword_test() {
  let tokens = tokenize.tokenize("SELECT id FROM users")
  let assert Ok(#(before, after)) = tokenize.split_at_keyword(tokens, "FROM")
  let assert [Word("SELECT"), Word("id")] = before
  let assert [Word("users")] = after
}

pub fn split_on_commas_test() {
  let tokens = tokenize.tokenize("a, b, c")
  let groups = tokenize.split_on_commas(tokens)
  let assert 3 = list.length(groups)
  let assert [[Word("a")], [Word("b")], [Word("c")]] = groups
}

pub fn split_on_commas_nested_test() {
  let tokens = tokenize.tokenize("a, f(b, c), d")
  let groups = tokenize.split_on_commas(tokens)
  let assert 3 = list.length(groups)
  let assert [
    [Word("a")],
    [Word("f"), OpenParen, Word("b"), Comma, Word("c"), CloseParen],
    [Word("d")],
  ] = groups
}

pub fn split_on_and_or_test() {
  let tokens = tokenize.tokenize("a = 1 AND b = 2 OR c = 3")
  let groups = tokenize.split_on_and_or(tokens)
  let assert 3 = list.length(groups)
}

pub fn split_on_and_or_nested_test() {
  let tokens = tokenize.tokenize("a = 1 AND b IN (SELECT id WHERE x AND y)")
  let groups = tokenize.split_on_and_or(tokens)
  // Only splits on the top-level AND, not the one inside parens
  let assert 2 = list.length(groups)
}

pub fn collect_inside_parens_test() {
  let tokens = tokenize.tokenize("(a, b, c) rest")
  case tokens {
    [OpenParen, ..rest] -> {
      let #(inner, remaining) = tokenize.collect_inside_parens(rest)
      let assert [Word("a"), Comma, Word("b"), Comma, Word("c")] = inner
      let assert [Word("rest")] = remaining
    }
    _ -> panic as "Expected OpenParen"
  }
}

pub fn take_until_keywords_test() {
  let tokens = tokenize.tokenize("id, name FROM users WHERE id = ?")
  let taken = tokenize.take_until_keywords(tokens, ["FROM", "WHERE"])
  let assert [Word("id"), Comma, Word("name")] = taken
}

pub fn find_last_keyword_test() {
  let tokens = tokenize.tokenize("a AS b AS c")
  let assert option.Some(3) = tokenize.find_last_keyword(tokens, "AS")
}

pub fn token_text_roundtrip_test() {
  let assert "SELECT" = tokenize.token_text(Word("SELECT"))
  let assert "?" = tokenize.token_text(ParamAnon)
  let assert "@id" = tokenize.token_text(ParamNamed("id"))
  let assert "'hello'" = tokenize.token_text(StringLit("hello"))
  let assert "\"col\"" = tokenize.token_text(QuotedId("col"))
  let assert "(" = tokenize.token_text(OpenParen)
  let assert "=" = tokenize.token_text(Eq)
}

// ---- Untested public functions ----

pub fn tokens_to_text_test() {
  let tokens = tokenize.tokenize("SELECT id, name FROM users")
  let text = tokenize.tokens_to_text(tokens)
  let assert "SELECT id , name FROM users" = text
}

pub fn tokens_to_text_with_params_test() {
  let tokens = tokenize.tokenize("SELECT id FROM users WHERE id = ?")
  let text = tokenize.tokens_to_text(tokens)
  let assert "SELECT id FROM users WHERE id = ?" = text
}

pub fn is_keyword_test() {
  let assert True = tokenize.is_keyword(Word("SELECT"), "select")
  let assert True = tokenize.is_keyword(Word("FROM"), "FROM")
  let assert False = tokenize.is_keyword(Word("TABLE"), "FROM")
  let assert False = tokenize.is_keyword(Comma, "FROM")
}

pub fn split_at_last_keyword_test() {
  let tokens = tokenize.tokenize("SELECT a AS b AS c FROM users")
  let assert Ok(#(before, after)) =
    tokenize.split_at_last_keyword(tokens, "AS")
  let assert "SELECT a AS b" = tokenize.tokens_to_text(before)
  let assert "c FROM users" = tokenize.tokens_to_text(after)
}

pub fn split_at_last_keyword_not_found_test() {
  let tokens = tokenize.tokenize("SELECT id FROM users")
  let assert Error(_) = tokenize.split_at_last_keyword(tokens, "WHERE")
}

pub fn drop_until_keyword_test() {
  let tokens = tokenize.tokenize("SELECT id, name FROM users WHERE id = ?")
  let rest = tokenize.drop_until_keyword(tokens, "FROM")
  let assert "FROM users WHERE id = ?" = tokenize.tokens_to_text(rest)
}

pub fn drop_until_keyword_not_found_test() {
  let tokens = tokenize.tokenize("SELECT id FROM users")
  let rest = tokenize.drop_until_keyword(tokens, "WHERE")
  let assert [] = rest
}

pub fn skip_matching_paren_test() {
  let tokens = tokenize.tokenize("SELECT (a, (b, c), d) FROM users")
  let after_select = list.drop(tokens, 1)
  // Consume the open paren then skip to matching close
  case after_select {
    [OpenParen, ..rest] -> {
      let remaining = tokenize.skip_matching_paren(rest, 1)
      let text = tokenize.tokens_to_text(remaining)
      let assert "FROM users" = text
    }
    _ -> panic as "expected open paren"
  }
}

pub fn skip_matching_paren_depth_zero_test() {
  let tokens = tokenize.tokenize("FROM users")
  let result = tokenize.skip_matching_paren(tokens, 0)
  let assert "FROM users" = tokenize.tokens_to_text(result)
}

pub fn first_word_simple_test() {
  let tokens = tokenize.tokenize("SELECT id FROM users")
  let assert "SELECT" = tokenize.first_word(tokens)
}

pub fn first_word_quoted_test() {
  let tokens = [
    tokenize.QuotedId("my column"),
    tokenize.Word("other"),
  ]
  let assert "my column" = tokenize.first_word(tokens)
}

pub fn first_word_empty_test() {
  let assert "" = tokenize.first_word([])
}

pub fn first_word_skips_non_text_test() {
  // First non-Word/QuotedId token is skipped
  let tokens = [tokenize.Comma, tokenize.Word("SELECT")]
  let assert "SELECT" = tokenize.first_word(tokens)
}

// ---- Edge case tokenization ----

pub fn tokenize_empty_string_test() {
  let assert [] = tokenize.tokenize("")
}

pub fn tokenize_unicode_identifier_test() {
  // Non-ASCII chars like é break the word — tokenizer only handles ASCII
  let tokens = tokenize.tokenize("SELECT café FROM menu")
  let assert "caf FROM menu" =
    tokenize.tokens_to_text(list.drop(tokens, 1))
  // The é character is dropped during tokenization
}

pub fn tokenize_hex_literal_test() {
  // x'deadbeef' tokenizes as Word("x") + StringLit("deadbeef")
  let tokens = tokenize.tokenize("SELECT x'deadbeef'")
  case tokens {
    [_, Word("x"), StringLit("deadbeef")] -> Nil
    _ -> panic as "unexpected token pattern"
  }
}

pub fn tokenize_newline_in_string_test() {
  let tokens = tokenize.tokenize("SELECT 'line1\nline2'")
  // StringLit preserves newline in string content
  case tokens {
    [_, StringLit("line1\nline2")] -> Nil
    _ -> panic as "expected string with newline"
  }
}

pub fn tokenize_carriage_return_in_sql_test() {
  // Carriage returns are treated as whitespace — they don't break tokenization
  let tokens = tokenize.tokenize("SELECT\r\nid\r\nFROM\r\nusers")
  let assert ["SELECT", "id", "FROM", "users"] =
    tokens |> list.map(tokenize.token_text)
}

// ---- Edge case tests for token walking utilities ----

pub fn take_until_keywords_skips_nested_test() {
  // Keyword inside parens at depth > 0 should not stop the take
  let tokens =
    tokenize.tokenize(
      "id, name FROM (SELECT user_id FROM other WHERE flag = 1)",
    )
  let taken = tokenize.take_until_keywords(tokens, ["FROM", "WHERE"])
  // Should stop at the first top-level FROM, not the one in the subquery
  let assert [Word("id"), Comma, Word("name")] = taken
}

pub fn split_on_and_or_between_nested_test() {
  // AND inside parens at depth > 0 should not be consumed as BETWEEN's AND.
  // The AND at depth 0 after the close paren IS BETWEEN's AND, so it should
  // be consumed into the BETWEEN group, resulting in 2 groups total.
  let tokens =
    tokenize.tokenize("a = 1 AND b BETWEEN (SELECT x AND y) AND c")
  let groups = tokenize.split_on_and_or(tokens)
  let assert 2 = list.length(groups)
  // Verify the BETWEEN group contains the nested AND (preserved) and the
  // depth-0 AND (consumed as BETWEEN's AND), both in the same group.
  let and_count =
    groups
    |> list.fold(0, fn(acc, g) {
      acc
      + list.length(list.filter(g, fn(t) { t == Word("AND") }))
    })
  let assert 2 = and_count
}

pub fn collect_inside_parens_nested_test() {
  // Should preserve inner parens and stop at the matching close paren
  let tokens = tokenize.tokenize("(a, (b, c), d) rest")
  case tokens {
    [OpenParen, ..rest] -> {
      let #(inner, remaining) = tokenize.collect_inside_parens(rest)
      let assert [
        Word("a"),
        Comma,
        OpenParen,
        Word("b"),
        Comma,
        Word("c"),
        CloseParen,
        Comma,
        Word("d"),
      ] = inner
      let assert [Word("rest")] = remaining
    }
    _ -> panic as "Expected OpenParen"
  }
}

// ---- Operator coverage gaps ----

pub fn tokenize_concat_operator_test() {
  let tokens = tokenize.tokenize("SELECT 'a' || 'b'")
  let text = tokenize.tokens_to_text(tokens)
  let assert ["SELECT", "'a'", "||", "'b'"] = text |> string.split(" ")
}
