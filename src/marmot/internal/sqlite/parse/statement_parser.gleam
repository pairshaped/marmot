//// Statement skeleton parser. Consumes a token stream from `tokenize.gleam`
//// and produces a typed `Statement` value carrying parsed structure for
//// boundaries that need it (FROM aliases, INSERT shape, CTEs) and clause
//// token slices for bodies that don't.
////
//// What this module does NOT parse:
////  - Expression internals (CAST, COALESCE, CASE, arithmetic, etc.)
////  - Subquery scoping (correlated references and inner-scope shadowing)
////  - USING-join column merging
////
//// All three are intentional. Expression-type inference stays incremental,
//// subquery scoping is left to the existing fallback path, and USING merging
//// is reported as an ambiguous bare reference in v1.

import gleam/bool
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import marmot/internal/error.{type MarmotError}
import marmot/internal/sqlite/tokenize.{type Token, Word}

pub type Identifier {
  Identifier(text: String, quoted: Bool)
}

pub type TableRef {
  TableRef(schema: Option(Identifier), name: Identifier)
}

pub type TableBinding {
  TableBinding(table: TableRef, alias: Option(String))
}

pub type FromItem {
  FromItem(binding: TableBinding, on: Option(List(Token)))
}

pub type CteDef {
  CteDef(name: String, columns: List(String), body: List(Token))
}

pub type SelectBody {
  SelectBody(
    is_distinct: Bool,
    select_list: List(Token),
    from_tokens: Option(List(Token)),
    from: List(FromItem),
    where: Option(List(Token)),
    group_by: Option(List(Token)),
    having: Option(List(Token)),
    order_by: Option(List(Token)),
    limit: Option(List(Token)),
  )
}

pub type SelectStmt {
  SelectStmt(ctes: List(CteDef), body: SelectBody)
}

pub type InsertConflictAction {
  ConflictAbort
  ConflictReplace
  ConflictIgnore
  ConflictFail
  ConflictRollback
}

pub type InsertSource {
  ValuesSource(raw: List(Token), rows: List(List(List(Token))))
  SelectSource(SelectStmt)
  DefaultValuesSource
}

pub type InsertStmt {
  InsertStmt(
    conflict_action: InsertConflictAction,
    target: TableBinding,
    column_list: Option(List(String)),
    source: InsertSource,
    upsert: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type UpdateStmt {
  UpdateStmt(
    target: TableBinding,
    set: List(Token),
    from: List(FromItem),
    where: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type DeleteStmt {
  DeleteStmt(
    target: TableBinding,
    where: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type Statement {
  Select(SelectStmt)
  Insert(InsertStmt)
  Update(UpdateStmt)
  Delete(DeleteStmt)
  Unsupported(tokens: List(Token))
}

/// Parse a token stream into a typed Statement. Returns `Unsupported(tokens)`
/// for SQL outside the supported skeleton (CREATE, PRAGMA, ATTACH, etc.).
/// MarmotError is reserved for parse failures that the agent has positively
/// identified, e.g. INSERT VALUES row-count mismatch.
pub fn parse(tokens: List(Token)) -> Result(Statement, MarmotError) {
  case classify(tokens) {
    SelectKind -> parse_select(tokens) |> result.map(Select)
    InsertKind -> parse_insert(tokens) |> result.map(Insert)
    UpdateKind -> parse_update(tokens) |> result.map(Update)
    DeleteKind -> parse_delete(tokens) |> result.map(Delete)
    _ -> Ok(Unsupported(tokens))
  }
}

type StatementKind {
  SelectKind
  InsertKind
  UpdateKind
  DeleteKind
  OtherKind
}

fn classify(tokens: List(Token)) -> StatementKind {
  let head = skip_with_for_classification(tokens)
  case head {
    [Word(w), ..] ->
      case string.uppercase(w) {
        "SELECT" -> SelectKind
        "INSERT" | "REPLACE" -> InsertKind
        "UPDATE" -> UpdateKind
        "DELETE" -> DeleteKind
        _ -> OtherKind
      }
    _ -> OtherKind
  }
}

fn skip_with_for_classification(tokens: List(Token)) -> List(Token) {
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == "WITH" {
        True -> skip_cte_definitions(rest)
        False -> tokens
      }
    _ -> tokens
  }
}

fn skip_cte_definitions(tokens: List(Token)) -> List(Token) {
  let tokens = case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == "RECURSIVE" {
        True -> rest
        False -> tokens
      }
    _ -> tokens
  }
  do_skip_ctes(tokens)
}

fn do_skip_ctes(tokens: List(Token)) -> List(Token) {
  case tokens {
    [Word(_), ..rest] -> {
      let after_cols = skip_cte_column_list(rest)
      skip_cte_body_after_columns(after_cols, tokens)
    }
    _ -> tokens
  }
}

fn skip_cte_column_list(tokens: List(Token)) -> List(Token) {
  case tokens {
    [tokenize.OpenParen, ..rest] -> {
      let #(_, after) = tokenize.collect_inside_parens(rest)
      after
    }
    _ -> tokens
  }
}

fn skip_cte_body_after_columns(
  after_cols: List(Token),
  original: List(Token),
) -> List(Token) {
  case after_cols {
    [Word(as_kw), tokenize.OpenParen, ..body_rest] ->
      skip_cte_body_after_as(as_kw, body_rest, original)
    _ -> original
  }
}

fn skip_cte_body_after_as(
  as_kw: String,
  body_rest: List(Token),
  original: List(Token),
) -> List(Token) {
  use <- bool.guard(string.uppercase(as_kw) != "AS", original)

  let #(_, after_body) = tokenize.collect_inside_parens(body_rest)
  case after_body {
    [tokenize.Comma, ..rest] -> do_skip_ctes(rest)
    other -> other
  }
}

fn parse_select(tokens: List(Token)) -> Result(SelectStmt, MarmotError) {
  let #(ctes, body_tokens) = parse_ctes(tokens)
  Ok(SelectStmt(ctes: ctes, body: parse_select_body(body_tokens)))
}

fn parse_ctes(tokens: List(Token)) -> #(List(CteDef), List(Token)) {
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == "WITH" {
        True -> collect_ctes(drop_recursive_keyword(rest), [])
        False -> #([], tokens)
      }
    _ -> #([], tokens)
  }
}

fn drop_recursive_keyword(tokens: List(Token)) -> List(Token) {
  case tokens {
    [Word(r), ..rest] ->
      case string.uppercase(r) == "RECURSIVE" {
        True -> rest
        False -> tokens
      }
    _ -> tokens
  }
}

fn collect_ctes(
  tokens: List(Token),
  acc: List(CteDef),
) -> #(List(CteDef), List(Token)) {
  case parse_one_cte(tokens) {
    Ok(#(cte, rest)) ->
      case rest {
        [tokenize.Comma, ..r2] -> collect_ctes(r2, [cte, ..acc])
        other -> #(list.reverse([cte, ..acc]), other)
      }
    Error(_) -> #(list.reverse(acc), tokens)
  }
}

fn parse_one_cte(tokens: List(Token)) -> Result(#(CteDef, List(Token)), Nil) {
  case tokens {
    [Word(name), ..rest] -> {
      let #(columns, after_cols) = parse_cte_columns(rest)
      parse_cte_body(name, columns, after_cols)
    }
    _ -> Error(Nil)
  }
}

fn parse_cte_columns(tokens: List(Token)) -> #(List(String), List(Token)) {
  case tokens {
    [tokenize.OpenParen, ..rest] -> {
      let #(inner, after) = tokenize.collect_inside_parens(rest)
      let cols =
        tokenize.split_on_commas(inner)
        |> list.filter_map(parse_cte_column_name)
      #(cols, after)
    }
    _ -> #([], tokens)
  }
}

fn parse_cte_column_name(group: List(Token)) -> Result(String, Nil) {
  case group {
    [Word(c)] | [tokenize.QuotedId(c)] -> Ok(c)
    _ -> Error(Nil)
  }
}

fn parse_cte_body(
  name: String,
  columns: List(String),
  after_cols: List(Token),
) -> Result(#(CteDef, List(Token)), Nil) {
  case after_cols {
    [Word(as_kw), tokenize.OpenParen, ..body_rest] ->
      parse_cte_body_after_as(name, columns, as_kw, body_rest)
    _ -> Error(Nil)
  }
}

fn parse_cte_body_after_as(
  name: String,
  columns: List(String),
  as_kw: String,
  body_rest: List(Token),
) -> Result(#(CteDef, List(Token)), Nil) {
  use <- bool.guard(string.uppercase(as_kw) != "AS", Error(Nil))
  let #(body, after_body) = tokenize.collect_inside_parens(body_rest)
  Ok(#(CteDef(name: name, columns: columns, body: body), after_body))
}

fn parse_select_body(tokens: List(Token)) -> SelectBody {
  let #(is_distinct, after_select) = case tokens {
    [Word(s), Word(d), ..rest] ->
      case
        string.uppercase(s) == "SELECT" && string.uppercase(d) == "DISTINCT"
      {
        True -> #(True, rest)
        False ->
          case string.uppercase(s) == "SELECT" {
            True -> #(False, [Word(d), ..rest])
            False -> #(False, tokens)
          }
      }
    [Word(s), ..rest] ->
      case string.uppercase(s) == "SELECT" {
        True -> #(False, rest)
        False -> #(False, tokens)
      }
    _ -> #(False, tokens)
  }

  let compound_boundaries = ["UNION", "INTERSECT", "EXCEPT"]
  let #(select_list, rest) =
    take_until_top_level_keyword(after_select, [
      "FROM",
      "WHERE",
      "GROUP",
      "HAVING",
      "ORDER",
      "LIMIT",
      ..compound_boundaries
    ])
  let #(from_tokens, rest) =
    take_clause(rest, "FROM", [
      "WHERE",
      "GROUP",
      "HAVING",
      "ORDER",
      "LIMIT",
      ..compound_boundaries
    ])
  let #(where, rest) =
    take_clause(rest, "WHERE", [
      "GROUP",
      "HAVING",
      "ORDER",
      "LIMIT",
      ..compound_boundaries
    ])
  let #(group_by, rest) =
    take_clause(rest, "GROUP", [
      "HAVING",
      "ORDER",
      "LIMIT",
      ..compound_boundaries
    ])
  let #(having, rest) =
    take_clause(rest, "HAVING", ["ORDER", "LIMIT", ..compound_boundaries])
  let #(order_by, rest) =
    take_clause(rest, "ORDER", ["LIMIT", ..compound_boundaries])
  let #(limit, _rest) = take_clause(rest, "LIMIT", compound_boundaries)

  let from = case from_tokens {
    Some(slice) -> parse_from_items(slice)
    None -> []
  }

  SelectBody(
    is_distinct: is_distinct,
    select_list: select_list,
    from_tokens: from_tokens,
    from: from,
    where: where,
    group_by: group_by,
    having: having,
    order_by: order_by,
    limit: limit,
  )
}

fn take_until_top_level_keyword(
  tokens: List(Token),
  keywords: List(String),
) -> #(List(Token), List(Token)) {
  let uppers = list.map(keywords, string.uppercase)
  do_take_until(tokens, uppers, [], 0)
}

fn do_take_until(
  tokens: List(Token),
  uppers: List(String),
  acc: List(Token),
  depth: Int,
) -> #(List(Token), List(Token)) {
  // Malformed SQL note: an extra `)` makes depth go negative. We don't recover;
  // depth stays negative and keyword matching stays suppressed for the rest of
  // the walk, so the caller sees garbage in the slice. Statement.parse() will
  // typically classify the result as Unsupported. We don't try to recover from
  // unbalanced parens here.
  case tokens {
    [] -> #(list.reverse(acc), [])
    [tokenize.OpenParen, ..rest] ->
      do_take_until(rest, uppers, [tokenize.OpenParen, ..acc], depth + 1)
    [tokenize.CloseParen, ..rest] ->
      do_take_until(rest, uppers, [tokenize.CloseParen, ..acc], depth - 1)
    [Word(w), ..rest] if depth == 0 ->
      case list.contains(uppers, string.uppercase(w)) {
        True -> #(list.reverse(acc), tokens)
        False -> do_take_until(rest, uppers, [Word(w), ..acc], depth)
      }
    [t, ..rest] -> do_take_until(rest, uppers, [t, ..acc], depth)
  }
}

fn take_clause(
  tokens: List(Token),
  keyword: String,
  next_boundaries: List(String),
) -> #(Option(List(Token)), List(Token)) {
  let upper = string.uppercase(keyword)
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == upper {
        True -> {
          let #(body, after) =
            take_until_top_level_keyword(rest, next_boundaries)
          #(Some(body), after)
        }
        False -> #(None, tokens)
      }
    _ -> #(None, tokens)
  }
}

fn parse_from_items(tokens: List(Token)) -> List(FromItem) {
  do_parse_from(tokens, [], [])
}

fn do_parse_from(
  tokens: List(Token),
  current: List(Token),
  acc: List(FromItem),
) -> List(FromItem) {
  case tokens {
    [] -> {
      let acc = case current {
        [] -> acc
        _ -> [parse_from_segment(list.reverse(current)), ..acc]
      }
      list.reverse(acc)
    }
    [tokenize.OpenParen, ..rest] -> {
      let #(inner, after) = tokenize.collect_inside_parens(rest)
      let nested =
        [tokenize.OpenParen, ..inner]
        |> list.append([tokenize.CloseParen])
      let current = list.append(list.reverse(nested), current)
      do_parse_from(after, current, acc)
    }
    [Word(w), ..rest] ->
      case string.uppercase(w) {
        "JOIN" -> {
          let acc = case current {
            [] -> acc
            _ -> [parse_from_segment(list.reverse(current)), ..acc]
          }
          do_parse_from(rest, [], acc)
        }
        "INNER" | "LEFT" | "RIGHT" | "CROSS" | "NATURAL" | "OUTER" | "FULL" ->
          do_parse_from(rest, current, acc)
        _ -> do_parse_from(rest, [Word(w), ..current], acc)
      }
    [tokenize.Comma, ..rest] -> {
      let acc = case current {
        [] -> acc
        _ -> [parse_from_segment(list.reverse(current)), ..acc]
      }
      do_parse_from(rest, [], acc)
    }
    [t, ..rest] -> do_parse_from(rest, [t, ..current], acc)
  }
}

fn join_modifiers() -> List(String) {
  ["INNER", "LEFT", "RIGHT", "CROSS", "NATURAL", "OUTER", "FULL"]
}

fn parse_from_segment(tokens: List(Token)) -> FromItem {
  let #(binding, after_binding) = parse_table_binding(tokens)
  let on = parse_on_or_using(after_binding)
  FromItem(binding: binding, on: on)
}

fn parse_table_binding(tokens: List(Token)) -> #(TableBinding, List(Token)) {
  let #(table, after_table) = parse_table_ref(tokens)
  let #(alias, after_alias) = parse_optional_alias(after_table)
  #(TableBinding(table: table, alias: alias), after_alias)
}

fn parse_table_ref(tokens: List(Token)) -> #(TableRef, List(Token)) {
  case tokens {
    [Word(schema), tokenize.Dot, Word(name), ..rest] -> #(
      TableRef(
        schema: Some(Identifier(schema, False)),
        name: Identifier(name, False),
      ),
      rest,
    )
    [Word(schema), tokenize.Dot, tokenize.QuotedId(name), ..rest] -> #(
      TableRef(
        schema: Some(Identifier(schema, False)),
        name: Identifier(name, True),
      ),
      rest,
    )
    [tokenize.QuotedId(schema), tokenize.Dot, Word(name), ..rest] -> #(
      TableRef(
        schema: Some(Identifier(schema, True)),
        name: Identifier(name, False),
      ),
      rest,
    )
    [tokenize.QuotedId(schema), tokenize.Dot, tokenize.QuotedId(name), ..rest] -> #(
      TableRef(
        schema: Some(Identifier(schema, True)),
        name: Identifier(name, True),
      ),
      rest,
    )
    [Word(name), ..rest] -> #(
      TableRef(schema: None, name: Identifier(name, False)),
      rest,
    )
    [tokenize.QuotedId(name), ..rest] -> #(
      TableRef(schema: None, name: Identifier(name, True)),
      rest,
    )
    // Subquery or other unrecognized FROM shape (e.g. `(SELECT ...) sub`).
    // Returns an empty-name TableRef as a safe sentinel; the alias resolver
    // ignores empty names and falls through to its existing fallback path.
    _ -> #(TableRef(schema: None, name: Identifier("", False)), tokens)
  }
}

fn parse_optional_alias(tokens: List(Token)) -> #(Option(String), List(Token)) {
  case tokens {
    [Word(as_kw), Word(alias), ..rest] ->
      case string.uppercase(as_kw) == "AS" {
        True -> #(Some(alias), rest)
        False -> parse_optional_alias_no_as(tokens)
      }
    _ -> parse_optional_alias_no_as(tokens)
  }
}

fn parse_optional_alias_no_as(
  tokens: List(Token),
) -> #(Option(String), List(Token)) {
  case tokens {
    [Word(alias), ..rest] ->
      case is_clause_or_join_keyword(alias) {
        True -> #(None, tokens)
        False -> #(Some(alias), rest)
      }
    _ -> #(None, tokens)
  }
}

fn is_clause_or_join_keyword(word: String) -> Bool {
  let upper = string.uppercase(word)
  list.contains(
    [
      "ON", "USING", "JOIN", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT",
      "VALUES", "DEFAULT", "SELECT", "WITH", "SET", "RETURNING", "UNION",
      "INTERSECT", "EXCEPT",
    ],
    upper,
  )
  || list.contains(join_modifiers(), upper)
}

fn parse_on_or_using(tokens: List(Token)) -> Option(List(Token)) {
  case tokens {
    [Word(on_kw), ..rest] ->
      case string.uppercase(on_kw) {
        "ON" -> Some(rest)
        "USING" -> Some([Word("USING"), ..rest])
        _ -> None
      }
    _ -> None
  }
}

fn parse_update(tokens: List(Token)) -> Result(UpdateStmt, MarmotError) {
  let after_kw = drop_update_keyword(tokens)
  let #(target, after_target) = parse_table_binding(after_kw)
  let after_set = drop_keyword(after_target, "SET")
  let #(set_tokens, after_set_body) =
    take_until_top_level_keyword(after_set, ["FROM", "WHERE", "RETURNING"])
  let #(from_slice, after_from) =
    take_clause(after_set_body, "FROM", ["WHERE", "RETURNING"])
  let from = case from_slice {
    Some(slice) -> parse_from_items(slice)
    None -> []
  }
  let #(where, after_where) = take_clause(after_from, "WHERE", ["RETURNING"])
  let #(returning, _rest) = take_clause(after_where, "RETURNING", [])
  Ok(UpdateStmt(
    target: target,
    set: set_tokens,
    from: from,
    where: where,
    returning: returning,
  ))
}

fn parse_delete(tokens: List(Token)) -> Result(DeleteStmt, MarmotError) {
  let after_delete = drop_keyword(tokens, "DELETE")
  let after_from = drop_keyword(after_delete, "FROM")
  let #(target, after_target) = parse_table_binding(after_from)
  let #(where, after_where) = take_clause(after_target, "WHERE", ["RETURNING"])
  let #(returning, _rest) = take_clause(after_where, "RETURNING", [])
  Ok(DeleteStmt(target: target, where: where, returning: returning))
}

fn drop_update_keyword(tokens: List(Token)) -> List(Token) {
  // Match UPDATE, optionally followed by OR <action>. The action keyword
  // is consumed but not preserved (UPDATE OR REPLACE etc. is not in the
  // typed AST yet; could be added later if needed).
  case tokens {
    [Word(u), Word(or_kw), Word(action), ..rest] ->
      case string.uppercase(u) == "UPDATE" && string.uppercase(or_kw) == "OR" {
        True -> rest
        False ->
          case string.uppercase(u) == "UPDATE" {
            True -> [Word(or_kw), Word(action), ..rest]
            False -> tokens
          }
      }
    [Word(u), ..rest] ->
      case string.uppercase(u) == "UPDATE" {
        True -> rest
        False -> tokens
      }
    _ -> tokens
  }
}

fn drop_keyword(tokens: List(Token), keyword: String) -> List(Token) {
  let upper = string.uppercase(keyword)
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == upper {
        True -> rest
        False -> tokens
      }
    _ -> tokens
  }
}

/// Slice the INSERT source body. Stops at top-level `ON CONFLICT` or
/// `RETURNING`. Plain `ON` (from a JOIN's ON clause) does NOT terminate the
/// slice.
fn take_until_insert_source_end(
  tokens: List(Token),
) -> #(List(Token), List(Token)) {
  do_take_insert_source(tokens, [], 0)
}

fn do_take_insert_source(
  tokens: List(Token),
  acc: List(Token),
  depth: Int,
) -> #(List(Token), List(Token)) {
  case tokens {
    [] -> #(list.reverse(acc), [])
    [tokenize.OpenParen, ..rest] ->
      do_take_insert_source(rest, [tokenize.OpenParen, ..acc], depth + 1)
    [tokenize.CloseParen, ..rest] ->
      do_take_insert_source(rest, [tokenize.CloseParen, ..acc], depth - 1)
    [Word(w1), Word(w2), ..] if depth == 0 ->
      case string.uppercase(w1) == "ON" && string.uppercase(w2) == "CONFLICT" {
        True -> #(list.reverse(acc), tokens)
        False ->
          case string.uppercase(w1) == "RETURNING" {
            True -> #(list.reverse(acc), tokens)
            False ->
              do_take_insert_source(
                list.drop(tokens, 1),
                [Word(w1), ..acc],
                depth,
              )
          }
      }
    [Word(w), ..rest] if depth == 0 ->
      case string.uppercase(w) == "RETURNING" {
        True -> #(list.reverse(acc), tokens)
        False -> do_take_insert_source(rest, [Word(w), ..acc], depth)
      }
    [t, ..rest] -> do_take_insert_source(rest, [t, ..acc], depth)
  }
}

fn parse_insert(tokens: List(Token)) -> Result(InsertStmt, MarmotError) {
  let #(action, after_kw) = parse_insert_conflict_action(tokens)
  let after_into = drop_into(after_kw)
  let #(target, after_target) = parse_table_binding(after_into)
  let #(column_list, after_cols) = parse_optional_column_list(after_target)
  let #(source_tokens, after_source) = take_until_insert_source_end(after_cols)
  let source = parse_insert_source(source_tokens)
  let #(upsert, after_upsert) = take_clause(after_source, "ON", ["RETURNING"])
  let #(returning, _rest) = take_clause(after_upsert, "RETURNING", [])
  Ok(InsertStmt(
    conflict_action: action,
    target: target,
    column_list: column_list,
    source: source,
    upsert: upsert,
    returning: returning,
  ))
}

fn parse_insert_source(tokens: List(Token)) -> InsertSource {
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) {
        "VALUES" -> parse_values_source(rest, tokens)
        "DEFAULT" ->
          case rest {
            [Word(v), ..] ->
              case string.uppercase(v) == "VALUES" {
                True -> DefaultValuesSource
                False -> ValuesSource(raw: tokens, rows: [])
              }
            _ -> ValuesSource(raw: tokens, rows: [])
          }
        "SELECT" ->
          case parse_select(tokens) {
            Ok(stmt) -> SelectSource(stmt)
            Error(_) -> ValuesSource(raw: tokens, rows: [])
          }
        "WITH" ->
          // INSERT INTO t WITH ... SELECT ...
          case parse_select(tokens) {
            Ok(stmt) -> SelectSource(stmt)
            Error(_) -> ValuesSource(raw: tokens, rows: [])
          }
        _ -> ValuesSource(raw: tokens, rows: [])
      }
    _ -> ValuesSource(raw: tokens, rows: [])
  }
}

fn parse_values_source(
  after_values: List(Token),
  raw: List(Token),
) -> InsertSource {
  let rows = parse_values_rows(after_values, [])
  ValuesSource(raw: raw, rows: rows)
}

fn parse_values_rows(
  tokens: List(Token),
  acc: List(List(List(Token))),
) -> List(List(List(Token))) {
  case tokens {
    [] -> list.reverse(acc)
    [tokenize.OpenParen, ..rest] -> {
      let #(inner, after) = tokenize.collect_inside_parens(rest)
      let exprs = tokenize.split_on_commas(inner)
      let acc = [exprs, ..acc]
      case after {
        [tokenize.Comma, ..r2] -> parse_values_rows(r2, acc)
        _ -> list.reverse(acc)
      }
    }
    [_, ..rest] -> parse_values_rows(rest, acc)
  }
}

fn parse_insert_conflict_action(
  tokens: List(Token),
) -> #(InsertConflictAction, List(Token)) {
  case tokens {
    [Word(insert_kw), ..rest] ->
      case string.uppercase(insert_kw) {
        "REPLACE" -> #(ConflictReplace, rest)
        "INSERT" ->
          case rest {
            [Word(or_kw), Word(action), ..after_action] ->
              parse_insert_or_conflict(or_kw, action, after_action, rest)
            _ -> #(ConflictAbort, rest)
          }
        _ -> #(ConflictAbort, tokens)
      }
    _ -> #(ConflictAbort, tokens)
  }
}

fn parse_insert_or_conflict(
  or_kw: String,
  action: String,
  after_action: List(Token),
  original: List(Token),
) -> #(InsertConflictAction, List(Token)) {
  use <- bool.guard(string.uppercase(or_kw) != "OR", #(ConflictAbort, original))

  case string.uppercase(action) {
    "REPLACE" -> #(ConflictReplace, after_action)
    "IGNORE" -> #(ConflictIgnore, after_action)
    "FAIL" -> #(ConflictFail, after_action)
    "ROLLBACK" -> #(ConflictRollback, after_action)
    "ABORT" -> #(ConflictAbort, after_action)
    _ -> #(ConflictAbort, original)
  }
}

fn drop_into(tokens: List(Token)) -> List(Token) {
  case tokens {
    [Word(w), ..rest] ->
      case string.uppercase(w) == "INTO" {
        True -> rest
        False -> tokens
      }
    _ -> tokens
  }
}

fn parse_optional_column_list(
  tokens: List(Token),
) -> #(Option(List(String)), List(Token)) {
  case tokens {
    [tokenize.OpenParen, ..rest] -> {
      let #(inner, after) = tokenize.collect_inside_parens(rest)
      let cols =
        tokenize.split_on_commas(inner)
        |> list.filter_map(fn(group) {
          case group {
            [Word(c)] -> Ok(c)
            [tokenize.QuotedId(c)] -> Ok(c)
            _ -> Error(Nil)
          }
        })
      #(Some(cols), after)
    }
    _ -> #(None, tokens)
  }
}
