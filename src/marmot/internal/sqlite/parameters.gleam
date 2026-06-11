//// Parameter extraction from opcodes plus parsed SQL context.

import gleam/bool
import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import marmot/internal/query.{type Column, type Parameter, Parameter, StringType}
import marmot/internal/sqlite/opcode.{type Opcode}
import marmot/internal/sqlite/parse/binder
import marmot/internal/sqlite/parse/parameters as parse_params
import marmot/internal/sqlite/parse/resolver
import marmot/internal/sqlite/parse/select
import marmot/internal/sqlite/parse/statement
import marmot/internal/sqlite/parse/statement_parser
import marmot/internal/sqlite/parse/subquery
import marmot/internal/sqlite/schema
import marmot/internal/sqlite/tokenize

/// Resolver-driven parameter extraction failures. The `sqlite.gleam` boundary
/// maps these to public `MarmotError` variants with the SQL file path attached.
pub type ParameterResolutionError {
  AmbiguousColumn(column: String, candidates: List(String))
  UnknownAlias(alias: String)
  UnknownColumnInTable(table: String, column: String)
  UnknownColumn(column: String)
  AliasMapCollision(name: String)
  InsertValuesCountMismatch(expected: Int, got: Int, row: Int)
}

/// Extract parameters from opcodes and parsed SQL context.
///
/// Uses EXPLAIN opcodes (Variable) for `?` placeholder positions and types,
/// with text-based parsing for named parameters (`@name`) and complex WHERE
/// clauses. The text-based parser handles simple `col = ?` patterns but may
/// miss types on deeply nested subqueries in WHERE; those fall back to
/// StringType. See "subquery in where" test in sqlite_test.
pub fn extract_parameters(
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_metadata: schema.TableMetadataV2,
  pk_columns: Dict(String, String),
  tokens: List(tokenize.Token),
) -> Result(List(Parameter), ParameterResolutionError) {
  let table_schemas =
    dict.map_values(table_metadata.columns, fn(_, metas) {
      list.map(metas, fn(m) { m.column })
    })

  // Build cursor -> index columns mapping for index cursors
  let cursor_index_columns =
    list.fold(opcodes, dict.new(), fn(acc, op) {
      case op.opcode {
        "OpenRead" | "OpenWrite" ->
          case dict.get(table_metadata.index_columns, op.p2) {
            Ok(cols) -> dict.insert(acc, op.p1, cols)
            Error(_) -> acc
          }
        _ -> acc
      }
    })

  let variable_ops =
    list.filter(opcodes, fn(op) { op.opcode == "Variable" })
    |> list.sort(fn(a, b) { int.compare(a.p1, b.p1) })
    |> opcode.dedupe_variables_by_p1

  let param_count = list.length(variable_ops)
  case param_count {
    0 -> Ok([])
    _ -> {
      let opcode_fallback = fn() {
        list.map(variable_ops, fn(var_op) {
          infer_parameter_or_string(
            var_op,
            opcodes,
            cursor_table,
            table_schemas,
            pk_columns,
            cursor_index_columns,
          )
        })
        |> preserve_opcode_named_parameter_names(tokens)
      }

      let body_result = case statement_parser.parse(tokens) {
        Ok(statement_parser.Select(_)) ->
          extract_select_parameters_resolved(
            table_schemas,
            tokens,
            statement.Select,
            param_count,
            opcode_fallback,
          )
        Ok(statement_parser.Delete(_)) ->
          extract_select_parameters_resolved(
            table_schemas,
            tokens,
            statement.Delete,
            param_count,
            opcode_fallback,
          )
        Ok(statement_parser.Update(_)) ->
          extract_update_parameters_resolved(
            table_schemas,
            tokens,
            param_count,
            opcode_fallback,
          )
        Ok(statement_parser.Insert(stmt)) ->
          extract_insert_body_result(
            stmt,
            table_metadata,
            table_schemas,
            tokens,
            param_count,
            opcode_fallback,
          )
        Ok(statement_parser.Unsupported(_)) | Error(_) -> Ok(opcode_fallback())
      }

      use body <- result.try(body_result)
      Ok(fix_limit_offset_param_types(body, tokens))
    }
  }
}

fn infer_parameter_or_string(
  var_op: Opcode,
  opcodes: List(Opcode),
  cursor_table: Dict(Int, String),
  table_schemas: Dict(String, List(Column)),
  pk_columns: Dict(String, String),
  cursor_index_columns: opcode.CursorIndexColumns,
) -> Parameter {
  case
    opcode.infer_parameter_type(
      var_op,
      opcodes,
      cursor_table,
      table_schemas,
      pk_columns,
      cursor_index_columns,
    )
  {
    Ok(p) -> p
    Error(_) ->
      Parameter(name: "param", column_type: StringType, nullable: False)
  }
}

fn extract_insert_values_body_result(
  stmt: statement_parser.InsertStmt,
  table_metadata: schema.TableMetadataV2,
  table_schemas: Dict(String, List(Column)),
  param_count: Int,
  opcode_fallback: fn() -> List(Parameter),
) -> Result(List(Parameter), ParameterResolutionError) {
  use values_params <- result.try(extract_insert_parameters_v2(
    stmt,
    table_metadata,
  ))
  let upsert_params = extract_anonymous_upsert_parameters(stmt, table_schemas)
  let parsed = list.append(values_params, upsert_params)
  // If the extracted param count doesn't match (e.g. upsert with ON CONFLICT DO
  // UPDATE SET adds more `?` parameters beyond the VALUES row), fall back to
  // opcode inference so those extra parameters aren't silently dropped.
  return_parsed_when_count_matches(parsed, param_count, opcode_fallback)
}

fn extract_insert_body_result(
  stmt: statement_parser.InsertStmt,
  table_metadata: schema.TableMetadataV2,
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
  param_count: Int,
  opcode_fallback: fn() -> List(Parameter),
) -> Result(List(Parameter), ParameterResolutionError) {
  case stmt.source {
    statement_parser.ValuesSource(_, _)
    | statement_parser.DefaultValuesSource ->
      extract_insert_values_body_result(
        stmt,
        table_metadata,
        table_schemas,
        param_count,
        opcode_fallback,
      )
    statement_parser.SelectSource(_) ->
      extract_insert_select_body_result(
        stmt,
        table_schemas,
        tokens,
        param_count,
        opcode_fallback,
      )
  }
}

fn extract_insert_select_body_result(
  stmt: statement_parser.InsertStmt,
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
  param_count: Int,
  opcode_fallback: fn() -> List(Parameter),
) -> Result(List(Parameter), ParameterResolutionError) {
  let parsed = extract_insert_select_parameters(stmt, table_schemas, tokens)
  return_parsed_when_count_matches(parsed, param_count, opcode_fallback)
}

fn return_parsed_when_count_matches(
  parsed: List(Parameter),
  param_count: Int,
  opcode_fallback: fn() -> List(Parameter),
) -> Result(List(Parameter), ParameterResolutionError) {
  use <- bool.guard(list.length(parsed) != param_count, Ok(opcode_fallback()))
  Ok(parsed)
}

fn extract_anonymous_upsert_parameters(
  stmt: statement_parser.InsertStmt,
  table_schemas: Dict(String, List(Column)),
) -> List(Parameter) {
  case stmt.upsert {
    option.None -> []
    option.Some(upsert_tokens) -> {
      use <- bool.guard(!has_anonymous_param(upsert_tokens), [])

      let table_name = stmt.target.table.name.text
      let set_params =
        extract_upsert_set_parameters(upsert_tokens, table_schemas, table_name)
      let where_params =
        extract_upsert_where_parameters(
          upsert_tokens,
          table_schemas,
          table_name,
        )
      list.append(set_params, where_params)
    }
  }
}

fn has_anonymous_param(tokens: List(tokenize.Token)) -> Bool {
  list.any(tokens, fn(t) {
    case t {
      tokenize.ParamAnon -> True
      _ -> False
    }
  })
}

fn extract_upsert_set_parameters(
  upsert_tokens: List(tokenize.Token),
  table_schemas: Dict(String, List(Column)),
  table_name: String,
) -> List(Parameter) {
  case tokenize.split_at_keyword(upsert_tokens, "SET") {
    Error(_) -> []
    Ok(#(_, after_set)) -> {
      let set_tokens = tokenize.take_until_keywords(after_set, ["WHERE"])
      extract_update_set_parameters_resolved(
        table_schemas,
        table_name,
        set_tokens,
      )
    }
  }
}

fn extract_upsert_where_parameters(
  upsert_tokens: List(tokenize.Token),
  table_schemas: Dict(String, List(Column)),
  table_name: String,
) -> List(Parameter) {
  case upsert_update_where_tokens(upsert_tokens) {
    Error(_) -> []
    Ok(where_tokens) -> {
      let where_params = parse_params.parse_where_body(where_tokens)
      list.map(where_params, fn(param) {
        let #(param_name, lookup_col) = param
        let found_col =
          find_column_in_table(table_schemas, table_name, lookup_col)
        parameter_from_column_result(param_name, found_col, False)
      })
    }
  }
}

fn upsert_update_where_tokens(
  upsert_tokens: List(tokenize.Token),
) -> Result(List(tokenize.Token), Nil) {
  use after_set <- result.try(
    tokenize.split_at_keyword(upsert_tokens, "SET")
    |> result.map(fn(split) { split.1 }),
  )
  tokenize.split_at_keyword(after_set, "WHERE")
  |> result.map(fn(split) { split.1 })
}

fn find_column_in_table(
  table_schemas: Dict(String, List(Column)),
  table_name: String,
  column_name: String,
) -> Result(Column, Nil) {
  query.find_column_ci(table_schemas, table_name, column_name)
}

// ---- Resolver-driven extraction for SELECT/DELETE/UPDATE ----
//
// Routes through `statement_parser.parse` and `resolver.build_alias_map` /
// `resolver.resolve_qualified` / `resolver.resolve_bare`. Falls back to the
// legacy text-based path when:
//   - statement_parser.parse fails (gives a worse-than-legacy error otherwise)
//   - the parsed param count disagrees with EXPLAIN's variable_ops count
//     (likely indicates a parser miss; opcode count is the ground truth)
// Resolver hard-failures (Ambiguous, UnknownAlias, UnknownColumn,
// UnknownColumnInTable) are *not* swallowed: they propagate as Errors.

fn extract_select_parameters_resolved(
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
  stmt_type: statement.StatementType,
  param_count: Int,
  opcode_fallback: fn() -> List(Parameter),
) -> Result(List(Parameter), ParameterResolutionError) {
  let typed_result = case statement_parser.parse(tokens) {
    Ok(statement_parser.Select(stmt)) -> {
      let bindings = list.map(stmt.body.from, fn(item) { item.binding })
      // Walk every clause that can hold parameters, not just WHERE. HAVING,
      // ORDER BY, LIMIT, and GROUP BY can all carry binders (`HAVING SUM(x) >
      // @min`, `LIMIT @n`, etc.). Restricting to WHERE would silently drop
      // those binders, then count-mismatch into opcode_fallback - which loses
      // the original named-parameter name.
      let body_tokens =
        list.flatten([
          stmt.body.select_list,
          option.unwrap(stmt.body.from_tokens, []),
          option.unwrap(stmt.body.where, []),
          option.unwrap(stmt.body.group_by, []),
          option.unwrap(stmt.body.having, []),
          option.unwrap(stmt.body.order_by, []),
          option.unwrap(stmt.body.limit, []),
        ])
      Ok(extract_via_resolver(table_schemas, bindings, body_tokens))
    }
    Ok(statement_parser.Delete(stmt)) -> {
      let bindings = [stmt.target]
      let where_tokens = option.unwrap(stmt.where, [])
      Ok(extract_via_resolver(table_schemas, bindings, where_tokens))
    }
    _ -> Error(Nil)
  }
  case typed_result {
    Ok(typed) ->
      case typed {
        Ok(params) ->
          case list.length(params) == param_count {
            True ->
              Ok(refine_string_parameter_types(
                params,
                opcode_fallback(),
                contextless_named_parameter_names(tokens),
              ))
            False -> Ok(opcode_fallback())
          }
        Error(e) -> Error(e)
      }
    Error(_) -> {
      let parsed = extract_select_parameters(table_schemas, tokens, stmt_type)
      case list.length(parsed) == param_count {
        True -> Ok(parsed)
        False -> Ok(opcode_fallback())
      }
    }
  }
}

fn extract_update_parameters_resolved(
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
  param_count: Int,
  opcode_fallback: fn() -> List(Parameter),
) -> Result(List(Parameter), ParameterResolutionError) {
  let typed_result = case statement_parser.parse(tokens) {
    Ok(statement_parser.Update(stmt)) -> {
      let from_bindings = list.map(stmt.from, fn(item) { item.binding })
      let bindings = [stmt.target, ..from_bindings]
      let set_params =
        extract_update_set_parameters_resolved(
          table_schemas,
          stmt.target.table.name.text,
          stmt.set,
        )
      let where_tokens = option.unwrap(stmt.where, [])
      let where_result =
        extract_via_resolver(table_schemas, bindings, where_tokens)
      Ok(
        result.map(where_result, fn(where_params) {
          list.append(set_params, where_params)
        }),
      )
    }
    _ -> Error(Nil)
  }
  case typed_result {
    Ok(typed) ->
      case typed {
        Ok(params) ->
          case list.length(params) == param_count {
            True ->
              Ok(refine_string_parameter_types(
                params,
                opcode_fallback(),
                contextless_named_parameter_names(tokens),
              ))
            False -> Ok(opcode_fallback())
          }
        Error(e) -> Error(e)
      }
    Error(_) -> {
      let parsed = extract_update_parameters(table_schemas, tokens)
      case list.length(parsed) == param_count {
        True -> Ok(parsed)
        False -> Ok(opcode_fallback())
      }
    }
  }
}

fn extract_update_set_parameters_resolved(
  table_schemas: Dict(String, List(Column)),
  table_name: String,
  set_tokens: List(tokenize.Token),
) -> List(Parameter) {
  let set_params = parse_params.parse_update_set_body(set_tokens)
  case dict.get(table_schemas, table_name) {
    Ok(table_cols) ->
      list.map(set_params, fn(param) {
        let #(param_name, lookup_col) = param
        case list.find(table_cols, fn(c) { c.name == lookup_col }) {
          Ok(col) ->
            Parameter(
              name: param_name,
              column_type: col.column_type,
              nullable: col.nullable,
            )
          Error(_) ->
            Parameter(
              name: param_name,
              column_type: StringType,
              nullable: False,
            )
        }
      })
    Error(_) ->
      list.map(set_params, fn(param) {
        let #(param_name, _) = param
        Parameter(name: param_name, column_type: StringType, nullable: False)
      })
  }
}

fn refine_string_parameter_types(
  params: List(Parameter),
  opcode_params: List(Parameter),
  contextless_named_params: List(String),
) -> List(Parameter) {
  case params, opcode_params {
    [], _ -> []
    [param, ..rest], [opcode_param, ..opcode_rest] -> [
      refine_string_parameter_type(
        param,
        opcode_param,
        contextless_named_params,
      ),
      ..refine_string_parameter_types(
        rest,
        opcode_rest,
        contextless_named_params,
      )
    ]
    [param, ..rest], [] -> [
      param,
      ..refine_string_parameter_types(rest, [], contextless_named_params)
    ]
  }
}

fn refine_string_parameter_type(
  param: Parameter,
  opcode_param: Parameter,
  contextless_named_params: List(String),
) -> Parameter {
  // Preserve named parameters from the SQL text while using opcode evidence
  // when the text resolver only found its generic StringType fallback.
  use <- bool.guard(
    !list.contains(contextless_named_params, param.name)
      || param.column_type != StringType
      || opcode_param.column_type == StringType,
    param,
  )
  Parameter(..param, column_type: opcode_param.column_type)
}

fn contextless_named_parameter_names(
  tokens: List(tokenize.Token),
) -> List(String) {
  binder.find_param_binder_occurrences(tokens)
  |> list.filter_map(fn(occurrence) {
    case occurrence.anonymous, occurrence.binder.binder_column {
      False, option.None -> Ok(occurrence.binder.name)
      _, _ -> Error(Nil)
    }
  })
  |> list.unique
}

fn preserve_opcode_named_parameter_names(
  params: List(Parameter),
  tokens: List(tokenize.Token),
) -> List(Parameter) {
  let overrides = collect_parameter_name_overrides(tokens, [], [])
  use <- bool.guard(list.length(overrides) != list.length(params), params)

  list.zip(params, overrides)
  |> list.map(fn(pair) {
    let #(param, override) = pair
    case override {
      option.Some(name) -> Parameter(..param, name: name)
      option.None -> param
    }
  })
}

fn collect_parameter_name_overrides(
  tokens: List(tokenize.Token),
  seen_named: List(String),
  acc: List(option.Option(String)),
) -> List(option.Option(String)) {
  case tokens {
    [] -> list.reverse(acc)
    [tokenize.ParamAnon, ..rest] ->
      collect_parameter_name_overrides(rest, seen_named, [option.None, ..acc])
    [tokenize.ParamNamed(name), ..rest] -> {
      case list.contains(seen_named, name) {
        True -> collect_parameter_name_overrides(rest, seen_named, acc)
        False ->
          collect_parameter_name_overrides(rest, [name, ..seen_named], [
            option.Some(name),
            ..acc
          ])
      }
    }
    [_, ..rest] -> collect_parameter_name_overrides(rest, seen_named, acc)
  }
}

fn extract_via_resolver(
  table_schemas: Dict(String, List(Column)),
  bindings: List(statement_parser.TableBinding),
  body_tokens: List(tokenize.Token),
) -> Result(List(Parameter), ParameterResolutionError) {
  let alias_map_result = resolver.build_alias_map(bindings)
  use map <- result.try(case alias_map_result {
    Ok(m) -> Ok(m)
    Error(resolver.AliasCollision(name)) -> Error(AliasMapCollision(name))
  })
  // Mark each binder with the depth at which it was discovered. The resolver
  // only sees the outer FROM's alias map; binders inside parenthesized
  // regions (subqueries) reference columns from an inner FROM the resolver
  // can't see. For those, fall back to the legacy search-all-tables
  // resolution (which transparently includes subquery tables), so the
  // resolver migration doesn't spuriously surface "unknown column" errors
  // for legitimate subquery-scoped binders.
  let depth_binders = find_binders_with_depth(body_tokens)
  let subquery_tables = subquery.find_all_subquery_tables(body_tokens)
  let outer_tables = dict.keys(map)
  let legacy_tables =
    list.append(outer_tables, subquery_tables)
    |> list.unique
    |> list.filter(fn(t) {
      case dict.get(table_schemas, t) {
        Ok(_) -> True
        Error(_) -> False
      }
    })
  list.try_map(depth_binders, fn(pair) {
    let #(b, depth) = pair
    case depth > 0 {
      True -> Ok(legacy_resolve_binder(b, legacy_tables, table_schemas))
      False -> resolve_one_binder(b, map, table_schemas)
    }
  })
}

/// Walk tokens and recover binders alongside the parenthesis depth at which
/// each was discovered. Mirrors `binder.find_param_binders` but threads a
/// depth counter so the caller can choose a different resolution strategy
/// for binders inside subqueries.
fn find_binders_with_depth(
  tokens: List(tokenize.Token),
) -> List(#(binder.Binder, Int)) {
  binder.find_param_binder_occurrences(tokens)
  |> dedupe_named_binders_with_depth([])
}

fn dedupe_named_binders_with_depth(
  occurrences: List(binder.BinderOccurrence),
  acc: List(#(binder.Binder, Int)),
) -> List(#(binder.Binder, Int)) {
  case occurrences {
    [] -> list.reverse(acc)
    [
      binder.BinderOccurrence(binder: b, depth: depth, anonymous: anonymous),
      ..rest
    ] -> {
      case anonymous {
        True -> dedupe_named_binders_with_depth(rest, [#(b, depth), ..acc])
        False -> dedupe_named_binder_occurrence(b, depth, rest, acc)
      }
    }
  }
}

fn dedupe_named_binder_occurrence(
  b: binder.Binder,
  depth: Int,
  rest: List(binder.BinderOccurrence),
  acc: List(#(binder.Binder, Int)),
) -> List(#(binder.Binder, Int)) {
  case find_existing_binder(b.name, acc) {
    Ok(existing) ->
      dedupe_existing_binder_occurrence(b, depth, rest, acc, existing)
    Error(_) -> dedupe_named_binders_with_depth(rest, [#(b, depth), ..acc])
  }
}

fn find_existing_binder(
  name: String,
  acc: List(#(binder.Binder, Int)),
) -> Result(binder.Binder, Nil) {
  list.find_map(acc, fn(pair) {
    let #(existing, _) = pair
    case existing.name == name {
      True -> Ok(existing)
      False -> Error(Nil)
    }
  })
}

fn dedupe_existing_binder_occurrence(
  b: binder.Binder,
  depth: Int,
  rest: List(binder.BinderOccurrence),
  acc: List(#(binder.Binder, Int)),
  existing: binder.Binder,
) -> List(#(binder.Binder, Int)) {
  case existing.binder_column, b.binder_column {
    option.None, option.Some(_) -> {
      let new_acc = replace_binder_occurrence(b, depth, acc)
      dedupe_named_binders_with_depth(rest, new_acc)
    }
    _, _ -> dedupe_named_binders_with_depth(rest, acc)
  }
}

fn replace_binder_occurrence(
  b: binder.Binder,
  depth: Int,
  acc: List(#(binder.Binder, Int)),
) -> List(#(binder.Binder, Int)) {
  list.map(acc, fn(pair) {
    let #(existing, _) = pair
    case existing.name == b.name {
      True -> #(b, depth)
      False -> pair
    }
  })
}

fn legacy_resolve_binder(
  b: binder.Binder,
  all_tables: List(String),
  table_schemas: Dict(String, List(Column)),
) -> Parameter {
  let names_to_try = case b.binder_column {
    option.Some(col) if col != b.name -> [b.name, col]
    _ -> [b.name]
  }
  // First try the local subquery scope (outer + FROM-discovered subquery
  // tables). Fall back to a global search across all known schemas so we
  // match the legacy behavior on JOIN'd tables inside subqueries (which
  // `subquery.find_all_subquery_tables` does not currently surface).
  let local =
    resolve_binder_type(names_to_try, all_tables, table_schemas)
    |> result.lazy_or(fn() {
      resolve_binder_type(names_to_try, dict.keys(table_schemas), table_schemas)
    })
  case local {
    Ok(col) ->
      Parameter(name: b.name, column_type: col.column_type, nullable: False)
    Error(_) ->
      Parameter(name: b.name, column_type: StringType, nullable: False)
  }
}

fn resolve_one_binder(
  b: binder.Binder,
  map: Dict(String, statement_parser.TableRef),
  table_schemas: Dict(String, List(Column)),
) -> Result(Parameter, ParameterResolutionError) {
  // No column context at all: the binder lives somewhere `find_param_binders`
  // couldn't tie back to a column reference (LIMIT/OFFSET, function args,
  // CASE branches, etc.). The resolver can't usefully type these; surfacing
  // a typed UnknownColumn would be a regression for queries like
  // `... LIMIT @n`. Drop to StringType silently; opcode fallback /
  // fix_limit_offset_param_types will refine the type from EXPLAIN context.
  case b.binder_column {
    option.None ->
      Ok(Parameter(name: b.name, column_type: StringType, nullable: False))
    option.Some(_) -> resolve_one_binder_with_column(b, map, table_schemas)
  }
}

fn resolve_one_binder_with_column(
  b: binder.Binder,
  map: Dict(String, statement_parser.TableRef),
  table_schemas: Dict(String, List(Column)),
) -> Result(Parameter, ParameterResolutionError) {
  let resolution = case b.binder_column {
    option.Some(col) ->
      case string.split_once(col, ".") {
        Ok(#(alias, col_name)) ->
          resolver.resolve_qualified(map, alias, col_name, table_schemas)
        Error(_) -> resolver.resolve_bare(map, col, table_schemas)
      }
    option.None -> resolver.resolve_bare(map, b.name, table_schemas)
  }
  case resolution {
    resolver.Resolved(rc) ->
      Ok(Parameter(
        name: b.name,
        column_type: rc.column.column_type,
        nullable: False,
      ))
    resolver.UnknownTableRef ->
      Ok(Parameter(name: b.name, column_type: StringType, nullable: False))
    resolver.AmbiguousColumn -> {
      let column_name = case b.binder_column {
        option.Some(col) ->
          case string.split_once(col, ".") {
            Ok(#(_, c)) -> c
            Error(_) -> col
          }
        option.None -> b.name
      }
      Error(AmbiguousColumn(column: column_name, candidates: dict.keys(map)))
    }
    resolver.UnknownQualifiedAlias -> {
      let alias = case b.binder_column {
        option.Some(col) ->
          case string.split_once(col, ".") {
            Ok(#(a, _)) -> a
            Error(_) -> col
          }
        option.None -> b.name
      }
      Error(UnknownAlias(alias: alias))
    }
    resolver.UnknownColumnInKnownTable -> {
      let #(table, col_name) = case b.binder_column {
        option.Some(col) ->
          case string.split_once(col, ".") {
            Ok(#(a, c)) -> #(a, c)
            Error(_) -> #("", col)
          }
        option.None -> #("", b.name)
      }
      Error(UnknownColumnInTable(table: table, column: col_name))
    }
    resolver.UnknownBareColumn -> {
      let col_name = case b.binder_column {
        option.Some(col) ->
          case string.split_once(col, ".") {
            Ok(#(_, c)) -> c
            Error(_) -> col
          }
        option.None -> b.name
      }
      Error(UnknownColumn(column: col_name))
    }
  }
}

/// Override LIMIT ? and OFFSET ? parameter types to IntType.
/// These never participate in column comparisons, so both text-based
/// and opcode-based inference miss them.
fn fix_limit_offset_param_types(
  params: List(Parameter),
  tokens: List(tokenize.Token),
) -> List(Parameter) {
  let limit_offset_positions =
    find_limit_offset_param_positions(tokens, 0, 0, [], False)
  list.index_map(params, fn(param, idx) {
    case list.contains(limit_offset_positions, idx) {
      True -> Parameter(..param, column_type: query.IntType, nullable: False)
      False -> param
    }
  })
}

fn find_limit_offset_param_positions(
  tokens: List(tokenize.Token),
  param_count: Int,
  depth: Int,
  acc: List(Int),
  prev_is_limit_offset: Bool,
) -> List(Int) {
  case tokens {
    [] -> list.reverse(acc)
    [tokenize.OpenParen, ..rest] ->
      find_limit_offset_param_positions(
        rest,
        param_count,
        depth + 1,
        acc,
        prev_is_limit_offset,
      )
    [tokenize.CloseParen, ..rest] ->
      find_limit_offset_param_positions(
        rest,
        param_count,
        depth - 1,
        acc,
        prev_is_limit_offset,
      )
    [tokenize.ParamAnon, ..rest] | [tokenize.ParamNamed(_), ..rest] -> {
      let new_acc = case prev_is_limit_offset && depth == 0 {
        True -> [param_count, ..acc]
        False -> acc
      }
      find_limit_offset_param_positions(
        rest,
        param_count + 1,
        depth,
        new_acc,
        False,
      )
    }
    [tokenize.Word(w), ..rest] -> {
      let upper = string.uppercase(w)
      let is_limit_or_offset = upper == "LIMIT" || upper == "OFFSET"
      let new_prev = case is_limit_or_offset && depth == 0 {
        True -> True
        False -> prev_is_limit_offset
      }
      find_limit_offset_param_positions(rest, param_count, depth, acc, new_prev)
    }
    [_, ..rest] ->
      find_limit_offset_param_positions(
        rest,
        param_count,
        depth,
        acc,
        prev_is_limit_offset,
      )
  }
}

/// Metadata-driven INSERT VALUES extraction.
///
/// When `column_list = None`, derives the bindable column list from the table
/// schema in declared order, skipping generated and hidden columns (hidden != 0).
/// When `column_list = Some(names)`, uses the named columns.
///
/// Row-count validation: every VALUES row must have the same number of
/// expressions as the number of bound columns. Mismatch returns
/// `InsertValuesCountMismatch` instead of falling back to opcode inference.
///
/// Write-nullability = column.nullable || is_rowid_alias. Rowid alias columns
/// (INTEGER PRIMARY KEY on an ordinary rowid table with a single-column PK)
/// accept NULL on INSERT for auto-assign semantics.
///
/// Only columns whose corresponding VALUES expression is a `?` placeholder
/// produce a Parameter. Literal expressions (strings, numbers, etc.) are
/// skipped -- the bound-column count must still match, but non-placeholder
/// expressions don't yield parameters.
fn extract_insert_parameters_v2(
  insert_stmt: statement_parser.InsertStmt,
  table_metadata: schema.TableMetadataV2,
) -> Result(List(Parameter), ParameterResolutionError) {
  let target_name = insert_stmt.target.table.name.text
  let metadatas = case dict.get(table_metadata.columns, target_name) {
    Ok(m) -> m
    Error(_) -> []
  }
  // Bindable = columns that are neither generated nor hidden.
  let bindable = list.filter(metadatas, fn(m) { m.hidden == 0 })

  let bound_metas = case insert_stmt.column_list {
    option.Some(names) ->
      list.filter_map(names, fn(name) {
        case list.find(bindable, fn(m) { m.column.name == name }) {
          Ok(m) -> Ok(m)
          Error(_) -> Error(Nil)
        }
      })
    option.None -> bindable
  }

  let bound_count = list.length(bound_metas)

  case insert_stmt.source {
    statement_parser.ValuesSource(_, rows) -> {
      use _ <- result.try(validate_row_counts(rows, bound_count, 1))
      // Walk every row. SQLite enforces equal expression count per row but not
      // equal placeholder positions, so placeholders may appear at different
      // column positions across rows.
      let params =
        list.flat_map(rows, fn(row) {
          params_from_insert_values_row(row, bound_metas)
        })
      Ok(params)
    }
    statement_parser.DefaultValuesSource -> Ok([])
    // SelectSource is handled before this function is called.
    statement_parser.SelectSource(_) -> Ok([])
  }
}

fn params_from_insert_values_row(
  row: List(List(tokenize.Token)),
  bound_metas: List(schema.ColumnMetadata),
) -> List(Parameter) {
  list.zip(row, bound_metas)
  |> list.filter_map(fn(pair) {
    let #(expr_tokens, meta) = pair
    param_from_insert_values_expr(expr_tokens, meta)
  })
}

fn param_from_insert_values_expr(
  expr_tokens: List(tokenize.Token),
  meta: schema.ColumnMetadata,
) -> Result(Parameter, Nil) {
  case expr_tokens {
    [tokenize.ParamAnon] | [tokenize.ParamNamed(_)] ->
      Ok(Parameter(
        name: param_name_from_insert_values_expr(expr_tokens, meta),
        column_type: meta.column.column_type,
        nullable: meta.column.nullable || meta.is_rowid_alias,
      ))
    _ -> Error(Nil)
  }
}

fn param_name_from_insert_values_expr(
  expr_tokens: List(tokenize.Token),
  meta: schema.ColumnMetadata,
) -> String {
  case expr_tokens {
    [tokenize.ParamNamed(n)] -> n
    _ -> meta.column.name
  }
}

/// Extraction-stage row count safety net. `sqlite.gleam` runs an equivalent
/// pre-flight check before EXPLAIN to surface a typed error in place of
/// SQLite's generic message; this version exists so the param pipeline
/// itself can never silently emit a malformed parameter list if the
/// pre-flight is bypassed (e.g. a future caller that goes straight to
/// `extract_parameters`). Both layers are intentional.
fn validate_row_counts(
  rows: List(List(List(tokenize.Token))),
  expected: Int,
  index: Int,
) -> Result(Nil, ParameterResolutionError) {
  case rows {
    [] -> Ok(Nil)
    [row, ..rest] -> {
      use <- bool.guard(
        list.length(row) != expected,
        Error(InsertValuesCountMismatch(
          expected: expected,
          got: list.length(row),
          row: index,
        )),
      )
      validate_row_counts(rest, expected, index + 1)
    }
  }
}

/// For INSERT ... SELECT, match parameters positionally.
///
/// Uses the typed `InsertStmt` for the target table and explicit column list,
/// avoiding the legacy `parse_insert_table_name` and `parse_insert_columns`
/// heuristics. The remaining token walk is for the SELECT body (params in the
/// projection and WHERE) where the parser hasn't surfaced those slices yet.
fn extract_insert_select_parameters(
  insert_stmt: statement_parser.InsertStmt,
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
) -> List(Parameter) {
  let target_cols = option.unwrap(insert_stmt.column_list, [])
  let target_table = insert_stmt.target.table.name.text
  let target_col_types = case dict.get(table_schemas, target_table) {
    Ok(cols) -> cols
    Error(_) -> []
  }

  // Find SELECT after the ) that closes the column list
  case tokenize.split_at_keyword(tokens, "SELECT") {
    Error(_) -> []
    Ok(#(_, after_select)) -> {
      // SELECT list ends at FROM
      let select_tokens = tokenize.take_until_keywords(after_select, ["FROM"])
      let items = tokenize.split_on_commas(select_tokens)
      let select_params =
        items
        |> list.index_map(fn(item_tokens, idx) {
          insert_select_projection_param(
            item_tokens,
            idx,
            target_cols,
            target_col_types,
          )
        })
        |> list.filter_map(fn(o) {
          case o {
            option.Some(p) -> Ok(p)
            option.None -> Error(Nil)
          }
        })

      // Tokens from FROM onward for WHERE params
      let from_onwards = tokenize.drop_until_keyword(after_select, "FROM")
      let where_tables =
        subquery.find_all_subquery_tables(from_onwards)
        |> list.filter(fn(t) { dict.has_key(table_schemas, t) })
      let where_binders = binder.find_param_binders(from_onwards)
      let select_param_names = list.map(select_params, fn(p) { p.name })
      let unmatched_where_binders =
        list.filter(where_binders, fn(b) {
          !list.contains(select_param_names, b.name)
        })
      let where_params =
        list.map(unmatched_where_binders, fn(binder) {
          insert_select_where_param(binder, where_tables, table_schemas)
        })
      list.append(select_params, where_params)
    }
  }
}

fn insert_select_projection_param(
  item_tokens: List(tokenize.Token),
  idx: Int,
  target_cols: List(String),
  target_col_types: List(Column),
) -> option.Option(Parameter) {
  use <- bool.guard(!is_param_expr(item_tokens), option.None)

  case list.drop(target_cols, idx) |> list.first {
    Ok(col_name) ->
      insert_select_projection_param_for_column(
        item_tokens,
        col_name,
        target_col_types,
      )
    Error(_) -> option.None
  }
}

fn insert_select_projection_param_for_column(
  item_tokens: List(tokenize.Token),
  col_name: String,
  target_col_types: List(Column),
) -> option.Option(Parameter) {
  let param_name = param_name_from_projection_expr(item_tokens, col_name)

  case list.find(target_col_types, fn(c) { c.name == col_name }) {
    Ok(col) ->
      option.Some(Parameter(
        name: param_name,
        column_type: col.column_type,
        nullable: col.nullable,
      ))
    Error(_) ->
      option.Some(Parameter(
        name: param_name,
        column_type: StringType,
        nullable: False,
      ))
  }
}

fn is_param_expr(tokens: List(tokenize.Token)) -> Bool {
  tokens == [tokenize.ParamAnon]
  || case tokens {
    [tokenize.ParamNamed(_)] -> True
    _ -> False
  }
}

fn param_name_from_projection_expr(
  item_tokens: List(tokenize.Token),
  col_name: String,
) -> String {
  case item_tokens {
    [tokenize.ParamNamed(n)] -> n
    _ -> col_name
  }
}

fn insert_select_where_param(
  b: binder.Binder,
  where_tables: List(String),
  table_schemas: Dict(String, List(Column)),
) -> Parameter {
  let names_to_try = binder_names_to_try(b)
  let found_col =
    list.find_map(names_to_try, fn(name) {
      list.find_map(where_tables, fn(table) {
        query.find_column(table_schemas, table, name)
      })
    })

  parameter_from_column_result(b.name, found_col, False)
}

fn binder_names_to_try(b: binder.Binder) -> List(String) {
  case b.binder_column {
    option.Some(col) -> {
      let bare_col = case string.split_once(col, ".") {
        Ok(#(_, after)) -> after
        Error(_) -> col
      }
      case bare_col == b.name {
        True -> [b.name]
        False -> [bare_col, b.name]
      }
    }
    option.None -> [b.name]
  }
}

/// Legacy fallback for SELECT/DELETE when resolver output cannot be trusted:
/// scan tokens for simple `column OP ?` patterns across known tables.
fn extract_select_parameters(
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
  stmt_type: statement.StatementType,
) -> List(Parameter) {
  let all_tables = collect_all_tables(tokens, stmt_type, table_schemas)
  let binders = binder.find_param_binders(tokens)
  case list.is_empty(binders) {
    True -> []
    False ->
      list.map(binders, fn(binder) {
        let names_to_try = case binder.binder_column {
          option.Some(col) if col != binder.name -> [binder.name, col]
          _ -> [binder.name]
        }
        let resolved =
          resolve_binder_type(names_to_try, all_tables, table_schemas)
        case resolved {
          Ok(col) ->
            Parameter(
              name: binder.name,
              column_type: col.column_type,
              nullable: False,
            )
          Error(_) ->
            Parameter(
              name: binder.name,
              column_type: StringType,
              nullable: False,
            )
        }
      })
  }
}

fn resolve_binder_type(
  names: List(String),
  all_tables: List(String),
  table_schemas: Dict(String, List(Column)),
) -> Result(Column, Nil) {
  case names {
    [] -> Error(Nil)
    [name, ..rest] -> {
      let bare = case string.split_once(name, ".") {
        Ok(#(_, after)) -> after
        Error(_) -> name
      }
      let found =
        list.find_map(all_tables, fn(table) {
          query.find_column(table_schemas, table, bare)
        })
      case found {
        Ok(c) -> Ok(c)
        Error(_) -> resolve_binder_type(rest, all_tables, table_schemas)
      }
    }
  }
}

fn collect_all_tables(
  tokens: List(tokenize.Token),
  stmt_type: statement.StatementType,
  table_schemas: Dict(String, List(Column)),
) -> List(String) {
  let from_tables = case stmt_type {
    statement.Select -> select.parse_from_tables(tokens)
    statement.Delete -> [statement.parse_delete_table_name(tokens)]
    _ -> []
  }
  let subquery_tables = subquery.find_all_subquery_tables(tokens)
  let combined = list.append(from_tables, subquery_tables)
  combined
  |> list.filter(fn(t) {
    case dict.get(table_schemas, t) {
      Ok(_) -> True
      Error(_) -> False
    }
  })
  |> list.unique
}

fn extract_update_parameters(
  table_schemas: Dict(String, List(Column)),
  tokens: List(tokenize.Token),
) -> List(Parameter) {
  let table_name = statement.parse_update_table_name(tokens)
  let set_params = parse_params.parse_update_set_columns(tokens)
  let where_params = parse_params.parse_where_columns(tokens)

  case dict.get(table_schemas, table_name) {
    Ok(table_cols) -> {
      let set_parameters =
        list.map(set_params, fn(param) {
          let #(param_name, lookup_col) = param
          let found_col = list.find(table_cols, fn(c) { c.name == lookup_col })
          parameter_from_column_result(param_name, found_col, True)
        })
      let where_parameters =
        list.map(where_params, fn(param) {
          let #(param_name, lookup_col) = param
          let found_col =
            list.find(table_cols, fn(c) { c.name == lookup_col })
            |> result.lazy_or(fn() {
              find_column_in_all_tables(table_schemas, lookup_col)
            })
          parameter_from_column_result(param_name, found_col, False)
        })
      list.append(set_parameters, where_parameters)
    }
    Error(_) -> {
      let set_parameters =
        list.map(set_params, fn(param) {
          let #(param_name, _lookup_col) = param
          Parameter(name: param_name, column_type: StringType, nullable: False)
        })
      let where_parameters =
        list.map(where_params, fn(param) {
          let #(param_name, _lookup_col) = param
          Parameter(name: param_name, column_type: StringType, nullable: False)
        })
      list.append(set_parameters, where_parameters)
    }
  }
}

fn find_column_in_all_tables(
  table_schemas: Dict(String, List(Column)),
  column_name: String,
) -> Result(Column, Nil) {
  dict.values(table_schemas)
  |> list.flatten
  |> list.find(fn(c) { c.name == column_name })
}

fn parameter_from_column_result(
  name: String,
  found_col: Result(Column, Nil),
  preserve_nullability: Bool,
) -> Parameter {
  case found_col {
    Ok(col) ->
      Parameter(
        name: name,
        column_type: col.column_type,
        nullable: preserve_nullability && col.nullable,
      )
    Error(_) -> Parameter(name: name, column_type: StringType, nullable: False)
  }
}

// ---- Parameter deduplication ----

/// Ensure parameter names are unique by appending _2, _3, etc. for duplicates.
pub fn deduplicate_parameter_names(params: List(Parameter)) -> List(Parameter) {
  deduplicate_params_loop(params, dict.new(), [])
  |> list.reverse
}

fn deduplicate_params_loop(
  params: List(Parameter),
  seen: Dict(String, Int),
  acc: List(Parameter),
) -> List(Parameter) {
  case params {
    [] -> acc
    [p, ..rest] ->
      case dict.get(seen, p.name) {
        Ok(count) -> {
          let new_name = p.name <> "_" <> int.to_string(count + 1)
          deduplicate_params_loop(rest, dict.insert(seen, p.name, count + 1), [
            Parameter(..p, name: new_name),
            ..acc
          ])
        }
        Error(_) ->
          deduplicate_params_loop(rest, dict.insert(seen, p.name, 1), [p, ..acc])
      }
  }
}
