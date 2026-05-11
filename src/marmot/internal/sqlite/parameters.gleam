//// Parameter extraction from opcodes plus parsed SQL context.

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
          case
            opcode.infer_parameter_type(
              var_op,
              opcodes,
              cursor_table,
              table_schemas,
              pk_columns,
            )
          {
            Ok(p) -> p
            Error(_) ->
              Parameter(name: "param", column_type: StringType, nullable: False)
          }
        })
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
        Ok(statement_parser.Insert(stmt)) -> {
          case stmt.source {
            statement_parser.ValuesSource(_, _)
            | statement_parser.DefaultValuesSource -> {
              use parsed <- result.try(extract_insert_parameters_v2(
                stmt,
                table_metadata,
              ))
              // If the extracted param count doesn't match (e.g. upsert with
              // ON CONFLICT DO UPDATE SET adds more `?` parameters beyond the
              // VALUES row), fall back to opcode inference so those extra
              // parameters aren't silently dropped.
              case list.length(parsed) == param_count {
                True -> Ok(parsed)
                False -> Ok(opcode_fallback())
              }
            }
            statement_parser.SelectSource(_) -> {
              let parsed =
                extract_insert_select_parameters(stmt, table_schemas, tokens)
              case list.length(parsed) == param_count {
                True -> Ok(parsed)
                False -> Ok(opcode_fallback())
              }
            }
          }
        }
        Ok(statement_parser.Unsupported(_)) | Error(_) -> Ok(opcode_fallback())
      }

      use body <- result.try(body_result)
      Ok(fix_limit_offset_param_types(body, tokens))
    }
  }
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
            True -> Ok(params)
            False -> {
              let _ = stmt_type
              Ok(opcode_fallback())
            }
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
            True -> Ok(params)
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
  let all = binder.find_param_binders(tokens)
  let depths = collect_param_depths(tokens, 0, [])
  // `find_param_binders` skips duplicates of the same named param; `depths`
  // emits one entry per occurrence. Walk both, advancing `depths` even when
  // a binder is skipped, so depths line up with the binders we kept.
  zip_binders_with_depths(all, depths, [])
}

fn collect_param_depths(
  tokens: List(tokenize.Token),
  depth: Int,
  acc: List(#(String, Int)),
) -> List(#(String, Int)) {
  case tokens {
    [] -> list.reverse(acc)
    [tokenize.OpenParen, ..rest] -> collect_param_depths(rest, depth + 1, acc)
    [tokenize.CloseParen, ..rest] -> collect_param_depths(rest, depth - 1, acc)
    [tokenize.ParamAnon, ..rest] ->
      collect_param_depths(rest, depth, [#("?", depth), ..acc])
    [tokenize.ParamNamed(name), ..rest] ->
      collect_param_depths(rest, depth, [#(name, depth), ..acc])
    [_, ..rest] -> collect_param_depths(rest, depth, acc)
  }
}

fn zip_binders_with_depths(
  binders: List(binder.Binder),
  depths: List(#(String, Int)),
  acc: List(#(binder.Binder, Int)),
) -> List(#(binder.Binder, Int)) {
  case binders, depths {
    [], _ -> list.reverse(acc)
    [_, ..], [] -> list.reverse(acc)
    [b, ..b_rest], [#(name, depth), ..d_rest] -> {
      case name == "?" {
        True ->
          // Anon binders: one depth entry per `?` token, one binder per `?`
          // (no dedup in `find_param_binders` for anon). Pair them 1:1.
          zip_binders_with_depths(b_rest, d_rest, [#(b, depth), ..acc])
        False ->
          // Named binders: `find_param_binders` keeps only the first
          // occurrence of each named param. Pair this depth entry with the
          // matching binder, then skip any later depth entries that repeat
          // the same name (they refer to a binder that wasn't kept).
          case name == b.name {
            True ->
              zip_binders_with_depths(b_rest, drop_dups(d_rest, name), [
                #(b, depth),
                ..acc
              ])
            False -> zip_binders_with_depths(binders, d_rest, acc)
          }
      }
    }
  }
}

fn drop_dups(
  depths: List(#(String, Int)),
  name: String,
) -> List(#(String, Int)) {
  case depths {
    [#(n, _), ..rest] if n == name -> drop_dups(rest, name)
    _ -> depths
  }
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

/// T17 metadata-driven INSERT extraction.
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
          list.zip(row, bound_metas)
          |> list.filter_map(fn(pair) {
            let #(expr_tokens, meta) = pair
            case expr_tokens {
              [tokenize.ParamAnon] | [tokenize.ParamNamed(_)] ->
                Ok(Parameter(
                  name: case expr_tokens {
                    [tokenize.ParamNamed(n)] -> n
                    _ -> meta.column.name
                  },
                  column_type: meta.column.column_type,
                  nullable: meta.column.nullable || meta.is_rowid_alias,
                ))
              _ -> Error(Nil)
            }
          })
        })
      Ok(params)
    }
    statement_parser.DefaultValuesSource -> Ok([])
    // SelectSource is handled before this function is called.
    statement_parser.SelectSource(_) -> Ok([])
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
    [row, ..rest] ->
      case list.length(row) == expected {
        True -> validate_row_counts(rest, expected, index + 1)
        False ->
          Error(InsertValuesCountMismatch(
            expected: expected,
            got: list.length(row),
            row: index,
          ))
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
          let is_anon = item_tokens == [tokenize.ParamAnon]
          let is_named = case item_tokens {
            [tokenize.ParamNamed(_)] -> True
            _ -> False
          }
          case is_anon || is_named {
            True ->
              case list.drop(target_cols, idx) |> list.first {
                Ok(col_name) ->
                  case
                    list.find(target_col_types, fn(c) { c.name == col_name })
                  {
                    Ok(col) -> {
                      let param_name = case item_tokens {
                        [tokenize.ParamNamed(n)] -> n
                        _ -> col_name
                      }
                      option.Some(Parameter(
                        name: param_name,
                        column_type: col.column_type,
                        nullable: col.nullable,
                      ))
                    }
                    Error(_) -> {
                      let param_name = case item_tokens {
                        [tokenize.ParamNamed(n)] -> n
                        _ -> col_name
                      }
                      option.Some(Parameter(
                        name: param_name,
                        column_type: StringType,
                        nullable: False,
                      ))
                    }
                  }
                Error(_) -> option.None
              }
            False -> option.None
          }
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
        |> list.filter(fn(t) {
          case dict.get(table_schemas, t) {
            Ok(_) -> True
            Error(_) -> False
          }
        })
      let where_binders = binder.find_param_binders(from_onwards)
      let select_param_names = list.map(select_params, fn(p) { p.name })
      let unmatched_where_binders =
        list.filter(where_binders, fn(b) {
          !list.contains(select_param_names, b.name)
        })
      let where_params =
        list.map(unmatched_where_binders, fn(binder) {
          let names_to_try = case binder.binder_column {
            option.Some(col) -> {
              let bare_col = case string.split_once(col, ".") {
                Ok(#(_, after)) -> after
                Error(_) -> col
              }
              case bare_col == binder.name {
                True -> [binder.name]
                False -> [bare_col, binder.name]
              }
            }
            option.None -> [binder.name]
          }
          case
            list.find_map(names_to_try, fn(name) {
              list.find_map(where_tables, fn(table) {
                query.find_column(table_schemas, table, name)
              })
            })
          {
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
      list.append(select_params, where_params)
    }
  }
}

/// For SELECT/DELETE statements, scan tokens for `column OP ?` patterns
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

/// Collect every table referenced in the tokens
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

/// For UPDATE statements
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
      let where_parameters =
        list.map(where_params, fn(param) {
          let #(param_name, lookup_col) = param
          let found_col =
            list.find(table_cols, fn(c) { c.name == lookup_col })
            |> result.lazy_or(fn() {
              dict.values(table_schemas)
              |> list.flatten
              |> list.find(fn(c) { c.name == lookup_col })
            })
          case found_col {
            Ok(col) ->
              Parameter(
                name: param_name,
                column_type: col.column_type,
                nullable: False,
              )
            Error(_) ->
              Parameter(
                name: param_name,
                column_type: StringType,
                nullable: False,
              )
          }
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
