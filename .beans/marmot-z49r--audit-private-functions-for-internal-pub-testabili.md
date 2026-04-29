---
# marmot-z49r
title: Audit private functions for @internal pub testability
status: todo
type: task
priority: normal
created_at: 2026-04-29T17:09:14Z
updated_at: 2026-04-29T17:28:42Z
---

Surveyed all private functions across 9 .gleam files under src/. Found ~100 private functions total.

Top candidates for @internal pub fn (high value, low risk):

Pure functions (easiest to test, no side effects):
- sql_error_hint (error.gleam:120) — 3 error patterns, pure string->string
- resolve_path (project.gleam:196) — path normalization with . / .. edge cases
- parse_cli_args_loop (project.gleam:77) — CLI flag parsing edge cases
- validate_returns_type_name + is_valid_pascal_case_identifier (sqlite.gleam:890/916) — pure validation
- deduplicate_params_loop (sqlite.gleam:823) — pure dedup with _2, _3 suffix logic
- is_valid_gleam_name (codegen.gleam:91) — pure identifier validation
- find_named_param_in_name (parse.gleam:705) — string scanning for @/:/$ params
- skip_operator_in_prev (parse.gleam:824) — 43 lines, operator recognition for param scanning
- do_normalize_column_ref (parse.gleam:975) — recursive function-wrapper stripping

Code generation (pure, already tested via snapshots, but isolation valuable):
- generate_imports (codegen.gleam:230) — conditional import combos
- decoder_body (codegen.gleam:336) — 56 lines, field decoder generation
- sqlight_encoder (codegen.gleam:517) — 38 lines, type x nullable x timestamp matrix
- column_decoder (codegen.gleam:570) — type-to-decoder mapping
- generate_row_type_named (codegen.gleam:285) — type definition gen
- generate_param_list (codegen.gleam:491) — parameter declaration formatting
- generate_query_function / generate_exec_function (codegen.gleam:432/464)

Pipeline functions (higher risk, need careful extraction):
- process_sql_file (marmot.gleam:222) — 64 lines, 5-stage pipeline with error handling
- extract_result_columns (sqlite.gleam:184) — 77 lines, opcode+text merge strategy
- extract_parameters (sqlite.gleam:331) — 68 lines, dispatch with fallback
- resolve_select_item (sqlite.gleam:264) — 36 lines, multi-strategy resolution
- extract_returning_columns (sqlite.gleam:304) — 22 lines, * / explicit paths
- extract_insert_parameters (sqlite.gleam:402) — 32 lines
- extract_select_parameters (sqlite.gleam:563) — 33 lines
- extract_update_parameters (sqlite.gleam:646) — 69 lines
- scan_for_returns (sqlite.gleam:858) — 30 lines, annotation scanning

Already done in marmot-yse8:
- infer_cast_type, infer_coalesce_type, infer_case_type (parse.gleam) — @internal pub fn with direct tests
