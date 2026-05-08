# Internal Readability Refactor Design

## Purpose

Marmot works, but parts of the internals ask too much of the reader. The main
pain is the amount of parser, introspection, fallback, and inference behavior
packed into a few large files.

This work improves the shape of the code so a maintainer can change one SQL
feature without loading the whole project into their head.

## Out of Scope

- User-facing contributor guide in this phase.
- SQL engine rewrite.
- CLI behavior changes, except when needed to preserve existing behavior.
- Public API redesign.
- Broad style churn.

## Public API Boundary

The following surfaces must remain stable unless a later release intentionally
changes them:

- `gleam run -m marmot`
- CLI flags: `--database`, `--output`
- `DATABASE_URL` database configuration
- `[tools.marmot]` config fields: `database`, `output`, `query_function`,
  `sql_dir`
- SQL file conventions and generated output paths
- Generated function names, labelled arguments, row type names, decoders,
  imports, helper functions, and error behavior
- SQL annotations that users can write today, including `-- returns: Name`
- Snapshot output for existing generated code, unless a deliberate bug fix
  changes it

Anything under `src/marmot/internal/**` can move, split, or change shape.
Existing tests may import internal modules, so tests should move with the new
module boundaries rather than freezing the current internals.

## Current Pain Points

### Large Files

`src/marmot/internal/sqlite/parse.gleam` contains several jobs:

- SQL normalization helpers
- statement classification
- select list parsing
- CTE and table extraction
- insert/update/delete parsing
- WHERE parameter binding
- subquery table discovery
- expression type inference
- generic list and range utilities

`src/marmot/internal/sqlite.gleam` combines orchestration, SQLite metadata
loading, opcode analysis coordination, parameter extraction, return column
handling, returns annotation parsing, and fallback logic.

The result is hard to browse. A small behavior change often requires scanning
hundreds of lines to find the right helper and its assumptions.

### Ambiguous Failure Values

Several helpers return plain fallback values when parsing fails:

- `""` for missing table names
- `[]` for either no result or failed parsing
- `StringType` with `nullable: True` for many unknown expression cases
- sentinel columns and parameters named `"unknown"` or `"param"`

These values are sometimes fine at the final fallback boundary. They are harder
to reason about when passed through multiple internal layers.

### Control Flow Shape

Much of the parser is written as recursive token scanning with nested `case`
expressions. Gleam supports this style, but the current code often hides the
intent of each pass. State machines exist implicitly in function arguments and
comments instead of named types.

### Mixed Abstraction Levels

Some functions do several levels of work at once. For example, a helper may
find SQL tokens, resolve table schemas, choose a parameter name, and build a
fallback parameter in the same block. This makes tests more valuable but the
implementation harder to change.

## Target Shape

The refactor should move the internals toward small modules with a clear job.
The first version needs better boundaries, even if they are imperfect.

Proposed internal modules:

- `marmot/internal/sqlite/parse.gleam`
  Temporary home for shared parser types (StatementType, SelectItem,
  NullabilityOverride, Binder) during the split. Shrinks as types move
  to their owning modules in Phase 5.

- `marmot/internal/sqlite/parse/text.gleam`
  SQL text normalization, nullability suffix stripping, and identifier quoting
  that happen before token-level parsing.

- `marmot/internal/sqlite/parse/statement.gleam`
  Statement classification and table name extraction.

- `marmot/internal/sqlite/parse/select.gleam`
  SELECT list parsing, CTE skipping, FROM table extraction, RETURNING parsing.

- `marmot/internal/sqlite/parse/parameters.gleam`
  WHERE/SET/VALUES parameter binding and binder discovery.

- `marmot/internal/sqlite/parse/expression.gleam`
  Expression type inference, CASE branch extraction, CAST and COALESCE handling.

- `marmot/internal/sqlite/parse/util.gleam`
  Small shared helpers used by the parser modules.

- `marmot/internal/sqlite/schema.gleam`
  SQLite table metadata loading, PK lookup, rootpage mapping.

- `marmot/internal/sqlite/parameters.gleam`
  Parameter extraction from opcodes plus parsed SQL context.

- `marmot/internal/sqlite/results.gleam`
  Result column extraction and RETURNING column handling.

This layout may change during implementation if the code shows a better split.
The guiding rule is simple: a module should have one reason to be opened.

## Initial Function Ownership Map

This map gives the implementing agent a concrete first pass. It is based on the
current code shape. If a move exposes a better boundary, update this spec before
continuing.

**Import rule (decided 2026-05-08):** Gleam rejects import cycles. If an
extracted submodule needs types that live in `parse.gleam`, then
`parse.gleam` cannot also delegate to that submodule. The rule:

- `parse.gleam` is a temporary home for shared parser types during the split.
- New parser submodules may import types from `parse.gleam`.
- Callers import the new submodule directly when they need moved functions.
- Phase 5 moves the remaining types to their owning modules, then
  `parse.gleam` is deleted or nearly empty.

Do not attempt facade re-exports for submodules that import from
`parse.gleam`. The pattern used for `expression.gleam` (direct caller
imports) applies to all further extractions.

### `parse/expression.gleam`

Move expression and column-shape inference here:

- `infer_expression_type`
- `infer_arithmetic_type`
- `infer_cast_type`
- `infer_coalesce_type`
- `infer_case_type`
- `extract_case_branches`
- `do_extract_case_branches`
- `strip_trailing_end`
- `infer_literal_token_type`
- `apply_override`, unless later phases show overrides belong with select items
- `CaseScanState`

### `parse/text.gleam`

Move string operations that run before token-level parsing here:

- `normalize_sql_whitespace`
- `strip_nullability_suffixes`
- `do_strip_suffixes`
- `quote_identifier`
- `is_ident_char`, if it remains only used by suffix stripping

### `parse/statement.gleam`

Move statement classification and table-name extraction here:

- `StatementType`
- `classify_statement`
- `extract_name_after_keyword`
- `parse_insert_table_name`
- `parse_update_table_name`
- `parse_delete_table_name`

### `parse/select.gleam`

Move result-list, CTE, FROM, and RETURNING parsing here:

- `NullabilityOverride`
- `SelectItem`
- `parse_select_items`
- `parse_select_item`
- `skip_select_keyword`
- `strip_override_from_alias`
- `detect_bare_column`
- `token_list_to_name`, if it remains select-specific
- `skip_with_prefix`
- `skip_cte_definitions`
- `parse_from_tables`
- `extract_table_names_from_from`
- `split_on_joins`
- `do_split_on_joins`
- `strip_trailing_join_modifiers`
- `parse_returning_columns`

### `parse/parameters.gleam`

Move SQL-text parameter parsing here:

- `Binder`
- `parse_insert_columns`
- `find_first_paren_group`
- `parse_values_placeholder_positions`
- `parse_update_set_columns`
- `split_tokens_on_eq`
- `do_split_on_eq`
- `has_param_token`
- `find_named_param_in_tokens`
- `parse_where_columns`
- `parse_where_condition`
- `has_subquery`
- `do_has_subquery`
- `parse_simple_where_condition`
- `extract_lhs_column`
- `do_extract_lhs`
- `tokens_to_column_name`
- `find_named_param_in_name`
- `take_ident_chars`
- `extract_column_before_param`
- `strip_trailing_arithmetic_op`
- `extract_all_named_params_from_tokens`
- `find_param_binders`
- `do_find_param_binders`
- `extract_column_from_prev`
- `skip_operator_in_prev`
- `skip_between_and`
- `do_skip_between`
- `extract_column_word`
- `collect_inside_reversed_parens`
- `strip_table_prefix`
- `find_all_subquery_tables`
- `do_find_subquery_tables`
- `normalize_column_ref`
- `do_normalize_column_ref`
- `starts_with_param_prefix`, if still used

### `parse/util.gleam`

Move only helpers used by more than one parser submodule:

- `is_ident_char`, if it is shared after `parse/text.gleam` is introduced
- `list_at`
- `make_range`
- `do_make_range`

Avoid dumping unrelated leftovers into `util.gleam`. If a helper has one
caller, leave it near that caller.

### `sqlite/schema.gleam`

Move SQLite metadata loading here:

- `get_table_metadata`
- `add_index_rootpages`
- PRAGMA decoders that are only used for schema loading

### `sqlite/results.gleam`

Move result column extraction here:

- `extract_result_columns`
- `resolve_select_item`
- `extract_returning_columns`

### `sqlite/parameters.gleam`

Move parameter extraction that combines opcodes, schema data, and parsed SQL
context here:

- `extract_parameters`
- `fix_limit_offset_param_types`
- `find_limit_offset_param_positions`
- `extract_insert_parameters`
- `extract_insert_select_parameters`
- `extract_select_parameters`
- `resolve_binder_type`
- `collect_all_tables`
- `extract_update_parameters`
- `deduplicate_parameter_names`
- `deduplicate_params_loop`

### Keep In `sqlite.gleam`

Leave the high-level entry points here unless a checkpoint explicitly changes
that decision:

- `QueryInfo`
- `introspect_columns`
- `introspect_query`
- `strip_nullability_suffixes`
- `ReturnsAnnotationError`
- `parse_returns_annotation`
- returns annotation helpers, until the reviewer approves moving them

## Error and Fallback Model

Do not try to remove every fallback at once. Start by naming the important
ones.

Preferred internal shapes:

```gleam
type ParsedTable {
  FoundTable(String)
  NoTableFound
}
```

```gleam
type InferredColumn {
  Resolved(Column)
  UnknownColumn(alias: String, reason: UnknownColumnReason)
}
```

```gleam
type ParseOutcome(value, reason) {
  Parsed(value)
  CouldNotParse(reason)
}
```

These types do not have to become public. They should help internal code defer
lossy fallback conversion until the edge where Marmot must produce generated
code or a user-facing error.

Sentinel functions such as `query.unknown_column()` can remain at final fallback
points. The improvement is to avoid using sentinel values as normal control
flow inside parsing passes.

## Phased Plan

The implementing agent must treat each phase below as a separate review unit.
Do not run phases together in one large diff. At each checkpoint, update this
spec or the active implementation plan with what changed, what was deferred,
what tests ran, and any behavior drift found. Then pause for reviewer feedback
before continuing.

Recommended commit shape:

- One commit per completed checkpoint.
- A separate commit for any behavior fix discovered during a move.
- A separate commit for spec or plan updates when the design changes.
- No commit should mix mechanical movement with logic changes unless the logic
  change is required for the moved code to compile.

Required checkpoint response format:

```txt
Checkpoint: <name>
Changed:
- <files/modules moved or added>

Public behavior:
- <unchanged, or exact intentional change>

Tests:
- <command>: <result>

Spec/plan updates:
- <none, or path and summary>

Questions for review:
- <specific question, or "none">
```

### Checkpoint 0: Preflight Plan

Before moving code, the implementing agent must:

- Read this spec and create or update an implementation plan.
- Record current `git status --short`.
- Run baseline checks: `gleam format --check src test`, `gleam check`,
  and `bin/test`.
- Record whether there are existing uncommitted files that should be ignored.
- List the first checkpoint they intend to implement and the files they expect
  to touch.
- Pause for reviewer feedback before editing source files.

### Phase 1: Parser Split With Behavior Preserved

Move small pre-tokenization text helpers and expression inference out of
`parse.gleam` first. Expression inference is the larger slice because it is
self-contained and heavily tested through query snapshots.

Steps:

- Create `marmot/internal/sqlite/parse/text.gleam`.
- Move `normalize_sql_whitespace`, `strip_nullability_suffixes`, and
  `quote_identifier`.
- Create `marmot/internal/sqlite/parse/expression.gleam`.
- Move `infer_expression_type`, CASE branch scanning, CAST/COALESCE inference,
  literal inference, and override application if it fits.
- Have callers import directly from the new module (see import rule, line 137).
- Move or add focused tests for expression inference.
- Run the full test suite and compare snapshots.

Expected result: `parse.gleam` becomes smaller with almost no call-site churn.

Checkpoint 1A:

- Commit suggested: `Refactor parser text helpers`
- Move pre-tokenization string helpers only.
- Have callers import directly from `parse/text.gleam` (or keep the
  parse.gleam delegations since text.gleam has no parse.gleam dependency).
- Pause after tests pass and wait for reviewer feedback.

Checkpoint 1B:

- Commit suggested: `Refactor parser expression inference`
- Update the implementation plan with every function moved out of
  `parse.gleam`.
- Update this spec if expression inference needs a different module boundary
  than `parse/expression.gleam`.
- Pause after tests pass and wait for reviewer feedback.

### Phase 2: Statement and Select Parsing

Move statement and result-shape parsing into smaller modules.

Steps:

- Create `parse/statement.gleam` for statement classification and table names.
- Create `parse/select.gleam` for select items, CTE handling, FROM tables, and
  RETURNING columns.
- Have callers import directly from the new modules.
- Rename internal helpers where names are vague after the split.

Expected result: SELECT/result parsing can be read without parameter binding or
expression inference nearby.

Checkpoint 2A:

- Commit suggested: `Refactor parser statement helpers`
- Stop after statement classification and table-name extraction move.
- Update the implementation plan with any caller changes.
- Pause for review before moving SELECT parsing.

Checkpoint 2B:

- Commit suggested: `Refactor parser select helpers`
- Stop after SELECT, CTE, FROM, and RETURNING parsing move.
- Update this spec if SELECT and RETURNING should not live together.
- Pause after tests pass and wait for reviewer feedback.

### Phase 3: Parameter Binding

Move binder discovery and parameter parsing into its own module.

Steps:

- Create `parse/parameters.gleam`.
- Move WHERE parsing, update SET parsing, insert column placeholders, binder
  scanning, and subquery table discovery if it is only used for parameter
  binding.
- Introduce named result types where `[]` currently means either no parameters
  or failed matching.
- Keep final behavior unchanged for generated output.

Expected result: adding a new SQL operator or parameter pattern has an obvious
home.

Checkpoint 3A:

- Commit suggested: `Refactor parser parameter binders`
- Move binder discovery and WHERE/SET/VALUES parsing only.
- Do not introduce new fallback result types in the same commit unless the move
  cannot compile without them.
- Pause for review before changing fallback representation.

Checkpoint 3B:

- Commit suggested: `Name parser parameter fallbacks`
- Introduce named result types only where the previous `[]` behavior is
  demonstrably ambiguous inside the code.
- Add or adjust focused tests before changing fallback representation.
- Update this spec with the exact new types and where lossy conversion happens.
- Pause after tests pass and wait for reviewer feedback.

Do not start Checkpoint 3B until Checkpoint 3A has been reviewed. It is too easy
to hide behavior changes inside the parameter move.

### Phase 4: Introspection Orchestration Split

Split `sqlite.gleam` after the parser shape settles.

Steps:

- Move schema loading into `sqlite/schema.gleam`.
- Move result column extraction into `sqlite/results.gleam`.
- Move parameter extraction into `sqlite/parameters.gleam`.
- Leave `sqlite.gleam` as the high-level entry point for `introspect_query`,
  `introspect_columns`, `strip_nullability_suffixes`, and
  `parse_returns_annotation`.

Expected result: `sqlite.gleam` reads as orchestration instead of a storage
place for all SQLite analysis.

Checkpoint 4A:

- Commit suggested: `Refactor SQLite schema loading`
- Move schema metadata loading only.
- Keep `sqlite.gleam` public/internal entry points stable.
- Pause after tests pass and wait for reviewer feedback.

Checkpoint 4B:

- Commit suggested: `Refactor SQLite result extraction`
- Move result column and RETURNING extraction only.
- Record any snapshot drift. If drift exists, stop and ask whether it is a bug
  fix or a regression before continuing.
- Pause after tests pass and wait for reviewer feedback.

Checkpoint 4C:

- Commit suggested: `Refactor SQLite parameter extraction`
- Move opcode plus parsed-context parameter extraction only.
- Update this spec if parameter extraction wants a different module boundary
  than `sqlite/parameters.gleam`.
- Pause after tests pass and wait for reviewer feedback.

### Phase 5: Move Remaining Types to Owning Modules

After all function extraction phases complete, move the shared parser types
out of `parse.gleam` into their owning modules. `parse.gleam` should become
empty or nearly empty.

Steps:

- Move `StatementType` into `parse/statement.gleam`.
- Move `SelectItem` and `NullabilityOverride` into `parse/select.gleam`.
- Move `Binder` into `parse/parameters.gleam`.
- Update every caller and test that imports these types from `parse.gleam`
  to import from the owning module instead.
- Delete `parse.gleam` if empty, or update its header to document only the
  remaining utilities.
- Re-run snapshots after each move.

Expected result: no shared-types module remains; each type lives with the
functions that operate on it.

Checkpoint 5:

- Commit suggested: `Move parser types to owning modules`
- Move one type at a time with its callers.
- Update this spec with the final module layout.
- Pause before deleting the last re-exports.

If type movement creates noisy test churn, split by type into separate
checkpoints and pause before deleting old imports.

## Testing Strategy

Every phase should run:

- `gleam format --check src test`
- `gleam check`
- `bin/test`

For each moved area, add or move focused unit tests where they make future work
easier. Snapshot tests remain the main protection for generated code.

Before changing fallback behavior, add a failing test that proves the old shape
is wrong or too vague. Pure moves should not change snapshots.

Snapshot rules:

- Do not update `birdie_snapshots/**` during pure movement checkpoints.
- If snapshots change during a pure move, stop and treat it as a regression
  until reviewed.
- If a later behavior fix intentionally changes snapshots, isolate it in its
  own commit and explain the before/after behavior.

If any command fails, stop at that checkpoint. Do not continue refactoring on
top of a failing phase. Update the implementation plan with the failure,
suspected cause, and next proposed fix.

## Review Strategy

Each phase should be reviewed as its own patch. A good phase has:

- A small list of moved functions
- No generated output drift unless the phase intentionally fixes behavior
- Public CLI and generated code behavior unchanged
- Fewer reasons to open the old large file

Avoid mixing code movement with logic changes. If a logic change is discovered
while moving code, land the move first, then fix the behavior in a separate
patch with tests.

## Agent Handoff Rules

The implementing agent should keep an implementation plan beside this spec.
That plan can live wherever the coding session normally tracks plans, but if a
repo file is useful, use:

`docs/superpowers/specs/2026-05-08-internal-readability-refactor-plan.md`

The plan must be updated:

- During Checkpoint 0, before source edits.
- Before starting each checkpoint.
- After finishing each checkpoint.
- When a proposed module boundary changes.
- When a behavior change is discovered.
- When a test failure changes the order of work.

The implementing agent must pause for reviewer feedback:

- After Checkpoint 0.
- After Checkpoint 1A and 1B.
- After Checkpoint 2A and 2B.
- After Checkpoint 3A and 3B.
- After Checkpoint 4A, 4B, and 4C.
- Before Checkpoint 5 moves remaining types.
- Before any intentional public behavior change.
- Before accepting snapshot drift.
- Before broad renames that touch more than one phase.

The pause should happen with a clean worktree or a single focused commit ready
for review. If the agent cannot produce a clean checkpoint, it should explain
why and stop.

## Reviewer Checklist

At each checkpoint, the reviewer should be able to answer yes to these:

- The diff matches the checkpoint scope.
- Public behavior is unchanged, or the change is explicitly called out.
- Snapshot files did not change during pure movement.
- The old large file got smaller or easier to scan.
- New modules have a clear reason to exist.
- The implementation plan or this spec records any boundary change.
- Tests listed in the checkpoint response actually ran.

## Success Criteria

- `parse.gleam` is a small coordinator for text operations, not the home for
  every SQL parser behavior.
- `sqlite.gleam` exposes the high-level introspection API while delegating
  schema, parameter, and result-column work.
- Public behavior remains stable across the refactor.
- A maintainer can locate the code for expression inference, statement parsing,
  parameter binding, or result extraction from filenames alone.
- New internal types name important unknown or failed states before converting
  them to final fallback values.

## Open Decisions

- ~~Whether to preserve a `parse.gleam` facade permanently or remove it after
  migration.~~ **Decided 2026-05-08:** No facade. Gleam's import-cycle rule
  prevents it when submodules need types from `parse.gleam`. Callers import
  submodules directly. `parse.gleam` holds shared types during the split and
  is deleted or emptied in Phase 5. See import rule above.
- Whether `ReturnsAnnotationError` belongs with SQLite introspection or in a
  small SQL-file metadata module.
- Whether `query.unknown_column()` and `query.unknown_param()` should remain in
  `query.gleam` or move closer to the fallback boundary.
