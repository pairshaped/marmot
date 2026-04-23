# TODO

No outstanding items.

## Completed (2026-04-23)

- Split `sqlite.gleam` (4185 lines) into three modules:
  `sqlite.gleam` (orchestration + public API),
  `sqlite/opcode.gleam` (opcode types and resolution),
  `sqlite/parse.gleam` (text-based SQL parsing)
- Fixed `validate_output` path traversal: `resolve_path` now preserves
  leading `..` segments so `../src/generated` is correctly rejected
- Added `/* block comment */` handling to `contains_semicolon_outside_strings`
- Added design-decision comments for SUM() -> FloatType mapping
- Fixed `find_param_binders` aborting on first unresolvable anonymous param
- Renamed `strip_line_comments` to `strip_comments` (now handles block comments too)
- Block comment stripping now inserts a space to prevent token fusion
- Consolidated duplicated `char_code` helpers into `query.gleam`
- Fixed unused/misplaced imports in `tokenize_test.gleam`
- Replaced `os:getenv/0` linear scan with single-var FFI (`marmot_ffi:get_env/1`)
- Added explanatory comment for float p4 decoder in `opcode.gleam`
- Added test for `--database` as trailing CLI arg with no value
- Replaced predictable temp file names with `crypto:strong_rand_bytes` +
  exclusive file creation via `marmot_ffi:make_tmp_file/2`
- `query_function` path validation already done (rejects `..` segments,
  validates identifiers)
- O(n^2) string scanning: all 6 listed functions were already replaced by
  tokenizer-based equivalents in commit `1598a5e`. No `string.pop_grapheme`
  or `string.drop_start(s, 1)` loops remain in any source file.
