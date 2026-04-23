# TODO

## Deferred from code review (2026-04-20)

### Performance: O(n^2) string scanning in remaining functions

Many grapheme-by-grapheme scanner functions use `string.pop_grapheme` and
`string.drop_start(s, 1)`, which are O(n) per call on Erlang binaries,
making the overall scan O(n^2). The recent commit `ecbe7ff` fixed this in
some functions, but many remain (now in `sqlite/parse.gleam`):

- `do_find_top_level_keyword`
- `do_find_keyword_idx`
- `do_split_on_and_or`
- `do_find_placeholder`
- `do_find_last_top_level_as`
- `do_mask_string_contents`

For typical SQL query sizes (under 1KB) this is negligible, but a proper
tokenizer or byte-offset-based approach would fix it systematically. This
overlaps with the README's "proper SQL tokenizer" known limitation. A
tokenizer would also eliminate the duplicated quote/paren-tracking state
machines across these functions.

### LOW: Temp file uses predictable name

`marmot.gleam:422-424` builds the temp file path with
`erlang:unique_integer()`, which is monotonic and guessable. On shared
systems another local user could pre-create a symlink at the expected path.
Risk is low because marmot is a single-user dev tool and the content is
generated Gleam code. Using `mkstemp`-equivalent (e.g., a random suffix or
Erlang's `file:mktemp`) would eliminate the race.

### INFO: `query_function` config has no path validation

`codegen.gleam:36` (`parse_query_function`) splits the config value and
interpolates it into a Gleam `import` statement with no path validation.
A crafted `gleam.toml` value could produce an unexpected import path.
Gleam's module resolver would likely reject invalid paths, but adding a
check that the module path contains no `..` segments would be defensive.

## Completed (2026-04-23)

- Split `sqlite.gleam` (4185 lines) into three modules:
  `sqlite.gleam` (orchestration + public API),
  `sqlite/opcode.gleam` (opcode types and resolution),
  `sqlite/parse.gleam` (text-based SQL parsing)
- Fixed `validate_output` path traversal: `resolve_path` now preserves
  leading `..` segments so `../src/generated` is correctly rejected
- Added `/* block comment */` handling to `contains_semicolon_outside_strings`
- Added design-decision comments for SUM() -> FloatType mapping
