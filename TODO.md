# TODO

## Deferred from code review (2026-04-20)

### Performance: O(n^2) string scanning in remaining functions

Many grapheme-by-grapheme scanner functions use `string.pop_grapheme` and
`string.drop_start(s, 1)`, which are O(n) per call on Erlang binaries,
making the overall scan O(n^2). The recent commit `ecbe7ff` fixed this in
some functions, but many remain:

- `do_find_top_level_keyword`
- `do_find_keyword_idx`
- `do_split_on_and_or`
- `do_find_placeholder`
- `do_find_last_top_level_as`
- `do_mask_string_contents`

For typical SQL query sizes (under 1KB) this is negligible, but a proper
tokenizer or byte-offset-based approach would fix it systematically. This
overlaps with the README's "proper SQL tokenizer" known limitation.

## Deferred from security review (2026-04-20)

### LOW: `validate_output` does not resolve path traversal

`project.gleam:179` checks `string.starts_with(output, "src/")` but does
not resolve `..` segments, so a path like `src/../../etc/foo` passes
validation. Since the attacker needs local write access to `gleam.toml` or
CLI args, and the written content is generated Gleam code (not arbitrary
data), the practical risk is minimal. A path-canonicalization step before
the prefix check would close this.

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
