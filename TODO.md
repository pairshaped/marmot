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
