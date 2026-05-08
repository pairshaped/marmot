import marmot/internal/sqlite/parse/text

// ---- String operations (pre-tokenization) ----

/// Normalize SQL whitespace: strip comments, replace newlines/tabs with spaces
/// (preserving string literals), collapse runs, and trim.
pub fn normalize_sql_whitespace(sql: String) -> String {
  text.normalize_sql_whitespace(sql)
}

/// Strip Marmot-specific `!`/`?` nullability suffixes from alias names
/// before sending SQL to SQLite's EXPLAIN. Kept as a string operation
/// because its output is sent to SQLite, not analyzed by Marmot.
///
/// Note: this does not track comment state (line or block comments) because
/// comments are already stripped by `query.strip_comments` before this
/// function is called. Suffixes inside string literals are protected by
/// the in_single/in_double tracking.
///
/// The grapheme-level `!`/`?` detection is safe because the input has already
/// been whitespace-normalized: `?` placeholders always follow an operator
/// or keyword (non-ident-char preceding context), so they pass through
/// unchanged. Nullability suffixes follow an ident char and a boundary,
/// which is the only case this function strips.
pub fn strip_nullability_suffixes(sql: String) -> String {
  text.strip_nullability_suffixes(sql)
}

/// Escape double quotes in an identifier to prevent SQL injection.
pub fn quote_identifier(name: String) -> String {
  text.quote_identifier(name)
}
