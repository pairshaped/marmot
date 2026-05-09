# Marmot internals

This guide covers the pipeline that turns `.sql` files into type-safe Gleam functions. Read it before changing the inference engine, codegen, or CLI orchestration.

## Architecture overview

```mermaid
flowchart TD
  Files[".sql files"] --> CLI["marmot.gleam<br/>CLI orchestration"]
  CLI --> Project["project.gleam<br/>config, discovery, output paths"]
  CLI --> SQLite["sqlite.gleam<br/>central introspection"]

  SQLite --> Schema["schema.gleam<br/>tables, PKs, rootpages"]
  SQLite --> Opcode["opcode.gleam<br/>EXPLAIN analysis"]
  SQLite --> Tokenize["tokenize.gleam<br/>SQL tokens"]
  SQLite --> Parse["parse/*.gleam<br/>text walkers"]
  SQLite --> Results["results.gleam<br/>result columns"]
  SQLite --> Params["parameters.gleam<br/>query parameters"]

  Schema --> Query["query.gleam<br/>domain types"]
  Opcode --> Query
  Tokenize --> Query
  Parse --> Query
  Results --> Query
  Params --> Query

  Query --> Codegen["codegen.gleam<br/>generated Gleam module"]
```

## Data flow, step by step

### 1. Entry point (`marmot.gleam`)

`main()` parses argv, then `run_generate()`:
- Reads `gleam.toml` for `[tools.marmot]` config
- Applies precedence: `DATABASE_URL` env > `--database` CLI > `gleam.toml`
- Opens the SQLite database via `sqlight`
- Calls `generate_all()`

`generate_all()`:
1. Finds SQL directories (default: `src/**/sql/`; or `sql_dir` config)
2. Detects output path collisions (multiple sql/ dirs writing to the same file)
3. For each directory, calls `generate_for_directory()`

`generate_for_directory()`:
1. Lists `.sql` files in the directory
2. For each file, calls `process_sql_file()` to produce a `Query`
3. Passes all queries to `codegen.generate_module_with_config()`
4. Runs `gleam format` on the output, writes to disk

### 2. Config (`project.gleam`)

`Config` holds four optional fields from `[tools.marmot]`:

| Field | Source | Purpose |
|---|---|---|
| `database` | env / CLI / toml | Path to SQLite file for introspection |
| `output` | CLI / toml | Output directory for generated modules |
| `query_function` | toml only | Custom wrapper replacing `sqlight.query` |
| `sql_dir` | toml only | Override sql/ directory discovery |

`find_sql_directories()` has two modes:
- **Default mode** (`sql_dir: None`): recursively walks `src/` for directories named `sql`
- **Configured mode** (`sql_dir: Some("path")`): recursively finds all directories under `path` that contain `.sql` files

`output_path()` computes where a generated module lands. It finds the longest common prefix between the output directory and the sql directory, strips it, removes `sql` segments, and joins with output. Examples:
- `src/app/users/sql` + default output -> `src/generated/sql/app/users_sql.gleam`
- `src/server/accounts/sql` + output `src/server/generated/sql` -> `src/server/generated/sql/accounts_sql.gleam`

### 3. SQL processing (`process_sql_file` in `marmot.gleam`)

For each `.sql` file:
1. Reads the file, extracts the function name from the filename
2. Strips comments, validates SQL (non-empty, single query, no stray semicolons)
3. Parses the `-- returns: TypeName` annotation (optional shared row type)
4. Calls `sqlite.introspect_query()` for type information
5. Checks for duplicate column names and generated name collisions
6. Returns a `Query` with name, SQL, path, parameters, and columns

### 4. SQLite introspection (`sqlite.gleam`)

`introspect_query()` is the central pipeline. It takes a DB connection and normalized SQL, returns a `QueryInfo` with columns and parameters.

```mermaid
flowchart TD
  Start["introspect_query(db, sql)"] --> Normalize["Normalize SQL<br/>strip comments, collapse whitespace"]
  Normalize --> Schema["Load schema metadata<br/>tables, columns, PKs, rootpages"]
  Normalize --> Explain["Run EXPLAIN<br/>decode opcodes"]
  Explain --> Cursors["Map cursors to tables"]
  Explain --> Nullability["Compute join nullability"]
  Normalize --> Tokens["Tokenize SQL"]
  Tokens --> Statement["Classify statement"]

  Schema --> Columns["Extract result columns"]
  Cursors --> Columns
  Nullability --> Columns
  Tokens --> Columns
  Statement --> Columns

  Schema --> Parameters["Extract parameters"]
  Cursors --> Parameters
  Explain --> Parameters
  Tokens --> Parameters

  Columns --> Info["QueryInfo"]
  Parameters --> Info
```

Pipeline stages:

1. **Normalize whitespace** (`parse.normalize_sql_whitespace`): strips comments via `query.strip_comments`, then normalizes whitespace in `parse/text.gleam` (newlines/tabs to spaces, collapse runs), preserving string literals.

2. **Load schema** (`schema.get_table_metadata`): queries `sqlite_master` for table names and rootpages, then `PRAGMA table_info` for each table to get column names, types, and nullability.

3. **EXPLAIN**: strips Marmot-specific `!`/`?` nullability suffixes from the SQL, wraps with `EXPLAIN`, and runs it against SQLite. The result is a list of `Opcode` values (addr, opcode name, p1-p5 operands).

4. **Cursor-to-table mapping**: scans opcodes for `OpenRead`/`OpenWrite`, which map SQLite's internal cursor numbers to table rootpages, then to table names via the rootpage mapping from step 2.

5. **Join nullability** (`opcode.compute_join_nullability`): identifies cursors that may produce NULL rows due to LEFT JOIN. When an outer join has no matching inner row, SQLite emits `NullRow` on the inner (right-side) table's cursor; `IfNullRow` tests this state. Columns resolved against nullable cursors are marked nullable in generated types. Also traces through `OpenAutoindex` cursors (transient indexes SQLite builds for unindexed JOINs).

6. **Tokenize** (`tokenize.tokenize`): grapheme-by-grapheme tokenizer that produces a `List(Token)`. Handles keywords, identifiers, string literals, quoted identifiers, numbers, parameters (`?`, `@name`), operators, and Marmot nullability overrides (`!` = non-null, `?` = nullable).

7. **Statement classification** (`statement.classify_statement`): determines whether the query is SELECT, INSERT, UPDATE, DELETE, or REPLACE. INSERT/UPDATE/DELETE without RETURNING produce empty column lists.

8. **Result column extraction** (`results.extract_result_columns`): two strategies combined:
   - **Opcode-based**: traces `ResultRow` opcodes back through `Column`/`Rowid` to find source table columns. Authoritative when the column maps to a real table column.
   - **Text-based fallback**: parses the SELECT list to get names and types when opcode tracing can't resolve (sorter pseudo-cursors, complex expressions, aggregates). Uses column aliases where available.
   - For INSERT/UPDATE/DELETE with RETURNING, `extract_returning_columns` handles the RETURNING clause.

9. **Parameter extraction** (`parameters.extract_parameters`): identifies `?` placeholders from `Variable` opcodes, infers types from comparison context (e.g., `col = ?` where `col` is INTEGER -> `Int`). Named parameters (`@name`) are discovered by the text-based parser. Deduplicates repeated parameters.

### 5. Domain model (`query.gleam`)

Defines the types that flow through the pipeline:

- **`ColumnType`**: `IntType | FloatType | StringType | BitArrayType | BoolType | TimestampType | DateType`
- **`Column`**: `name: String`, `column_type: ColumnType`, `nullable: Bool`
- **`Parameter`**: same shape as Column, representing a `?` or `@name` parameter
- **`Query`**: `name`, `sql`, `path`, `parameters`, `columns`, `custom_type_name`

Also provides helpers: `parse_sqlite_type` (SQLite type string to ColumnType), `sanitize_identifier` (kebab/snake case to Gleam), `strip_comments` (comment removal preserving string literals), `function_name` (filename to function name), `gleam_type` (ColumnType to Gleam type string).

The comment/whitespace helpers (`strip_comments`, `normalize_whitespace`) live in `query.gleam` because they're needed before any SQL analysis (validation, annotation parsing). The lower-level SQL text operations in `sqlite/parse/text.gleam` are for post-normalization use (whitespace normalization of already-stripped SQL, suffix stripping).

### 6. Code generation (`codegen.gleam`)

`generate_module_with_config()` assembles a complete Gleam module from a list of `Query` values. Build phases:

1. **Import selection**: scans all queries and parameters to determine which imports are needed (`sqlight`, `decode`, `option`, `timestamp`, `calendar`). Also adds the custom query function's import if configured.

2. **Helper emission**: conditionally adds private helper functions:
   - `timestamp_to_int()` when any parameter is `TimestampType`
   - `date_decoder()` when any column is `DateType`
   - `date_to_string()` when any parameter is `DateType`

3. **Shared row groups**: queries annotated with `-- returns: TypeName` in the same directory are grouped. All queries in a group must return identical column shapes (same names, types, nullability, order). Generates one shared row type + decoder per group.

4. **Row type generation**: for unannotated queries, generates a `TypeNameRow` custom type with labelled fields. Field names are sanitized SQL column names (snake_case -> snake_case, kebab-case -> snake_case).

5. **Function generation**:
   - **SELECT/returning queries**: generates `pub fn name(db db: Connection, param type: ..., ...) -> Result(List(RowType), sqlight.Error)` with inline decoder
   - **INSERT/UPDATE/DELETE without RETURNING**: generates `pub fn name(db db: Connection, ...) -> Result(List(Nil), sqlight.Error)` with `decode.success(Nil)`
   - **Shared-type queries**: calls the shared decoder instead of inlining

6. **Encoder/decoder mapping**: each `ColumnType` maps to a specific encoder (`sqlight.int`, `sqlight.text`, etc.) and decoder (`decode.int`, `decode.string`, etc.). Nullable columns wrap in `decode.optional`. `Timestamp` columns decode from Unix seconds; `Date` columns decode from ISO strings.

The module also handles name collision detection: generated function names and row type names are checked for duplicates within a module.

## Error handling (`error.gleam`)

`MarmotError` is a union of all error variants. `to_string()` produces pretty-printed error messages with box-drawing characters and hints. Errors flow as `Result` types through the pipeline; the CLI layer converts them to stderr output and non-zero exit codes.

## Architecture decisions

- **EXPLAIN-based inference**: types come from live SQLite introspection via `EXPLAIN` and `PRAGMA table_info`. The database must exist with the current schema at generation time.
- **Single-pass schema loading**: `get_table_metadata()` loads all table schemas, PKs, and rootpages in one pass to avoid repeated `PRAGMA` calls.
- **Tokenize once, analyze many**: the tokenizer runs once per query; its output feeds statement classification, result extraction, and parameter extraction.
- **Suffix-based nullability overrides**: `col_name!` in SQL aliases forces non-null, `col_name?` forces nullable. These are Marmot extensions stripped before EXPLAIN but preserved in tokenized form for type inference.
- **Zero external tool dependencies**: everything runs through `sqlight` (Erlang NIF).
