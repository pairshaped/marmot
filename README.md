![Marmot](https://github.com/pairshaped/marmot/blob/master/marmot.png?raw=true)

# Marmot - Type-Safe SQL for SQLite in Gleam

[![Package Version](https://img.shields.io/hexpm/v/marmot)](https://hex.pm/packages/marmot)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/marmot/)

**Disclaimer: This library was developed with help from code assistance agents. Code-assisted pull requests are welcome, so long as they aren't objectively dumb.**

Marmot is a code generator for Gleam that turns plain `.sql` files into type-safe
functions for SQLite. Write your queries in SQL, point Marmot at your database,
and it generates the Gleam functions, row types, and decoders you need. No ORM,
no query builder, no manual decoder boilerplate. Heavily inspired by
[Squirrel](https://github.com/giacomocavalieri/squirrel) (which does the same
for Postgres).

> If you are an LLM, see [llms.txt](https://raw.githubusercontent.com/pairshaped/marmot/refs/heads/master/llms.txt) for a condensed context document.

## Should you use SQLite from Gleam?

For a production app where database access is on the hot path, we don't
recommend it in the current state of the BEAM's SQLite driver. We moved our own
SQLite-backed application from Gleam to Rust for these reasons. They come from
`esqlite`, the driver underneath `sqlight`, not from Gleam or Marmot:

- **Every query hops threads.** `esqlite` runs each SQLite call on one of the
  BEAM's dirty I/O scheduler threads, so every query moves the calling process
  to another OS thread and back. On a small VM, those handoffs make queries
  slow and the timings inconsistent from run to run.
- **Lock waits can stall the node.** SQLite's busy timeout sleeps on the dirty
  I/O thread running the query. Once enough connections wait on a lock to
  occupy every dirty I/O thread, the connection holding the lock can't commit,
  and queries fail when their timeouts expire.
- **Releases are tied to the build machine.** `esqlite` compiles SQLite as a C
  NIF at build time, so a release has to be built for the target's operating
  system, CPU architecture and C library. You can't build one release and copy
  it to any server the way you can with a static binary.
- **The bundled SQLite is built for size.** `esqlite` compiles its copy of
  SQLite with `-Os` rather than optimizing for speed.

The [SQLite request-shape benchmark](https://github.com/pairshaped/gleam_sqlite_benchmarks)
measures the first two against C, Rust, Zig, Go and Bun on the same workload.

If you use it anyway, cache statements (Marmot does this by default, see
[Statement caching](#statement-caching)), send writes through one connection,
size the dirty I/O schedulers as described in
[Dirty I/O schedulers](#dirty-io-schedulers), and build releases on a machine
that matches production.

## Why Marmot?

If you need to talk to a SQLite database in Gleam, you'll have to write something
like this:

```gleam
import gleam/dynamic/decode
import sqlight

pub type FindUserRow {
  FindUserRow(name: String, email: String)
}

pub fn find_user(db: sqlight.Connection, username: String) {
  let decoder = {
    use name <- decode.field(0, decode.string)
    use email <- decode.field(1, decode.string)
    decode.success(FindUserRow(name:, email:))
  }

  sqlight.query(
    "select name, email from users where username = ?",
    on: db,
    with: [sqlight.text(username)],
    expecting: decoder,
  )
}
```

This is fine for a small project but gets hairy when you have a lot of queries:

- Embedding SQL in strings means no syntax highlighting, no formatting, and
  no way to run queries on their own with external tools.
- You have to keep the decoder in sync with the query output manually.

Instead of hiding SQL behind an ORM, Marmot _embraces it._ Write your queries
in plain `*.sql` files and Marmot generates the corresponding functions.

Instead of the hand-written example above, write the following query:

```sql
-- we're in file `src/users/sql/find_user.sql`
select name, email
from users
where username = ?
```

Run `gleam run -m marmot`. You now have a type-safe
function `find_user` you can use as expected:

```gleam
import sqlight
import generated/sql/users_sql

pub fn main() {
  use db <- sqlight.with_connection("app.sqlite")
  let assert Ok([user]) = users_sql.find_user(db: db, username: "alice")
  // user.name, user.email are fully typed
}
```

Generated functions use labelled arguments
(`users_sql.find_user(db: db, username: "alice")`) so call sites are
self-documenting.

- Marmot keeps encoders and decoders in sync with the query output for you.
- Type safety is preserved: Marmot connects to your SQLite database and uses
  SQLite schema metadata plus `EXPLAIN` to understand query types.
- Each query is a standalone `*.sql` file, so you can `explain` or lint it
  independently.
- No external tools required. No `sqlc` binary, no `sqlite3` CLI. Marmot uses
  `sqlight` directly.

## Usage

Add Marmot as a dev dependency:

```sh
gleam add marmot --dev
```

Add `sqlight` as a runtime dependency (the generated code calls it):

```sh
gleam add sqlight
```

Generate code by running the `marmot` module:

```sh
gleam run -m marmot
```

As long as you follow a couple of conventions, Marmot will work out of the box:

- Marmot looks for all `*.sql` files in any `sql` directory under your
  project's `src` directory.
- Each `sql` directory becomes a single Gleam module containing a function
  for each `*.sql` file inside it. Generated modules are placed in
  `src/generated/sql/` by default, with a `_sql` suffix to avoid import
  aliasing conflicts.
- Each `*.sql` file _must contain a single SQL query._ The filename becomes
  the generated function name.

> Imagine you have a Gleam project that looks like this
>
> ```txt
> src
> ├── users
> │   └── sql
> │       ├── find_user.sql
> │       └── list_users.sql
> ├── users.gleam
> ├── generated
> │   └── sql
> │       └── users_sql.gleam   -- generated by marmot
> └── app.gleam
> ```
>
> Running `gleam run -m marmot` generates `src/generated/sql/users_sql.gleam`
> with two functions `find_user` and `list_users` you can then import and use:
>
> ```gleam
> import generated/sql/users_sql
>
> users_sql.find_user(db: db, id: 1)
> ```

### Talking to the database

In order to understand the types of your queries, Marmot needs to open the
SQLite database file where your schema is defined. Marmot reads the database
path with the following precedence:

1. `--database` CLI flag
2. `DATABASE_URL` environment variable
3. `database` field in `[tools.marmot]` section of `gleam.toml`

Pass `--database` when you run Marmot:

```sh
gleam run -m marmot -- --database dev.sqlite
```

Or configure `gleam.toml`:

```toml
[tools.marmot]
database = "dev.sqlite"
```

Marmot also falls back to `DATABASE_URL` for single-database projects when no
CLI database path is provided.

Then run Marmot:

```sh
gleam run -m marmot
```

If no database is configured, Marmot shows an error message listing the
available options.

#### Initializing the codegen connection

If Marmot needs extra setup before it can inspect your queries, configure
`init_sql` with a path to a SQL file:

```toml
[tools.marmot]
database = "dev.sqlite"
init_sql = "db/marmot_init.sql"
```

Marmot runs that file on the database connection it opens for generation,
before reading schema metadata or running `EXPLAIN`.

Warning: this is an escape hatch. Marmot does not sandbox the SQL, wrap it in a
rollback-only transaction, or check whether it changes schema or data. If the
file says `DROP TABLE`, Marmot will ask SQLite to do exactly that.

Use `init_sql` for setup that belongs to Marmot's introspection connection:
`ATTACH`, temporary tables, PRAGMAs, or other statements your queries need at
codegen time. Do not treat it as a migration system. It only runs during Marmot
generation, and it does not run when your application starts.

Also note that `init_sql` does not magically enable native SQLite extension
loading. If your SQL driver does not expose extension loading, a statement such
as `SELECT load_extension(...)` can still fail.

### Multiple Databases

For projects with more than one SQLite database, define named database
references in `gleam.toml`. Do not combine this form with
`[tools.marmot].database`.

```toml
[[tools.marmot.databases]]
name = "app"

[[tools.marmot.databases]]
name = "analytics"
```

For `name = "app"`, Marmot derives these defaults:

- SQLite database: `db/app.sqlite`
- Migrations: `db/migrations/app`
- Seeds: `db/seeds/app`
- SQL files: `src/sql/app`
- Generated modules: `src/generated/sql/app`

You can override any derived path:

```toml
[[tools.marmot.databases]]
name = "analytics"
path = "priv/analytics.sqlite"
migrations_dir = "priv/analytics/migrations"
seeds_dir = "priv/analytics/seeds"
sql_dir = "src/analytics_queries"
init_sql = "priv/analytics/marmot_init.sql"
output = "src/analytics/generated"
```

Named databases inherit `[tools.marmot].init_sql` when it is set. Set
`init_sql` on a named database to override it for that database.

Run commands against every named reference:

```sh
gleam run -m marmot
gleam run -m marmot migrate
gleam run -m marmot seed
gleam run -m marmot reset
```

Pick one named database when you only want one:

```sh
gleam run -m marmot -- --database-name analytics
gleam run -m marmot migrate --database-name analytics
gleam run -m marmot seed --database-name analytics
gleam run -m marmot reset --database-name analytics
```

The existing `database = "dev.sqlite"` form is still supported for
single-database projects and keeps the existing default paths.

To print CLI usage:

```sh
gleam run -m marmot help
```

### Migrations

Marmot includes an opinionated, forward-only take on SQLite migrations. It is
generic Marmot functionality, separate from code generation and any web
framework. Use it if you want a small migration runner with simple conventions.
If you want down migrations, branching migration graphs, checksums, or a larger
migration system, bring your own migration library.

Migration files live in `db/migrations/` by default:

```txt
db/migrations/
```

Files must use this naming shape:

```txt
NNN_description.sql
```

For example:

```txt
001_create_users.sql
002_add_email_to_users.sql
003_create_sessions.sql
```

Run migrations with:

```sh
gleam run -m marmot migrate --database dev.sqlite
```

You can also select the database with `--database-name`, `DATABASE_URL`, or
`[tools.marmot].database`, using the same precedence as code generation.
To use another migration directory, set `migrations_dir` in `gleam.toml`:

```toml
[tools.marmot]
database = "db/app.sqlite"
migrations_dir = "db/migrations/app"
```

The runner:

- Reads the configured migration directory in filename sort order.
- Uses the filename stem as the version, such as `001_create_users`.
- Creates `schema_migrations(version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)`
  if needed.
- Applies only versions that are missing from `schema_migrations`.
- Runs each file in a transaction and records the version after the SQL succeeds.
- Stops on the first failure.

Running the command twice is safe. The second run skips migrations already
recorded in `schema_migrations`.

The filename shape is fixed. Marmot does not support down migrations or
branching migration graphs.

### Seeds

Seed files live in `db/seeds/` by default:

```txt
db/seeds/
```

They use the same filename shape:

```txt
NNN_description.sql
```

Run seeds with:

```sh
gleam run -m marmot seed --database dev.sqlite
```

To use another seed directory, set `seeds_dir` in `gleam.toml`:

```toml
[tools.marmot]
database = "db/app.sqlite"
seeds_dir = "db/seeds/app"
```

The seed runner:

- Reads the configured seed directory in filename sort order.
- Runs every seed file it finds.
- Runs each file in a transaction.
- Stops on the first failure.

Marmot does not track seed files. Running `seed` twice runs the same files
twice. Seed SQL should be written with that in mind if repeat runs matter for
your project.

### Reset

Reset deletes the configured SQLite database file, then runs migrations and
seeds:

```sh
gleam run -m marmot reset --database dev.sqlite
```

`reset` uses the same migration and seed directory config as `migrate` and
`seed`.

It also removes SQLite sidecar files for that path (`-wal`, `-shm`, and
`-journal`) when they exist. Reset does not compile or inspect your application
code.

### Configuring the output directory

By default, generated modules are placed in `src/generated/sql/`. If you
prefer a different location, set it in `gleam.toml` or via CLI flag:

```toml
[tools.marmot]
output = "src/server/generated/sql"
```

```sh
gleam run -m marmot -- --output src/server/generated/sql
```

The output directory must be under `src/` (Gleam compiles modules from there).

### Custom SQL directory (`sql_dir`)

By default, Marmot finds `.sql` files by walking `src/` for directories named `sql`.
If your project uses a different convention, point `sql_dir` at a specific directory
and Marmot will find all directories under it that contain `.sql` files:

```toml
[tools.marmot]
sql_dir = "src/queries"
```

With this config, Marmot recursively finds all directories under `src/queries/`
that contain `.sql` files. This is useful when your SQL files live outside `sql/`
directories or when you want to limit generation to a specific subtree.

### Generated transaction helper

Marmot writes a small transaction helper beside the generated query modules:

```txt
src/generated/sql/transaction.gleam
```

For a named database with `output = "src/generated/sql/app"`, the helper is
written to that output directory.

Import the generated module when a workflow spans multiple SQL statements:

```gleam
import generated/sql/transaction
import generated/sql/users_sql
import gleam/result

pub fn rename_user(db, id, name) {
  transaction.savepoint(db: db, name: "rename_user", run: fn() {
    use _ <- result.try(users_sql.update_user_name(db: db, id: id, name: name))
    use _ <- result.try(users_sql.insert_audit_log(db: db, user_id: id))
    Ok(Nil)
  })
}
```

`savepoint` uses SQLite `SAVEPOINT`, `ROLLBACK TO`, and `RELEASE`, so it can
nest inside another transaction. The helper is generated into your project:
app runtime code imports generated code, not Marmot.

### Statement caching

Generated query functions reuse compiled SQLite statements. `sqlight.query`
compiles the SQL again on every call. Marmot instead writes a `statement_cache`
module beside `transaction.gleam`, plus a small Erlang file next to it, and the
generated functions call `statement_cache.query`. It has the same signature as
`sqlight.query` and returns the same rows and decode errors. One difference:
when the database is busy, `sqlight.query` crashes inside `esqlite`, while
`statement_cache.query` returns an `Error` with the `Busy` code. Function
signatures, row types and decoders do not change. Nothing in your app depends on Marmot at
runtime, only on `sqlight` (the Erlang file calls `esqlite`, which `sqlight`
already pulls in). On the JavaScript target `statement_cache.query` falls back
to `sqlight.query`.

**What is cached.** One compiled statement for each connection and SQL text.
The SQL strings are fixed in the generated code, so the cache never holds more
than one statement per generated query per connection, and it never evicts.
That is deliberate. A small fixed-size cache (rusqlite keeps 16 statements by
default, Bun keeps 20) thrashes when a request cycles through more statements
than it holds, and ends up slower than no cache at all. This cache is sized by
your queries, so it can't thrash.

**Where it lives.** In the process dictionary of the process that runs the
query. A process only ever uses its own statements, so a statement is never
stepped by two callers at once, and the statements are released when the
process ends. In practice:

- A long-lived process keeps the cache warm: an actor or pool worker that owns
  the connection, a background job, a script, a CLI. Each query is compiled once.
- A process-per-request web server (Mist, Wisp) starts every request with an
  empty cache. It only helps queries that run more than once inside one request.
  A request that runs each query once gets no benefit; a query repeated inside
  the request does.

**Getting the benefit.** Keep the connection, and the queries that use it, in a
long-lived process. Don't open a connection or run queries in each short-lived
request process. Opening a connection per request (for example
`sqlight.with_connection` in every handler) throws the cache away with the
connection, because compiled statements belong to one connection. If you route
database work through a single process that owns the connection, remember that
requests then wait on that process. Measure with your own queries before you
restructure an app for this.

**Why it matters.** Compiling a statement can cost more than running a small
indexed query, so a request made of many small queries spends much of its time
compiling when nothing is cached. How much you gain depends on how expensive
your queries are to compile and how often a process repeats them. The
[SQLite request-shape benchmark](https://github.com/pairshaped/gleam_sqlite_benchmarks) measures statement caching across
languages, including why a cache too small for the request is worse than none.

**Connection lifetime.** SQLite keeps a connection's database file open while
compiled statements for it are alive, even after `sqlight.close` returns `Ok`.
A WAL database is not checkpointed or cleaned up until the statements go away.
Marmot guards against this:

- A query on a closed connection never runs a cached statement. It drops that
  process's statements for the connection and then fails the same way
  `sqlight.query` fails on a closed connection.
- The first query on a new connection drops that process's cached statements
  for any connection that has been closed since.
- `statement_cache.close(db: db)` drops the process's statements and then closes
  the connection. Use it instead of `sqlight.close` in a long-lived process.
  `statement_cache.forget(db: db)` drops them without closing.
- When a process ends, all of its statements go with it. A short-lived process
  that uses `sqlight.with_connection` needs nothing extra.

These calls only clear the calling process's cache. If several processes used
the same connection, each one releases its own statements when it calls
`forget`, queries the closed connection, opens another connection, or ends.

Always close connections explicitly in a long-lived process. A cached statement
keeps its connection alive, so a connection you drop without closing is not
garbage collected as it would be with `sqlight.query`. It stays open until the
process ends. Cleanup also can't see a close that happens elsewhere at the same
instant: if another process closes a shared connection between a query's
open-check and its statement, that one call can still run on the closing
connection (a window of microseconds).

**Schema changes.** SQLite recompiles a cached statement by itself after a schema
change, with one gap: `esqlite` fixes a statement's result column count when it
first compiles it. After `ALTER TABLE ... ADD COLUMN`, a cached `SELECT *` or
`RETURNING *` keeps returning the old columns until the process calls `forget`
or restarts. Generated decoders read columns by position and your generated
code changes with the schema anyway, so restarting the app after a migration
covers this.

### Dirty I/O schedulers

If your app sends all writes through one connection, set `+SDio` to the number
of CPUs on machines with fewer than 10. Read the lock-wait warning below before
you do. This matters for every app that uses `sqlight`, with or without Marmot,
and on a small VM it can matter more than statement caching.

`esqlite` runs SQLite calls, including every `step`, on the BEAM's dirty I/O
schedulers. Each query moves the calling process to a dirty I/O thread and back.
The BEAM starts 10 dirty I/O threads no matter how many CPUs the machine has.
On a small VM, those extra threads fight over the cores, and waking one up for
each handoff is slow, so every query pays for it.

On a 2-vCPU VM, the default made reads several times slower and inconsistent
from run to run with no other change. On one such VM, capping the threads at
the vCPU count made every run fast; on another, with a newer CPU, it didn't
help. Measure on your own hardware before relying on it. The [SQLite request-shape benchmark](https://github.com/pairshaped/gleam_sqlite_benchmarks) has the
measurements: its README explains the setting and its per-machine reports have
the numbers.

Set it in `vm.args` for a release, or with `ERL_FLAGS` when running directly:

```sh
ERL_FLAGS="+SDio 2" gleam run
```

**Lock waits hold dirty I/O threads.** When a connection waits for a SQLite lock,
the busy timeout sleeps inside `sqlite3_step`, on the dirty I/O thread running
it. If as many connections are waiting as there are dirty I/O threads, every
thread is asleep, and the connection holding the lock can't get a thread to
run its `COMMIT`. Everything stalls until the busy timeouts expire, and those
queries fail with `Busy`. The benchmark reproduced this with 4 writer
connections and `+SDio 2`, and with 12 writers on the default 10 threads.

So keep the number of connections that can wait on a lock below the number of
dirty I/O threads:

- Send writes through one connection, for example a single writer process that
  owns it and queues requests. Readers in WAL mode don't wait on writers, so
  only that one connection ever waits on the write lock, and capping `+SDio` at
  the CPU count is safe.
- If many connections write, don't lower `+SDio`, and keep the number of writer
  connections below it. The default of 10 already stalls with more than 10
  writers waiting at once.

Don't go below the CPU count to chase single-request speed. `+SDio 1` measured
the same as `+SDio 2` in a one-request-at-a-time benchmark, but it funnels every
SQLite call and every file operation on the node through one thread, so
concurrent requests queue behind each other, and any lock wait stalls the whole
node. On machines with 10 or more CPUs the default already fits. As with any
scheduler setting, confirm it under your own concurrent load.

### Custom query wrapper (`query_function`)

By default the generated code calls `statement_cache.query`. If you want to
add logging, timing, or other instrumentation without forking Marmot, you can
point `query_function` at your own wrapper:

```toml
[tools.marmot]
database = "dev.sqlite"
query_function = "app/db.query"
```

With that config, the generated code imports your module and calls your
function instead:

```gleam
// Without query_function (default):
import generated/sql/statement_cache
import sqlight

pub fn find_user(db db: sqlight.Connection, username username: String) {
  statement_cache.query(
    "select name, email from users where username = ?",
    on: db,
    with: [sqlight.text(username)],
    expecting: decoder,
  )
}

// With query_function = "app/db.query":
import app/db
import sqlight

pub fn find_user(db db: sqlight.Connection, username username: String) {
  db.query(
    "select name, email from users where username = ?",
    on: db,
    with: [sqlight.text(username)],
    expecting: decoder,
  )
}
```

A `query_function` replaces the statement cache. Generated code no longer calls
`statement_cache`, so your wrapper decides whether statements are reused. A
wrapper that delegates to `sqlight.query` compiles the SQL on every call, as
before. Marmot still writes the `statement_cache` module when `query_function`
is set, so a wrapper can keep the default caching and add its own behavior by
delegating to `statement_cache.query`; pass it only fixed SQL strings like the
generated code does, because it never evicts, so dynamic SQL would be cached
forever. A wrapper can also run its own cache, for
example in a process that owns the connection. It only has to run the SQL and
return decoded rows.

Your wrapper must match `sqlight.query`'s labelled signature exactly:

```gleam
import generated/sql/statement_cache
import gleam/dynamic/decode
import sqlight

pub fn query(
  sql: String,
  on connection: sqlight.Connection,
  with parameters: List(sqlight.Value),
  expecting decoder: decode.Decoder(a),
) -> Result(List(a), sqlight.Error) {
  // Your logging / timing / instrumentation here, then delegate. Use
  // sqlight.query instead of statement_cache.query to skip statement reuse.
  // Only pass fixed SQL to statement_cache.query: it never evicts.
  statement_cache.query(sql, on: connection, with: parameters, expecting: decoder)
}
```

## Shared return types

When multiple queries in the same `sql/` directory return the same shape, you
can annotate them to share a single Row type and decoder:

```sql
-- src/app/sql/get_org.sql
-- returns: OrgRow
SELECT id, name FROM orgs WHERE id = @id
```

```sql
-- src/app/sql/list_orgs.sql
-- returns: OrgRow
SELECT id, name FROM orgs
```

Both queries now share one `pub type OrgRow` and one decoder function in the
generated module. This eliminates the need for adapter boilerplate when multiple
queries return the same columns.

**Rules:**
- Annotation must appear before the first SQL statement
- Type name must end in `Row` and be valid PascalCase (`OrgRow`, `UserProfileRow`)
- All queries with the same annotation must return the exact same columns (names,
  types, nullability, order). Mismatch is a generation-time error.
- Scope is per-directory: `OrgRow` in `admin/sql/` is distinct from `OrgRow` in
  `public/sql/`
- Unannotated queries keep their per-query Row type (backwards compatible)

## Supported types

Marmot maps SQLite column types to Gleam types. The types that are currently
supported are:

| SQLite declared type       | Gleam type                                                                                                        | Notes                       |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------- | --------------------------- |
| `INTEGER`, `INT`           | `Int`                                                                                                             |                             |
| `REAL`, `FLOAT`, `DOUBLE`  | `Float`                                                                                                           |                             |
| `TEXT`, `VARCHAR`, `CHAR`   | `String`                                                                                                          |                             |
| `BLOB`                     | `BitArray`                                                                                                        |                             |
| `BOOLEAN`, `BOOL`          | `Bool`                                                                                                            | Stored as `0`/`1`           |
| `TIMESTAMP`, `DATETIME`    | [`timestamp.Timestamp`](https://hexdocs.pm/gleam_time/gleam/time/timestamp.html#Timestamp)                        | Stored as Unix seconds      |
| `DATE`                     | [`calendar.Date`](https://hexdocs.pm/gleam_time/gleam/time/calendar.html#Date)                                    | Stored as ISO 8601 text     |
| nullable column            | `Option(T)`                                                                                                       |                             |

### Parameter nullability

Marmot treats read parameters and write parameters differently.

| SQL pattern | Meaning | Generated parameter |
| ----------- | ------- | ------------------- |
| `WHERE account_id = @account_id` against a nullable column | Read filter. Callers pass a concrete value. | `account_id: Int` |
| `WHERE season IS @season` against a nullable integer column | Read filter using SQL's NULL-aware comparison. Marmot still follows the read rule. | `season: Int` |
| `WHERE @from_date IS NULL OR created_at >= @from_date` | Optional-filter idiom in SQL. Marmot does not infer optional read parameters from this shape. | Required parameter, with the type Marmot can infer from the predicate |
| `INSERT INTO tasks (deleted_at) VALUES (@deleted_at)` where `deleted_at` is nullable | Write position. SQLite accepts NULL for this column. | `deleted_at: Option(Int)` |
| `UPDATE tasks SET deleted_at = @deleted_at` where `deleted_at` is nullable | Write position. SQLite accepts NULL for this column. | `deleted_at: Option(Int)` |

This mirrors Squirrel's surface for reads: a parameter in a read predicate is
required unless Marmot adds an explicit override syntax in a future release.
For writes, Marmot follows the schema because the database really accepts or
rejects NULL at that position. `INTEGER PRIMARY KEY` rowid aliases are nullable
on INSERT writes because binding NULL asks SQLite to assign the rowid.

## Known Limitations

### Expression inference gaps

Marmot's type inference for expressions is incremental. Some shapes resolve
to `StringType` when Marmot can't trace the underlying column.

- **Complex result expressions and CTE result columns.** `COALESCE`, `CASE`,
  and columns coming out of CTEs may resolve to `StringType` rather than the
  underlying column's type. Use `CAST(... AS TYPE)` in the SQL or alias with
  `!`/`?` for nullability.
- **Parameters inside `CASE` expressions.** Marmot can preserve the parameter
  name but may miss the column type evidence. For example, a parameter used
  inside `CASE WHEN @rank = 0 THEN status_rank ELSE @rank END = status_rank`
  may fall back to `StringType`. Prefer a simpler predicate shape or an
  explicit `CAST(@rank AS INTEGER)`.

### Limited scope of alias resolution

Alias-aware column resolution applies to the current parsed statement scope.

- **Subquery scoping.** Parameters inside subqueries use a fallback
  inference path that doesn't fully model nested scopes. Many common shapes
  are supported, including SELECT-list subqueries and derived tables, but
  deeply nested or ambiguous subqueries may need explicit qualification or
  `CAST` to get the right type.
- **`USING` joins.** `JOIN x USING (col)` is supported, but bare `col`
  references in `WHERE` may be reported as ambiguous. Workaround: use
  `ON` syntax or qualify the column.

### Design decisions, not bugs

These work by choice. We could change them, but it would be a design shift.

- **Runtime helpers are generated into the app.** Shared helpers such as
  transactions and the statement cache live beside generated query modules, so
  application runtime code imports generated code instead of depending on
  Marmot.
- **The statement cache is per process.** It is simple and can't hand one
  statement to two callers, but it only stays warm in long-lived processes. See
  [Statement caching](#statement-caching). A cache shared across processes
  would need a long-lived owner process and bookkeeping to release statements
  when a connection closes, so Marmot doesn't do that for you.
- **`TIMESTAMP` and `DATETIME` columns are stored as Unix seconds (integer).**
  Sub-second precision (nanoseconds) is not preserved. We picked integer
  seconds for simplicity and interop with `strftime('%s', ...)`. Preserving
  nanos would need a new storage format or type mapping.

### Hard limits

Not Marmot's to fix.

- **Repeated anonymous `?` placeholders** that refer to the same value
  generate a separate function argument for each occurrence
  (`WHERE org_id = ? AND ... WHERE org_id = ?` produces `org_id` and
  `org_id_2`). This is a SQLite protocol limitation: anonymous `?` are
  always distinct bind slots. **Use named parameters (`@name` or `:name`)
  instead.** SQLite deduplicates them natively, so `WHERE org_id = @org_id
  AND ... WHERE org_id = @org_id` generates a single `org_id` argument.
  Named parameters are also self-documenting and generally preferable.
- **`WHERE id IN (?)` with a dynamic list is not supported.** SQLite has no
  native array parameter type, so there is no way to bind a list to a single
  `?`. Workarounds:
  - Write separate queries for known list sizes
  - Use `json_each(?)` with a JSON array string:
    `WHERE id IN (SELECT value FROM json_each(?))`
  - Build the query string dynamically in your application code (outside Marmot)

## Differences from Squirrel

Marmot mirrors [Squirrel's](https://github.com/giacomocavalieri/squirrel)
ergonomics but targets SQLite instead of Postgres. The surface conventions
(SQL file layout, code generation, formatted output, and explicit generated
return types) are deliberately similar so switching between the two feels
familiar. The type-inference engine underneath had to be built from scratch
because Postgres and SQLite expose type information in fundamentally
incompatible ways.

**Where Marmot diverges on the surface (might change over time):**

- **Output directory**: Marmot defaults to `src/generated/sql/` with a `_sql`
  filename suffix (e.g., `users_sql.gleam`). Squirrel places `sql.gleam` as a
  sibling of the `sql/` directory.
- **Named parameters**: Marmot supports `@name`, `:name`, and `$name`
  placeholders natively. Squirrel uses Postgres `$1` positional parameters and
  infers parameter names where it can, falling back to names like `arg_1` and
  `arg_2`.
- **Shared row annotations**: Marmot supports optional `-- returns: TypeName`
  comments for reusing one row type across multiple queries. Squirrel does not
  have an equivalent SQL comment annotation.

**Where it diverges under the hood:**

- Postgres' wire protocol returns full type info for parameters and result
  columns when a statement is prepared. SQLite doesn't.
- SQLite columns have *affinity*, not types. A `TEXT` column can store an
  `INTEGER`, and the engine never commits to a type for a result slot.
- Result columns have to be traced through `EXPLAIN` opcodes
  (`OpenRead` / `Column` / `Rowid` / `ResultRow`) back to physical table columns,
  then joined against SQLite schema metadata for the declared type.
- Nullability has to be derived from joins, subqueries, and `COALESCE` usage
  instead of reported by the engine. Marmot tracks nullable-cursor sets
  through the opcode trace.
- Computed columns (`COUNT`, `CAST`, `COALESCE`, `CASE`, `SUM`, window
  functions) carry no type information and need a shape-based inference
  fallback that pattern-matches on expression text.
- Parameter types are reverse-engineered from usage sites (`WHERE col = ?`,
  `LIMIT ?`, `CAST(? AS TEXT)`) instead of reported.
- Repeated `@name` parameters need a dedup/unification pass to pick a single
  inferred type across occurrences.
- SQLite-specific quirks (`WITHOUT ROWID`, `STRICT` tables, `INTEGER PRIMARY
  KEY` aliasing `rowid`, affinity rules) have no Postgres analogue and
  required their own handling.

## License

[MIT](https://github.com/pairshaped/marmot/blob/master/LICENSE)
