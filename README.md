# Marmot - Type-Safe SQL for SQLite in Gleam

[![Package Version](https://img.shields.io/hexpm/v/marmot)](https://hex.pm/packages/marmot)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/marmot/)

Marmot is a code generator for Gleam that turns plain `.sql` files into type-safe
functions for SQLite. Write your queries in SQL, point Marmot at your database,
and it generates the Gleam functions, row types, and decoders you need. No ORM,
no query builder, no manual decoder boilerplate. Inspired by
[Squirrel](https://github.com/giacomocavalieri/squirrel) (which does the same
for Postgres).

> If you are an LLM, see [llms.txt](https://raw.githubusercontent.com/pairshaped/marmot/refs/heads/master/llms.txt) for a condensed context document.

## What's Marmot?

If you need to talk to a SQLite database in Gleam you'll have to write something
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

This is probably fine if you have a few small queries but it can become quite
the burden when you have a lot of queries:

- The SQL query you write is just a plain string, you do not get syntax
  highlighting, auto formatting, suggestions... all the little niceties you
  would otherwise get if you were writing a plain `*.sql` file.
- This also means you lose the ability to run these queries on their own with
  other external tools, inspect them and so on.
- You have to manually keep in sync the decoder with the query's output.

One might be tempted to hide all of this by reaching for something like an ORM.
Marmot proposes a different approach: instead of trying to hide the SQL it
_embraces it and leaves you in control._
You write the SQL queries in plain old `*.sql` files and Marmot will take care
of generating all the corresponding functions.

A code snippet is worth a thousand words, so let's have a look at an example.
Instead of the hand written example shown earlier you can instead just write the
following query:

```sql
-- we're in file `src/my_app/sql/find_user.sql`
select name, email
from users
where username = ?
```

And run `gleam run -m marmot`. Just like magic you'll now have a type-safe
function `find_user` you can use just as you'd expect:

```gleam
import sqlight
import my_app/sql

pub fn main() {
  use db <- sqlight.with_connection("my_app.sqlite")
  let assert Ok([user]) = sql.find_user(db: db, username: "alice")
  // user.name, user.email are fully typed
}
```

Behind the scenes Marmot generates the decoders and functions you need; and
it's pretty-printed, standard Gleam code (actually it's exactly like the hand
written example I showed you earlier)! Generated functions use labelled
arguments (`sql.find_user(db: db, username: "alice")`) so call sites are
self-documenting.
So now you get the best of both worlds:

- You don't have to take care of keeping encoders and decoders in sync, Marmot
  does that for you.
- And you're not compromising on type safety either: Marmot connects to your
  SQLite database and uses `PRAGMA table_info` and `EXPLAIN` to understand the
  types of your queries.
- You can stick to writing plain SQL in `*.sql` files. You'll have better
  editor support, syntax highlighting and completions.
- You can run each query on its own: need to `explain` a query?
  No big deal, it's just a plain old `*.sql` file.
- No external tools required. No `sqlc` binary, no `sqlite3` CLI. Marmot uses
  `sqlight` directly.

## Usage

First you'll need to add Marmot to your project as a dev dependency:

```sh
gleam add marmot --dev
```

You'll also need `sqlight` as a runtime dependency (the generated code calls it):

```sh
gleam add sqlight
```

Then you can ask it to generate code running the `marmot` module:

```sh
gleam run -m marmot
```

And that's it! As long as you follow a couple of conventions Marmot will just
work:

- Marmot will look for all `*.sql` files in any `sql` directory under your
  project's `src` directory.
- Each `sql` directory will be turned into a single Gleam module containing a
  function for each `*.sql` file inside it. The generated Gleam module is going
  to be located in the same directory as the corresponding `sql` directory and
  its name is `sql.gleam`.
- Each `*.sql` file _must contain a single SQL query._ And the name of the file
  is going to be the name of the corresponding Gleam function to run that query.

> Let's make an example. Imagine you have a Gleam project that looks like this
>
> ```txt
> ├── src
> │   ├── my_app
> │   │   └── sql
> │   │       ├── find_user.sql
> │   │       └── list_users.sql
> │   └── my_app.gleam
> └── test
>     └── my_app_test.gleam
> ```
>
> Running `gleam run -m marmot` will create a `src/my_app/sql.gleam` file
> defining two functions `find_user` and `list_users` you can then
> import and use in your code.

### CLI Commands

- `gleam run -m marmot`: Generates type-safe Gleam code for all SQL queries
  found in `src/**/sql/*.sql`.
- `gleam run -m marmot check`: Validates that the generated Gleam code is
  up-to-date with the SQL queries. This is particularly useful to run in a CI
  pipeline to make sure you don't forget to run `gleam run -m marmot`.

### Talking to the database

In order to understand the types of your queries, Marmot needs to open the
SQLite database file where your schema is defined. Marmot reads the database
path with the following precedence:

1. `DATABASE_URL` environment variable
2. `--database` CLI flag
3. `database` field in `[marmot]` section of `gleam.toml`

```sh
# Environment variable
DATABASE_URL=dev.sqlite gleam run -m marmot

# CLI flag
gleam run -m marmot -- --database dev.sqlite

# gleam.toml
# [marmot]
# database = "dev.sqlite"
```

If no database is configured, Marmot will show a helpful error message listing
all three options.

### Configuring the output directory

By default, the generated `sql.gleam` file is placed as a sibling of the `sql/`
directory:

```txt
src/
├── app/
│   ├── users/
│   │   ├── sql/
│   │   │   ├── find_user.sql
│   │   │   └── list_users.sql
│   │   └── sql.gleam          -- generated
│   └── orders/
│       ├── sql/
│       │   ├── create_order.sql
│       │   └── list_orders.sql
│       └── sql.gleam          -- generated
```

You can override this with an output directory:

```toml
[marmot]
database = "dev.sqlite"
output = "src/generated/sql"
```

Or via CLI flag:

```sh
gleam run -m marmot -- --output src/generated/sql
```

The output directory must be under `src/` (Gleam compiles modules from there).

When you have multiple `sql/` directories, Marmot automatically preserves the
directory structure relative to the longest common path shared between `output`
and each `sql/` directory. For example, given this config:

```toml
[marmot]
database = "dev.sqlite"
output = "src/generated/sql"
```

Marmot generates:

```txt
src/
├── app/
│   ├── users/
│   │   └── sql/
│   │       ├── find_user.sql
│   │       └── list_users.sql
│   └── orders/
│       └── sql/
│           ├── create_order.sql
│           └── list_orders.sql
├── generated/
│   └── sql/
│       └── app/
│           ├── users.gleam    -- generated
│           └── orders.gleam   -- generated
```

The common path between `src/generated/sql` and `src/app/users/sql` is `src/`,
so the relative path `app/users/sql` is used, the trailing `sql` is stripped,
and the result is `src/generated/sql/app/users.gleam`.

Nested entity paths are preserved, so `src/app/admin/orders/sql/` would
generate `src/generated/sql/app/admin/orders.gleam`.

### Custom query wrapper (`query_function`)

By default the generated code calls `sqlight.query` directly. If you want to
add logging, timing, or other instrumentation without forking Marmot, you can
point `query_function` at your own wrapper:

```toml
[marmot]
database = "dev.sqlite"
query_function = "app/db.query"
```

With that config, the generated code imports your module and calls your
function instead of `sqlight.query`:

```gleam
// Without query_function (default):
import sqlight

pub fn find_user(db db: sqlight.Connection, username username: String) {
  sqlight.query(
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

Your wrapper must match `sqlight.query`'s labelled signature exactly:

```gleam
import gleam/dynamic/decode
import sqlight

pub fn query(
  sql: String,
  on connection: sqlight.Connection,
  with parameters: List(sqlight.Value),
  expecting decoder: decode.Decoder(a),
) -> Result(List(a), sqlight.Error) {
  // Your logging / timing / instrumentation here, then delegate:
  sqlight.query(sql, on: connection, with: parameters, expecting: decoder)
}
```

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

## FAQ

### What flavour of SQL does Marmot support?

Marmot only supports SQLite.

### Why not use Squirrel?

[Squirrel](https://github.com/giacomocavalieri/squirrel) is excellent and you
should use it if you use Postgres. Marmot exists because Squirrel doesn't
support SQLite, and SQLite introspection works fundamentally differently
(PRAGMAs and EXPLAIN instead of the Postgres wire protocol).

### How does Marmot infer types?

Marmot uses `PRAGMA table_info` for column types and nullability, and SQLite's
`EXPLAIN` to trace query result columns and parameters back to their source
table columns. This is a heuristic approach that works well for straightforward
queries.

### Why isn't Marmot configurable in any way?

Following Squirrel's lead, Marmot leans heavily on convention over
configuration. Small projects should just work with zero config. The opt-in
knobs Marmot does expose -- `output` and `query_function` -- are there for
larger codebases where the default conventions become awkward (scattered
`sql/` directories, custom logging/instrumentation around every query). They
don't change the generated code's shape, just where it lands and which
function it calls.

## Known Limitations

- Table names containing SQL keywords (`RETURNING`, `INTO`) may confuse the SQL
  parser. Use simple table names.
- `INSERT INTO t VALUES (?, ?)` without an explicit column list will not infer
  parameter names or types correctly. Always specify columns:
  `INSERT INTO t (col1, col2) VALUES (?, ?)`.
- Complex expressions (subqueries, CTEs, `COALESCE`) may not have their types
  inferred. Use `CAST(... AS TYPE)` to help Marmot.
- `TIMESTAMP` and `DATETIME` columns are stored as Unix seconds (integer).
  Sub-second precision (nanoseconds) is not preserved.
- `DATE` columns are stored as ISO 8601 text (`YYYY-MM-DD`). Malformed date
  strings in the database will produce a decode error.
- Repeated anonymous `?` placeholders that refer to the same value generate
  a separate function argument for each occurrence
  (`WHERE org_id = ? AND ... WHERE org_id = ?` produces `org_id` and `org_id_2`).
  This is a SQLite protocol limitation: anonymous `?` are always distinct
  bind slots. **Use named parameters (`@name` or `:name`) instead** -- SQLite
  deduplicates them natively, so `WHERE org_id = @org_id AND ... WHERE
  org_id = @org_id` generates a single `org_id` argument. Named parameters
  are also self-documenting and generally preferable.

## Credits

Marmot's design, conventions, and approach are directly inspired by
[Squirrel](https://github.com/giacomocavalieri/squirrel) by
[Giacomo Cavalieri](https://github.com/giacomocavalieri).
Squirrel targets Postgres with beautiful ergonomics. Marmot brings that same
experience to SQLite. In fact, Marmot aims to be a 1:1 mirror of Squirrel's
syntax and conventions, so switching between the two should feel seamless.

If you use Postgres, use Squirrel. If you use SQLite, use Marmot.

## Contributing

If you think there's any way to improve this package, or if you spot a bug don't
be afraid to open PRs, issues or requests of any kind! Any contribution is
welcome.
