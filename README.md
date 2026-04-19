# marmot

Type-safe SQL for SQLite in Gleam.

Marmot reads your `.sql` files, connects to your SQLite database to infer types, and generates ready-to-call Gleam functions. No ORM, no query builder, no boilerplate.

Heavily inspired by [Squirrel](https://github.com/giacomocavalieri/squirrel) by Giacomo Cavalieri, which does the same for Postgres. Marmot brings that experience to SQLite.

## Quick Start

Add marmot as a dev dependency:

```sh
gleam add marmot --dev
```

Add sqlight as a runtime dependency:

```sh
gleam add sqlight
```

Create a SQL file at `src/your_app/sql/find_user.sql`:

```sql
SELECT id, username, email
FROM users
WHERE username = ?
```

Run marmot (pointing at your SQLite database):

```sh
DATABASE_URL=dev.sqlite gleam run -m marmot
```

This generates `src/your_app/sql.gleam` with typed functions you can call directly:

```gleam
import your_app/sql

let assert Ok([user]) = sql.find_user(db, "alice")
io.println(user.email)
```

## Configuration

All configuration is optional. By default, marmot follows Squirrel's conventions.

### gleam.toml

```toml
[marmot]
database = "dev.sqlite"
output = "src/my_app/generated"
```

### CLI flags

```sh
gleam run -m marmot -- --database dev.sqlite --output src/my_app/generated
```

### Environment variables

```sh
DATABASE_URL=dev.sqlite gleam run -m marmot
```

Precedence: environment variable > CLI flag > gleam.toml.

## Conventions

- One SQL query per `.sql` file
- Filename becomes the function name (`find_user.sql` -> `find_user()`)
- Place `.sql` files in `sql/` directories under `src/`
- Parameters use `?` placeholders

## Type Mappings

| SQLite type | Gleam type |
|---|---|
| `INTEGER` | `Int` |
| `REAL` | `Float` |
| `TEXT` | `String` |
| `BLOB` | `BitArray` |
| `BOOLEAN` | `Bool` |
| `TIMESTAMP` / `DATETIME` | `timestamp.Timestamp` (Unix seconds) |
| `DATE` | `calendar.Date` (ISO 8601 text) |
| nullable columns | `Option(T)` |

## CI

Use check mode to verify generated code is up to date:

```sh
gleam run -m marmot check
```

Exits with code 0 if everything is current, code 1 if regeneration is needed.

## Known Limitations

- Table names containing SQL keywords (`RETURNING`, `INTO`) may confuse the SQL parser. Use simple table names.
- `INSERT INTO t VALUES (?, ?)` without an explicit column list will not infer parameter names or types correctly. Always specify columns: `INSERT INTO t (col1, col2) VALUES (?, ?)`.

## Credits

Marmot's design, conventions, and approach are directly inspired by [Squirrel](https://github.com/giacomocavalieri/squirrel) by [Giacomo Cavalieri](https://github.com/giacomocavalieri). Squirrel targets Postgres with beautiful ergonomics — marmot brings that same experience to SQLite.

If you use Postgres, use Squirrel. If you use SQLite, use marmot.
