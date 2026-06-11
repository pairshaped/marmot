# Marmot SQLite Benchmark Report

## Summary

This benchmark compares Marmot-generated Gleam SQLite calls against a few useful
reference points:

- raw Rust `rusqlite` as a low-level SQLite baseline
- Ruby ActiveRecord as a higher-level ORM reference
- Gleam Marmot SQLite with normal measurement
- Gleam SQLite with runtime probes enabled
- Gleam Postgres through `pog`

The workload is synthetic, but app-shaped. It uses dummy data and request shapes
based on ordinary server-side pages: many small indexed reads, a parent lookup,
and a short transactional update.

The dedicated benchmark host is:

- OS: Debian GNU/Linux 13 (trixie)
- CPU: AMD Ryzen 7 9700X, 8 cores / 16 threads
- Memory: 62 GiB
- Architecture: x86_64

See the [full benchmark report](https://github.com/pairshaped/gleam_sqlite_benchmarks/blob/master/REPORT.md)
for the complete cross-runtime comparison.

## Results

These are median rows from a five-run benchmark with 10,000 simulated requests
per run.

| Runner | Case | Median Time | Median us/item | Relative Notes |
| --- | --- | ---: | ---: | --- |
| Rust `rusqlite` | `rust_rusqlite/app_request/admin_item_edit` | 591,459us | 59 | low-level SQLite baseline |
| Rust `rusqlite` | `rust_rusqlite/app_request/admin_item_update` | 284,919us | 28 | low-level SQLite baseline |
| Gleam Marmot SQLite | `gleam_marmot/app_request/admin_item_edit` | 1,239,871us | 123 | generated `sqlight` calls |
| Gleam Marmot SQLite | `gleam_marmot/app_request/admin_item_update` | 467,107us | 46 | generated `sqlight` calls |
| Gleam SQLite, probed | `probed_app_request/admin_item_edit` | 1,823,617us | 182 | BEAM runtime interference probe |
| Gleam SQLite, probed | `probed_app_request/admin_item_update` | 640,988us | 64 | BEAM runtime interference probe |
| Gleam Postgres (`pog`) | `app_request/admin_item_edit` | 7,963,548us | 796 | same request shape through local Postgres |
| Gleam Postgres (`pog`) | `batched_request/admin_item_edit` | 4,657,410us | 465 | fewer protocol round-trips |
| Gleam Postgres (`pog`) | `app_request/admin_item_update` | 17,309,572us | 1,730 | same request shape through local Postgres |
| Gleam Postgres (`pog`) | `batched_request/admin_item_update` | 16,276,321us | 1,627 | fewer protocol round-trips |
| Ruby ActiveRecord SQLite | `active_record/app_request/admin_item_edit` | 12,732,861us | 1,273 | ORM reference |
| Ruby ActiveRecord SQLite | `active_record/app_request/admin_item_update` | 4,230,022us | 423 | ORM reference |

## Methodology

Each benchmark row prints:

```text
case,items,micros,us_per_item,check
```

The benchmark uses fixed dummy seed data. The row count controls how many
simulated requests run, not how many seed rows are created.

SQLite connections use:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;
```

### `app_request/admin_item_edit`

This represents a read-heavy admin edit page. It performs many small indexed
queries, including point selects, filtered counts, small lookup reads, and one
parent lookup.

### `app_request/admin_item_update`

This represents a short save request. It opens a transaction, performs a few
request-sized reads, updates one row, and commits.

### Postgres Rows

The `pog` runner has two shapes:

- `app_request/*`: the same many-small-query request shape used by the SQLite
  cases
- `batched_request/*`: the same logical work with fewer, larger SQL statements

These Postgres rows are useful for comparing the cost of running the same
request-shaped work through `pog`, but they should not be read as ideal
Postgres application design. A Postgres-backed application would often reshape
some of this work into fewer queries, wider joins, CTEs, or server-side
aggregation instead of preserving the SQLite-oriented many-small-query shape.

The batched rows are included because Postgres often rewards fewer protocol
round-trips. They are not meant to say the `app_request/*` shape is invalid;
they show the cost of choosing that shape over a local Postgres connection.

## Probe Rows

The benchmark harness measures a probe-enabled Gleam SQLite path. During the
measured request loop, separate lightweight workers repeatedly exercise:

- BEAM scheduler heartbeat timing
- `file:sendfile/2`
- `file:read_file/1`

Those rows answer a different question from plain throughput: what happens to
other runtime and file IO work while SQLite requests are running through the
Gleam path.

The plain Marmot rows are the fair throughput comparison for generated query
code. The probed rows answer a different question: how SQLite NIF work affects
the rest of a BEAM application while similar request-shaped SQLite work is
running. They are included because that effect is easy to miss if the benchmark
only reports request throughput.
