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

These are median results from five-run benchmarks. Each timing value is the
average request time from a 10,000-request benchmark run.

### `app_request/admin_item_edit`

| Runner | Time (us/item) | vs `rusqlite` | req/sec |
| --- | ---: | ---: | ---: |
| `rusqlite` | 59 | 1.0x | 16,907 |
| Gleam Marmot | 123 | 2.1x | 8,065 |
| Gleam SQLite, probed | 182 | 3.1x | 5,484 |
| Gleam Postgres (`pog`) | 796 | 13.5x | 1,256 |
| Gleam Postgres (`pog`) batched | 465 | 7.9x | 2,147 |
| Gleam Postgres (`pog`), probed | 1,243 | 21.0x | 804 |
| Gleam Postgres (`pog`) batched, probed | 589 | 10.0x | 1,695 |
| Ruby ActiveRecord SQLite | 1,273 | 21.5x | 785 |

### `app_request/admin_item_update`

| Runner | Time (us/item) | vs `rusqlite` | req/sec |
| --- | ---: | ---: | ---: |
| `rusqlite` | 28 | 1.0x | 35,098 |
| Gleam Marmot | 46 | 1.6x | 21,408 |
| Gleam SQLite, probed | 64 | 2.2x | 15,601 |
| Gleam Postgres (`pog`) | 1,730 | 60.8x | 578 |
| Gleam Postgres (`pog`) batched | 1,627 | 57.1x | 614 |
| Gleam Postgres (`pog`), probed | 1,795 | 63.0x | 557 |
| Gleam Postgres (`pog`) batched, probed | 1,663 | 58.4x | 601 |
| Ruby ActiveRecord SQLite | 423 | 14.8x | 2,364 |

## Probe Impact

The probe rows measure the same request shape while separate BEAM workers
exercise scheduler timing, `file:sendfile/2`, and `file:read_file/1`.

### `admin_item_edit`

| Runner | Shape | Time (us/item) | Probed (us/item) | Slowdown |
| --- | --- | ---: | ---: | ---: |
| Gleam SQLite | `app_request` | 113 | 182 | 1.6x |
| Gleam Postgres (`pog`) | `app_request` | 796 | 1,243 | 1.6x |
| Gleam Postgres (`pog`) | `batched_request` | 465 | 589 | 1.3x |

### `admin_item_update`

| Runner | Shape | Time (us/item) | Probed (us/item) | Slowdown |
| --- | --- | ---: | ---: | ---: |
| Gleam SQLite | `app_request` | 45 | 64 | 1.4x |
| Gleam Postgres (`pog`) | `app_request` | 1,730 | 1,795 | 1.0x |
| Gleam Postgres (`pog`) | `batched_request` | 1,627 | 1,663 | 1.0x |

The SQLite probe impact is real, but it is not catastrophic in this workload.
The Postgres read-heavy shape shows similar sensitivity to the probes, while the
Postgres update shape barely moves because the transaction path is already much
slower than the probe pressure.

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

The benchmark harness measures probe-enabled Gleam SQLite and Gleam Postgres
paths. During the measured request loop, separate lightweight workers repeatedly
exercise:

- BEAM scheduler heartbeat timing
- `file:sendfile/2`
- `file:read_file/1`

Those rows answer a different question from plain throughput: what happens to
other runtime and file IO work while database requests are running through the
Gleam path.

The plain Marmot rows are the fair throughput comparison for generated query
code. The SQLite probe rows show how SQLite NIF work affects the rest of a BEAM
application while request-shaped SQLite work is running. The Postgres probe rows
provide a useful comparison point for similar BEAM-side request work that does
not run SQLite through a NIF. They are included because runtime interference is
easy to miss if the benchmark only reports request throughput.
