import gleam/dynamic/decode
import gleam/option.{Some}
import gleeunit
import sqlight

import generated/sql/aggregation_sql as aggs
import generated/sql/basic_sql as basic
import generated/sql/expressions_sql as exprs
import generated/sql/filtering_sql as filters
import generated/sql/joins_sql as joins
import generated/sql/modifiers_sql as mods
import generated/sql/nullability_overrides_sql as nulls
import generated/sql/parameters_sql as params
import generated/sql/returning_sql as ret
import generated/sql/sorting_sql as sort
import generated/sql/subqueries_ctes_sql as subs
import generated/sql/upserts_sql as upserts
import generated/sql/window_functions_sql as wins

fn exec(db: sqlight.Connection, sql: String) {
  let nil = decode.success(Nil)
  let _ = sqlight.query(sql, on: db, with: [], expecting: nil)
  Nil
}

fn setup(db: sqlight.Connection) {
  exec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, created_at INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1)")
  exec(db, "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL, body TEXT, published_at INTEGER)")
  exec(db, "CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, body TEXT NOT NULL, created_at INTEGER NOT NULL)")
  exec(db, "CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
  exec(db, "CREATE TABLE post_tags (post_id INTEGER NOT NULL, tag_id INTEGER NOT NULL, PRIMARY KEY (post_id, tag_id))")
  exec(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, price REAL NOT NULL, created_at INTEGER NOT NULL)")
  exec(db, "CREATE TABLE archived_posts (id INTEGER PRIMARY KEY, title TEXT NOT NULL, user_id INTEGER NOT NULL)")

  exec(db, "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 1714400000, 1)")
  exec(db, "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 1714486400, 1)")
  exec(db, "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 1714572800, 0)")
  exec(db, "INSERT INTO users VALUES (4, 'Diana', 'diana@example.com', 1714659200, 1)")
  exec(db, "INSERT INTO users VALUES (5, 'Eve', 'eve@example.com', 1714745600, 0)")

  exec(db, "INSERT INTO posts VALUES (1, 1, 'Getting Started with Gleam', 'Gleam is a type-safe language...', 1714500000)")
  exec(db, "INSERT INTO posts VALUES (2, 1, 'Why SQLite is Underrated', 'SQLite is fast and reliable...', 1714600000)")
  exec(db, "INSERT INTO posts VALUES (3, 2, 'Building APIs with Marmot', 'Marmot generates type-safe SQL...', NULL)")
  exec(db, "INSERT INTO posts VALUES (4, 2, 'A Guide to Functional Programming', 'Functional programming is...', 1714800000)")
  exec(db, "INSERT INTO posts VALUES (5, 3, 'Draft: Untitled', NULL, NULL)")
  exec(db, "INSERT INTO posts VALUES (6, 4, '10 Tips for Better Queries', 'Writing good SQL...', 1715000000)")

  exec(db, "INSERT INTO comments VALUES (1, 1, 2, 'Great intro!', 1714550000)")
  exec(db, "INSERT INTO comments VALUES (2, 1, 4, 'Thanks for writing this.', 1714560000)")
  exec(db, "INSERT INTO comments VALUES (3, 2, 5, 'Totally agree, SQLite is amazing.', 1714650000)")
  exec(db, "INSERT INTO comments VALUES (4, 4, 1, 'The section on pattern matching is great.', 1714850000)")
  exec(db, "INSERT INTO comments VALUES (5, 4, 3, 'Could you explain monads?', 1714900000)")

  exec(db, "INSERT INTO tags VALUES (1, 'gleam')")
  exec(db, "INSERT INTO tags VALUES (2, 'sqlite')")
  exec(db, "INSERT INTO tags VALUES (3, 'tutorial')")
  exec(db, "INSERT INTO tags VALUES (4, 'opinion')")

  exec(db, "INSERT INTO post_tags VALUES (1, 1)")
  exec(db, "INSERT INTO post_tags VALUES (1, 3)")
  exec(db, "INSERT INTO post_tags VALUES (2, 2)")
  exec(db, "INSERT INTO post_tags VALUES (2, 4)")
  exec(db, "INSERT INTO post_tags VALUES (3, 1)")
  exec(db, "INSERT INTO post_tags VALUES (3, 2)")
  exec(db, "INSERT INTO post_tags VALUES (3, 3)")
  exec(db, "INSERT INTO post_tags VALUES (4, 1)")
  exec(db, "INSERT INTO post_tags VALUES (4, 3)")

  exec(db, "INSERT INTO orders VALUES (1, 1, 29.99, 1714600000)")
  exec(db, "INSERT INTO orders VALUES (2, 1, 12.50, 1714700000)")
  exec(db, "INSERT INTO orders VALUES (3, 2, 45.00, 1714800000)")
  exec(db, "INSERT INTO orders VALUES (4, 4, 8.75, 1714900000)")
  exec(db, "INSERT INTO orders VALUES (5, 4, 22.00, 1715000000)")

  Nil
}

fn with_setup(f: fn(sqlight.Connection) -> Nil) {
  let result =
    sqlight.with_connection(":memory:", fn(db: sqlight.Connection) {
      setup(db)
      f(db)
      Ok(Nil)
    })
  let assert Ok(_) = result
}

pub fn main() {
  gleeunit.main()
}

pub fn basic_test() {
  with_setup(fn(db) {
    let assert Ok(_) = basic.select_all(db:)
    let assert Ok(_) = basic.select_by_id(db:, id: 1)
    let assert Ok(_) = basic.select_with_anon_param(db:, name: "Alice")
    let assert Ok(_) = basic.insert_user(db:, name: "Test", email: "t@t.com", created_at: 0)
    let assert Ok(_) = basic.update_user_email(db:, email: "updated@t.com", id: 1)
    let assert Ok(_) = basic.delete_user(db:, id: 5)
    let assert Ok(_) = basic.replace_user(db:, id: 1, name: "Alice", email: "alice@example.com", created_at: 1714400000)
    Nil
  })
}

pub fn joins_test() {
  with_setup(fn(db) {
    let assert Ok(_) = joins.inner_join(db:)
    let assert Ok(_) = joins.left_join(db:)
    let assert Ok(_) = joins.right_join(db:)
    let assert Ok(_) = joins.cross_join(db:)
    let assert Ok(_) = joins.join_using(db:)
    let assert Ok(_) = joins.natural_join(db:)
    let assert Ok(_) = joins.multiple_joins(db:)
    let assert Ok(_) = joins.left_join_where_reduces_nullability(db:, comment_id: 1)
    Nil
  })
}

pub fn filtering_test() {
  with_setup(fn(db) {
    let assert Ok(_) = filters.where_comparison(db:, since: Some(1714500000), until: Some(1715000000), exclude_user_id: 3)
    let assert Ok(_) = filters.where_like(db:, domain_pattern: "%@example.com", exclude_pattern: "ZZZ%")
    let assert Ok(_) = filters.where_is_null(db:)
    let assert Ok(_) = filters.where_in_list(db:, user_ids: "1,2,3")
    let assert Ok(_) = filters.where_in_subquery(db:, active: 1)
    let assert Ok(_) = filters.where_between(db:, from: Some(1714500000), to: Some(1715000000))
    let assert Ok(_) = filters.where_and_or(db:, since: Some(1714500000), user_id: 1)
    let assert Ok(_) = filters.where_not_operators(db:, pattern: "ZZZ%")
    Nil
  })
}

pub fn aggregation_test() {
  with_setup(fn(db) {
    let assert Ok(_) = aggs.count_group_by(db:)
    let assert Ok(_) = aggs.having(db:, min_posts: "0")
    let assert Ok(_) = aggs.sum_avg_min_max(db:)
    let assert Ok(_) = aggs.count_distinct(db:)
    let assert Ok(_) = aggs.filter_clause(db:)
    let assert Ok(_) = aggs.group_by_with_where(db:)
    Nil
  })
}

pub fn expressions_test() {
  with_setup(fn(db) {
    let assert Ok(_) = exprs.cast(db:)
    let assert Ok(_) = exprs.coalesce(db:)
    let assert Ok(_) = exprs.case_searched(db:, cutoff: Some(1714500000))
    let assert Ok(_) = exprs.case_simple(db:)
    let assert Ok(_) = exprs.case_nested(db:)
    let assert Ok(_) = exprs.arithmetic(db:)
    let assert Ok(_) = exprs.concat(db:)
    Nil
  })
}

pub fn sorting_test() {
  with_setup(fn(db) {
    let assert Ok(_) = sort.order_by(db:)
    let assert Ok(_) = sort.order_by_multiple(db:)
    let assert Ok(_) = sort.limit_offset(db:, limit: 2, offset: 0)
    let assert Ok(_) = sort.limit_only(db:, param: 3)
    Nil
  })
}

pub fn subqueries_ctes_test() {
  with_setup(fn(db) {
    let assert Ok(_) = subs.subquery_in_where(db:, active: 1)
    let assert Ok(_) = subs.subquery_in_select(db:)
    let assert Ok(_) = subs.exists_subquery(db:)
    let assert Ok(_) = subs.not_in_subquery(db:)
    let assert Ok(_) = subs.cte_simple(db:)
    let assert Ok(_) = subs.cte_recursive(db:)
    Nil
  })
}

pub fn window_functions_test() {
  with_setup(fn(db) {
    let assert Ok(_) = wins.row_number(db:)
    let assert Ok(_) = wins.rank_dense_rank(db:)
    let assert Ok(_) = wins.row_number_partition(db:)
    let assert Ok(_) = wins.ntile(db:)
    Nil
  })
}

pub fn parameters_test() {
  with_setup(fn(db) {
    let assert Ok(_) = params.anonymous_params(db:, name: "Alice")
    let assert Ok(_) = params.named_params(db:, user_id: 1, since: Some(0), pattern: "%")
    let assert Ok(_) = params.repeated_named_param(db:, pattern: "%")
    let assert Ok(_) = params.limit_param(db:, limit: 2)
    Nil
  })
}

pub fn modifiers_test() {
  with_setup(fn(db) {
    let assert Ok(_) = mods.distinct(db:)
    let assert Ok(_) = mods.distinct_multiple(db:)
    let assert Ok(_) = mods.union(db:, since: 0)
    let assert Ok(_) = mods.union_all(db:, user_id: 1)
    let assert Ok(_) = mods.intersect(db:)
    let assert Ok(_) = mods.except(db:)
    let assert Ok(_) = mods.compound_subquery(db:, since: 0)
    Nil
  })
}

pub fn nullability_overrides_test() {
  with_setup(fn(db) {
    let assert Ok(_) = nulls.force_non_null(db:)
    let assert Ok(_) = nulls.force_nullable(db:)
    Nil
  })
}

pub fn returning_test() {
  with_setup(fn(db) {
    let assert Ok(_) = ret.insert_returning(db:, name: "Frank", email: "frank@example.com", created_at: 1715000000)
    let assert Ok(_) = ret.insert_returning_star(db:, name: "Grace", email: "grace@example.com", created_at: 1715000000)
    let assert Ok(_) = ret.update_returning(db:, active: 0, id: 1)
    let assert Ok(_) = ret.delete_returning(db:)
    let assert Ok(_) = ret.insert_from_select_returning(db:, cutoff: Some(1715000000))
    Nil
  })
}

pub fn upserts_test() {
  with_setup(fn(db) {
    let assert Ok(_) = upserts.insert_or_replace(db:, id: 1, name: "Alice", email: "alice@example.com", created_at: 1714400000)
    let assert Ok(_) = upserts.insert_or_ignore(db:, post_id: 1, tag_id: 1)
    let assert Ok(_) = upserts.on_conflict_do_nothing(db:, id: 1, name: "Alice", email: "alice@example.com", created_at: 1714400000)
    let assert Ok(_) = upserts.on_conflict_do_update(db:, id: 1, name: "Alice", email: "alice@example.com", created_at: 1714400000)
    Nil
  })
}
