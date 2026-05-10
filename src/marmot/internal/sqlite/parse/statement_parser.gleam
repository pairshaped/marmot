//// Statement skeleton parser. Consumes a token stream from `tokenize.gleam`
//// and produces a typed `Statement` value carrying parsed structure for
//// boundaries that need it (FROM aliases, INSERT shape, CTEs) and clause
//// token slices for bodies that don't.
////
//// What this module does NOT parse:
////  - Expression internals (CAST, COALESCE, CASE, arithmetic, etc.)
////  - Subquery scoping (correlated references and inner-scope shadowing)
////  - USING-join column merging
////
//// All three are intentional. Expression-type inference stays incremental,
//// subquery scoping is left to the existing fallback path, and USING merging
//// is reported as an ambiguous bare reference in v1.

import gleam/option.{type Option, None}
import marmot/internal/error.{type MarmotError}
import marmot/internal/sqlite/tokenize.{type Token}

pub type Identifier {
  Identifier(text: String, quoted: Bool)
}

pub type TableRef {
  TableRef(schema: Option(Identifier), name: Identifier)
}

pub type TableBinding {
  TableBinding(table: TableRef, alias: Option(String))
}

pub type FromItem {
  FromItem(binding: TableBinding, on: Option(List(Token)))
}

pub type CteDef {
  CteDef(name: String, columns: List(String), body: List(Token))
}

pub type SelectBody {
  SelectBody(
    is_distinct: Bool,
    select_list: List(Token),
    from: List(FromItem),
    where: Option(List(Token)),
    group_by: Option(List(Token)),
    having: Option(List(Token)),
    order_by: Option(List(Token)),
    limit: Option(List(Token)),
  )
}

pub type SelectStmt {
  SelectStmt(ctes: List(CteDef), body: SelectBody)
}

pub type InsertConflictAction {
  ConflictAbort
  ConflictReplace
  ConflictIgnore
  ConflictFail
  ConflictRollback
}

pub type InsertSource {
  ValuesSource(raw: List(Token), rows: List(List(List(Token))))
  SelectSource(SelectStmt)
  DefaultValuesSource
}

pub type InsertStmt {
  InsertStmt(
    conflict_action: InsertConflictAction,
    target: TableBinding,
    column_list: Option(List(String)),
    source: InsertSource,
    upsert: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type UpdateStmt {
  UpdateStmt(
    target: TableBinding,
    set: List(Token),
    from: List(FromItem),
    where: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type DeleteStmt {
  DeleteStmt(
    target: TableBinding,
    where: Option(List(Token)),
    returning: Option(List(Token)),
  )
}

pub type Statement {
  Select(SelectStmt)
  Insert(InsertStmt)
  Update(UpdateStmt)
  Delete(DeleteStmt)
  Unsupported(tokens: List(Token))
}

/// Parse a token stream into a typed Statement. Returns `Unsupported(tokens)`
/// for SQL outside the supported skeleton (CREATE, PRAGMA, ATTACH, etc.).
/// MarmotError is reserved for parse failures that the agent has positively
/// identified, e.g. INSERT VALUES row-count mismatch.
pub fn parse(tokens: List(Token)) -> Result(Statement, MarmotError) {
  Ok(Unsupported(tokens))
}
