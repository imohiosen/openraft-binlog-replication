# RFC: Missing RDBMS Essentials

**Status:** Draft  
**Date:** 2026-04-26

---

## Summary

This RFC catalogs the essential RDBMS features not yet present in the SQL engine and proposes a prioritized implementation plan. Each feature is rated by impact (how often users hit the gap) and effort.

---

## 1. Query Features

### 1.1 SELECT DISTINCT

| | |
|---|---|
| **Impact** | High — extremely common in real queries |
| **Effort** | Low |
| **Current** | No deduplication support in `query_select()` |
| **Proposal** | Add a `distinct: bool` field to `SelectPlan`. After projection, deduplicate result rows with a `HashSet` before ORDER BY. |

### 1.2 CASE WHEN

| | |
|---|---|
| **Impact** | High — primary way to express conditional logic |
| **Effort** | Low–Medium |
| **Current** | No `Expr::Case` variant |
| **Proposal** | Add `Expr::Case { operand, when_clauses, else_result }`. Translate from `ast::Expr::Case`. Evaluate sequentially in `eval()`. |

### 1.3 Subqueries

| | |
|---|---|
| **Impact** | High |
| **Effort** | High |
| **Current** | Rejected in parser: `"non-SELECT query body"`, `"subquery in function"` |
| **Proposal** | Phase 1: scalar subqueries in WHERE/SELECT (execute inner `SelectPlan` against current `SqlState`). Phase 2: `EXISTS`/`NOT EXISTS`. Phase 3: correlated subqueries. |

### 1.4 Set Operations (UNION / INTERSECT / EXCEPT)

| | |
|---|---|
| **Impact** | Medium |
| **Effort** | Medium |
| **Current** | Parser rejects `SetExpr` variants other than `Select` |
| **Proposal** | Translate `SetExpr::SetOperation` into a new `QueryPlan::SetOp { left, right, op, all }`. Execute both sides, then combine/subtract rows. |

### 1.5 CTEs (WITH / WITH RECURSIVE)

| | |
|---|---|
| **Impact** | Medium |
| **Effort** | Medium–High |
| **Current** | Not handled |
| **Proposal** | Materialize non-recursive CTEs as temporary virtual tables in `SqlState` before executing the main query. Recursive CTEs via iterative fixpoint. |

---

## 2. JOIN Types

### 2.1 LEFT / RIGHT / FULL OUTER JOIN

| | |
|---|---|
| **Impact** | High — LEFT JOIN is the second most used join type |
| **Effort** | Medium |
| **Current** | `JoinKind` enum has only `Inner` |
| **Proposal** | Extend `JoinKind` with `Left`, `Right`, `FullOuter`. In `resolve_from_clause`, track unmatched rows and emit NULL-padded rows for the outer side. |

### 2.2 CROSS JOIN / NATURAL JOIN

| | |
|---|---|
| **Impact** | Low–Medium |
| **Effort** | Low |
| **Current** | Implicit cross joins work via `FROM a, b` (translated as INNER JOIN ON TRUE) |
| **Proposal** | Accept explicit `CROSS JOIN` syntax. For `NATURAL JOIN`, auto-detect shared column names and synthesize the ON clause. |

---

## 3. Data Types

### 3.1 DATE / TIME / TIMESTAMP

| | |
|---|---|
| **Impact** | High — almost every real schema has temporal columns |
| **Effort** | High |
| **Current** | Only `Int`, `BigInt`, `Text`, `Bool`, `Real` |
| **Proposal** | Add `SqlType::Date`, `SqlType::Time`, `SqlType::Timestamp`. Store internally as `Value::Date(i32)` (days since epoch), `Value::Timestamp(i64)` (micros since epoch). Add `NOW()`, `CURRENT_DATE`, date arithmetic. |

### 3.2 DECIMAL / NUMERIC

| | |
|---|---|
| **Impact** | Medium — required for financial/precise calculations |
| **Effort** | Medium |
| **Current** | Only IEEE 754 `Real` |
| **Proposal** | Add `SqlType::Decimal(precision, scale)`. Use a `rust_decimal::Decimal` or i128 fixed-point representation in `Value::Decimal`. |

### 3.3 BLOB / BYTEA

| | |
|---|---|
| **Impact** | Low–Medium |
| **Effort** | Low |
| **Current** | Not supported |
| **Proposal** | Add `SqlType::Blob`, `Value::Blob(Vec<u8>)`. Accept hex literals `X'...'`. |

### 3.4 SERIAL / AUTO_INCREMENT

| | |
|---|---|
| **Impact** | Medium — sequences exist internally but aren't user-facing |
| **Effort** | Low |
| **Current** | `self.sequences` is per-table row ID counter only |
| **Proposal** | Detect `SERIAL`/`BIGSERIAL` in parser → set column type to Int/BigInt + auto-populate from a per-column sequence on INSERT when value is omitted. |

---

## 4. Constraints & Schema

### 4.1 DEFAULT Values

| | |
|---|---|
| **Impact** | High — basic DDL expectation |
| **Effort** | Low |
| **Current** | `Column` has no `default` field; unspecified columns get NULL |
| **Proposal** | Add `default: Option<Expr>` to `Column`. In `insert()`, evaluate the default expression for any unspecified columns. Parse from `ColumnOption::Default`. |

### 4.2 CHECK Constraints

| | |
|---|---|
| **Impact** | Medium |
| **Effort** | Low–Medium |
| **Current** | Not supported |
| **Proposal** | Add `check_constraints: Vec<CheckDef>` to `TableSchema`. Evaluate each check expression against the row on INSERT/UPDATE. |

### 4.3 ALTER TABLE

| | |
|---|---|
| **Impact** | High — essential for schema evolution |
| **Effort** | Medium |
| **Current** | Not supported |
| **Proposal** | Add `SqlCommand::AlterTable` variants: `AddColumn`, `DropColumn`, `RenameColumn`, `AlterColumnType`, `AddConstraint`, `DropConstraint`. Backfill defaults for existing rows on `AddColumn`. |

### 4.4 FK CASCADE / SET NULL Actions

| | |
|---|---|
| **Impact** | Medium |
| **Effort** | Medium |
| **Current** | FK defs exist but no `on_delete`/`on_update` actions |
| **Proposal** | Add `on_delete: FkAction` and `on_update: FkAction` to `ForeignKeyDef` (Restrict, Cascade, SetNull, NoAction). Enforce during DELETE/UPDATE. |

### 4.5 Views

| | |
|---|---|
| **Impact** | Medium |
| **Effort** | Medium |
| **Current** | Not supported |
| **Proposal** | Store `views: HashMap<String, SelectPlan>` in `SqlState`. On query, expand view references in FROM into their underlying `SelectPlan`. |

---

## 5. Expressions & Functions

### 5.1 LIKE / ILIKE Pattern Matching

| | |
|---|---|
| **Impact** | High — most common text filter |
| **Effort** | Low |
| **Current** | Not handled in expression translation or evaluation |
| **Proposal** | Add `BinOp::Like` and `BinOp::ILike`. Translate `%` and `_` wildcards to a regex (or implement a simple matcher). Handle `ast::Expr::Like` in the parser. |

### 5.2 Scalar Functions

| | |
|---|---|
| **Impact** | High |
| **Effort** | Medium |
| **Current** | `Expr::Function` is only recognized for aggregates; all other functions error |
| **Proposal** | In `eval()`, before erroring on `Expr::Function`, check a scalar function registry. Implement in tiers: |

**Tier 1 (essential):**
- `COALESCE(a, b, ...)`, `NULLIF(a, b)`
- `UPPER(s)`, `LOWER(s)`, `LENGTH(s)`, `TRIM(s)`
- `ABS(n)`, `ROUND(n, d)`

**Tier 2 (common):**
- `SUBSTRING(s, start, len)`, `REPLACE(s, from, to)`, `CONCAT(a, b, ...)`
- `CEIL(n)`, `FLOOR(n)`, `POWER(n, e)`
- `CAST(expr AS type)`

**Tier 3 (nice to have):**
- `POSITION(sub IN s)`, `LEFT(s, n)`, `RIGHT(s, n)`, `LPAD`, `RPAD`
- `GREATEST(a, b, ...)`, `LEAST(a, b, ...)`

### 5.3 COUNT(DISTINCT ...)

| | |
|---|---|
| **Impact** | Medium |
| **Effort** | Low |
| **Current** | Parser notes `duplicate_treatment` but ignores it |
| **Proposal** | Propagate `distinct: bool` on `Expr::Function`. In `compute_aggregate`, collect values into a `HashSet` before counting/summing. |

---

## 6. Transactions

| | |
|---|---|
| **Impact** | Critical for production use |
| **Effort** | Very High |
| **Current** | Each statement is independently committed via Raft |
| **Proposal** | This is architecturally the hardest feature. Options: |

- **Option A — Batched commands:** Group multiple `SqlCommand`s into a single Raft log entry (`SqlCommand::Transaction(Vec<SqlCommand>)`). Apply atomically. No isolation (snapshot reads) but provides atomicity.
- **Option B — MVCC:** Add version stamps to rows, keep old versions, implement snapshot isolation. Major redesign.
- **Recommendation:** Start with Option A for atomicity. Defer full MVCC.

---

## 7. Bug Fixes

### 7.1 Composite Primary Key

| | |
|---|---|
| **Current** | `insert()` only checks `pk_cols[0]` for uniqueness, ignoring additional PK columns |
| **Location** | `engine.rs` lines ~246-247 |
| **Fix** | Build a composite key `Vec<Value>` from all PK columns and compare against existing rows. |

### 7.2 Type Validation on INSERT/UPDATE

| | |
|---|---|
| **Current** | NULL is checked, but inserting `Text` into an `Int` column is silently accepted |
| **Fix** | Add type coercion/validation in `insert()` and `update()` after building the full row. |

---

## 8. Other

| Feature | Impact | Effort | Notes |
|---|---|---|---|
| **RETURNING** clause | Medium | Low | Return inserted/updated/deleted rows |
| **UPSERT** (`ON CONFLICT`) | Medium | Medium | Needs conflict target detection |
| **EXPLAIN** | Low | Low | Dump the `SelectPlan` as text |
| **Prepared statements** | Low | Medium | Parameterized queries |
| **String concatenation (`\|\|`)** | Low | Low | Map to `BinOp::Concat` |

---

## Proposed Priority

| Phase | Features | Rationale |
|---|---|---|
| **Phase 1 — Quick wins** | DISTINCT, LIKE, DEFAULT values, CASE WHEN, composite PK fix, type validation fix | High impact, low effort |
| **Phase 2 — Core gaps** | LEFT/RIGHT JOIN, scalar functions (Tier 1), ALTER TABLE, SERIAL, COUNT(DISTINCT) | Unlocks real-world schemas |
| **Phase 3 — Power features** | Subqueries, set operations, RETURNING, UPSERT, FK cascades | Enables complex queries |
| **Phase 4 — Types & time** | DATE/TIMESTAMP, DECIMAL, temporal functions | Required for most domains |
| **Phase 5 — Transactions** | Batched atomic commands, BEGIN/COMMIT/ROLLBACK | Architectural change |
| **Phase 6 — Advanced** | CTEs, views, window functions, MVCC | Long-term roadmap |
