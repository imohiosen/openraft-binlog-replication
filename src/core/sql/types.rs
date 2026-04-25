use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};

// ── SQL type system ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlType {
    Int,
    BigInt,
    Text,
    Bool,
    Real,
}

impl std::fmt::Display for SqlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int => write!(f, "INT"),
            Self::BigInt => write!(f, "BIGINT"),
            Self::Text => write!(f, "TEXT"),
            Self::Bool => write!(f, "BOOL"),
            Self::Real => write!(f, "REAL"),
        }
    }
}

// ── Value ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Real(f64),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Real(a), Value::Real(b)) => a.to_bits() == b.to_bits(),
            (Value::Int(a), Value::Real(b)) => (*a as f64).to_bits() == b.to_bits(),
            (Value::Real(a), Value::Int(b)) => a.to_bits() == (*b as f64).to_bits(),
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Bool(_), _) => Ordering::Less,
            (_, Value::Bool(_)) => Ordering::Greater,

            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Int(a), Value::Real(b)) => (*a as f64).total_cmp(b),
            (Value::Real(a), Value::Int(b)) => a.total_cmp(&(*b as f64)),
            (Value::Int(_), _) => Ordering::Less,
            (_, Value::Int(_)) => Ordering::Greater,

            (Value::Real(a), Value::Real(b)) => a.total_cmp(b),
            (Value::Real(_), _) => Ordering::Less,
            (_, Value::Real(_)) => Ordering::Greater,

            (Value::Text(a), Value::Text(b)) => a.cmp(b),
        }
    }
}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Int(v) => v.hash(state),
            Value::Text(v) => v.hash(state),
            Value::Bool(v) => v.hash(state),
            Value::Real(v) => v.to_bits().hash(state),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(v) => write!(f, "{}", v),
            Value::Text(v) => write!(f, "{}", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Real(v) => write!(f, "{}", v),
        }
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn is_truthy(&self) -> bool {
        matches!(self, Value::Bool(true))
    }
}

// ── Schema types ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Column {
    pub name: String,
    pub ty: SqlType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexDef {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<IndexDef>,
}

impl TableSchema {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    pub fn pk_columns(&self) -> Vec<(usize, &Column)> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .collect()
    }
}

pub type Row = Vec<Value>;

// ── Expressions (serializable — replicated through Raft) ─────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Expr {
    Column(String),
    QualifiedColumn { table: String, column: String },
    Literal(Value),
    BinOp { op: BinOp, left: Box<Expr>, right: Box<Expr> },
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Function { name: String, args: Vec<Expr> },
    Star,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

// ── SELECT plan (serializable for future use, but not replicated) ────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectPlan {
    pub projections: Vec<Projection>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Projection {
    Star,
    QualifiedStar(String),
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FromClause {
    Table { name: String, alias: Option<String> },
    Join {
        left: Box<FromClause>,
        right: Box<FromClause>,
        on: Expr,
        kind: JoinKind,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderByItem {
    pub expr: Expr,
    pub asc: bool,
}

// ── SQL commands (replicated through Raft) ───────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlCommand {
    CreateTable {
        schema: TableSchema,
        if_not_exists: bool,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    CreateIndex {
        def: IndexDef,
        if_not_exists: bool,
    },
    DropIndex {
        name: String,
        table: String,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        rows: Vec<Row>,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    },
    Delete {
        table: String,
        where_clause: Option<Expr>,
    },
    Truncate {
        table: String,
    },
}

impl std::fmt::Display for SqlCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateTable { schema, .. } => write!(f, "CREATE TABLE {}", schema.name),
            Self::DropTable { name, .. } => write!(f, "DROP TABLE {}", name),
            Self::CreateIndex { def, .. } => write!(f, "CREATE INDEX {}", def.name),
            Self::DropIndex { name, .. } => write!(f, "DROP INDEX {}", name),
            Self::Insert { table, rows, .. } => {
                write!(f, "INSERT INTO {} ({} rows)", table, rows.len())
            }
            Self::Update { table, .. } => write!(f, "UPDATE {}", table),
            Self::Delete { table, .. } => write!(f, "DELETE FROM {}", table),
            Self::Truncate { table } => write!(f, "TRUNCATE {}", table),
        }
    }
}

// ── SQL result (returned after apply or SELECT) ──────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SqlResult {
    Ack { rows_affected: u64 },
    Created,
    Dropped,
    Truncated,
    Rows { columns: Vec<String>, rows: Vec<Row> },
    Error(String),
}

// ── Eval context ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub name: String,
}

// ── SQL state (in-memory, serializable for snapshots) ────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SqlState {
    pub schemas: HashMap<String, TableSchema>,
    pub tables: HashMap<String, BTreeMap<u64, Row>>,
    pub sequences: HashMap<String, u64>,
    /// index_name → IndexData (Vec-based to be JSON-serializable since Value can't be a JSON key)
    pub indexes: HashMap<String, IndexData>,
}

/// Stores index entries as a sorted vec of (key_value, set_of_row_ids).
/// Can't use BTreeMap<Value, ...> because Value isn't a valid JSON map key.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexData {
    pub entries: Vec<(Value, BTreeSet<u64>)>,
}

impl IndexData {
    pub fn get(&self, key: &Value) -> Option<&BTreeSet<u64>> {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    pub fn get_mut(&mut self, key: &Value) -> Option<&mut BTreeSet<u64>> {
        self.entries
            .iter_mut()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    pub fn contains_key(&self, key: &Value) -> bool {
        self.entries.iter().any(|(k, _)| k == key)
    }

    pub fn entry_or_insert(&mut self, key: Value) -> &mut BTreeSet<u64> {
        let pos = self.entries.iter().position(|(k, _)| k == &key);
        match pos {
            Some(i) => &mut self.entries[i].1,
            None => {
                self.entries.push((key, BTreeSet::new()));
                let last = self.entries.len() - 1;
                &mut self.entries[last].1
            }
        }
    }

    pub fn remove(&mut self, key: &Value) {
        self.entries.retain(|(k, _)| k != key);
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}
