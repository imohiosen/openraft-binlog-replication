use std::fmt;

#[derive(Debug, Clone)]
pub enum SqlError {
    TableNotFound(String),
    TableAlreadyExists(String),
    IndexNotFound(String),
    IndexAlreadyExists(String),
    ColumnNotFound { table: String, column: String },
    TypeMismatch { column: String, expected: String, got: String },
    NullViolation(String),
    UniqueViolation { index: String, value: String },
    DuplicatePrimaryKey { table: String, value: String },
    ColumnCountMismatch { expected: usize, got: usize },
    Unsupported(String),
    ParseError(String),
    EvalError(String),
    AmbiguousColumn(String),
    NoTable,
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TableNotFound(t) => write!(f, "table '{}' not found", t),
            Self::TableAlreadyExists(t) => write!(f, "table '{}' already exists", t),
            Self::IndexNotFound(i) => write!(f, "index '{}' not found", i),
            Self::IndexAlreadyExists(i) => write!(f, "index '{}' already exists", i),
            Self::ColumnNotFound { table, column } => {
                write!(f, "column '{}' not found in table '{}'", column, table)
            }
            Self::TypeMismatch {
                column,
                expected,
                got,
            } => write!(
                f,
                "type mismatch for '{}': expected {}, got {}",
                column, expected, got
            ),
            Self::NullViolation(col) => write!(f, "NOT NULL violation for column '{}'", col),
            Self::UniqueViolation { index, value } => write!(
                f,
                "unique constraint violation on index '{}' for value {}",
                index, value
            ),
            Self::DuplicatePrimaryKey { table, value } => {
                write!(f, "duplicate primary key in table '{}': {}", table, value)
            }
            Self::ColumnCountMismatch { expected, got } => {
                write!(f, "column count mismatch: expected {}, got {}", expected, got)
            }
            Self::Unsupported(msg) => write!(f, "unsupported: {}", msg),
            Self::ParseError(msg) => write!(f, "parse error: {}", msg),
            Self::EvalError(msg) => write!(f, "evaluation error: {}", msg),
            Self::AmbiguousColumn(col) => write!(f, "ambiguous column reference: '{}'", col),
            Self::NoTable => write!(f, "no table specified"),
        }
    }
}

impl std::error::Error for SqlError {}
