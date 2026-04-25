use super::error::SqlError;
use super::types::{BinOp, ColumnRef, Expr, UnaryOp, Value};

/// Evaluate an expression against a row.
pub fn eval(columns: &[ColumnRef], values: &[Value], expr: &Expr) -> Result<Value, SqlError> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),

        Expr::Column(name) => resolve_column(columns, values, None, name),

        Expr::QualifiedColumn { table, column } => {
            resolve_column(columns, values, Some(table), column)
        }

        Expr::BinOp { op, left, right } => {
            let l = eval(columns, values, left)?;
            let r = eval(columns, values, right)?;
            apply_binop(*op, &l, &r)
        }

        Expr::UnaryOp { op, expr } => {
            let v = eval(columns, values, expr)?;
            apply_unaryop(*op, &v)
        }

        Expr::IsNull(e) => {
            let v = eval(columns, values, e)?;
            Ok(Value::Bool(v.is_null()))
        }

        Expr::IsNotNull(e) => {
            let v = eval(columns, values, e)?;
            Ok(Value::Bool(!v.is_null()))
        }

        Expr::Function { .. } => Err(SqlError::EvalError(
            "aggregate functions must be evaluated in aggregate context".into(),
        )),

        Expr::Star => Err(SqlError::EvalError("cannot evaluate *".into())),
    }
}

fn resolve_column(
    columns: &[ColumnRef],
    values: &[Value],
    table: Option<&String>,
    name: &str,
) -> Result<Value, SqlError> {
    let mut found = None;
    for (i, col) in columns.iter().enumerate() {
        let name_match = col.name.eq_ignore_ascii_case(name);
        let table_match = match table {
            Some(t) => col
                .table
                .as_ref()
                .map_or(false, |ct| ct.eq_ignore_ascii_case(t)),
            None => true,
        };
        if name_match && table_match {
            if found.is_some() {
                return Err(SqlError::AmbiguousColumn(name.to_string()));
            }
            found = Some(i);
        }
    }
    match found {
        Some(i) => Ok(values[i].clone()),
        None => Err(SqlError::EvalError(format!("column '{}' not found", name))),
    }
}

// ── Binary operations ────────────────────────────────────────────────

fn apply_binop(op: BinOp, left: &Value, right: &Value) -> Result<Value, SqlError> {
    // Short-circuit logic ops (three-valued)
    match op {
        BinOp::And => return eval_and(left, right),
        BinOp::Or => return eval_or(left, right),
        _ => {}
    }

    // NULL propagation for all other ops
    if left.is_null() || right.is_null() {
        return match op {
            BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq => {
                Ok(Value::Null) // comparisons with NULL yield NULL
            }
            _ => Ok(Value::Null),
        };
    }

    match op {
        BinOp::Eq => Ok(Value::Bool(left == right)),
        BinOp::NotEq => Ok(Value::Bool(left != right)),
        BinOp::Lt => Ok(Value::Bool(left < right)),
        BinOp::LtEq => Ok(Value::Bool(left <= right)),
        BinOp::Gt => Ok(Value::Bool(left > right)),
        BinOp::GtEq => Ok(Value::Bool(left >= right)),
        BinOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        BinOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        BinOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        BinOp::Div => {
            // Check for division by zero
            match right {
                Value::Int(0) => Err(SqlError::EvalError("division by zero".into())),
                Value::Real(r) if *r == 0.0 => {
                    Err(SqlError::EvalError("division by zero".into()))
                }
                _ => numeric_op(left, right, |a, b| a / b, |a, b| a / b),
            }
        }
        BinOp::Mod => match (left, right) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 {
                    Err(SqlError::EvalError("modulo by zero".into()))
                } else {
                    Ok(Value::Int(a % b))
                }
            }
            _ => Err(SqlError::EvalError("MOD requires integer operands".into())),
        },
        BinOp::And | BinOp::Or => unreachable!(),
    }
}

fn numeric_op(
    left: &Value,
    right: &Value,
    int_op: impl Fn(i64, i64) -> i64,
    real_op: impl Fn(f64, f64) -> f64,
) -> Result<Value, SqlError> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(int_op(*a, *b))),
        (Value::Real(a), Value::Real(b)) => Ok(Value::Real(real_op(*a, *b))),
        (Value::Int(a), Value::Real(b)) => Ok(Value::Real(real_op(*a as f64, *b))),
        (Value::Real(a), Value::Int(b)) => Ok(Value::Real(real_op(*a, *b as f64))),
        _ => Err(SqlError::EvalError(format!(
            "cannot perform arithmetic on {:?} and {:?}",
            left, right
        ))),
    }
}

// ── Three-valued logic ───────────────────────────────────────────────

fn eval_and(left: &Value, right: &Value) -> Result<Value, SqlError> {
    let l = to_tribool(left);
    let r = to_tribool(right);
    match (l, r) {
        (Some(false), _) | (_, Some(false)) => Ok(Value::Bool(false)),
        (Some(true), Some(true)) => Ok(Value::Bool(true)),
        _ => Ok(Value::Null),
    }
}

fn eval_or(left: &Value, right: &Value) -> Result<Value, SqlError> {
    let l = to_tribool(left);
    let r = to_tribool(right);
    match (l, r) {
        (Some(true), _) | (_, Some(true)) => Ok(Value::Bool(true)),
        (Some(false), Some(false)) => Ok(Value::Bool(false)),
        _ => Ok(Value::Null),
    }
}

fn to_tribool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::Null => None,
        Value::Int(0) => Some(false),
        Value::Int(_) => Some(true),
        _ => Some(true), // non-null, non-zero → truthy
    }
}

// ── Unary operations ─────────────────────────────────────────────────

fn apply_unaryop(op: UnaryOp, val: &Value) -> Result<Value, SqlError> {
    if val.is_null() {
        return Ok(Value::Null);
    }
    match op {
        UnaryOp::Not => match val {
            Value::Bool(b) => Ok(Value::Bool(!b)),
            _ => Err(SqlError::EvalError("NOT requires boolean operand".into())),
        },
        UnaryOp::Neg => match val {
            Value::Int(n) => Ok(Value::Int(-n)),
            Value::Real(r) => Ok(Value::Real(-r)),
            _ => Err(SqlError::EvalError("negation requires numeric operand".into())),
        },
    }
}

// ── Aggregate computation ────────────────────────────────────────────

/// Evaluate an expression that may contain aggregate function calls.
/// `group` is the set of rows in the current group.
pub fn eval_with_aggregates(
    columns: &[ColumnRef],
    group: &[&[Value]],
    expr: &Expr,
) -> Result<Value, SqlError> {
    match expr {
        Expr::Function { name, args } => compute_aggregate(columns, group, name, args),
        Expr::BinOp { op, left, right } => {
            let l = eval_with_aggregates(columns, group, left)?;
            let r = eval_with_aggregates(columns, group, right)?;
            apply_binop(*op, &l, &r)
        }
        Expr::UnaryOp { op, expr: inner } => {
            let v = eval_with_aggregates(columns, group, inner)?;
            apply_unaryop(*op, &v)
        }
        // Non-aggregate: evaluate against the first row of the group
        _ => {
            if group.is_empty() {
                return Ok(Value::Null);
            }
            eval(columns, group[0], expr)
        }
    }
}

fn compute_aggregate(
    columns: &[ColumnRef],
    group: &[&[Value]],
    name: &str,
    args: &[Expr],
) -> Result<Value, SqlError> {
    let upper = name.to_uppercase();
    match upper.as_str() {
        "COUNT" => {
            if args.len() == 1 && args[0] == Expr::Star {
                Ok(Value::Int(group.len() as i64))
            } else if args.len() == 1 {
                let count = group
                    .iter()
                    .filter(|row| {
                        eval(columns, row, &args[0])
                            .map(|v| !v.is_null())
                            .unwrap_or(false)
                    })
                    .count();
                Ok(Value::Int(count as i64))
            } else {
                Err(SqlError::EvalError("COUNT takes 1 argument".into()))
            }
        }
        "SUM" => {
            if args.len() != 1 {
                return Err(SqlError::EvalError("SUM takes 1 argument".into()));
            }
            let mut sum_int: i64 = 0;
            let mut sum_real: f64 = 0.0;
            let mut is_real = false;
            let mut has_value = false;
            for row in group {
                let v = eval(columns, row, &args[0])?;
                match v {
                    Value::Null => {}
                    Value::Int(n) => {
                        sum_int += n;
                        has_value = true;
                    }
                    Value::Real(r) => {
                        sum_real += r;
                        is_real = true;
                        has_value = true;
                    }
                    _ => {
                        return Err(SqlError::EvalError(
                            "SUM requires numeric values".into(),
                        ))
                    }
                }
            }
            if !has_value {
                Ok(Value::Null)
            } else if is_real {
                Ok(Value::Real(sum_real + sum_int as f64))
            } else {
                Ok(Value::Int(sum_int))
            }
        }
        "AVG" => {
            if args.len() != 1 {
                return Err(SqlError::EvalError("AVG takes 1 argument".into()));
            }
            let mut sum: f64 = 0.0;
            let mut count: u64 = 0;
            for row in group {
                let v = eval(columns, row, &args[0])?;
                match v {
                    Value::Null => {}
                    Value::Int(n) => {
                        sum += n as f64;
                        count += 1;
                    }
                    Value::Real(r) => {
                        sum += r;
                        count += 1;
                    }
                    _ => {
                        return Err(SqlError::EvalError(
                            "AVG requires numeric values".into(),
                        ))
                    }
                }
            }
            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Real(sum / count as f64))
            }
        }
        "MIN" => agg_minmax(columns, group, &args, false),
        "MAX" => agg_minmax(columns, group, &args, true),
        _ => Err(SqlError::EvalError(format!(
            "unknown aggregate function: {}",
            name
        ))),
    }
}

fn agg_minmax(
    columns: &[ColumnRef],
    group: &[&[Value]],
    args: &[Expr],
    is_max: bool,
) -> Result<Value, SqlError> {
    if args.len() != 1 {
        return Err(SqlError::EvalError(format!(
            "{} takes 1 argument",
            if is_max { "MAX" } else { "MIN" }
        )));
    }
    let mut result: Option<Value> = None;
    for row in group {
        let v = eval(columns, row, &args[0])?;
        if v.is_null() {
            continue;
        }
        result = Some(match result {
            None => v,
            Some(cur) => {
                if is_max {
                    if v > cur { v } else { cur }
                } else {
                    if v < cur { v } else { cur }
                }
            }
        });
    }
    Ok(result.unwrap_or(Value::Null))
}

/// Check if an expression tree contains any aggregate function calls.
pub fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, .. } => {
            let upper = name.to_uppercase();
            matches!(
                upper.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
            )
        }
        Expr::BinOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        _ => false,
    }
}
