use sqlparser::ast::{self, SetExpr, Statement, TableFactor, SelectItem, GroupByExpr};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::core::sql::error::SqlError;
use crate::core::sql::types::*;

/// Parsed SQL: either a mutating command (goes through Raft) or a read-only query.
pub enum ParsedStatement {
    Command(SqlCommand),
    Query(SelectPlan),
    Catalog(CatalogQuery),
}

/// Parse one or more SQL statements.
pub fn parse_sql(sql: &str) -> Result<Vec<ParsedStatement>, SqlError> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::ParseError(e.to_string()))?;
    stmts.into_iter().map(translate_statement).collect()
}

fn translate_statement(stmt: Statement) -> Result<ParsedStatement, SqlError> {
    match stmt {
        Statement::CreateTable(ct) => {
            let name = object_name_to_string(&ct.name);
            let mut columns = Vec::new();
            let mut pk_columns: Vec<String> = Vec::new();

            let mut unique_constraints: Vec<UniqueConstraintDef> = Vec::new();
            let mut foreign_keys: Vec<ForeignKeyDef> = Vec::new();

            // Extract constraints from table-level constraints
            for constraint in &ct.constraints {
                match constraint {
                    ast::TableConstraint::PrimaryKey { columns: cols, .. } => {
                        for col in cols {
                            pk_columns.push(col.value.clone());
                        }
                    }
                    ast::TableConstraint::Unique { columns: cols, name, .. } => {
                        unique_constraints.push(UniqueConstraintDef {
                            name: name.as_ref().map(|n| n.value.clone()),
                            columns: cols.iter().map(|c| c.value.clone()).collect(),
                        });
                    }
                    ast::TableConstraint::ForeignKey { columns: cols, foreign_table, referred_columns, name, .. } => {
                        foreign_keys.push(ForeignKeyDef {
                            name: name.as_ref().map(|n| n.value.clone()),
                            columns: cols.iter().map(|c| c.value.clone()).collect(),
                            ref_table: object_name_to_string(foreign_table),
                            ref_columns: referred_columns.iter().map(|c| c.value.clone()).collect(),
                        });
                    }
                    _ => {}
                }
            }

            for col_def in &ct.columns {
                let col_name = col_def.name.value.clone();
                let ty = translate_data_type(&col_def.data_type)?;
                let mut nullable = true;
                let mut is_pk = pk_columns.iter().any(|c| c.eq_ignore_ascii_case(&col_name));

                for opt in &col_def.options {
                    match &opt.option {
                        ast::ColumnOption::NotNull => nullable = false,
                        ast::ColumnOption::Null => nullable = true,
                        ast::ColumnOption::Unique { is_primary, .. } => {
                            if *is_primary {
                                is_pk = true;
                                nullable = false;
                            } else {
                                // Column-level UNIQUE constraint
                                unique_constraints.push(UniqueConstraintDef {
                                    name: None,
                                    columns: vec![col_name.clone()],
                                });
                            }
                        }
                        ast::ColumnOption::ForeignKey { foreign_table, referred_columns, .. } => {
                            foreign_keys.push(ForeignKeyDef {
                                name: None,
                                columns: vec![col_name.clone()],
                                ref_table: object_name_to_string(foreign_table),
                                ref_columns: referred_columns.iter().map(|c| c.value.clone()).collect(),
                            });
                        }
                        _ => {}
                    }
                }

                if is_pk {
                    nullable = false;
                }

                columns.push(Column {
                    name: col_name,
                    ty,
                    nullable,
                    primary_key: is_pk,
                });
            }

            let schema = TableSchema {
                name,
                columns,
                indexes: vec![],
                unique_constraints,
                foreign_keys,
            };
            Ok(ParsedStatement::Command(SqlCommand::CreateTable {
                schema,
                if_not_exists: ct.if_not_exists,
            }))
        }

        Statement::Drop { object_type, names, if_exists, .. } => {
            match object_type {
                ast::ObjectType::Table => {
                    if names.len() != 1 {
                        return Err(SqlError::Unsupported("multi-table DROP".into()));
                    }
                    let name = object_name_to_string(&names[0]);
                    Ok(ParsedStatement::Command(SqlCommand::DropTable {
                        name,
                        if_exists,
                    }))
                }
                ast::ObjectType::Index => {
                    if names.len() != 1 {
                        return Err(SqlError::Unsupported("multi-index DROP".into()));
                    }
                    let parts = &names[0].0;
                    let (table, idx_name) = if parts.len() == 2 {
                        (parts[0].value.clone(), parts[1].value.clone())
                    } else if parts.len() == 1 {
                        // DROP INDEX idx_name — table must be inferred at execute time
                        // For now, require table.index_name syntax
                        return Err(SqlError::Unsupported(
                            "DROP INDEX requires table.index_name syntax".into(),
                        ));
                    } else {
                        return Err(SqlError::ParseError("invalid index name".into()));
                    };
                    Ok(ParsedStatement::Command(SqlCommand::DropIndex {
                        name: idx_name,
                        table,
                    }))
                }
                _ => Err(SqlError::Unsupported(format!("DROP {:?}", object_type))),
            }
        }

        Statement::CreateIndex(ci) => {
            let idx_name = ci.name
                .map(|n| object_name_to_string(&n))
                .unwrap_or_else(|| "unnamed_index".to_string());
            let table = object_name_to_string(&ci.table_name);
            let columns: Vec<String> = ci.columns
                .iter()
                .map(|c| c.expr.to_string())
                .collect();
            let unique = ci.unique;
            let if_not_exists = ci.if_not_exists;

            Ok(ParsedStatement::Command(SqlCommand::CreateIndex {
                def: IndexDef {
                    name: idx_name,
                    table,
                    columns,
                    unique,
                },
                if_not_exists,
            }))
        }

        Statement::Insert(insert) => {
            let table = object_name_to_string(&insert.table_name);
            let columns = if insert.columns.is_empty() {
                None
            } else {
                Some(insert.columns.iter().map(|c| c.value.clone()).collect())
            };

            let body = insert.source.as_ref().ok_or_else(|| {
                SqlError::ParseError("INSERT requires VALUES".into())
            })?;
            let rows = extract_insert_rows(body)?;

            Ok(ParsedStatement::Command(SqlCommand::Insert {
                table,
                columns,
                rows,
            }))
        }

        Statement::Update { table, assignments, selection, .. } => {
            let table_name = match &table.relation {
                TableFactor::Table { name, .. } => object_name_to_string(name),
                _ => return Err(SqlError::Unsupported("complex UPDATE target".into())),
            };

            let assigns: Vec<(String, Expr)> = assignments
                .iter()
                .map(|a| {
                    let col_name = assignment_target_to_string(&a.target);
                    let expr = translate_expr(&a.value)?;
                    Ok((col_name, expr))
                })
                .collect::<Result<_, SqlError>>()?;

            let where_clause = selection.map(|e| translate_expr(&e)).transpose()?;

            Ok(ParsedStatement::Command(SqlCommand::Update {
                table: table_name,
                assignments: assigns,
                where_clause,
            }))
        }

        Statement::Delete(delete) => {
            let table_name = match &delete.from {
                ast::FromTable::WithFromKeyword(tables) => {
                    if tables.is_empty() {
                        return Err(SqlError::ParseError("DELETE requires FROM".into()));
                    }
                    match &tables[0].relation {
                        TableFactor::Table { name, .. } => object_name_to_string(name),
                        _ => return Err(SqlError::Unsupported("complex DELETE target".into())),
                    }
                }
                ast::FromTable::WithoutKeyword(tables) => {
                    if tables.is_empty() {
                        return Err(SqlError::ParseError("DELETE requires FROM".into()));
                    }
                    match &tables[0].relation {
                        TableFactor::Table { name, .. } => object_name_to_string(name),
                        _ => return Err(SqlError::Unsupported("complex DELETE target".into())),
                    }
                }
            };

            let where_clause = delete.selection.map(|e| translate_expr(&e)).transpose()?;

            Ok(ParsedStatement::Command(SqlCommand::Delete {
                table: table_name,
                where_clause,
            }))
        }

        Statement::Truncate { table_names, .. } => {
            if table_names.is_empty() {
                return Err(SqlError::ParseError("TRUNCATE requires table name".into()));
            }
            let name = object_name_to_string(&table_names[0].name);
            Ok(ParsedStatement::Command(SqlCommand::Truncate { table: name }))
        }

        Statement::Query(query) => {
            let plan = translate_query(*query)?;
            Ok(ParsedStatement::Query(plan))
        }

        Statement::ShowTables { .. } => {
            Ok(ParsedStatement::Catalog(CatalogQuery::ShowTables))
        }

        Statement::ShowDatabases { .. } => {
            Ok(ParsedStatement::Catalog(CatalogQuery::ShowDatabases))
        }

        Statement::ShowColumns { show_options, .. } => {
            let table_name = show_options
                .show_in
                .and_then(|si| si.parent_name)
                .map(|n| object_name_to_string(&n))
                .ok_or_else(|| SqlError::ParseError("SHOW COLUMNS requires FROM <table>".into()))?;
            Ok(ParsedStatement::Catalog(CatalogQuery::DescribeTable {
                name: table_name,
            }))
        }

        Statement::ExplainTable { table_name, .. } => {
            let name = object_name_to_string(&table_name);
            Ok(ParsedStatement::Catalog(CatalogQuery::DescribeTable { name }))
        }

        _ => Err(SqlError::Unsupported(format!(
            "statement type: {}",
            stmt
        ))),
    }
}

// ── Query (SELECT) translation ───────────────────────────────────────

fn translate_query(query: ast::Query) -> Result<SelectPlan, SqlError> {
    let select = match *query.body {
        SetExpr::Select(sel) => *sel,
        _ => return Err(SqlError::Unsupported("non-SELECT query body".into())),
    };

    let projections = select
        .projection
        .iter()
        .map(translate_select_item)
        .collect::<Result<_, _>>()?;

    let from = if select.from.is_empty() {
        None
    } else {
        Some(translate_from(&select.from)?)
    };

    let where_clause = select.selection.map(|e| translate_expr(&e)).transpose()?;

    let group_by = match select.group_by {
        GroupByExpr::All(_) => {
            return Err(SqlError::Unsupported("GROUP BY ALL".into()));
        }
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| translate_expr(e))
            .collect::<Result<_, _>>()?,
    };

    let having = select.having.map(|e| translate_expr(&e)).transpose()?;

    let order_by = match query.order_by {
        Some(ob) => translate_order_by(&ob)?,
        None => vec![],
    };

    let limit = query
        .limit
        .map(|e| expr_to_u64(&e))
        .transpose()?;

    let offset = query
        .offset
        .map(|o| expr_to_u64(&o.value))
        .transpose()?;

    Ok(SelectPlan {
        projections,
        from,
        where_clause,
        group_by,
        having,
        order_by,
        limit,
        offset,
    })
}

fn translate_select_item(item: &SelectItem) -> Result<Projection, SqlError> {
    match item {
        SelectItem::UnnamedExpr(expr) => Ok(Projection::Expr {
            expr: translate_expr(expr)?,
            alias: None,
        }),
        SelectItem::ExprWithAlias { expr, alias } => Ok(Projection::Expr {
            expr: translate_expr(expr)?,
            alias: Some(alias.value.clone()),
        }),
        SelectItem::Wildcard(_) => Ok(Projection::Star),
        SelectItem::QualifiedWildcard(name, _) => {
            Ok(Projection::QualifiedStar(object_name_to_string(name)))
        }
    }
}

fn translate_from(from: &[ast::TableWithJoins]) -> Result<FromClause, SqlError> {
    if from.is_empty() {
        return Err(SqlError::ParseError("empty FROM clause".into()));
    }

    let first = &from[0];
    let mut result = translate_table_factor(&first.relation)?;

    for join in &first.joins {
        let right = translate_table_factor(&join.relation)?;
        let on = match &join.join_operator {
            ast::JoinOperator::Inner(constraint) => match constraint {
                ast::JoinConstraint::On(expr) => translate_expr(expr)?,
                _ => return Err(SqlError::Unsupported("JOIN without ON".into())),
            },
            _ => return Err(SqlError::Unsupported("only INNER JOIN is supported".into())),
        };
        result = FromClause::Join {
            left: Box::new(result),
            right: Box::new(right),
            on,
            kind: JoinKind::Inner,
        };
    }

    // Handle implicit cross-joins (FROM a, b)
    for twj in &from[1..] {
        let right = translate_table_factor(&twj.relation)?;
        // Treat as cross join — ON TRUE
        result = FromClause::Join {
            left: Box::new(result),
            right: Box::new(right),
            on: Expr::Literal(Value::Bool(true)),
            kind: JoinKind::Inner,
        };
    }

    Ok(result)
}

fn translate_table_factor(tf: &TableFactor) -> Result<FromClause, SqlError> {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let table_name = object_name_to_string(name);
            let alias = alias.as_ref().map(|a| a.name.value.clone());
            Ok(FromClause::Table {
                name: table_name,
                alias,
            })
        }
        _ => Err(SqlError::Unsupported("complex table factor".into())),
    }
}

fn translate_order_by(order_by: &ast::OrderBy) -> Result<Vec<OrderByItem>, SqlError> {
    order_by.exprs.iter().map(|o| {
        let expr = translate_expr(&o.expr)?;
        let asc = o.asc.unwrap_or(true);
        Ok(OrderByItem { expr, asc })
    }).collect()
}

// ── Expression translation ───────────────────────────────────────────

fn translate_expr(expr: &ast::Expr) -> Result<Expr, SqlError> {
    match expr {
        ast::Expr::Identifier(ident) => Ok(Expr::Column(ident.value.clone())),

        ast::Expr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                Ok(Expr::QualifiedColumn {
                    table: idents[0].value.clone(),
                    column: idents[1].value.clone(),
                })
            } else {
                Err(SqlError::Unsupported(format!(
                    "compound identifier with {} parts",
                    idents.len()
                )))
            }
        }

        ast::Expr::Value(val) => translate_value(val),

        ast::Expr::BinaryOp { left, op, right } => {
            let l = translate_expr(left)?;
            let r = translate_expr(right)?;
            let op = translate_binop(op)?;
            Ok(Expr::BinOp {
                op,
                left: Box::new(l),
                right: Box::new(r),
            })
        }

        ast::Expr::UnaryOp { op, expr } => {
            let e = translate_expr(expr)?;
            match op {
                ast::UnaryOperator::Not => Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(e),
                }),
                ast::UnaryOperator::Minus => Ok(Expr::UnaryOp {
                    op: UnaryOp::Neg,
                    expr: Box::new(e),
                }),
                ast::UnaryOperator::Plus => Ok(e),
                _ => Err(SqlError::Unsupported(format!("unary op: {:?}", op))),
            }
        }

        ast::Expr::IsNull(e) => Ok(Expr::IsNull(Box::new(translate_expr(e)?))),
        ast::Expr::IsNotNull(e) => Ok(Expr::IsNotNull(Box::new(translate_expr(e)?))),

        ast::Expr::Nested(e) => translate_expr(e),

        ast::Expr::Function(func) => {
            let name = object_name_to_string(&func.name);
            let args = match &func.args {
                ast::FunctionArguments::None => vec![],
                ast::FunctionArguments::Subquery(_) => {
                    return Err(SqlError::Unsupported("subquery in function".into()));
                }
                ast::FunctionArguments::List(arg_list) => {
                    if arg_list.duplicate_treatment.is_some() {
                        // DISTINCT in aggregates - not supported yet
                    }
                    arg_list.args.iter().map(|a| {
                        match a {
                            ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                                ast::FunctionArgExpr::Expr(e) => translate_expr(e),
                                ast::FunctionArgExpr::Wildcard => Ok(Expr::Star),
                                ast::FunctionArgExpr::QualifiedWildcard(n) => {
                                    Ok(Expr::QualifiedColumn {
                                        table: object_name_to_string(n),
                                        column: "*".to_string(),
                                    })
                                }
                            },
                            ast::FunctionArg::Named { arg, .. } => match arg {
                                ast::FunctionArgExpr::Expr(e) => translate_expr(e),
                                _ => Err(SqlError::Unsupported("named function arg".into())),
                            },
                            ast::FunctionArg::ExprNamed { arg, .. } => match arg {
                                ast::FunctionArgExpr::Expr(e) => translate_expr(e),
                                _ => Err(SqlError::Unsupported("named function arg".into())),
                            },
                        }
                    }).collect::<Result<_, _>>()?
                }
            };
            Ok(Expr::Function {
                name: name.to_uppercase(),
                args,
            })
        }

        ast::Expr::Between { expr, negated, low, high } => {
            let e = translate_expr(expr)?;
            let l = translate_expr(low)?;
            let h = translate_expr(high)?;
            let between = Expr::BinOp {
                op: BinOp::And,
                left: Box::new(Expr::BinOp {
                    op: BinOp::GtEq,
                    left: Box::new(e.clone()),
                    right: Box::new(l),
                }),
                right: Box::new(Expr::BinOp {
                    op: BinOp::LtEq,
                    left: Box::new(e),
                    right: Box::new(h),
                }),
            };
            if *negated {
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(between),
                })
            } else {
                Ok(between)
            }
        }

        ast::Expr::InList { expr, list, negated } => {
            let e = translate_expr(expr)?;
            let mut result: Option<Expr> = None;
            for item in list {
                let eq = Expr::BinOp {
                    op: BinOp::Eq,
                    left: Box::new(e.clone()),
                    right: Box::new(translate_expr(item)?),
                };
                result = Some(match result {
                    None => eq,
                    Some(prev) => Expr::BinOp {
                        op: BinOp::Or,
                        left: Box::new(prev),
                        right: Box::new(eq),
                    },
                });
            }
            let result = result.unwrap_or(Expr::Literal(Value::Bool(false)));
            if *negated {
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(result),
                })
            } else {
                Ok(result)
            }
        }

        _ => Err(SqlError::Unsupported(format!("expression: {}", expr))),
    }
}

fn translate_value(val: &ast::Value) -> Result<Expr, SqlError> {
    match val {
        ast::Value::Number(s, _) => {
            if let Ok(i) = s.parse::<i64>() {
                Ok(Expr::Literal(Value::Int(i)))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(Expr::Literal(Value::Real(f)))
            } else {
                Err(SqlError::ParseError(format!("invalid number: {}", s)))
            }
        }
        ast::Value::SingleQuotedString(s) => Ok(Expr::Literal(Value::Text(s.clone()))),
        ast::Value::DoubleQuotedString(s) => Ok(Expr::Literal(Value::Text(s.clone()))),
        ast::Value::Boolean(b) => Ok(Expr::Literal(Value::Bool(*b))),
        ast::Value::Null => Ok(Expr::Literal(Value::Null)),
        _ => Err(SqlError::Unsupported(format!("value: {:?}", val))),
    }
}

fn translate_binop(op: &ast::BinaryOperator) -> Result<BinOp, SqlError> {
    match op {
        ast::BinaryOperator::Eq => Ok(BinOp::Eq),
        ast::BinaryOperator::NotEq => Ok(BinOp::NotEq),
        ast::BinaryOperator::Lt => Ok(BinOp::Lt),
        ast::BinaryOperator::LtEq => Ok(BinOp::LtEq),
        ast::BinaryOperator::Gt => Ok(BinOp::Gt),
        ast::BinaryOperator::GtEq => Ok(BinOp::GtEq),
        ast::BinaryOperator::And => Ok(BinOp::And),
        ast::BinaryOperator::Or => Ok(BinOp::Or),
        ast::BinaryOperator::Plus => Ok(BinOp::Add),
        ast::BinaryOperator::Minus => Ok(BinOp::Sub),
        ast::BinaryOperator::Multiply => Ok(BinOp::Mul),
        ast::BinaryOperator::Divide => Ok(BinOp::Div),
        ast::BinaryOperator::Modulo => Ok(BinOp::Mod),
        _ => Err(SqlError::Unsupported(format!("binary op: {:?}", op))),
    }
}

fn translate_data_type(dt: &ast::DataType) -> Result<SqlType, SqlError> {
    match dt {
        ast::DataType::Int(_) | ast::DataType::Integer(_) => Ok(SqlType::Int),
        ast::DataType::BigInt(_) => Ok(SqlType::BigInt),
        ast::DataType::Text
        | ast::DataType::Varchar(_)
        | ast::DataType::CharVarying(_)
        | ast::DataType::Char(_)
        | ast::DataType::Character(_)
        | ast::DataType::CharacterVarying(_)
        | ast::DataType::String(_) => Ok(SqlType::Text),
        ast::DataType::Boolean | ast::DataType::Bool => Ok(SqlType::Bool),
        ast::DataType::Real | ast::DataType::Float(_) | ast::DataType::Double | ast::DataType::DoublePrecision => {
            Ok(SqlType::Real)
        }
        _ => Err(SqlError::Unsupported(format!("data type: {:?}", dt))),
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn object_name_to_string(name: &ast::ObjectName) -> String {
    name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
}

fn assignment_target_to_string(target: &ast::AssignmentTarget) -> String {
    match target {
        ast::AssignmentTarget::ColumnName(name) => {
            object_name_to_string(name)
        }
        ast::AssignmentTarget::Tuple(names) => {
            names.iter().map(|n| object_name_to_string(n)).collect::<Vec<_>>().join(", ")
        }
    }
}

fn extract_insert_rows(query: &ast::Query) -> Result<Vec<Row>, SqlError> {
    match query.body.as_ref() {
        SetExpr::Values(values) => {
            let mut rows = Vec::new();
            for row_exprs in &values.rows {
                let mut row = Vec::new();
                for expr in row_exprs {
                    let value = eval_const_expr(expr)?;
                    row.push(value);
                }
                rows.push(row);
            }
            Ok(rows)
        }
        _ => Err(SqlError::Unsupported(
            "INSERT ... SELECT not supported".into(),
        )),
    }
}

/// Evaluate a constant expression (for INSERT VALUES).
fn eval_const_expr(expr: &ast::Expr) -> Result<Value, SqlError> {
    match expr {
        ast::Expr::Value(val) => match val {
            ast::Value::Number(s, _) => {
                if let Ok(i) = s.parse::<i64>() {
                    Ok(Value::Int(i))
                } else if let Ok(f) = s.parse::<f64>() {
                    Ok(Value::Real(f))
                } else {
                    Err(SqlError::ParseError(format!("invalid number: {}", s)))
                }
            }
            ast::Value::SingleQuotedString(s) => Ok(Value::Text(s.clone())),
            ast::Value::DoubleQuotedString(s) => Ok(Value::Text(s.clone())),
            ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
            ast::Value::Null => Ok(Value::Null),
            _ => Err(SqlError::Unsupported(format!("value: {:?}", val))),
        },
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr,
        } => {
            let v = eval_const_expr(expr)?;
            match v {
                Value::Int(n) => Ok(Value::Int(-n)),
                Value::Real(r) => Ok(Value::Real(-r)),
                _ => Err(SqlError::EvalError("cannot negate non-numeric value".into())),
            }
        }
        _ => Err(SqlError::Unsupported(format!(
            "non-constant expression in VALUES: {}",
            expr
        ))),
    }
}

fn expr_to_u64(expr: &ast::Expr) -> Result<u64, SqlError> {
    match expr {
        ast::Expr::Value(ast::Value::Number(s, _)) => s
            .parse::<u64>()
            .map_err(|_| SqlError::ParseError(format!("expected integer: {}", s))),
        _ => Err(SqlError::ParseError(format!(
            "expected integer literal, got: {}",
            expr
        ))),
    }
}
