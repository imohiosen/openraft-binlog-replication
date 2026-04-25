use std::collections::HashMap;

use super::error::SqlError;
use super::expr::{contains_aggregate, eval, eval_with_aggregates};
use super::types::*;

impl SqlState {
    /// Execute a SELECT query. Read-only — does not mutate state.
    pub fn query_select(&self, plan: &SelectPlan) -> Result<SqlResult, SqlError> {
        // 1. Resolve FROM → (columns, rows)
        let (columns, mut rows) = self.resolve_from(&plan.from)?;

        // 2. WHERE filter
        if let Some(ref where_expr) = plan.where_clause {
            rows.retain(|row| {
                eval(&columns, row, where_expr)
                    .map(|v| v.is_truthy())
                    .unwrap_or(false)
            });
        }

        // 3. Check if this is an aggregate query
        let is_aggregate = !plan.group_by.is_empty() || projections_have_aggregates(&plan.projections);

        if is_aggregate {
            return self.query_with_grouping(&columns, &rows, plan);
        }

        // 4. Project
        let (result_cols, mut result_rows) = project(&columns, &rows, &plan.projections)?;

        // 5. ORDER BY
        if !plan.order_by.is_empty() {
            let col_refs = strings_to_column_refs(&result_cols);
            sort_rows(&col_refs, &mut result_rows, &plan.order_by)?;
        }

        // 6. LIMIT / OFFSET
        apply_limit_offset(&mut result_rows, plan.limit, plan.offset);

        Ok(SqlResult::Rows {
            columns: result_cols,
            rows: result_rows,
        })
    }

    // ── FROM resolution ──────────────────────────────────────────────

    fn resolve_from(
        &self,
        from: &Option<FromClause>,
    ) -> Result<(Vec<ColumnRef>, Vec<Row>), SqlError> {
        match from {
            None => Ok((vec![], vec![vec![]])),
            Some(fc) => self.resolve_from_clause(fc),
        }
    }

    fn resolve_from_clause(
        &self,
        from: &FromClause,
    ) -> Result<(Vec<ColumnRef>, Vec<Row>), SqlError> {
        match from {
            FromClause::Table { name, alias } => {
                let schema = self
                    .schemas
                    .get(name)
                    .ok_or_else(|| SqlError::TableNotFound(name.clone()))?;

                let table_alias = alias.as_ref().unwrap_or(name).clone();
                let columns: Vec<ColumnRef> = schema
                    .columns
                    .iter()
                    .map(|c| ColumnRef {
                        table: Some(table_alias.clone()),
                        name: c.name.clone(),
                    })
                    .collect();

                let table_data = self
                    .tables
                    .get(name)
                    .ok_or_else(|| SqlError::TableNotFound(name.clone()))?;

                let rows: Vec<Row> = table_data.values().cloned().collect();
                Ok((columns, rows))
            }
            FromClause::Join {
                left,
                right,
                on,
                kind: _,
            } => {
                let (left_cols, left_rows) = self.resolve_from_clause(left)?;
                let (right_cols, right_rows) = self.resolve_from_clause(right)?;

                let mut combined_cols = left_cols.clone();
                combined_cols.extend(right_cols);

                // Nested-loop join
                let mut result_rows = Vec::new();
                for lrow in &left_rows {
                    for rrow in &right_rows {
                        let mut combined = lrow.clone();
                        combined.extend(rrow.clone());

                        if eval(&combined_cols, &combined, on)
                            .map(|v| v.is_truthy())
                            .unwrap_or(false)
                        {
                            result_rows.push(combined);
                        }
                    }
                }

                Ok((combined_cols, result_rows))
            }
        }
    }

    // ── Aggregate query ──────────────────────────────────────────────

    fn query_with_grouping(
        &self,
        columns: &[ColumnRef],
        rows: &[Row],
        plan: &SelectPlan,
    ) -> Result<SqlResult, SqlError> {
        // Group rows
        let groups = if plan.group_by.is_empty() {
            // All rows in one group
            let refs: Vec<&[Value]> = rows.iter().map(|r| r.as_slice()).collect();
            vec![(vec![Value::Null], refs)]
        } else {
            group_rows(columns, rows, &plan.group_by)?
        };

        let mut result_rows = Vec::new();
        let mut result_cols: Option<Vec<String>> = None;

        for (_key, group) in &groups {
            // Apply HAVING
            if let Some(ref having) = plan.having {
                let val = eval_with_aggregates(columns, group, having)?;
                if !val.is_truthy() {
                    continue;
                }
            }

            // Project
            let (cols, row) = project_aggregate(columns, group, &plan.projections)?;
            if result_cols.is_none() {
                result_cols = Some(cols);
            }
            result_rows.push(row);
        }

        let result_cols = result_cols.unwrap_or_default();

        // ORDER BY
        if !plan.order_by.is_empty() {
            let col_refs: Vec<ColumnRef> = result_cols
                .iter()
                .map(|n| ColumnRef {
                    table: None,
                    name: n.clone(),
                })
                .collect();
            sort_rows(&col_refs, &mut result_rows, &plan.order_by)?;
        }

        // LIMIT / OFFSET
        apply_limit_offset(&mut result_rows, plan.limit, plan.offset);

        Ok(SqlResult::Rows {
            columns: result_cols,
            rows: result_rows,
        })
    }
}

// ── Projection ───────────────────────────────────────────────────────

fn project(
    columns: &[ColumnRef],
    rows: &[Row],
    projections: &[Projection],
) -> Result<(Vec<String>, Vec<Row>), SqlError> {
    let expanded = expand_projections(columns, projections)?;
    let col_names: Vec<String> = expanded.iter().map(|(name, _)| name.clone()).collect();

    let mut result_rows = Vec::new();
    for row in rows {
        let mut out = Vec::new();
        for (_, expr) in &expanded {
            out.push(eval(columns, row, expr)?);
        }
        result_rows.push(out);
    }
    Ok((col_names, result_rows))
}

fn project_aggregate(
    columns: &[ColumnRef],
    group: &[&[Value]],
    projections: &[Projection],
) -> Result<(Vec<String>, Row), SqlError> {
    let expanded = expand_projections(columns, projections)?;
    let col_names: Vec<String> = expanded.iter().map(|(name, _)| name.clone()).collect();

    let mut row = Vec::new();
    for (_, expr) in &expanded {
        row.push(eval_with_aggregates(columns, group, expr)?);
    }
    Ok((col_names, row))
}

fn expand_projections(
    columns: &[ColumnRef],
    projections: &[Projection],
) -> Result<Vec<(String, Expr)>, SqlError> {
    let mut result = Vec::new();
    for proj in projections {
        match proj {
            Projection::Star => {
                for col in columns {
                    result.push((col.name.clone(), Expr::Column(col.name.clone())));
                }
            }
            Projection::QualifiedStar(table) => {
                for col in columns {
                    if col
                        .table
                        .as_ref()
                        .map_or(false, |t| t.eq_ignore_ascii_case(table))
                    {
                        result.push((col.name.clone(), Expr::Column(col.name.clone())));
                    }
                }
            }
            Projection::Expr { expr, alias } => {
                let name = alias
                    .clone()
                    .unwrap_or_else(|| expr_display_name(expr));
                result.push((name, expr.clone()));
            }
        }
    }
    Ok(result)
}

fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::QualifiedColumn { table, column } => format!("{}.{}", table, column),
        Expr::Function { name, args } => {
            let arg_strs: Vec<String> = args.iter().map(|a| expr_display_name(a)).collect();
            format!("{}({})", name, arg_strs.join(", "))
        }
        Expr::Star => "*".to_string(),
        Expr::Literal(v) => format!("{}", v),
        _ => "?".to_string(),
    }
}

// ── Grouping ─────────────────────────────────────────────────────────

fn group_rows<'a>(
    columns: &[ColumnRef],
    rows: &'a [Row],
    group_by: &[Expr],
) -> Result<Vec<(Vec<Value>, Vec<&'a [Value]>)>, SqlError> {
    let mut groups: HashMap<Vec<Value>, Vec<&'a [Value]>> = HashMap::new();
    let mut order: Vec<Vec<Value>> = Vec::new();

    for row in rows {
        let key: Vec<Value> = group_by
            .iter()
            .map(|expr| eval(columns, row, expr))
            .collect::<Result<_, _>>()?;

        if !groups.contains_key(&key) {
            order.push(key.clone());
        }
        groups.entry(key).or_default().push(row.as_slice());
    }

    Ok(order
        .into_iter()
        .map(|key| {
            let rows = groups.remove(&key).unwrap();
            (key, rows)
        })
        .collect())
}

// ── Sorting ──────────────────────────────────────────────────────────

fn sort_rows(
    columns: &[ColumnRef],
    rows: &mut Vec<Row>,
    order_by: &[OrderByItem],
) -> Result<(), SqlError> {
    rows.sort_by(|a, b| {
        for item in order_by {
            let va = eval(columns, a, &item.expr).unwrap_or(Value::Null);
            let vb = eval(columns, b, &item.expr).unwrap_or(Value::Null);
            let cmp = va.cmp(&vb);
            let cmp = if item.asc { cmp } else { cmp.reverse() };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

// ── LIMIT / OFFSET ───────────────────────────────────────────────────

fn apply_limit_offset(rows: &mut Vec<Row>, limit: Option<u64>, offset: Option<u64>) {
    let off = offset.unwrap_or(0) as usize;
    if off > 0 {
        if off >= rows.len() {
            rows.clear();
            return;
        }
        *rows = rows[off..].to_vec();
    }
    if let Some(lim) = limit {
        rows.truncate(lim as usize);
    }
}

fn projections_have_aggregates(projections: &[Projection]) -> bool {
    projections.iter().any(|p| match p {
        Projection::Expr { expr, .. } => contains_aggregate(expr),
        _ => false,
    })
}

fn strings_to_column_refs(names: &[String]) -> Vec<ColumnRef> {
    names
        .iter()
        .map(|n| ColumnRef {
            table: None,
            name: n.clone(),
        })
        .collect()
}
