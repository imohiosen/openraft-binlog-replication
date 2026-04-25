use std::collections::BTreeMap;

use super::error::SqlError;
use super::expr::eval;
use super::types::*;

impl SqlState {
    pub fn execute(&mut self, cmd: SqlCommand) -> Result<SqlResult, SqlError> {
        match cmd {
            SqlCommand::CreateTable {
                schema,
                if_not_exists,
            } => self.create_table(schema, if_not_exists),
            SqlCommand::DropTable { name, if_exists } => self.drop_table(&name, if_exists),
            SqlCommand::CreateIndex {
                def,
                if_not_exists,
            } => self.create_index(def, if_not_exists),
            SqlCommand::DropIndex { name, table } => self.drop_index(&name, &table),
            SqlCommand::Insert {
                table,
                columns,
                rows,
            } => self.insert(&table, columns, rows),
            SqlCommand::Update {
                table,
                assignments,
                where_clause,
            } => self.update(&table, assignments, where_clause),
            SqlCommand::Delete {
                table,
                where_clause,
            } => self.delete(&table, where_clause),
            SqlCommand::Truncate { table } => self.truncate(&table),
        }
    }

    // ── CREATE TABLE ─────────────────────────────────────────────────

    fn create_table(
        &mut self,
        schema: TableSchema,
        if_not_exists: bool,
    ) -> Result<SqlResult, SqlError> {
        let name = schema.name.clone();
        if self.schemas.contains_key(&name) {
            if if_not_exists {
                return Ok(SqlResult::Created);
            }
            return Err(SqlError::TableAlreadyExists(name));
        }
        self.schemas.insert(name.clone(), schema);
        self.tables.insert(name.clone(), BTreeMap::new());
        self.sequences.insert(name, 0);
        Ok(SqlResult::Created)
    }

    // ── DROP TABLE ───────────────────────────────────────────────────

    fn drop_table(&mut self, name: &str, if_exists: bool) -> Result<SqlResult, SqlError> {
        if !self.schemas.contains_key(name) {
            if if_exists {
                return Ok(SqlResult::Dropped);
            }
            return Err(SqlError::TableNotFound(name.to_string()));
        }
        // Remove indexes
        if let Some(schema) = self.schemas.get(name) {
            let idx_names: Vec<String> = schema.indexes.iter().map(|i| i.name.clone()).collect();
            for idx_name in idx_names {
                self.indexes.remove(&idx_name);
            }
        }
        self.schemas.remove(name);
        self.tables.remove(name);
        self.sequences.remove(name);
        Ok(SqlResult::Dropped)
    }

    // ── CREATE INDEX ─────────────────────────────────────────────────

    fn create_index(
        &mut self,
        def: IndexDef,
        if_not_exists: bool,
    ) -> Result<SqlResult, SqlError> {
        // Check index already exists
        if self.indexes.contains_key(&def.name) {
            if if_not_exists {
                return Ok(SqlResult::Created);
            }
            return Err(SqlError::IndexAlreadyExists(def.name.clone()));
        }

        let schema = self
            .schemas
            .get(&def.table)
            .ok_or_else(|| SqlError::TableNotFound(def.table.clone()))?;

        // Validate columns exist
        let col_indices: Vec<usize> = def
            .columns
            .iter()
            .map(|c| {
                schema.column_index(c).ok_or_else(|| SqlError::ColumnNotFound {
                    table: def.table.clone(),
                    column: c.clone(),
                })
            })
            .collect::<Result<_, _>>()?;

        // Build index from existing rows
        let mut idx_data = IndexData::default();
        if let Some(table_data) = self.tables.get(&def.table) {
            for (&row_id, row) in table_data {
                let key = make_index_key(row, &col_indices);
                if def.unique && idx_data.contains_key(&key) {
                    return Err(SqlError::UniqueViolation {
                        index: def.name.clone(),
                        value: format!("{}", key),
                    });
                }
                idx_data.entry_or_insert(key).insert(row_id);
            }
        }

        self.indexes.insert(def.name.clone(), idx_data);

        // Add index to schema
        let schema = self.schemas.get_mut(&def.table).unwrap();
        schema.indexes.push(def);

        Ok(SqlResult::Created)
    }

    // ── DROP INDEX ───────────────────────────────────────────────────

    fn drop_index(&mut self, name: &str, table: &str) -> Result<SqlResult, SqlError> {
        let schema = self
            .schemas
            .get_mut(table)
            .ok_or_else(|| SqlError::TableNotFound(table.to_string()))?;

        let idx_pos = schema
            .indexes
            .iter()
            .position(|i| i.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| SqlError::IndexNotFound(name.to_string()))?;

        schema.indexes.remove(idx_pos);
        self.indexes.remove(name);
        Ok(SqlResult::Dropped)
    }

    // ── INSERT ───────────────────────────────────────────────────────

    fn insert(
        &mut self,
        table_name: &str,
        columns: Option<Vec<String>>,
        rows: Vec<Row>,
    ) -> Result<SqlResult, SqlError> {
        let schema = self
            .schemas
            .get(table_name)
            .ok_or_else(|| SqlError::TableNotFound(table_name.to_string()))?
            .clone();

        let col_order: Vec<usize> = match &columns {
            Some(cols) => cols
                .iter()
                .map(|c| {
                    schema.column_index(c).ok_or_else(|| SqlError::ColumnNotFound {
                        table: table_name.to_string(),
                        column: c.clone(),
                    })
                })
                .collect::<Result<_, _>>()?,
            None => (0..schema.columns.len()).collect(),
        };

        let mut inserted = 0u64;
        for input_row in &rows {
            if input_row.len() != col_order.len() {
                return Err(SqlError::ColumnCountMismatch {
                    expected: col_order.len(),
                    got: input_row.len(),
                });
            }

            // Build full row with NULLs for unspecified columns
            let mut full_row: Row = vec![Value::Null; schema.columns.len()];
            for (i, &col_idx) in col_order.iter().enumerate() {
                full_row[col_idx] = input_row[i].clone();
            }

            // Validate NOT NULL and type constraints
            for (i, col) in schema.columns.iter().enumerate() {
                if !col.nullable && full_row[i].is_null() {
                    return Err(SqlError::NullViolation(col.name.clone()));
                }
            }

            // Check primary key uniqueness
            let pk_cols = schema.pk_columns();
            if !pk_cols.is_empty() {
                let pk_val = if pk_cols.len() == 1 {
                    full_row[pk_cols[0].0].clone()
                } else {
                    // composite PK — just use first for now
                    full_row[pk_cols[0].0].clone()
                };
                if let Some(table_data) = self.tables.get(table_name) {
                    for (_, existing_row) in table_data {
                        let existing_pk = if pk_cols.len() == 1 {
                            existing_row[pk_cols[0].0].clone()
                        } else {
                            existing_row[pk_cols[0].0].clone()
                        };
                        if existing_pk == pk_val && !pk_val.is_null() {
                            return Err(SqlError::DuplicatePrimaryKey {
                                table: table_name.to_string(),
                                value: format!("{}", pk_val),
                            });
                        }
                    }
                }
            }

            // Assign row_id
            let seq = self.sequences.get_mut(table_name).unwrap();
            let row_id = *seq;
            *seq += 1;

            // Insert row
            self.tables
                .get_mut(table_name)
                .unwrap()
                .insert(row_id, full_row.clone());

            // Update indexes
            self.update_indexes_insert(table_name, row_id, &full_row, &schema)?;
            inserted += 1;
        }

        Ok(SqlResult::Ack {
            rows_affected: inserted,
        })
    }

    // ── UPDATE ───────────────────────────────────────────────────────

    fn update(
        &mut self,
        table_name: &str,
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    ) -> Result<SqlResult, SqlError> {
        let schema = self
            .schemas
            .get(table_name)
            .ok_or_else(|| SqlError::TableNotFound(table_name.to_string()))?
            .clone();

        let columns: Vec<ColumnRef> = schema
            .columns
            .iter()
            .map(|c| ColumnRef {
                table: Some(table_name.to_string()),
                name: c.name.clone(),
            })
            .collect();

        // Find matching row IDs
        let matching: Vec<u64> = {
            let table_data = self
                .tables
                .get(table_name)
                .ok_or_else(|| SqlError::TableNotFound(table_name.to_string()))?;
            table_data
                .iter()
                .filter(|(_, row)| match &where_clause {
                    None => true,
                    Some(expr) => eval(&columns, row, expr)
                        .map(|v| v.is_truthy())
                        .unwrap_or(false),
                })
                .map(|(&id, _)| id)
                .collect()
        };

        let mut updated = 0u64;
        for row_id in matching {
            let old_row = self.tables[table_name][&row_id].clone();

            // Compute new values
            let mut new_row = old_row.clone();
            for (col_name, expr) in &assignments {
                let col_idx = schema.column_index(col_name).ok_or_else(|| {
                    SqlError::ColumnNotFound {
                        table: table_name.to_string(),
                        column: col_name.clone(),
                    }
                })?;
                let new_val = eval(&columns, &old_row, expr)?;
                new_row[col_idx] = new_val;
            }

            // Update indexes (remove old, insert new)
            self.update_indexes_delete(table_name, row_id, &old_row, &schema);
            self.update_indexes_insert(table_name, row_id, &new_row, &schema)?;

            // Write new row
            self.tables
                .get_mut(table_name)
                .unwrap()
                .insert(row_id, new_row);
            updated += 1;
        }

        Ok(SqlResult::Ack {
            rows_affected: updated,
        })
    }

    // ── DELETE ────────────────────────────────────────────────────────

    fn delete(
        &mut self,
        table_name: &str,
        where_clause: Option<Expr>,
    ) -> Result<SqlResult, SqlError> {
        let schema = self
            .schemas
            .get(table_name)
            .ok_or_else(|| SqlError::TableNotFound(table_name.to_string()))?
            .clone();

        let columns: Vec<ColumnRef> = schema
            .columns
            .iter()
            .map(|c| ColumnRef {
                table: Some(table_name.to_string()),
                name: c.name.clone(),
            })
            .collect();

        let to_delete: Vec<(u64, Row)> = {
            let table_data = self
                .tables
                .get(table_name)
                .ok_or_else(|| SqlError::TableNotFound(table_name.to_string()))?;
            table_data
                .iter()
                .filter(|(_, row)| match &where_clause {
                    None => true,
                    Some(expr) => eval(&columns, row, expr)
                        .map(|v| v.is_truthy())
                        .unwrap_or(false),
                })
                .map(|(&id, row)| (id, row.clone()))
                .collect()
        };

        let deleted = to_delete.len() as u64;
        for (row_id, row) in &to_delete {
            self.update_indexes_delete(table_name, *row_id, row, &schema);
            self.tables.get_mut(table_name).unwrap().remove(row_id);
        }

        Ok(SqlResult::Ack {
            rows_affected: deleted,
        })
    }

    // ── TRUNCATE ─────────────────────────────────────────────────────

    fn truncate(&mut self, table_name: &str) -> Result<SqlResult, SqlError> {
        if !self.schemas.contains_key(table_name) {
            return Err(SqlError::TableNotFound(table_name.to_string()));
        }

        // Clear table data
        if let Some(data) = self.tables.get_mut(table_name) {
            data.clear();
        }

        // Clear all indexes for this table
        if let Some(schema) = self.schemas.get(table_name) {
            for idx in &schema.indexes {
                if let Some(idx_data) = self.indexes.get_mut(&idx.name) {
                    idx_data.clear();
                }
            }
        }

        // Reset sequence
        if let Some(seq) = self.sequences.get_mut(table_name) {
            *seq = 0;
        }

        Ok(SqlResult::Truncated)
    }

    // ── Index helpers ────────────────────────────────────────────────

    fn update_indexes_insert(
        &mut self,
        _: &str,
        row_id: u64,
        row: &Row,
        schema: &TableSchema,
    ) -> Result<(), SqlError> {
        for idx_def in &schema.indexes {
            let col_indices: Vec<usize> = idx_def
                .columns
                .iter()
                .filter_map(|c| schema.column_index(c))
                .collect();
            let key = make_index_key(row, &col_indices);

            let idx_data = self.indexes.entry(idx_def.name.clone()).or_insert_with(IndexData::default);
            if idx_def.unique {
                if let Some(existing) = idx_data.get(&key) {
                    if !existing.is_empty() && !key.is_null() {
                        return Err(SqlError::UniqueViolation {
                            index: idx_def.name.clone(),
                            value: format!("{}", key),
                        });
                    }
                }
            }
            idx_data.entry_or_insert(key).insert(row_id);
        }
        Ok(())
    }

    fn update_indexes_delete(&mut self, _: &str, row_id: u64, row: &Row, schema: &TableSchema) {
        for idx_def in &schema.indexes {
            let col_indices: Vec<usize> = idx_def
                .columns
                .iter()
                .filter_map(|c| schema.column_index(c))
                .collect();
            let key = make_index_key(row, &col_indices);

            if let Some(idx_data) = self.indexes.get_mut(&idx_def.name) {
                let should_remove = if let Some(set) = idx_data.get_mut(&key) {
                    set.remove(&row_id);
                    set.is_empty()
                } else {
                    false
                };
                if should_remove {
                    idx_data.remove(&key);
                }
            }
        }
    }
}

fn make_index_key(row: &Row, col_indices: &[usize]) -> Value {
    if col_indices.len() == 1 {
        row[col_indices[0]].clone()
    } else {
        // For multi-column indexes, create a composite text key
        let parts: Vec<String> = col_indices.iter().map(|&i| format!("{}", row[i])).collect();
        Value::Text(parts.join("\0"))
    }
}
