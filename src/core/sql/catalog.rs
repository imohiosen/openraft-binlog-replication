use super::error::SqlError;
use super::types::*;

impl SqlState {
    // ── SHOW TABLES ──────────────────────────────────────────────────

    pub fn show_tables(&self) -> SqlResult {
        let mut names: Vec<&String> = self.schemas.keys().collect();
        names.sort();
        let rows = names
            .into_iter()
            .map(|n| vec![Value::Text(n.clone())])
            .collect();
        SqlResult::Rows {
            columns: vec!["table_name".to_string()],
            rows,
        }
    }

    // ── SHOW DATABASES ───────────────────────────────────────────────

    pub fn show_databases(&self) -> SqlResult {
        SqlResult::Rows {
            columns: vec!["database_name".to_string()],
            rows: vec![vec![Value::Text("default".to_string())]],
        }
    }

    // ── DESCRIBE TABLE ───────────────────────────────────────────────

    pub fn describe_table(&self, name: &str) -> Result<SqlResult, SqlError> {
        let schema = self
            .schemas
            .get(name)
            .ok_or_else(|| SqlError::TableNotFound(name.to_string()))?;

        let rows = schema
            .columns
            .iter()
            .map(|col| {
                let key = if col.primary_key {
                    "PRI".to_string()
                } else {
                    // Check if this column is part of any index
                    let in_index = schema.indexes.iter().any(|idx| {
                        idx.columns.iter().any(|c| c.eq_ignore_ascii_case(&col.name))
                    });
                    if in_index { "MUL".to_string() } else { String::new() }
                };
                vec![
                    Value::Text(col.name.clone()),
                    Value::Text(col.ty.to_string()),
                    Value::Text(if col.nullable { "YES" } else { "NO" }.to_string()),
                    Value::Text(key),
                ]
            })
            .collect();

        Ok(SqlResult::Rows {
            columns: vec![
                "Field".to_string(),
                "Type".to_string(),
                "Null".to_string(),
                "Key".to_string(),
            ],
            rows,
        })
    }

    // ── INFORMATION_SCHEMA virtual tables ────────────────────────────

    pub fn catalog_query(&self, query: &CatalogQuery) -> Result<SqlResult, SqlError> {
        match query {
            CatalogQuery::ShowTables => Ok(self.show_tables()),
            CatalogQuery::ShowDatabases => Ok(self.show_databases()),
            CatalogQuery::DescribeTable { name } => self.describe_table(name),
        }
    }

    /// Generate virtual rows for `information_schema.tables`.
    pub fn information_schema_tables(&self) -> (Vec<ColumnRef>, Vec<Row>) {
        let columns = vec![
            ColumnRef { table: Some("information_schema.tables".into()), name: "table_catalog".into() },
            ColumnRef { table: Some("information_schema.tables".into()), name: "table_schema".into() },
            ColumnRef { table: Some("information_schema.tables".into()), name: "table_name".into() },
            ColumnRef { table: Some("information_schema.tables".into()), name: "table_type".into() },
        ];

        let mut names: Vec<&String> = self.schemas.keys().collect();
        names.sort();

        let rows = names
            .into_iter()
            .map(|name| {
                vec![
                    Value::Text("default".into()),
                    Value::Text("public".into()),
                    Value::Text(name.clone()),
                    Value::Text("BASE TABLE".into()),
                ]
            })
            .collect();

        (columns, rows)
    }

    /// Generate virtual rows for `information_schema.columns`.
    pub fn information_schema_columns(&self) -> (Vec<ColumnRef>, Vec<Row>) {
        let tbl = "information_schema.columns";
        let columns = vec![
            ColumnRef { table: Some(tbl.into()), name: "table_catalog".into() },
            ColumnRef { table: Some(tbl.into()), name: "table_schema".into() },
            ColumnRef { table: Some(tbl.into()), name: "table_name".into() },
            ColumnRef { table: Some(tbl.into()), name: "column_name".into() },
            ColumnRef { table: Some(tbl.into()), name: "ordinal_position".into() },
            ColumnRef { table: Some(tbl.into()), name: "is_nullable".into() },
            ColumnRef { table: Some(tbl.into()), name: "data_type".into() },
            ColumnRef { table: Some(tbl.into()), name: "column_key".into() },
        ];

        let mut table_names: Vec<&String> = self.schemas.keys().collect();
        table_names.sort();

        let mut rows = Vec::new();
        for table_name in table_names {
            let schema = &self.schemas[table_name];
            for (i, col) in schema.columns.iter().enumerate() {
                let key = if col.primary_key {
                    "PRI"
                } else if schema.indexes.iter().any(|idx| {
                    idx.columns.iter().any(|c| c.eq_ignore_ascii_case(&col.name))
                }) {
                    "MUL"
                } else {
                    ""
                };
                rows.push(vec![
                    Value::Text("default".into()),
                    Value::Text("public".into()),
                    Value::Text(table_name.clone()),
                    Value::Text(col.name.clone()),
                    Value::Int((i + 1) as i64),
                    Value::Text(if col.nullable { "YES" } else { "NO" }.into()),
                    Value::Text(col.ty.to_string()),
                    Value::Text(key.into()),
                ]);
            }
        }

        (columns, rows)
    }

    /// Generate virtual rows for `information_schema.statistics` (indexes).
    pub fn information_schema_statistics(&self) -> (Vec<ColumnRef>, Vec<Row>) {
        let tbl = "information_schema.statistics";
        let columns = vec![
            ColumnRef { table: Some(tbl.into()), name: "table_catalog".into() },
            ColumnRef { table: Some(tbl.into()), name: "table_schema".into() },
            ColumnRef { table: Some(tbl.into()), name: "table_name".into() },
            ColumnRef { table: Some(tbl.into()), name: "non_unique".into() },
            ColumnRef { table: Some(tbl.into()), name: "index_name".into() },
            ColumnRef { table: Some(tbl.into()), name: "column_name".into() },
            ColumnRef { table: Some(tbl.into()), name: "seq_in_index".into() },
        ];

        let mut table_names: Vec<&String> = self.schemas.keys().collect();
        table_names.sort();

        let mut rows = Vec::new();
        for table_name in table_names {
            let schema = &self.schemas[table_name];
            for idx in &schema.indexes {
                for (seq, col_name) in idx.columns.iter().enumerate() {
                    rows.push(vec![
                        Value::Text("default".into()),
                        Value::Text("public".into()),
                        Value::Text(idx.table.clone()),
                        Value::Int(if idx.unique { 0 } else { 1 }),
                        Value::Text(idx.name.clone()),
                        Value::Text(col_name.clone()),
                        Value::Int((seq + 1) as i64),
                    ]);
                }
            }
        }

        (columns, rows)
    }
}
