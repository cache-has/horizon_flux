// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Friendly SQL preprocessor that expands DuckDB-style convenience syntax into
//! standard SQL before passing queries to DataFusion.
//!
//! Supported expansions:
//! - `FROM table` → `SELECT * FROM table` (bare FROM)
//! - `GROUP BY ALL` → explicit GROUP BY with non-aggregate columns
//! - `SELECT * EXCLUDE(col1, col2)` → explicit column list minus excluded
//! - `SELECT COLUMNS('regex')` → explicit column list matching pattern

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;
use regex::Regex;
use sqlparser::ast::*;
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

/// Errors that can occur during SQL preprocessing.
#[derive(Debug, thiserror::Error)]
pub enum PreprocessError {
    #[error("SQL parse error: {0}")]
    Parse(#[from] sqlparser::parser::ParserError),

    #[error("EXCLUDE column `{0}` not found in any table in scope")]
    ExcludeColumnNotFound(String),

    #[error("COLUMNS pattern `{0}` is not a valid regex: {1}")]
    InvalidColumnsRegex(String, String),

    #[error("COLUMNS pattern `{0}` matched no columns")]
    NoColumnsMatch(String),

    #[error("no tables in scope for wildcard expansion")]
    NoTablesInScope,
}

/// Column information resolved from tables in scope.
struct TableColumns {
    /// Table alias (or original name if no alias).
    alias: String,
    /// Original table name.
    #[allow(dead_code)]
    name: String,
    /// Column names in order.
    columns: Vec<String>,
}

/// Preprocess SQL by expanding DuckDB-style convenience syntax into standard SQL.
///
/// `table_schemas` maps registered table names to their Arrow schemas. The
/// preprocessor uses this to resolve column names for EXCLUDE and COLUMNS
/// expansions.
///
/// If no friendly syntax is detected, the original SQL is returned unchanged
/// (preserving formatting).
pub fn preprocess_sql(
    sql: &str,
    table_schemas: &HashMap<String, SchemaRef>,
) -> Result<String, PreprocessError> {
    // Pre-parse: expand bare FROM → SELECT * FROM.
    let sql = expand_bare_from(sql);

    let dialect = DuckDbDialect {};
    let statements = Parser::parse_sql(&dialect, &sql)?;

    let mut modified = false;
    let mut result = Vec::new();

    for mut stmt in statements {
        if let Statement::Query(ref mut query) = stmt {
            modified |= transform_query(query, table_schemas)?;
        }
        result.push(stmt);
    }

    if !modified {
        return Ok(sql.to_string());
    }

    Ok(result
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

/// Transform a top-level query, recursing into subqueries.
fn transform_query(
    query: &mut Query,
    schemas: &HashMap<String, SchemaRef>,
) -> Result<bool, PreprocessError> {
    let mut modified = false;
    match query.body.as_mut() {
        SetExpr::Select(select) => {
            modified |= transform_select(select, schemas)?;
        }
        SetExpr::SetOperation { left, right, .. } => {
            if let SetExpr::Select(sel) = left.as_mut() {
                modified |= transform_select(sel, schemas)?;
            }
            if let SetExpr::Select(sel) = right.as_mut() {
                modified |= transform_select(sel, schemas)?;
            }
        }
        _ => {}
    }
    Ok(modified)
}

/// Transform a SELECT statement, expanding friendly syntax.
fn transform_select(
    select: &mut Select,
    schemas: &HashMap<String, SchemaRef>,
) -> Result<bool, PreprocessError> {
    let mut modified = false;

    let tables_in_scope = resolve_tables_in_scope(&select.from, schemas);

    // Expand SELECT * EXCLUDE(col, ...) and COLUMNS('regex').
    // These may produce a new projection list.
    let mut new_projection = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(opts) if has_exclude(opts) => {
                let expanded = expand_exclude_item(opts, &tables_in_scope, None)?;
                new_projection.extend(expanded);
                modified = true;
            }
            SelectItem::QualifiedWildcard(kind, opts) if has_exclude(opts) => {
                let qualifier = match kind {
                    SelectItemQualifiedWildcardKind::ObjectName(name) => Some(name.to_string()),
                    SelectItemQualifiedWildcardKind::Expr(expr) => Some(expr.to_string()),
                };
                let expanded = expand_exclude_item(opts, &tables_in_scope, qualifier.as_deref())?;
                new_projection.extend(expanded);
                modified = true;
            }
            SelectItem::UnnamedExpr(expr) if is_columns_call(expr) => {
                let expanded = expand_columns_call(expr, &tables_in_scope)?;
                new_projection.extend(expanded);
                modified = true;
            }
            SelectItem::ExprWithAlias { expr, .. } if is_columns_call(expr) => {
                // COLUMNS() with alias doesn't make sense for multiple columns,
                // but we expand it anyway.
                let expanded = expand_columns_call(expr, &tables_in_scope)?;
                new_projection.extend(expanded);
                modified = true;
            }
            other => {
                new_projection.push(other.clone());
            }
        }
    }
    if modified {
        select.projection = new_projection;
    }

    // Expand GROUP BY ALL → explicit column list.
    if let GroupByExpr::All(modifiers) = &select.group_by {
        let modifiers = modifiers.clone();
        let group_exprs = compute_group_by_all(&select.projection);
        if !group_exprs.is_empty() {
            select.group_by = GroupByExpr::Expressions(group_exprs, modifiers);
            modified = true;
        }
    }

    // Recurse into subqueries in FROM (e.g., derived tables).
    for table_with_joins in &mut select.from {
        modified |= transform_table_factor(&mut table_with_joins.relation, schemas)?;
        for join in &mut table_with_joins.joins {
            modified |= transform_table_factor(&mut join.relation, schemas)?;
        }
    }

    Ok(modified)
}

/// Recurse into table factors to handle subqueries.
fn transform_table_factor(
    factor: &mut TableFactor,
    schemas: &HashMap<String, SchemaRef>,
) -> Result<bool, PreprocessError> {
    match factor {
        TableFactor::Derived { subquery, .. } => transform_query(subquery, schemas),
        _ => Ok(false),
    }
}

// ---------------------------------------------------------------------------
// Bare FROM expansion (string-level, pre-parse)
// ---------------------------------------------------------------------------

/// Detect queries starting with `FROM` and prepend `SELECT * `.
fn expand_bare_from(sql: &str) -> String {
    let trimmed = sql.trim_start();
    if trimmed.len() >= 4 {
        let first_word = &trimmed[..4];
        if first_word.eq_ignore_ascii_case("FROM")
            && trimmed[4..].starts_with(|c: char| c.is_whitespace() || c == '(')
        {
            return format!("SELECT * {trimmed}");
        }
    }
    sql.to_string()
}

// ---------------------------------------------------------------------------
// Table / schema resolution
// ---------------------------------------------------------------------------

/// Resolve tables referenced in FROM clauses to their column information.
fn resolve_tables_in_scope(
    from: &[TableWithJoins],
    schemas: &HashMap<String, SchemaRef>,
) -> Vec<TableColumns> {
    let mut result = Vec::new();
    for twj in from {
        collect_table_columns(&twj.relation, schemas, &mut result);
        for join in &twj.joins {
            collect_table_columns(&join.relation, schemas, &mut result);
        }
    }
    result
}

fn collect_table_columns(
    factor: &TableFactor,
    schemas: &HashMap<String, SchemaRef>,
    out: &mut Vec<TableColumns>,
) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.to_string();
            if let Some(schema) = schemas.get(&table_name) {
                let alias_str = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table_name.clone());
                out.push(TableColumns {
                    alias: alias_str,
                    name: table_name,
                    columns: schema.fields().iter().map(|f| f.name().clone()).collect(),
                });
            }
        }
        TableFactor::Derived {
            alias: Some(_alias),
            ..
        } => {
            // For derived tables we don't have schema info at this stage.
            // They'll need to be handled separately if needed.
        }
        TableFactor::Derived { alias: None, .. } => {}
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// SELECT * EXCLUDE expansion
// ---------------------------------------------------------------------------

fn has_exclude(opts: &WildcardAdditionalOptions) -> bool {
    opts.opt_exclude.is_some()
}

/// Expand `* EXCLUDE(col1, col2)` or `t.* EXCLUDE(col)` into an explicit
/// column list.
fn expand_exclude_item(
    opts: &WildcardAdditionalOptions,
    tables: &[TableColumns],
    qualifier: Option<&str>,
) -> Result<Vec<SelectItem>, PreprocessError> {
    let exclude_cols: Vec<String> = match &opts.opt_exclude {
        Some(ExcludeSelectItem::Single(ident)) => vec![ident.value.clone()],
        Some(ExcludeSelectItem::Multiple(idents)) => {
            idents.iter().map(|i| i.value.clone()).collect()
        }
        None => {
            return Ok(vec![SelectItem::Wildcard(
                WildcardAdditionalOptions::default(),
            )]);
        }
    };

    // Determine which tables to expand.
    let target_tables: Vec<&TableColumns> = if let Some(q) = qualifier {
        tables.iter().filter(|t| t.alias == q).collect()
    } else {
        tables.iter().collect()
    };

    if target_tables.is_empty() {
        return Err(PreprocessError::NoTablesInScope);
    }

    // Validate that all excluded columns exist.
    let all_cols: Vec<&str> = target_tables
        .iter()
        .flat_map(|t| t.columns.iter().map(|c| c.as_str()))
        .collect();

    for exc in &exclude_cols {
        if !all_cols.iter().any(|c| c.eq_ignore_ascii_case(exc)) {
            return Err(PreprocessError::ExcludeColumnNotFound(exc.clone()));
        }
    }

    // Build explicit column list.
    let mut items = Vec::new();
    for table in &target_tables {
        let use_qualifier = qualifier.is_some() || target_tables.len() > 1;
        for col in &table.columns {
            if exclude_cols.iter().any(|e| e.eq_ignore_ascii_case(col)) {
                continue;
            }
            let expr = if use_qualifier {
                Expr::CompoundIdentifier(vec![Ident::new(&table.alias), Ident::new(col)])
            } else {
                Expr::Identifier(Ident::new(col))
            };
            items.push(SelectItem::UnnamedExpr(expr));
        }
    }

    Ok(items)
}

// ---------------------------------------------------------------------------
// COLUMNS('regex') expansion
// ---------------------------------------------------------------------------

fn is_columns_call(expr: &Expr) -> bool {
    if let Expr::Function(func) = expr {
        let name = func.name.to_string().to_uppercase();
        name == "COLUMNS"
    } else {
        false
    }
}

/// Expand `COLUMNS('pattern')` into explicit column references.
fn expand_columns_call(
    expr: &Expr,
    tables: &[TableColumns],
) -> Result<Vec<SelectItem>, PreprocessError> {
    let Expr::Function(func) = expr else {
        return Ok(vec![SelectItem::UnnamedExpr(expr.clone())]);
    };

    // Extract the regex pattern from the first argument.
    let pattern = extract_columns_pattern(func)?;
    let re = Regex::new(&pattern)
        .map_err(|e| PreprocessError::InvalidColumnsRegex(pattern.clone(), e.to_string()))?;

    let use_qualifier = tables.len() > 1;
    let mut items = Vec::new();
    for table in tables {
        for col in &table.columns {
            if re.is_match(col) {
                let expr = if use_qualifier {
                    Expr::CompoundIdentifier(vec![Ident::new(&table.alias), Ident::new(col)])
                } else {
                    Expr::Identifier(Ident::new(col))
                };
                items.push(SelectItem::UnnamedExpr(expr));
            }
        }
    }

    if items.is_empty() {
        return Err(PreprocessError::NoColumnsMatch(pattern));
    }

    Ok(items)
}

fn extract_columns_pattern(func: &Function) -> Result<String, PreprocessError> {
    match &func.args {
        FunctionArguments::List(arg_list) => {
            if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))) =
                arg_list.args.first()
            {
                match &v.value {
                    Value::SingleQuotedString(s) => Ok(s.clone()),
                    Value::DoubleQuotedString(s) => Ok(s.clone()),
                    _ => Err(PreprocessError::InvalidColumnsRegex(
                        v.to_string(),
                        "expected a string literal".to_string(),
                    )),
                }
            } else {
                Err(PreprocessError::InvalidColumnsRegex(
                    func.to_string(),
                    "expected a single string argument".to_string(),
                ))
            }
        }
        _ => Err(PreprocessError::InvalidColumnsRegex(
            func.to_string(),
            "expected function arguments".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// GROUP BY ALL expansion
// ---------------------------------------------------------------------------

/// Compute the GROUP BY columns for GROUP BY ALL: every SELECT expression that
/// does not contain an aggregate function or window function.
fn compute_group_by_all(projection: &[SelectItem]) -> Vec<Expr> {
    let mut group_exprs = Vec::new();

    for item in projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            // Wildcards cannot appear alongside GROUP BY ALL in practice.
            _ => continue,
        };

        if !contains_aggregate(expr) && !contains_window(expr) {
            group_exprs.push(expr.clone());
        }
    }

    group_exprs
}

/// Known aggregate function names (case-insensitive check).
const AGGREGATE_FUNCTIONS: &[&str] = &[
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "VARIANCE",
    "VAR_POP",
    "VAR_SAMP",
    "ARRAY_AGG",
    "STRING_AGG",
    "GROUP_CONCAT",
    "BOOL_AND",
    "BOOL_OR",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "FIRST",
    "FIRST_VALUE",
    "LAST",
    "LAST_VALUE",
    "ANY_VALUE",
    "MEDIAN",
    "APPROX_DISTINCT",
    "APPROX_MEDIAN",
    "APPROX_PERCENTILE_CONT",
    "APPROX_PERCENTILE_CONT_WITH_WEIGHT",
    "CORR",
    "COVAR_POP",
    "COVAR_SAMP",
    "REGR_SLOPE",
    "REGR_INTERCEPT",
    "REGR_COUNT",
    "REGR_R2",
    "REGR_AVGX",
    "REGR_AVGY",
    "REGR_SXX",
    "REGR_SYY",
    "REGR_SXY",
    "NTH_VALUE",
    "LISTAGG",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
];

/// Returns true if the expression contains an aggregate function call anywhere.
fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            if AGGREGATE_FUNCTIONS.iter().any(|a| *a == name) {
                return true;
            }
            // Check arguments recursively.
            if let FunctionArguments::List(args) = &func.args {
                for arg in &args.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } = arg
                    {
                        if contains_aggregate(e) {
                            return true;
                        }
                    }
                }
            }
            false
        }
        Expr::BinaryOp { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expr::Nested(inner) => contains_aggregate(inner),
        Expr::Cast { expr, .. } => contains_aggregate(expr),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_ref().is_some_and(|e| contains_aggregate(e))
                || conditions
                    .iter()
                    .any(|cw| contains_aggregate(&cw.condition) || contains_aggregate(&cw.result))
                || else_result.as_ref().is_some_and(|e| contains_aggregate(e))
        }
        Expr::InSubquery { expr, .. } => contains_aggregate(expr),
        Expr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        Expr::IsNull(e) | Expr::IsNotNull(e) => contains_aggregate(e),
        _ => false,
    }
}

/// Returns true if the expression contains a window function.
fn contains_window(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            if func.over.is_some() {
                return true;
            }
            if let FunctionArguments::List(args) = &func.args {
                for arg in &args.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } = arg
                    {
                        if contains_window(e) {
                            return true;
                        }
                    }
                }
            }
            false
        }
        Expr::BinaryOp { left, right, .. } => contains_window(left) || contains_window(right),
        Expr::UnaryOp { expr, .. } => contains_window(expr),
        Expr::Nested(inner) => contains_window(inner),
        Expr::Cast { expr, .. } => contains_window(expr),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schemas() -> HashMap<String, SchemaRef> {
        let mut schemas = HashMap::new();
        schemas.insert(
            "orders".to_string(),
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("region", DataType::Utf8, false),
                Field::new("status", DataType::Utf8, false),
                Field::new("amount", DataType::Float64, false),
                Field::new("created_at", DataType::Utf8, false),
            ])),
        );
        schemas.insert(
            "products".to_string(),
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("price_usd", DataType::Float64, false),
                Field::new("price_eur", DataType::Float64, false),
                Field::new("category", DataType::Utf8, false),
            ])),
        );
        schemas
    }

    #[test]
    fn test_passthrough_standard_sql() {
        let schemas = test_schemas();
        let sql = "SELECT id, region FROM orders WHERE status = 'active'";
        let result = preprocess_sql(sql, &schemas).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn test_bare_from() {
        let schemas = test_schemas();
        let sql = "FROM orders WHERE status = 'active'";
        let result = preprocess_sql(sql, &schemas).unwrap();
        // DuckDB dialect parser handles bare FROM by adding SELECT *.
        assert!(result.to_uppercase().contains("SELECT"));
        assert!(result.to_uppercase().contains("FROM ORDERS"));
        assert!(result.contains("active"));
    }

    #[test]
    fn test_bare_from_with_limit() {
        let schemas = test_schemas();
        let sql = "FROM orders LIMIT 10";
        let result = preprocess_sql(sql, &schemas).unwrap();
        assert!(result.to_uppercase().contains("SELECT"));
        assert!(result.to_uppercase().contains("LIMIT 10"));
    }

    #[test]
    fn test_group_by_all() {
        let schemas = test_schemas();
        let sql = "SELECT region, status, COUNT(*) FROM orders GROUP BY ALL";
        let result = preprocess_sql(sql, &schemas).unwrap();
        let upper = result.to_uppercase();
        assert!(!upper.contains("GROUP BY ALL"));
        assert!(upper.contains("GROUP BY"));
        assert!(upper.contains("REGION"));
        assert!(upper.contains("STATUS"));
        // COUNT(*) should NOT appear in GROUP BY.
        let group_by_part = upper.split("GROUP BY").nth(1).unwrap();
        assert!(!group_by_part.contains("COUNT"));
    }

    #[test]
    fn test_group_by_all_with_expression() {
        let schemas = test_schemas();
        let sql = "SELECT region, SUM(amount), AVG(amount) FROM orders GROUP BY ALL";
        let result = preprocess_sql(sql, &schemas).unwrap();
        let upper = result.to_uppercase();
        assert!(!upper.contains("GROUP BY ALL"));
        // Only region should be in GROUP BY.
        let group_by_part = upper.split("GROUP BY").nth(1).unwrap();
        assert!(group_by_part.contains("REGION"));
        assert!(!group_by_part.contains("SUM"));
        assert!(!group_by_part.contains("AVG"));
    }

    #[test]
    fn test_group_by_all_skips_window_functions() {
        let schemas = test_schemas();
        let sql =
            "SELECT region, COUNT(*), ROW_NUMBER() OVER (ORDER BY region) FROM orders GROUP BY ALL";
        let result = preprocess_sql(sql, &schemas).unwrap();
        let upper = result.to_uppercase();
        let group_by_part = upper.split("GROUP BY").nth(1).unwrap();
        assert!(group_by_part.contains("REGION"));
        assert!(!group_by_part.contains("ROW_NUMBER"));
        assert!(!group_by_part.contains("COUNT"));
    }

    #[test]
    fn test_exclude_single_column() {
        let schemas = test_schemas();
        let sql = "SELECT * EXCLUDE(id) FROM orders";
        let result = preprocess_sql(sql, &schemas).unwrap();
        let upper = result.to_uppercase();
        assert!(!upper.contains("EXCLUDE"));
        assert!(!upper.contains(" ID"));
        assert!(upper.contains("REGION"));
        assert!(upper.contains("STATUS"));
        assert!(upper.contains("AMOUNT"));
        assert!(upper.contains("CREATED_AT"));
    }

    #[test]
    fn test_exclude_multiple_columns() {
        let schemas = test_schemas();
        let sql = "SELECT * EXCLUDE(id, created_at) FROM orders";
        let result = preprocess_sql(sql, &schemas).unwrap();
        let upper = result.to_uppercase();
        assert!(!upper.contains("EXCLUDE"));
        assert!(upper.contains("REGION"));
        assert!(upper.contains("STATUS"));
        assert!(upper.contains("AMOUNT"));
        // id and created_at should be gone.
        let select_part = upper.split("FROM").next().unwrap();
        assert!(!select_part.contains(" ID"));
        assert!(!select_part.contains("CREATED_AT"));
    }

    #[test]
    fn test_exclude_column_not_found() {
        let schemas = test_schemas();
        let sql = "SELECT * EXCLUDE(nonexistent) FROM orders";
        let err = preprocess_sql(sql, &schemas).unwrap_err();
        assert!(matches!(err, PreprocessError::ExcludeColumnNotFound(_)));
    }

    #[test]
    fn test_columns_regex() {
        let schemas = test_schemas();
        let sql = "SELECT COLUMNS('price_.*') FROM products";
        let result = preprocess_sql(sql, &schemas).unwrap();
        let upper = result.to_uppercase();
        assert!(!upper.contains("COLUMNS("));
        assert!(upper.contains("PRICE_USD"));
        assert!(upper.contains("PRICE_EUR"));
        // Other columns should not appear.
        let select_part = upper.split("FROM").next().unwrap();
        assert!(!select_part.contains("CATEGORY"));
        assert!(!select_part.contains(" NAME"));
    }

    #[test]
    fn test_columns_no_match() {
        let schemas = test_schemas();
        let sql = "SELECT COLUMNS('zzz_.*') FROM products";
        let err = preprocess_sql(sql, &schemas).unwrap_err();
        assert!(matches!(err, PreprocessError::NoColumnsMatch(_)));
    }

    #[test]
    fn test_columns_invalid_regex() {
        let schemas = test_schemas();
        let sql = "SELECT COLUMNS('[invalid') FROM products";
        let err = preprocess_sql(sql, &schemas).unwrap_err();
        assert!(matches!(err, PreprocessError::InvalidColumnsRegex(..)));
    }

    #[test]
    fn test_unrecognized_syntax_passthrough() {
        let schemas = test_schemas();
        // Standard SQL that has no friendly syntax should pass through unchanged.
        let sql = "SELECT a, b FROM unknown_table WHERE x > 1";
        let result = preprocess_sql(sql, &schemas).unwrap();
        assert_eq!(result, sql);
    }
}
