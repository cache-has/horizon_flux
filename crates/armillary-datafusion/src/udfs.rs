// Copyright (c) 2026 Horizon Analytic Studios, LLC. All rights reserved.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Reusable SQL user-defined functions (Layer 1 of the reuse story; see
//! `planning/29-reusable-transforms.md`).
//!
//! A UDF is a single-expression scalar function declared in a `*.sql` file
//! using PostgreSQL's `CREATE FUNCTION` syntax. At pipeline load time the
//! [`UdfRegistry`] discovers and parses every UDF in `udfs_dir`. Before each
//! SQL transform executes, [`UdfRegistry::inline`] rewrites calls to those
//! functions in the user's SQL into the function body, with the call's
//! argument expressions substituted for the parameter identifiers.
//!
//! Inlining operates entirely on the parsed sqlparser AST — no string
//! splicing, no quoting concerns. Bodies must be a single SQL expression;
//! anything else (multi-statement, RETURN, BEGIN/END) is rejected at load
//! time. This is the deliberate trade-off described in doc 29: scalar-only
//! UDFs cover ~60% of macro use cases without any of the templating-language
//! footguns.
//!
//! Example UDF file:
//!
//! ```sql
//! CREATE OR REPLACE FUNCTION normalize_name(s VARCHAR) RETURNS VARCHAR
//! AS $$
//!     LOWER(TRIM(s))
//! $$ LANGUAGE SQL IMMUTABLE;
//! ```

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use sqlparser::ast::{
    CreateFunctionBody, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName,
    Statement, Value, ValueWithSpan, visit_expressions_mut,
};
use sqlparser::dialect::{DuckDbDialect, PostgreSqlDialect};
use sqlparser::parser::{Parser, ParserError};

/// A single parsed UDF definition.
#[derive(Debug, Clone)]
pub struct UdfDefinition {
    /// Function name, lowercased for case-insensitive lookup.
    pub name: String,
    /// Parameter names in declaration order. Type strings are kept for
    /// display only — types are not enforced at inline time (DataFusion
    /// validates them when the inlined SQL runs).
    pub params: Vec<UdfParam>,
    /// Optional return type, for display.
    pub return_type: Option<String>,
    /// Parsed body expression (a single sqlparser `Expr`).
    pub body: Expr,
    /// Source file the UDF was loaded from.
    pub source_path: PathBuf,
}

/// A single UDF parameter.
#[derive(Debug, Clone)]
pub struct UdfParam {
    pub name: String,
    pub data_type: String,
}

impl UdfDefinition {
    /// Render a human-readable signature like `normalize_name(s VARCHAR) -> VARCHAR`.
    pub fn signature(&self) -> String {
        let args = self
            .params
            .iter()
            .map(|p| format!("{} {}", p.name, p.data_type))
            .collect::<Vec<_>>()
            .join(", ");
        match &self.return_type {
            Some(rt) => format!("{}({}) -> {}", self.name, args, rt),
            None => format!("{}({})", self.name, args),
        }
    }
}

/// A registry of UDFs available to a pipeline.
#[derive(Debug, Clone, Default)]
pub struct UdfRegistry {
    defs: HashMap<String, UdfDefinition>,
}

impl UdfRegistry {
    /// Build an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` when no UDFs are registered.
    pub fn is_empty(&self) -> bool {
        self.defs.is_empty()
    }

    /// Number of registered UDFs.
    pub fn len(&self) -> usize {
        self.defs.len()
    }

    /// Iterate over registered definitions in stable (alphabetical) order.
    pub fn iter(&self) -> impl Iterator<Item = &UdfDefinition> {
        let mut sorted: Vec<&UdfDefinition> = self.defs.values().collect();
        sorted.sort_by(|a, b| a.name.cmp(&b.name));
        sorted.into_iter()
    }

    /// Look up a UDF by case-insensitive name.
    pub fn get(&self, name: &str) -> Option<&UdfDefinition> {
        self.defs.get(&name.to_ascii_lowercase())
    }

    /// Discover and parse every `*.sql` file under `dir`. Returns the populated
    /// registry. Files are sorted by path so collisions and errors are
    /// deterministic.
    pub fn load_from_dir(dir: &Path) -> Result<Self, UdfError> {
        let mut paths: Vec<PathBuf> = Vec::new();
        let entries = std::fs::read_dir(dir).map_err(|e| UdfError::ReadDir {
            path: dir.to_path_buf(),
            source: e,
        })?;
        for entry in entries {
            let entry = entry.map_err(|e| UdfError::ReadDir {
                path: dir.to_path_buf(),
                source: e,
            })?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("sql") {
                paths.push(path);
            }
        }
        paths.sort();

        let mut registry = UdfRegistry::new();
        for path in paths {
            let sql = std::fs::read_to_string(&path).map_err(|e| UdfError::ReadFile {
                path: path.clone(),
                source: e,
            })?;
            let dialect = PostgreSqlDialect {};
            let statements = Parser::parse_sql(&dialect, &sql).map_err(|e| UdfError::Parse {
                path: path.clone(),
                detail: e.to_string(),
            })?;
            if statements.is_empty() {
                return Err(UdfError::Empty { path });
            }
            for stmt in statements {
                let def = parse_create_function(stmt, &path)?;
                if let Some(prev) = registry.defs.insert(def.name.clone(), def.clone()) {
                    return Err(UdfError::DuplicateName {
                        name: def.name,
                        first: prev.source_path,
                        second: def.source_path,
                    });
                }
            }
        }
        Ok(registry)
    }

    /// Inline UDF calls in `sql`. Returns the rewritten SQL with every call to
    /// a registered UDF replaced by its body expression (with parameters
    /// substituted). When the registry is empty, returns `sql` unchanged.
    pub fn inline(&self, sql: &str) -> Result<String, UdfError> {
        if self.is_empty() {
            return Ok(sql.to_string());
        }
        let dialect = DuckDbDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).map_err(|e| UdfError::CallSite {
            detail: format!("could not parse SQL for UDF inlining: {e}"),
        })?;

        let mut error: Option<UdfError> = None;
        let _ = visit_expressions_mut(&mut statements, |expr| {
            if let Expr::Function(func) = expr {
                if let Some(name) = single_ident(&func.name) {
                    if let Some(def) = self.get(&name) {
                        match expand_call(def, &func.args) {
                            Ok(replacement) => {
                                *expr = replacement;
                            }
                            Err(e) => {
                                error = Some(e);
                                return ControlFlow::Break(());
                            }
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        });
        if let Some(e) = error {
            return Err(e);
        }

        Ok(statements
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; "))
    }
}

fn single_ident(name: &ObjectName) -> Option<String> {
    if name.0.len() != 1 {
        return None;
    }
    name.0
        .first()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.to_ascii_lowercase())
}

fn parse_create_function(stmt: Statement, path: &Path) -> Result<UdfDefinition, UdfError> {
    let cf = match stmt {
        Statement::CreateFunction(cf) => cf,
        other => {
            return Err(UdfError::NotCreateFunction {
                path: path.to_path_buf(),
                statement: other.to_string(),
            });
        }
    };

    let name = single_ident(&cf.name).ok_or_else(|| UdfError::QualifiedName {
        path: path.to_path_buf(),
        name: cf.name.to_string(),
    })?;

    let mut params = Vec::new();
    if let Some(args) = &cf.args {
        for (idx, arg) in args.iter().enumerate() {
            let pname = arg.name.as_ref().map(|i| i.value.clone()).ok_or_else(|| {
                UdfError::UnnamedParam {
                    path: path.to_path_buf(),
                    function: name.clone(),
                    index: idx,
                }
            })?;
            params.push(UdfParam {
                name: pname,
                data_type: arg.data_type.to_string(),
            });
        }
    }

    let return_type = cf.return_type.as_ref().map(|t| t.to_string());

    let body_string = match cf.function_body {
        Some(CreateFunctionBody::AsBeforeOptions { body, .. })
        | Some(CreateFunctionBody::AsAfterOptions(body)) => extract_body_string(body, path, &name)?,
        Some(_) | None => {
            return Err(UdfError::UnsupportedBody {
                path: path.to_path_buf(),
                function: name,
            });
        }
    };

    let dialect = PostgreSqlDialect {};
    let body_expr = Parser::new(&dialect)
        .try_with_sql(&body_string)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e: ParserError| UdfError::BodyParse {
            path: path.to_path_buf(),
            function: name.clone(),
            detail: e.to_string(),
        })?;

    Ok(UdfDefinition {
        name,
        params,
        return_type,
        body: body_expr,
        source_path: path.to_path_buf(),
    })
}

fn extract_body_string(expr: Expr, path: &Path, function: &str) -> Result<String, UdfError> {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            Value::DollarQuotedString(s) => Ok(s.value),
            Value::SingleQuotedString(s)
            | Value::DoubleQuotedString(s)
            | Value::TripleSingleQuotedString(s)
            | Value::TripleDoubleQuotedString(s) => Ok(s),
            other => Err(UdfError::UnsupportedBody {
                path: path.to_path_buf(),
                function: format!("{function} (unsupported body literal: {other:?})"),
            }),
        },
        // Some dialects parse the body directly as an Expr; render it back to SQL.
        other => Ok(other.to_string()),
    }
}

fn expand_call(def: &UdfDefinition, args: &FunctionArguments) -> Result<Expr, UdfError> {
    let arg_exprs: Vec<Expr> = match args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e.clone()),
                FunctionArg::ExprNamed {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } => Ok(e.clone()),
                FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } => Ok(e.clone()),
                _ => Err(UdfError::CallSite {
                    detail: format!(
                        "UDF `{}` was called with a wildcard or qualified-wildcard argument",
                        def.name
                    ),
                }),
            })
            .collect::<Result<Vec<_>, _>>()?,
        FunctionArguments::None => Vec::new(),
        FunctionArguments::Subquery(_) => {
            return Err(UdfError::CallSite {
                detail: format!("UDF `{}` was called with a subquery argument", def.name),
            });
        }
    };

    if arg_exprs.len() != def.params.len() {
        return Err(UdfError::CallSite {
            detail: format!(
                "UDF `{}` expects {} argument(s) but was called with {}",
                def.name,
                def.params.len(),
                arg_exprs.len()
            ),
        });
    }

    let bindings: HashMap<String, Expr> = def
        .params
        .iter()
        .zip(arg_exprs)
        .map(|(p, e)| (p.name.to_ascii_lowercase(), e))
        .collect();

    let mut body = def.body.clone();
    substitute_in_expr(&mut body, &bindings);
    // Wrap in parentheses to preserve operator precedence at the call site.
    Ok(Expr::Nested(Box::new(body)))
}

/// Substitute parameter identifiers inside `expr` with their bound argument
/// expressions. Only single, unqualified [`Expr::Identifier`] occurrences are
/// substituted; compound identifiers like `t.col` are left alone, so users
/// passing column references as arguments still get the right resolution.
fn substitute_in_expr(expr: &mut Expr, bindings: &HashMap<String, Expr>) {
    let _ = visit_expressions_mut(expr, |e| {
        if let Expr::Identifier(Ident { value, .. }) = e {
            if let Some(replacement) = bindings.get(&value.to_ascii_lowercase()) {
                *e = Expr::Nested(Box::new(replacement.clone()));
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Errors produced while loading or inlining UDFs.
#[derive(Debug, thiserror::Error)]
pub enum UdfError {
    #[error("could not read UDF directory `{path}`: {source}")]
    ReadDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("could not read UDF file `{path}`: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("UDF file `{path}` parse error: {detail}")]
    Parse { path: PathBuf, detail: String },
    #[error("UDF file `{path}` is empty")]
    Empty { path: PathBuf },
    #[error("UDF file `{path}` contains a non-`CREATE FUNCTION` statement: `{statement}`")]
    NotCreateFunction { path: PathBuf, statement: String },
    #[error(
        "UDF file `{path}` declares a qualified function name `{name}`; only single-segment names are supported"
    )]
    QualifiedName { path: PathBuf, name: String },
    #[error("UDF `{function}` in `{path}` has an unnamed parameter at position {index}")]
    UnnamedParam {
        path: PathBuf,
        function: String,
        index: usize,
    },
    #[error(
        "UDF `{function}` in `{path}` has an unsupported body (must be `AS $$ <expression> $$`)"
    )]
    UnsupportedBody { path: PathBuf, function: String },
    #[error("UDF `{function}` body in `{path}` is not a valid SQL expression: {detail}")]
    BodyParse {
        path: PathBuf,
        function: String,
        detail: String,
    },
    #[error("UDF `{name}` is defined twice (`{first}` and `{second}`)")]
    DuplicateName {
        name: String,
        first: PathBuf,
        second: PathBuf,
    },
    #[error("UDF call-site error: {detail}")]
    CallSite { detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn write_udf(dir: &Path, name: &str, body: &str) {
        fs::write(dir.join(name), body).unwrap();
    }

    #[test]
    fn loads_and_inlines_normalize_name() {
        let dir = tempdir().unwrap();
        write_udf(
            dir.path(),
            "normalize_name.sql",
            r#"CREATE OR REPLACE FUNCTION normalize_name(s VARCHAR) RETURNS VARCHAR
AS $$
    LOWER(TRIM(s))
$$ LANGUAGE SQL IMMUTABLE;
"#,
        );

        let registry = UdfRegistry::load_from_dir(dir.path()).expect("load");
        assert_eq!(registry.len(), 1);
        let def = registry.get("normalize_name").unwrap();
        assert_eq!(def.params.len(), 1);
        assert_eq!(def.params[0].name, "s");

        let out = registry
            .inline("SELECT id, normalize_name(customer_name) AS name FROM customers")
            .unwrap();
        // The body LOWER(TRIM(s)) should appear with `s` replaced by the
        // call-site argument `customer_name`.
        let lower = out.to_ascii_lowercase();
        assert!(lower.contains("lower(trim("), "got: {out}");
        assert!(lower.contains("customer_name"), "got: {out}");
        // The original call must be gone.
        assert!(
            !lower.contains("normalize_name("),
            "call should be inlined, got: {out}"
        );
    }

    #[test]
    fn arg_count_mismatch_is_an_error() {
        let dir = tempdir().unwrap();
        write_udf(
            dir.path(),
            "f.sql",
            "CREATE FUNCTION f(a VARCHAR, b VARCHAR) RETURNS VARCHAR AS $$ a $$ LANGUAGE SQL;",
        );
        let registry = UdfRegistry::load_from_dir(dir.path()).unwrap();
        let err = registry.inline("SELECT f(x) FROM t").unwrap_err();
        assert!(matches!(err, UdfError::CallSite { .. }), "got: {err:?}");
    }

    #[test]
    fn unrelated_calls_are_left_alone() {
        let dir = tempdir().unwrap();
        write_udf(
            dir.path(),
            "f.sql",
            "CREATE FUNCTION f(a VARCHAR) RETURNS VARCHAR AS $$ a $$ LANGUAGE SQL;",
        );
        let registry = UdfRegistry::load_from_dir(dir.path()).unwrap();
        let out = registry.inline("SELECT upper(name) FROM t").unwrap();
        assert!(out.to_ascii_lowercase().contains("upper(name)"));
    }

    #[test]
    fn duplicate_definitions_rejected() {
        let dir = tempdir().unwrap();
        write_udf(
            dir.path(),
            "a.sql",
            "CREATE FUNCTION dup(s VARCHAR) RETURNS VARCHAR AS $$ s $$ LANGUAGE SQL;",
        );
        write_udf(
            dir.path(),
            "b.sql",
            "CREATE FUNCTION dup(s VARCHAR) RETURNS VARCHAR AS $$ s $$ LANGUAGE SQL;",
        );
        let err = UdfRegistry::load_from_dir(dir.path()).unwrap_err();
        assert!(
            matches!(err, UdfError::DuplicateName { .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn parse_error_includes_path() {
        let dir = tempdir().unwrap();
        write_udf(dir.path(), "broken.sql", "this is not sql at all");
        let err = UdfRegistry::load_from_dir(dir.path()).unwrap_err();
        match err {
            UdfError::Parse { path, .. } => assert!(path.ends_with("broken.sql")),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn signature_renders() {
        let dir = tempdir().unwrap();
        write_udf(
            dir.path(),
            "f.sql",
            "CREATE FUNCTION f(a VARCHAR, b INT) RETURNS VARCHAR AS $$ a $$ LANGUAGE SQL;",
        );
        let registry = UdfRegistry::load_from_dir(dir.path()).unwrap();
        let sig = registry.get("f").unwrap().signature();
        assert!(sig.contains("f("), "got: {sig}");
        assert!(sig.contains("->"), "got: {sig}");
    }
}
