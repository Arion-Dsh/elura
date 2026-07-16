use elura_core::{Error, Result};
use sqlx::{MySqlPool, PgPool};

pub(crate) async fn pending_postgres(pool: &PgPool, component: &str, target: i64) -> Result<bool> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS elura_schema_migrations (component VARCHAR(64) PRIMARY KEY, version BIGINT NOT NULL)",
    )
    .execute(pool)
    .await
    .map_err(sql_error)?;
    let current = sqlx::query_scalar::<_, i64>(
        "SELECT version FROM elura_schema_migrations WHERE component=$1",
    )
    .bind(component)
    .fetch_optional(pool)
    .await
    .map_err(sql_error)?;
    validate_version(component, current, target)
}

pub(crate) async fn pending_mysql(pool: &MySqlPool, component: &str, target: i64) -> Result<bool> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS elura_schema_migrations (component VARCHAR(64) PRIMARY KEY, version BIGINT NOT NULL)",
    )
    .execute(pool)
    .await
    .map_err(sql_error)?;
    let current = sqlx::query_scalar::<_, i64>(
        "SELECT version FROM elura_schema_migrations WHERE component=?",
    )
    .bind(component)
    .fetch_optional(pool)
    .await
    .map_err(sql_error)?;
    validate_version(component, current, target)
}

pub(crate) async fn record_postgres(pool: &PgPool, component: &str, version: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO elura_schema_migrations (component,version) VALUES ($1,$2) ON CONFLICT (component) DO NOTHING",
    )
    .bind(component)
    .bind(version)
    .execute(pool)
    .await
    .map_err(sql_error)?;
    Ok(())
}

pub(crate) async fn record_mysql(pool: &MySqlPool, component: &str, version: i64) -> Result<()> {
    sqlx::query("INSERT IGNORE INTO elura_schema_migrations (component,version) VALUES (?,?)")
        .bind(component)
        .bind(version)
        .execute(pool)
        .await
        .map_err(sql_error)?;
    Ok(())
}

fn validate_version(component: &str, current: Option<i64>, target: i64) -> Result<bool> {
    match current {
        None => Ok(true),
        Some(version) if version == target => Ok(false),
        Some(version) => Err(Error::InvalidConfig(format!(
            "unsupported {component} schema version {version}; expected {target}"
        ))),
    }
}

fn sql_error(error: sqlx::Error) -> Error {
    Error::Internal(format!("SQL migration: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_versions_are_explicit_and_never_guessed() {
        assert!(validate_version("outbox", None, 1).unwrap());
        assert!(!validate_version("outbox", Some(1), 1).unwrap());
        assert!(matches!(
            validate_version("outbox", Some(2), 1),
            Err(Error::InvalidConfig(_))
        ));
    }
}
