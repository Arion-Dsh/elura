use async_trait::async_trait;
use elura_core::account_version::{
    AccountVersionKey, AccountVersionStore, MutableAccountVersionStore,
};
use elura_core::{Error, Result};
use sqlx::{MySqlPool, PgPool};

use super::{signed_version, validate_write};

pub const ACCOUNT_VERSION_SCHEMA_VERSION: i64 = 1;

#[derive(Clone)]
#[non_exhaustive]
pub enum SqlAccountVersionStore {
    Postgres(PgPool),
    MySql(MySqlPool),
}

impl SqlAccountVersionStore {
    pub async fn connect_postgres(url: &str) -> Result<Self> {
        Ok(Self::Postgres(
            PgPool::connect(url).await.map_err(sql_error)?,
        ))
    }

    pub async fn connect_mysql(url: &str) -> Result<Self> {
        Ok(Self::MySql(
            MySqlPool::connect(url).await.map_err(sql_error)?,
        ))
    }

    pub fn postgres(pool: PgPool) -> Self {
        Self::Postgres(pool)
    }

    pub fn mysql(pool: MySqlPool) -> Self {
        Self::MySql(pool)
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        match self {
            Self::Postgres(pool) => {
                if !crate::sql_migration::pending_postgres(
                    pool,
                    "account_version",
                    ACCOUNT_VERSION_SCHEMA_VERSION,
                )
                .await?
                {
                    return Ok(());
                }
                sqlx::query(
                    r#"CREATE TABLE IF NOT EXISTS elura_account_versions (
region_id BIGINT NOT NULL,
realm_id BIGINT NOT NULL,
user_id BIGINT NOT NULL,
version BIGINT NOT NULL CHECK (version > 0),
PRIMARY KEY (region_id, realm_id, user_id)
)"#,
                )
                .execute(pool)
                .await
                .map_err(sql_error)?;
                crate::sql_migration::record_postgres(
                    pool,
                    "account_version",
                    ACCOUNT_VERSION_SCHEMA_VERSION,
                )
                .await?;
            }
            Self::MySql(pool) => {
                if !crate::sql_migration::pending_mysql(
                    pool,
                    "account_version",
                    ACCOUNT_VERSION_SCHEMA_VERSION,
                )
                .await?
                {
                    return Ok(());
                }
                sqlx::query(
                    r#"CREATE TABLE IF NOT EXISTS elura_account_versions (
region_id BIGINT UNSIGNED NOT NULL,
realm_id BIGINT UNSIGNED NOT NULL,
user_id BIGINT NOT NULL,
version BIGINT UNSIGNED NOT NULL,
PRIMARY KEY (region_id, realm_id, user_id)
)"#,
                )
                .execute(pool)
                .await
                .map_err(sql_error)?;
                crate::sql_migration::record_mysql(
                    pool,
                    "account_version",
                    ACCOUNT_VERSION_SCHEMA_VERSION,
                )
                .await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AccountVersionStore for SqlAccountVersionStore {
    async fn current(&self, key: AccountVersionKey) -> Result<Option<u64>> {
        key.validate()?;
        let version = match self {
            Self::Postgres(pool) => {
                let version = sqlx::query_scalar::<_, i64>(
                    "SELECT version FROM elura_account_versions WHERE region_id=$1 AND realm_id=$2 AND user_id=$3",
                )
                .bind(i64::from(key.region_id))
                .bind(i64::from(key.realm_id))
                .bind(key.user_id)
                .fetch_optional(pool)
                .await
                .map_err(sql_error)?;
                version
                    .map(|value| {
                        u64::try_from(value)
                            .ok()
                            .filter(|version| *version != 0)
                            .ok_or_else(|| Error::Internal("invalid PostgreSQL account version".into()))
                    })
                    .transpose()?
            }
            Self::MySql(pool) => sqlx::query_scalar::<_, u64>(
                "SELECT version FROM elura_account_versions WHERE region_id=? AND realm_id=? AND user_id=?",
            )
            .bind(key.region_id)
            .bind(key.realm_id)
            .bind(key.user_id)
            .fetch_optional(pool)
            .await
            .map_err(sql_error)?,
        };
        if version == Some(0) {
            return Err(Error::Internal("invalid SQL account version".into()));
        }
        Ok(version)
    }
}

#[async_trait]
impl MutableAccountVersionStore for SqlAccountVersionStore {
    async fn set(&self, key: AccountVersionKey, version: u64) -> Result<()> {
        validate_write(key, version)?;
        match self {
            Self::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO elura_account_versions (region_id,realm_id,user_id,version) VALUES ($1,$2,$3,$4) ON CONFLICT (region_id,realm_id,user_id) DO UPDATE SET version=EXCLUDED.version",
                )
                .bind(i64::from(key.region_id))
                .bind(i64::from(key.realm_id))
                .bind(key.user_id)
                .bind(signed_version(version)?)
                .execute(pool)
                .await
                .map_err(sql_error)?;
            }
            Self::MySql(pool) => {
                sqlx::query(
                    "INSERT INTO elura_account_versions (region_id,realm_id,user_id,version) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE version=VALUES(version)",
                )
                .bind(key.region_id)
                .bind(key.realm_id)
                .bind(key.user_id)
                .bind(version)
                .execute(pool)
                .await
                .map_err(sql_error)?;
            }
        }
        Ok(())
    }

    async fn increment(&self, key: AccountVersionKey) -> Result<u64> {
        key.validate()?;
        match self {
            Self::Postgres(pool) => {
                let version = sqlx::query_scalar::<_, i64>(
                    "INSERT INTO elura_account_versions (region_id,realm_id,user_id,version) VALUES ($1,$2,$3,1) ON CONFLICT (region_id,realm_id,user_id) DO UPDATE SET version=elura_account_versions.version+1 RETURNING version",
                )
                .bind(i64::from(key.region_id))
                .bind(i64::from(key.realm_id))
                .bind(key.user_id)
                .fetch_one(pool)
                .await
                .map_err(sql_error)?;
                u64::try_from(version)
                    .ok()
                    .filter(|version| *version != 0)
                    .ok_or_else(|| Error::Internal("invalid PostgreSQL account version".into()))
            }
            Self::MySql(pool) => {
                let mut transaction = pool.begin().await.map_err(sql_error)?;
                sqlx::query(
                    "INSERT INTO elura_account_versions (region_id,realm_id,user_id,version) VALUES (?,?,?,1) ON DUPLICATE KEY UPDATE version=version+1",
                )
                .bind(key.region_id)
                .bind(key.realm_id)
                .bind(key.user_id)
                .execute(&mut *transaction)
                .await
                .map_err(sql_error)?;
                let version = sqlx::query_scalar::<_, u64>(
                    "SELECT version FROM elura_account_versions WHERE region_id=? AND realm_id=? AND user_id=?",
                )
                .bind(key.region_id)
                .bind(key.realm_id)
                .bind(key.user_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(sql_error)?;
                transaction.commit().await.map_err(sql_error)?;
                if version == 0 {
                    return Err(Error::Internal("invalid MySQL account version".into()));
                }
                Ok(version)
            }
        }
    }
}

fn sql_error(error: sqlx::Error) -> Error {
    Error::Internal(format!("SQL account version: {error}"))
}
