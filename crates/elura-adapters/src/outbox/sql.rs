use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use elura_core::{Error, Result};
use sqlx::{MySql, MySqlPool, PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use super::contract::validate_reason;
use super::{DeadLetter, OutboxDelivery, OutboxEvent, OutboxStore};

pub const OUTBOX_SCHEMA_VERSION: i64 = 1;

pub enum SqlOutbox {
    Postgres(PgPool),
    MySql(MySqlPool),
}

impl SqlOutbox {
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
                if !crate::sql_migration::pending_postgres(pool, "outbox", OUTBOX_SCHEMA_VERSION)
                    .await?
                {
                    return Ok(());
                }
                sqlx::query(
                    r#"CREATE TABLE IF NOT EXISTS elura_outbox (
id VARCHAR(36) PRIMARY KEY,
event_json TEXT NOT NULL,
created_at BIGINT NOT NULL,
available_at BIGINT NOT NULL,
attempts INTEGER NOT NULL DEFAULT 0,
leased_by TEXT NULL,
lease_token VARCHAR(36) NULL,
lease_until BIGINT NULL,
last_error TEXT NULL,
dead_at BIGINT NULL,
completed_at BIGINT NULL
)"#,
                )
                .execute(pool)
                .await
                .map_err(sql_error)?;
                sqlx::query(
                    "CREATE INDEX IF NOT EXISTS elura_outbox_ready ON elura_outbox (available_at, created_at) WHERE dead_at IS NULL AND completed_at IS NULL",
                )
                .execute(pool)
                .await
                .map_err(sql_error)?;
                crate::sql_migration::record_postgres(pool, "outbox", OUTBOX_SCHEMA_VERSION)
                    .await?;
            }
            Self::MySql(pool) => {
                if !crate::sql_migration::pending_mysql(pool, "outbox", OUTBOX_SCHEMA_VERSION)
                    .await?
                {
                    return Ok(());
                }
                sqlx::query(
                    r#"CREATE TABLE IF NOT EXISTS elura_outbox (
id VARCHAR(36) PRIMARY KEY,
event_json LONGTEXT NOT NULL,
created_at BIGINT NOT NULL,
available_at BIGINT NOT NULL,
attempts INTEGER NOT NULL DEFAULT 0,
leased_by TEXT NULL,
lease_token VARCHAR(36) NULL,
lease_until BIGINT NULL,
last_error TEXT NULL,
dead_at BIGINT NULL,
completed_at BIGINT NULL,
INDEX elura_outbox_ready (available_at, created_at)
)"#,
                )
                .execute(pool)
                .await
                .map_err(sql_error)?;
                crate::sql_migration::record_mysql(pool, "outbox", OUTBOX_SCHEMA_VERSION).await?;
            }
        }
        Ok(())
    }

    /// Appends an event using the caller's PostgreSQL transaction, allowing the
    /// business mutation and outbox record to commit atomically.
    pub async fn append_postgres_tx(
        tx: &mut Transaction<'_, Postgres>,
        event: &OutboxEvent,
    ) -> Result<()> {
        event.validate()?;
        let encoded = serde_json::to_string(event)?;
        let inserted = sqlx::query(
            "INSERT INTO elura_outbox (id,event_json,created_at,available_at) VALUES ($1,$2,$3,$4) ON CONFLICT (id) DO NOTHING",
        )
        .bind(event.id.to_string())
        .bind(&encoded)
        .bind(millis(event.created_at)?)
        .bind(millis(event.available_at)?)
        .execute(&mut **tx)
        .await
        .map_err(sql_error)?
        .rows_affected();
        if inserted == 1 {
            return Ok(());
        }
        let existing: String =
            sqlx::query_scalar("SELECT event_json FROM elura_outbox WHERE id=$1")
                .bind(event.id.to_string())
                .fetch_one(&mut **tx)
                .await
                .map_err(sql_error)?;
        compare_existing(&existing, event)
    }

    /// MySQL equivalent of [`Self::append_postgres_tx`].
    pub async fn append_mysql_tx(
        tx: &mut Transaction<'_, MySql>,
        event: &OutboxEvent,
    ) -> Result<()> {
        event.validate()?;
        let encoded = serde_json::to_string(event)?;
        let inserted = sqlx::query(
            "INSERT IGNORE INTO elura_outbox (id,event_json,created_at,available_at) VALUES (?,?,?,?)",
        )
        .bind(event.id.to_string())
        .bind(&encoded)
        .bind(millis(event.created_at)?)
        .bind(millis(event.available_at)?)
        .execute(&mut **tx)
        .await
        .map_err(sql_error)?
        .rows_affected();
        if inserted == 1 {
            return Ok(());
        }
        let existing: String = sqlx::query_scalar("SELECT event_json FROM elura_outbox WHERE id=?")
            .bind(event.id.to_string())
            .fetch_one(&mut **tx)
            .await
            .map_err(sql_error)?;
        compare_existing(&existing, event)
    }

    async fn acquire_postgres(
        pool: &PgPool,
        worker: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<OutboxDelivery>> {
        validate_lease(worker, limit, lease)?;
        let now = now_millis()?;
        let lease_until = SystemTime::now() + lease;
        let lease_until_ms = millis(lease_until)?;
        let mut tx = pool.begin().await.map_err(sql_error)?;
        let rows = sqlx::query(
            "SELECT id,event_json,attempts,available_at FROM elura_outbox WHERE completed_at IS NULL AND dead_at IS NULL AND available_at<=$1 AND (lease_until IS NULL OR lease_until<=$1) ORDER BY created_at,id LIMIT $2 FOR UPDATE SKIP LOCKED",
        )
        .bind(now)
        .bind(limit_i64(limit)?)
        .fetch_all(&mut *tx)
        .await
        .map_err(sql_error)?;
        let mut deliveries = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id").map_err(sql_error)?;
            let token = Uuid::new_v4();
            let attempt = attempt(row.try_get::<i32, _>("attempts").map_err(sql_error)?)? + 1;
            sqlx::query("UPDATE elura_outbox SET attempts=$2,leased_by=$3,lease_token=$4,lease_until=$5 WHERE id=$1")
                .bind(&id)
                .bind(i32::try_from(attempt).map_err(|_| Error::Internal("outbox attempt overflow".into()))?)
                .bind(worker)
                .bind(token.to_string())
                .bind(lease_until_ms)
                .execute(&mut *tx)
                .await
                .map_err(sql_error)?;
            deliveries.push(delivery_from_row(row, worker, token, attempt, lease_until)?);
        }
        tx.commit().await.map_err(sql_error)?;
        Ok(deliveries)
    }

    async fn acquire_mysql(
        pool: &MySqlPool,
        worker: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<OutboxDelivery>> {
        validate_lease(worker, limit, lease)?;
        let now = now_millis()?;
        let lease_until = SystemTime::now() + lease;
        let lease_until_ms = millis(lease_until)?;
        let mut tx = pool.begin().await.map_err(sql_error)?;
        let rows = sqlx::query(
            "SELECT id,event_json,attempts,available_at FROM elura_outbox WHERE completed_at IS NULL AND dead_at IS NULL AND available_at<=? AND (lease_until IS NULL OR lease_until<=?) ORDER BY created_at,id LIMIT ? FOR UPDATE SKIP LOCKED",
        )
        .bind(now)
        .bind(now)
        .bind(limit_i64(limit)?)
        .fetch_all(&mut *tx)
        .await
        .map_err(sql_error)?;
        let mut deliveries = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id").map_err(sql_error)?;
            let token = Uuid::new_v4();
            let attempt = attempt(row.try_get::<i32, _>("attempts").map_err(sql_error)?)? + 1;
            sqlx::query("UPDATE elura_outbox SET attempts=?,leased_by=?,lease_token=?,lease_until=? WHERE id=?")
                .bind(i32::try_from(attempt).map_err(|_| Error::Internal("outbox attempt overflow".into()))?)
                .bind(worker)
                .bind(token.to_string())
                .bind(lease_until_ms)
                .bind(&id)
                .execute(&mut *tx)
                .await
                .map_err(sql_error)?;
            deliveries.push(delivery_from_row(row, worker, token, attempt, lease_until)?);
        }
        tx.commit().await.map_err(sql_error)?;
        Ok(deliveries)
    }
}

#[async_trait]
impl OutboxStore for SqlOutbox {
    async fn append(&self, event: OutboxEvent) -> Result<()> {
        match self {
            Self::Postgres(pool) => {
                let mut tx = pool.begin().await.map_err(sql_error)?;
                Self::append_postgres_tx(&mut tx, &event).await?;
                tx.commit().await.map_err(sql_error)
            }
            Self::MySql(pool) => {
                let mut tx = pool.begin().await.map_err(sql_error)?;
                Self::append_mysql_tx(&mut tx, &event).await?;
                tx.commit().await.map_err(sql_error)
            }
        }
    }

    async fn acquire(
        &self,
        worker: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<OutboxDelivery>> {
        match self {
            Self::Postgres(pool) => Self::acquire_postgres(pool, worker, limit, lease).await,
            Self::MySql(pool) => Self::acquire_mysql(pool, worker, limit, lease).await,
        }
    }

    async fn ack(&self, delivery: &OutboxDelivery) -> Result<()> {
        let now = now_millis()?;
        let affected = match self {
            Self::Postgres(pool) => sqlx::query("UPDATE elura_outbox SET completed_at=$5,leased_by=NULL,lease_token=NULL,lease_until=NULL WHERE id=$1 AND leased_by=$2 AND lease_token=$3 AND lease_until>$4 AND completed_at IS NULL AND dead_at IS NULL")
                .bind(delivery.event.id.to_string()).bind(&delivery.worker).bind(delivery.token.to_string()).bind(now).bind(now).execute(pool).await.map_err(sql_error)?.rows_affected(),
            Self::MySql(pool) => sqlx::query("UPDATE elura_outbox SET completed_at=?,leased_by=NULL,lease_token=NULL,lease_until=NULL WHERE id=? AND leased_by=? AND lease_token=? AND lease_until>? AND completed_at IS NULL AND dead_at IS NULL")
                .bind(now).bind(delivery.event.id.to_string()).bind(&delivery.worker).bind(delivery.token.to_string()).bind(now).execute(pool).await.map_err(sql_error)?.rows_affected(),
        };
        fenced(affected)
    }

    async fn renew(&self, delivery: &OutboxDelivery, lease: Duration) -> Result<()> {
        if lease.is_zero() {
            return Err(Error::InvalidConfig("invalid outbox lease".into()));
        }
        let now = now_millis()?;
        let lease_until = millis(SystemTime::now() + lease)?;
        let affected = match self {
            Self::Postgres(pool) => sqlx::query("UPDATE elura_outbox SET lease_until=$5 WHERE id=$1 AND leased_by=$2 AND lease_token=$3 AND lease_until>$4 AND completed_at IS NULL AND dead_at IS NULL")
                .bind(delivery.event.id.to_string()).bind(&delivery.worker).bind(delivery.token.to_string()).bind(now).bind(lease_until).execute(pool).await.map_err(sql_error)?.rows_affected(),
            Self::MySql(pool) => sqlx::query("UPDATE elura_outbox SET lease_until=? WHERE id=? AND leased_by=? AND lease_token=? AND lease_until>? AND completed_at IS NULL AND dead_at IS NULL")
                .bind(lease_until).bind(delivery.event.id.to_string()).bind(&delivery.worker).bind(delivery.token.to_string()).bind(now).execute(pool).await.map_err(sql_error)?.rows_affected(),
        };
        fenced(affected)
    }

    async fn retry(
        &self,
        delivery: &OutboxDelivery,
        available_at: SystemTime,
        reason: &str,
    ) -> Result<()> {
        validate_reason(reason)?;
        let now = now_millis()?;
        let mut event = delivery.event.clone();
        event.available_at = available_at.max(SystemTime::now());
        let encoded = serde_json::to_string(&event)?;
        let available = millis(event.available_at)?;
        let affected = match self {
            Self::Postgres(pool) => sqlx::query("UPDATE elura_outbox SET event_json=$5,available_at=$6,last_error=$7,leased_by=NULL,lease_token=NULL,lease_until=NULL WHERE id=$1 AND leased_by=$2 AND lease_token=$3 AND lease_until>$4 AND completed_at IS NULL AND dead_at IS NULL")
                .bind(event.id.to_string()).bind(&delivery.worker).bind(delivery.token.to_string()).bind(now).bind(encoded).bind(available).bind(reason).execute(pool).await.map_err(sql_error)?.rows_affected(),
            Self::MySql(pool) => sqlx::query("UPDATE elura_outbox SET event_json=?,available_at=?,last_error=?,leased_by=NULL,lease_token=NULL,lease_until=NULL WHERE id=? AND leased_by=? AND lease_token=? AND lease_until>? AND completed_at IS NULL AND dead_at IS NULL")
                .bind(encoded).bind(available).bind(reason).bind(event.id.to_string()).bind(&delivery.worker).bind(delivery.token.to_string()).bind(now).execute(pool).await.map_err(sql_error)?.rows_affected(),
        };
        fenced(affected)
    }

    async fn dead_letter(&self, delivery: &OutboxDelivery, reason: &str) -> Result<()> {
        validate_reason(reason)?;
        let now = now_millis()?;
        let affected = match self {
            Self::Postgres(pool) => sqlx::query("UPDATE elura_outbox SET dead_at=$5,last_error=$6,leased_by=NULL,lease_token=NULL,lease_until=NULL WHERE id=$1 AND leased_by=$2 AND lease_token=$3 AND lease_until>$4 AND completed_at IS NULL AND dead_at IS NULL")
                .bind(delivery.event.id.to_string()).bind(&delivery.worker).bind(delivery.token.to_string()).bind(now).bind(now).bind(reason).execute(pool).await.map_err(sql_error)?.rows_affected(),
            Self::MySql(pool) => sqlx::query("UPDATE elura_outbox SET dead_at=?,last_error=?,leased_by=NULL,lease_token=NULL,lease_until=NULL WHERE id=? AND leased_by=? AND lease_token=? AND lease_until>? AND completed_at IS NULL AND dead_at IS NULL")
                .bind(now).bind(reason).bind(delivery.event.id.to_string()).bind(&delivery.worker).bind(delivery.token.to_string()).bind(now).execute(pool).await.map_err(sql_error)?.rows_affected(),
        };
        fenced(affected)
    }

    async fn list_dead_letters(&self, limit: usize) -> Result<Vec<DeadLetter>> {
        if limit == 0 {
            return Err(Error::InvalidConfig("dead-letter limit is zero".into()));
        }
        match self {
            Self::Postgres(pool) => sqlx::query("SELECT event_json,attempts,last_error,dead_at FROM elura_outbox WHERE dead_at IS NOT NULL ORDER BY dead_at DESC LIMIT $1")
                .bind(limit_i64(limit)?).fetch_all(pool).await.map_err(sql_error)?
                .into_iter().map(dead_from_row).collect(),
            Self::MySql(pool) => sqlx::query("SELECT event_json,attempts,last_error,dead_at FROM elura_outbox WHERE dead_at IS NOT NULL ORDER BY dead_at DESC LIMIT ?")
                .bind(limit_i64(limit)?).fetch_all(pool).await.map_err(sql_error)?
                .into_iter().map(dead_from_row).collect(),
        }
    }

    async fn replay_dead_letter(&self, id: Uuid, available_at: SystemTime) -> Result<()> {
        let available_at = available_at.max(SystemTime::now());
        let encoded = match self {
            Self::Postgres(pool) => sqlx::query_scalar::<_, String>(
                "SELECT event_json FROM elura_outbox WHERE id=$1 AND dead_at IS NOT NULL",
            )
            .bind(id.to_string())
            .fetch_optional(pool)
            .await
            .map_err(sql_error)?,
            Self::MySql(pool) => sqlx::query_scalar::<_, String>(
                "SELECT event_json FROM elura_outbox WHERE id=? AND dead_at IS NOT NULL",
            )
            .bind(id.to_string())
            .fetch_optional(pool)
            .await
            .map_err(sql_error)?,
        };
        let Some(encoded) = encoded else {
            return Err(Error::OutboxNotFound);
        };
        let mut event: OutboxEvent = serde_json::from_str(&encoded)?;
        event.available_at = available_at;
        let encoded = serde_json::to_string(&event)?;
        let available = millis(available_at)?;
        let affected = match self {
            Self::Postgres(pool) => sqlx::query("UPDATE elura_outbox SET event_json=$2,available_at=$3,attempts=0,leased_by=NULL,lease_token=NULL,lease_until=NULL,last_error=NULL,dead_at=NULL WHERE id=$1 AND dead_at IS NOT NULL")
                .bind(id.to_string()).bind(encoded).bind(available).execute(pool).await.map_err(sql_error)?.rows_affected(),
            Self::MySql(pool) => sqlx::query("UPDATE elura_outbox SET event_json=?,available_at=?,attempts=0,leased_by=NULL,lease_token=NULL,lease_until=NULL,last_error=NULL,dead_at=NULL WHERE id=? AND dead_at IS NOT NULL")
                .bind(encoded).bind(available).bind(id.to_string()).execute(pool).await.map_err(sql_error)?.rows_affected(),
        };
        if affected == 1 {
            Ok(())
        } else {
            Err(Error::OutboxNotFound)
        }
    }
}

fn delivery_from_row<R: Row>(
    row: R,
    worker: &str,
    token: Uuid,
    attempt: u32,
    lease_until: SystemTime,
) -> Result<OutboxDelivery>
where
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    let encoded: String = row.try_get(1).map_err(sql_error)?;
    let available: i64 = row.try_get(3).map_err(sql_error)?;
    let mut event: OutboxEvent = serde_json::from_str(&encoded)?;
    event.available_at = from_millis(available)?;
    Ok(OutboxDelivery {
        event,
        attempt,
        worker: worker.to_owned(),
        token,
        lease_until,
    })
}

fn dead_from_row<R: Row>(row: R) -> Result<DeadLetter>
where
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i32: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    usize: sqlx::ColumnIndex<R>,
{
    let encoded: String = row.try_get(0).map_err(sql_error)?;
    Ok(DeadLetter {
        event: serde_json::from_str(&encoded)?,
        attempt: attempt(row.try_get(1).map_err(sql_error)?)?,
        reason: row
            .try_get::<Option<String>, _>(2)
            .map_err(sql_error)?
            .unwrap_or_default(),
        failed_at: from_millis(row.try_get(3).map_err(sql_error)?)?,
    })
}

fn compare_existing(encoded: &str, event: &OutboxEvent) -> Result<()> {
    let existing: OutboxEvent = serde_json::from_str(encoded)?;
    if existing.same_identity(event) {
        Ok(())
    } else {
        Err(Error::DuplicateEvent)
    }
}

fn validate_lease(worker: &str, limit: usize, lease: Duration) -> Result<()> {
    if worker.trim().is_empty()
        || worker.len() > 128
        || limit == 0
        || limit > 4096
        || lease.is_zero()
    {
        Err(Error::InvalidConfig("invalid outbox lease".into()))
    } else {
        Ok(())
    }
}

fn fenced(affected: u64) -> Result<()> {
    if affected == 1 {
        Ok(())
    } else {
        Err(Error::OutboxLeaseLost)
    }
}

fn attempt(value: i32) -> Result<u32> {
    u32::try_from(value).map_err(|_| Error::Internal("negative outbox attempt".into()))
}

fn limit_i64(value: usize) -> Result<i64> {
    i64::try_from(value).map_err(|_| Error::InvalidConfig("outbox limit overflow".into()))
}

fn now_millis() -> Result<i64> {
    millis(SystemTime::now())
}

fn millis(time: SystemTime) -> Result<i64> {
    let value = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::InvalidConfig("outbox time precedes unix epoch".into()))?
        .as_millis();
    i64::try_from(value).map_err(|_| Error::InvalidConfig("outbox time overflow".into()))
}

fn from_millis(value: i64) -> Result<SystemTime> {
    let value = u64::try_from(value).map_err(|_| Error::Internal("negative outbox time".into()))?;
    Ok(UNIX_EPOCH + Duration::from_millis(value))
}

fn sql_error(error: sqlx::Error) -> Error {
    Error::Internal(format!("sql: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_identity_ignores_schedule_changes() {
        let event = OutboxEvent::new("mail", vec![1, 2]).unwrap();
        let mut retry = event.clone();
        retry.available_at += Duration::from_secs(10);
        assert!(compare_existing(&serde_json::to_string(&retry).unwrap(), &event).is_ok());
    }
}
