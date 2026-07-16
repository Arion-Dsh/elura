use std::fmt;
use std::future::Future;
use std::time::{Duration, Instant};

use chrono::Utc;
use elura_core::{Error, Result};
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::{Api, Client, api::PostParams};
use tokio_util::sync::CancellationToken;

use super::kube_error;

#[derive(Debug)]
#[non_exhaustive]
pub enum LeadershipError {
    InvalidConfig(String),
    Kubernetes(Error),
    Lost,
    Runner(Error),
}

impl fmt::Display for LeadershipError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(value) => {
                write!(formatter, "invalid leader election config: {value}")
            }
            Self::Kubernetes(error) => write!(formatter, "leader election: {error}"),
            Self::Lost => formatter.write_str("Kubernetes coordinator leadership lost"),
            Self::Runner(error) => write!(formatter, "leader task: {error}"),
        }
    }
}

impl std::error::Error for LeadershipError {}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct LeaderElectionConfig {
    pub namespace: String,
    pub region_id: u32,
    pub realm_id: u32,
    pub identity: String,
    pub lease_duration: Duration,
    pub renew_deadline: Duration,
    pub retry_period: Duration,
    pub release_on_cancel: bool,
}

impl LeaderElectionConfig {
    pub fn new(
        namespace: impl Into<String>,
        region_id: u32,
        realm_id: u32,
        identity: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            region_id,
            realm_id,
            identity: identity.into(),
            lease_duration: Duration::from_secs(15),
            renew_deadline: Duration::from_secs(10),
            retry_period: Duration::from_secs(2),
            release_on_cancel: true,
        }
    }

    fn validate(&self) -> std::result::Result<(), LeadershipError> {
        if self.namespace.trim().is_empty()
            || self.region_id == 0
            || self.realm_id == 0
            || self.identity.trim().is_empty()
        {
            return Err(LeadershipError::InvalidConfig(
                "namespace, region, realm and identity are required".into(),
            ));
        }
        if self.retry_period.is_zero()
            || self.retry_period >= self.renew_deadline
            || self.renew_deadline >= self.lease_duration
            || self.lease_duration.as_secs() > i32::MAX as u64
        {
            return Err(LeadershipError::InvalidConfig(
                "require retry period < renew deadline < lease duration".into(),
            ));
        }
        Ok(())
    }
}

pub async fn run_leader_elected<F, Fut>(
    client: Client,
    config: LeaderElectionConfig,
    shutdown: CancellationToken,
    runner: F,
) -> std::result::Result<(), LeadershipError>
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    config.validate()?;
    let api: Api<Lease> = Api::namespaced(client, &config.namespace);
    let name = format!(
        "elura-r{}-realm{}-shard-coordinator",
        config.region_id, config.realm_id
    );
    let mut retry = tokio::time::interval(config.retry_period);
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = retry.tick() => match acquire_or_renew(&api, &name, &config).await {
                Ok(true) => break,
                Ok(false) => {},
                Err(error) => tracing::warn!(%error, "Kubernetes leader acquisition failed"),
            }
        }
    }

    tracing::info!(identity = %config.identity, "became shard coordinator leader");
    let runner_shutdown = shutdown.child_token();
    let mut task = tokio::spawn(runner(runner_shutdown.clone()));
    let mut last_renewed = Instant::now();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                runner_shutdown.cancel();
                let result = task.await.map_err(|error| LeadershipError::Runner(Error::Internal(error.to_string())))?;
                if config.release_on_cancel { let _ = release(&api, &name, &config.identity).await; }
                return result.map_err(LeadershipError::Runner);
            }
            completed = &mut task => {
                if config.release_on_cancel { let _ = release(&api, &name, &config.identity).await; }
                return completed
                    .map_err(|error| LeadershipError::Runner(Error::Internal(error.to_string())))?
                    .map_err(LeadershipError::Runner);
            }
            _ = retry.tick() => {
                match acquire_or_renew(&api, &name, &config).await {
                    Ok(true) => last_renewed = Instant::now(),
                    Ok(false) => {
                        runner_shutdown.cancel();
                        let _ = task.await;
                        return Err(LeadershipError::Lost);
                    }
                    Err(error) if last_renewed.elapsed() < config.renew_deadline => {
                        tracing::warn!(%error, "Kubernetes leader renewal failed; retrying");
                    }
                    Err(_) => {
                        runner_shutdown.cancel();
                        let _ = task.await;
                        return Err(LeadershipError::Lost);
                    }
                }
            }
        }
    }
}

async fn acquire_or_renew(
    api: &Api<Lease>,
    name: &str,
    config: &LeaderElectionConfig,
) -> Result<bool> {
    let now = Utc::now();
    let duration = config.lease_duration.as_secs() as i32;
    let current = api.get_opt(name).await.map_err(kube_error)?;
    let lease = if let Some(mut lease) = current {
        let spec = lease.spec.get_or_insert_default();
        let held_by_self = spec.holder_identity.as_deref() == Some(&config.identity);
        let expired = spec.renew_time.as_ref().is_none_or(|renewed| {
            let seconds = spec.lease_duration_seconds.unwrap_or(duration).max(1);
            now >= renewed.0 + chrono::Duration::seconds(i64::from(seconds))
        });
        if !held_by_self && !expired {
            return Ok(false);
        }
        if !held_by_self {
            spec.lease_transitions = Some(spec.lease_transitions.unwrap_or(0).saturating_add(1));
            spec.holder_identity = Some(config.identity.clone());
            spec.acquire_time = Some(MicroTime(now));
        }
        spec.lease_duration_seconds = Some(duration);
        spec.renew_time = Some(MicroTime(now));
        lease
    } else {
        Lease {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some(config.namespace.clone()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(config.identity.clone()),
                lease_duration_seconds: Some(duration),
                acquire_time: Some(MicroTime(now)),
                renew_time: Some(MicroTime(now)),
                lease_transitions: Some(1),
                ..Default::default()
            }),
        }
    };
    let operation = if lease.metadata.resource_version.is_some() {
        api.replace(name, &PostParams::default(), &lease).await
    } else {
        api.create(&PostParams::default(), &lease).await
    };
    match operation {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(response)) if response.code == 409 => Ok(false),
        Err(error) => Err(kube_error(error)),
    }
}

async fn release(api: &Api<Lease>, name: &str, identity: &str) -> Result<()> {
    let Some(mut lease) = api.get_opt(name).await.map_err(kube_error)? else {
        return Ok(());
    };
    let spec = lease.spec.get_or_insert_default();
    if spec.holder_identity.as_deref() != Some(identity) {
        return Ok(());
    }
    spec.holder_identity = Some(String::new());
    spec.lease_duration_seconds = Some(1);
    spec.renew_time = Some(MicroTime(Utc::now()));
    api.replace(name, &PostParams::default(), &lease)
        .await
        .map_err(kube_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_timing_order() {
        let mut config = LeaderElectionConfig::new("games", 1, 2, "gateway-a");
        assert!(config.validate().is_ok());
        config.retry_period = config.renew_deadline;
        assert!(config.validate().is_err());
    }
}
