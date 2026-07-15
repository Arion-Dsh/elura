use async_trait::async_trait;
use elura_runtime::observability::{AdminDiagnostics, PrometheusText, Readiness};
use serde_json::Value;

use crate::WorldDiagnostics;

#[async_trait]
impl AdminDiagnostics for WorldDiagnostics {
    async fn readiness(&self) -> Readiness {
        if self.ready() {
            Readiness::ready()
        } else {
            Readiness::unavailable("World is not ready")
        }
    }

    async fn prometheus(&self) -> String {
        let stats = self.stats();
        let mut metrics = PrometheusText::default();
        metrics
            .gauge_float(
                "elura_world_uptime_seconds",
                "World process uptime in seconds.",
                stats.uptime_millis as f64 / 1000.0,
            )
            .counter(
                "elura_world_commands_total",
                "World commands received.",
                stats.commands,
            )
            .gauge(
                "elura_world_commands_active",
                "World commands currently executing.",
                stats.active_commands,
            )
            .counter(
                "elura_world_commands_succeeded_total",
                "World commands completed successfully.",
                stats.succeeded,
            )
            .counter(
                "elura_world_business_failures_total",
                "World business failures.",
                stats.business_failures,
            )
            .counter(
                "elura_world_internal_failures_total",
                "World internal failures.",
                stats.internal_failures,
            )
            .counter(
                "elura_world_timeouts_total",
                "World command timeouts.",
                stats.timeouts,
            )
            .counter(
                "elura_world_handler_panics_total",
                "Recovered World handler panics.",
                stats.panics,
            )
            .counter_float(
                "elura_world_command_duration_seconds_total",
                "Cumulative World command duration in seconds.",
                stats.duration_nanos as f64 / 1e9,
            )
            .histogram(
                "elura_world_command_duration_seconds",
                "World command duration in seconds.",
                &[0.001, 0.005, 0.01, 0.05, 0.1, 0.0],
                &stats.latency_buckets,
                stats.duration_nanos as f64 / 1e9,
                stats.commands,
            );
        metrics.finish()
    }

    async fn stats(&self) -> Value {
        serde_json::to_value(self.stats()).unwrap_or(Value::Null)
    }

    async fn routes(&self) -> Option<Value> {
        serde_json::to_value(self.route_manifest()).ok()
    }
}
