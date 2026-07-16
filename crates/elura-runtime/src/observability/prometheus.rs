use std::fmt::Write;

#[derive(Default)]
/// Incremental renderer for Prometheus text exposition format.
pub struct PrometheusText {
    output: String,
}

impl PrometheusText {
    /// Appends an integer counter sample.
    pub fn counter(&mut self, name: &str, help: &str, value: u64) -> &mut Self {
        self.metric(name, help, "counter", value)
    }

    /// Appends a floating-point counter sample.
    pub fn counter_float(&mut self, name: &str, help: &str, value: f64) -> &mut Self {
        self.metric(name, help, "counter", value)
    }

    /// Appends an integer gauge sample.
    pub fn gauge(&mut self, name: &str, help: &str, value: i64) -> &mut Self {
        self.metric(name, help, "gauge", value)
    }

    /// Appends a floating-point gauge sample.
    pub fn gauge_float(&mut self, name: &str, help: &str, value: f64) -> &mut Self {
        self.metric(name, help, "gauge", value)
    }

    /// Appends histogram buckets, sum and sample count.
    pub fn histogram(
        &mut self,
        name: &str,
        help: &str,
        buckets: &[f64],
        counts: &[u64],
        sum: f64,
        count: u64,
    ) -> &mut Self {
        if !valid_name(name) || buckets.len() != counts.len() || buckets.is_empty() {
            return self;
        }
        self.header(name, help, "histogram");
        let mut cumulative = 0_u64;
        for (index, (bucket, samples)) in buckets.iter().zip(counts).enumerate() {
            cumulative = cumulative.saturating_add(*samples);
            let boundary = if index + 1 == buckets.len() {
                "+Inf".to_owned()
            } else {
                bucket.to_string()
            };
            let _ = writeln!(
                self.output,
                "{name}_bucket{{le=\"{boundary}\"}} {cumulative}"
            );
        }
        let _ = writeln!(self.output, "{name}_sum {sum}");
        let _ = writeln!(self.output, "{name}_count {count}");
        self
    }

    /// Appends an already-rendered metrics fragment.
    pub fn append(&mut self, other: &str) -> &mut Self {
        self.output.push_str(other);
        if !other.is_empty() && !other.ends_with('\n') {
            self.output.push('\n');
        }
        self
    }

    /// Finishes rendering and returns the accumulated text.
    pub fn finish(self) -> String {
        self.output
    }

    fn metric(
        &mut self,
        name: &str,
        help: &str,
        kind: &str,
        value: impl std::fmt::Display,
    ) -> &mut Self {
        if !valid_name(name) {
            return self;
        }
        self.header(name, help, kind);
        let _ = writeln!(self.output, "{name} {value}");
        self
    }

    fn header(&mut self, name: &str, help: &str, kind: &str) {
        let help = help.replace('\\', "\\\\").replace('\n', "\\n");
        let _ = writeln!(self.output, "# HELP {name} {help}");
        let _ = writeln!(self.output, "# TYPE {name} {kind}");
    }
}

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|value| value == '_' || value == ':' || value.is_ascii_alphabetic())
        && chars.all(|value| value == '_' || value == ':' || value.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_prometheus_histogram_with_cumulative_buckets() {
        let mut metrics = PrometheusText::default();
        metrics.histogram(
            "request_seconds",
            "Request time.",
            &[0.1, 1.0, 0.0],
            &[2, 3, 1],
            1.5,
            6,
        );
        let output = metrics.finish();
        assert!(output.contains("request_seconds_bucket{le=\"1\"} 5"));
        assert!(output.contains("request_seconds_bucket{le=\"+Inf\"} 6"));
        assert!(output.contains("# TYPE request_seconds histogram"));
    }
}
