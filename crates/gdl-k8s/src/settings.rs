//! What a segment's pods are given from its settings file: where they
//! answer their probes, how long they may take to stop, and the prefix of
//! their metrics. Read from the file alone, so that each has one source.

use std::time::Duration;

/// How long past its drain bound a segment's pod is let stop: the
/// runtime's exit passes the bound by a few seconds.
const STOP_MARGIN: u64 = 30;

/// A segment's settings, as its pods use them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// Its system listener's port, where its probes ask.
    pub port: u16,
    /// Where it answers liveness.
    pub live: String,
    /// Where it answers readiness.
    pub health: String,
    /// Its drain bound.
    pub drain: Duration,
    /// Its `[system] metrics_prefix`, when it sets one.
    pub metrics_prefix: Option<String>,
}

impl Settings {
    /// The settings in `text`, a segment's TOML. Refused when the text is
    /// not TOML, or its system listener is off or takes an ephemeral port,
    /// since a probe needs a port it knows.
    pub fn read(text: &str) -> Result<Self, String> {
        let table: toml::Table = toml::from_str(text).map_err(|e| e.to_string())?;
        Self::of(&table)
    }

    /// The settings of `table`, refused as [`Settings::read`] says.
    pub fn of(table: &toml::Table) -> Result<Self, String> {
        let system = table.get("system").and_then(toml::Value::as_table);
        let text = |key: &str, default: &str| {
            system
                .and_then(|system| system.get(key))
                .and_then(toml::Value::as_str)
                .unwrap_or(default)
                .to_string()
        };
        let port = system
            .and_then(|system| system.get("port"))
            .and_then(toml::Value::as_integer)
            .and_then(|port| u16::try_from(port).ok())
            .filter(|port| *port > 0)
            .ok_or("a pod's probes ask the segment's system listener, so it takes a fixed port")?;
        Ok(Self {
            port,
            live: text("live_path", "/live"),
            health: text("health_path", "/health"),
            drain: drain_bound(table),
            metrics_prefix: system
                .and_then(|system| system.get("metrics_prefix"))
                .and_then(toml::Value::as_str)
                .map(str::to_string),
        })
    }

    /// How long its pod is let stop, in seconds: its drain bound and a
    /// margin.
    pub fn grace(&self) -> u64 {
        stop_grace(self.drain)
    }
}

/// A segment's drain bound, from its settings: `[runtime] drain_ms`, else
/// `default_deadline_ms`, else the default deadline.
pub fn drain_bound(table: &toml::Table) -> Duration {
    let runtime = table.get("runtime").and_then(toml::Value::as_table);
    let ms = |key: &str| {
        runtime
            .and_then(|runtime| runtime.get(key))
            .and_then(toml::Value::as_integer)
            .and_then(|ms| u64::try_from(ms).ok())
    };
    ms("drain_ms")
        .or_else(|| ms("default_deadline_ms"))
        .map_or(gdl_model::DEFAULT_DEADLINE, Duration::from_millis)
}

/// How long a segment of drain bound `drain` is let stop, in seconds.
pub fn stop_grace(drain: Duration) -> u64 {
    drain.as_secs() + STOP_MARGIN
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_probes_need_a_fixed_port() {
        assert!(Settings::read("[system]\nport = 0\n").is_err());
        assert!(Settings::read("listen = \"x\"\n").is_err());
        let settings = Settings::read(
            "[system]\nport = 9090\nhealth_path = \"/ready\"\nmetrics_prefix = \"api\"\n[runtime]\ndrain_ms = 90000\n",
        )
        .expect("settings");
        assert_eq!(settings.port, 9090);
        assert_eq!(settings.live, "/live");
        assert_eq!(settings.health, "/ready");
        assert_eq!(settings.metrics_prefix.as_deref(), Some("api"));
        assert_eq!(settings.grace(), 120);
    }
}
