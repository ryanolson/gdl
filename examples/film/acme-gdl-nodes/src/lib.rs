//! A response-only vendor extension with bounded, best-effort accounting.

use futures::StreamExt;
use gdl::{Backward, Build, Cx, Items};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[gdl::export]
pub use gdl_demo_nodes::{Chunk, Usage};

/// Forwards the response unchanged and posts its first usage record once.
#[derive(gdl::Node)]
#[gdl(config = TapConfig, metrics = TapMetrics)]
pub struct UsageTap {
    config: TapConfig,
    client: reqwest::Client,
    metrics: TapMetrics,
}

impl Build for UsageTap {
    type Config = TapConfig;
    type Metrics = TapMetrics;
    async fn build(config: TapConfig, metrics: TapMetrics) -> gdl::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .map_err(|error| gdl::Error::node(error.to_string()))?;
        Ok(Self {
            config,
            client,
            metrics,
        })
    }
}

impl Backward for UsageTap {
    type In = Chunk;
    type Out = Chunk;
    fn backward(&self, _: &Cx, input: Items<Chunk>) -> Items<Chunk> {
        if !self.config.enabled {
            return input;
        }
        let state = AccountingStream {
            input,
            sent: false,
            tenant: self.config.tenant.clone(),
            url: self.config.accounting_url.clone(),
            client: self.client.clone(),
            events: self.metrics.accounting_events_total.clone(),
            errors: self.metrics.accounting_errors_total.clone(),
        };
        gdl::items(futures::stream::unfold(state, |mut state| async move {
            let item = state.input.next().await?;
            if !state.sent
                && let Ok(Chunk::Usage(usage)) = &item
            {
                // Mark before the attempt: a failed best-effort POST is never retried by this stream.
                state.sent = true;
                let event = UsageEvent {
                    tenant: state.tenant.clone(),
                    usage: usage.clone(),
                };
                match state
                    .client
                    .post(&state.url)
                    .json(&event)
                    .send()
                    .await
                    .and_then(reqwest::Response::error_for_status)
                {
                    Ok(_) => state.events.inc(),
                    Err(_) => state.errors.inc(),
                }
            }
            Some((item, state))
        }))
    }
}

/// Graph settings and the runtime-only accounting destination.
#[derive(Debug, Deserialize, gdl::Config)]
#[serde(deny_unknown_fields)]
pub struct TapConfig {
    /// Tenant label attached to each accounting event.
    #[gdl(graph, default = "acme-film", label = "Tenant")]
    pub tenant: String,
    /// Enable the accounting side effect without changing response types.
    #[gdl(graph, default = true, label = "Accounting enabled")]
    pub enabled: bool,
    /// Runtime-only URL of the mock accounting receiver.
    #[serde(default = "accounting_url")]
    pub accounting_url: String,
}
fn accounting_url() -> String {
    "http://127.0.0.1:8091/usage".into()
}

/// Counters for accepted accounting events and failed attempts.
#[derive(gdl::Metrics)]
pub struct TapMetrics {
    /// Accounting POSTs accepted with a successful HTTP status.
    accounting_events_total: gdl::prometheus::IntCounter,
    /// Accounting attempts that failed transport or returned an error status.
    accounting_errors_total: gdl::prometheus::IntCounter,
}

/// One known usage event consumed by the mock accounting receiver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEvent {
    /// Tenant selected in the graph settings.
    pub tenant: String,
    /// The engine's terminal demonstration counts.
    pub usage: Usage,
}

struct AccountingStream {
    input: Items<Chunk>,
    sent: bool,
    tenant: String,
    url: String,
    client: reqwest::Client,
    events: gdl::prometheus::IntCounter,
    errors: gdl::prometheus::IntCounter,
}
