use serde::{Deserialize, Serialize};

/// A model a cell serves, as a client names it: the model's name, and the
/// serving partition it is in, if any. A LoRA is a model of its own name.
/// `routing_key` is a partition of the workers that serve the model, not
/// access by key. A declared model's settings name no other field.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelKey {
    /// The name a client sends.
    pub model: String,
    /// The serving partition, when the cell has more than one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,
}

impl ModelKey {
    /// The key of `model`, in no partition.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            routing_key: None,
        }
    }

    /// Whether the key can be published: a model name, and a routing key
    /// that is not empty when there is one.
    pub fn check(&self) -> Result<(), String> {
        if self.model.is_empty() {
            return Err("a model's name is empty".into());
        }
        if self.routing_key.as_deref() == Some("") {
            return Err(format!("model `{}`: its routing key is empty", self.model));
        }
        Ok(())
    }
}

/// What an entry instance tells its cell's hub of one model it has. A
/// field a later build adds is ignored, so builds roll one instance at a
/// time.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstanceModel {
    /// The name a client sends.
    pub model: String,
    /// The serving partition, when the cell has more than one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,
    /// Whether the instance can serve it now.
    pub state: InstanceModelState,
}

impl InstanceModel {
    /// The model's key.
    pub fn key(&self) -> ModelKey {
        ModelKey {
            model: self.model.clone(),
            routing_key: self.routing_key.clone(),
        }
    }
}

/// Whether an entry instance can serve a model it has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceModelState {
    /// It is loading the model, and cannot serve it yet.
    Starting,
    /// It can serve the model now.
    Ready,
    /// It is removing the model on purpose, and takes no new work for it.
    Draining,
}
