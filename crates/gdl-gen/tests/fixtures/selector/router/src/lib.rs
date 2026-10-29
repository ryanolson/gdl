//! A selector that reads the prepared request, and refuses one whose text
//! its settings name, with the reason they give. A test sees it chose by
//! the refusal: no other router of the crossing refuses a request.

use gdl::{Build, Choice, Error, Instances, Request, Result, Selector};
use selector_messages::Prepared;
use serde::Deserialize;

/// The selector's settings.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ByTextSettings {
    /// The text it refuses.
    pub refuse: String,
    /// What the refusal says.
    pub reason: String,
}

/// Sends every request to the first instance, but refuses one whose text
/// is its settings' `refuse`.
pub struct ByText {
    refuse: String,
    reason: String,
}

impl Build for ByText {
    type Config = ByTextSettings;
    type Metrics = ();

    async fn build(settings: ByTextSettings, (): ()) -> Result<Self> {
        Ok(Self {
            refuse: settings.refuse,
            reason: settings.reason,
        })
    }
}

impl Selector for ByText {
    type Head = Prepared;

    async fn select<'a>(
        &'a self,
        request: Request<'a, Prepared>,
        instances: Instances<'a>,
    ) -> Result<Choice<'a>> {
        let head = request.head();
        if head.text == self.refuse {
            return Err(Error::routing(format!(
                "by_text refused a request of {} words: {}",
                head.words, self.reason
            )));
        }
        Ok(Choice::new(instances.get(0).expect("never empty")))
    }
}
