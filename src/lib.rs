pub mod queue_client;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildEvent {
    pub event_id: String,
    pub build_id: String,
    pub repository: String,
    pub commit_sha: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReleaseOperation {
    pub event_id: String,
    pub operation: String,
    pub build_id: String,
    pub repository: String,
    pub commit_sha: String,
}

impl From<BuildEvent> for ReleaseOperation {
    fn from(build: BuildEvent) -> Self {
        Self {
            event_id: build.event_id,
            operation: "release.deploy".to_owned(),
            build_id: build.build_id,
            repository: build.repository,
            commit_sha: build.commit_sha,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryDecision {
    AckDelivered,
    AckRejected,
    Retry,
}

pub fn delivery_decision(status: u16) -> DeliveryDecision {
    match status {
        200..=299 => DeliveryDecision::AckDelivered,
        408 | 429 | 500..=599 => DeliveryDecision::Retry,
        _ => DeliveryDecision::AckRejected,
    }
}

#[cfg(test)]
mod tests {
    use super::{delivery_decision, DeliveryDecision};

    #[test]
    fn retries_transient_delivery_and_acks_terminal_results() {
        assert_eq!(delivery_decision(204), DeliveryDecision::AckDelivered);
        assert_eq!(delivery_decision(422), DeliveryDecision::AckRejected);
        assert_eq!(delivery_decision(429), DeliveryDecision::Retry);
        assert_eq!(delivery_decision(503), DeliveryDecision::Retry);
    }
}

