use build_release_webhook_queue::{
    delivery_decision, queue_client::InfraiQueue, BuildEvent, DeliveryDecision, ReleaseOperation,
};
use reqwest::{Client, Method};
use thiserror::Error;

#[derive(Debug, Error)]
enum WorkerError {
    #[error(transparent)]
    Queue(#[from] build_release_webhook_queue::queue_client::QueueError),
    #[error("RELEASE_WEBHOOK_URL is not set")]
    MissingWebhookUrl,
    #[error("webhook transport failed: {0}")]
    Webhook(#[from] reqwest::Error),
    #[error("unknown command: {0}")]
    UnknownCommand(String),
}

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    let command = std::env::args().nth(1).unwrap_or_else(|| "worker".to_owned());
    let queue = InfraiQueue::from_env()?;
    match command.as_str() {
        "enqueue" => enqueue_build(&queue).await,
        "worker" => deliver_batch(&queue).await,
        other => Err(WorkerError::UnknownCommand(other.to_owned())),
    }
}

async fn enqueue_build(queue: &InfraiQueue) -> Result<(), WorkerError> {
    let build = BuildEvent {
        event_id: env_or("BUILD_EVENT_ID", "evt_build_1042"),
        build_id: env_or("BUILD_ID", "build_1042"),
        repository: env_or("BUILD_REPOSITORY", "acme/compiler"),
        commit_sha: env_or("BUILD_COMMIT_SHA", "8f31c2a"),
    };
    let queued = queue.publish(&build, &build.event_id).await?;
    println!("queued event={} message={}", build.event_id, queued.message_id);
    Ok(())
}

async fn deliver_batch(queue: &InfraiQueue) -> Result<(), WorkerError> {
    let webhook_url = std::env::var("RELEASE_WEBHOOK_URL")
        .map_err(|_| WorkerError::MissingWebhookUrl)?;
    let http = Client::new();

    for message in queue.consume::<BuildEvent>(10, 60).await? {
        let operation = ReleaseOperation::from(message.payload);
        let response = http
            .request(Method::POST, &webhook_url)
            .header("Idempotency-Key", &operation.event_id)
            .json(&operation)
            .send()
            .await?;
        let status = response.status().as_u16();

        match delivery_decision(status) {
            DeliveryDecision::AckDelivered => {
                queue.ack(&message.message_id).await?;
                println!("delivered event={} status={} action=acked", operation.event_id, status);
            }
            DeliveryDecision::AckRejected => {
                queue.ack(&message.message_id).await?;
                println!("rejected event={} status={} action=acked", operation.event_id, status);
            }
            DeliveryDecision::Retry => {
                println!("deferred event={} status={} action=retry", operation.event_id, status);
            }
        }
    }
    Ok(())
}

fn env_or(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}
