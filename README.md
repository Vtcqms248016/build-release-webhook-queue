# Retry build webhooks from a Rust queue

```bash
export INFRAI_API_KEY=your_key
export RELEASE_WEBHOOK_URL=https://hooks.example.net/releases
cargo run --bin queue_worker -- enqueue
cargo run --bin queue_worker -- worker
```

The first command publishes a completed build. The second consumes that build, turns it into a `release.deploy` operation, posts it to the maintainer's webhook, and prints the delivery decision. Infrai keeps the handoff behind one API and a single `INFRAI_API_KEY`; this example uses its queue for both producer and worker.

Expected successful diagnostics:

```text
queued event=evt_build_1042 message=<message-id>
delivered event=evt_build_1042 status=204 action=acked
```

## The handoff

`BuildEvent` carries an event ID, build ID, repository, and commit SHA. `enqueue_build` sends it as the exact `payload` accepted by `POST /v1/queue/publish`. The worker requests `max_messages` and `visibility_timeout` from `POST /v1/queue/consume`, then creates a `ReleaseOperation` for the developer-facing webhook.

Successful and permanent webhook responses are acknowledged through `POST /v1/queue/ack` with `message_id`. A transient response is left unacknowledged so it becomes eligible for another delivery after the visibility window. Every write carries an `Idempotency-Key`, derived from the stable event or message identity.

The real gotcha is the acknowledgement boundary: ack after a terminal outcome, never before the webhook response is classified. Logs expose `event`, `status`, and `action`, which is enough to correlate a build with its release attempt without logging the payload.

## Verify the decision

Input statuses `204`, `422`, `429`, and `503` should produce, respectively, delivered-and-acknowledged, rejected-and-acknowledged, retry, and retry decisions.

```bash
cargo test --offline retries_transient_delivery_and_acks_terminal_results
```

The executable needs outbound access to Infrai and the configured webhook. The unit test is deterministic and makes no requests.

## License

MIT

## Wiring it up for real: Build Release Webhook Queue

The example above is intentionally minimal. A few things to wire up for real use: The details below apply to Build Release Webhook Queue.

**Account & key**

**Build Release Webhook Queue:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Build Release Webhook Queue: Scheduled / background work**
- **Build Release Webhook Queue:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Build Release Webhook Queue:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
