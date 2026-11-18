# IDE Workflow Management Service

A Go service that carries out the background work behind cloud-hosted analysis environments. It listens for IDE requests on Google Cloud Pub/Sub, runs each one as an Argo workflow on Kubernetes, and records the result. Scheduled jobs in the same service keep the fleet healthy and cost-controlled.

> This is a sanitized copy of a service used in the Allen Institute for Immunology. It is shared as a code sample. It will not build or run on its own because it depends on private internal libraries and cloud infrastructure. Project names, hostnames, and credentials have been replaced with placeholders.

## What it does

- **IDE lifecycle.** Create, provision, start, stop, and delete JupyterLab instances. Each step updates the IDE's status, volume snapshot history, and route health, and a failed step sets a failure status instead of leaving the IDE half-built.
- **IDE changes.** Edit machine configuration, change the Jupyter idle timeout, install the SDK, and create a route for a dashboard app that runs in the IDE.
- **Disk management.** Scale user storage, including temporary increases that a daily job reverts. Users get warning emails before a revert.
- **Access.** Grant the owner access through Identity-Aware Proxy (IAP) policy, and wait until the policy has propagated.
- **Quotas.** Stop a user's IDEs when they reach their usage quota, and return them to the stopped state when the monthly quota refreshes.
- **Fleet health.** Find pods stuck in bad states and run a diagnosis workflow, email users when an IDE recovers, and stop IDEs that pass their timeout.
- **Cleanup.** Warn about and soft-delete stale IDEs, permanently delete soft-deleted IDEs after 30 days, and remove orphaned volume claims and snapshots.
- **Environment builds and uploads.** React to failed conda pack builds and failed file uploads by updating status and emailing the user, and harvest artifacts from training image builds.
- **Security notices.** Read package scan results from Pub/Sub and email the owners of vulnerable environments.

## How it works

The service is one long-running process with two jobs: a message pipeline and a set of cron jobs.

```
Pub/Sub topics ──► listeners ──► dispatch ──► processor ──► Argo workflow ──► GKE
                                                 │                │
                                                 │                └──► result, ack / nack
                                                 └──► internal data services (tracer, ledger, billing, email)

cron jobs ──► internal data services, Argo workflows, Kubernetes, Pub/Sub, email
```

1. **Listen.** Five listeners subscribe to topics for IDE flow requests, completed workflows, conda pack builds, quota events, and training-image artifacts. Each message is tracked while it is in flight, and a duplicate delivery of the same message is dropped.
2. **Dispatch.** [process/Runner.go](process/Runner.go) offers the message to each registered processor. A processor claims the messages whose `Action` it owns.
3. **Process.** The claiming processor reads the IDE record, submits an Argo workflow built from a template in [flows/](flows/), waits for it, and writes the new status back.
4. **Acknowledge.** A message is acked when the work finishes or fails for good. It is nacked when the failure is transient, so Pub/Sub redelivers it.
5. **Cron.** Nine cron jobs register with the shared cron runner. A job never overlaps itself, and each run is recorded in a ledger service.

## Working with the IDE Management Service

This service is the back half of a pair. The IDE Management Service is the HTTP API that users and the UI call. It checks roles and quota, creates the IDE record, and submits the first Argo workflow. It then publishes an IDE flow request on Pub/Sub that carries the workflow, and this service takes over from there.

- **API to this service.** The API publishes a request for each IDE action: create, scale storage, edit machine config, install SDK, revert user disk, and set IAP policy. This service claims the message, waits for the workflow, writes the final status, and publishes follow-up requests, such as the IAP policy step after a create.
- **This service to the API.** Some actions must go through the API's own logic. The quota processor and the cron jobs call back over HTTP, authenticated as a service account, to stop an IDE, permanently delete one, or revert a user disk.
- **The contract between them.** The message types, topic names, and API clients come from the shared `hise-data-contracts` module, so the two services share definitions and not copied code.
- **Why split them.** The API stays fast and answers each request in one round trip. Slow work, retries, and scheduled cleanup run here, so a restart or backlog in this service never blocks users.

## Layout

| Path | Purpose |
| --- | --- |
| [main.go](main.go) | Entry point. Starts the cron jobs and Pub/Sub listeners, and handles shutdown. |
| [handler/](handler/) | Message processors and the helpers they share. |
| [handler/processor.go](handler/processor.go) | Shared processor pieces for claiming, decoding, and default methods. |
| [handler/common.go](handler/common.go) | Argo submission, ETag handling, and status updates. |
| [process/](process/) | Processor registry and the dispatch loop. |
| [pubsub/](pubsub/) | Listener definitions, SIGTERM handling, and in-flight message tracking. |
| [cron/](cron/) | Scheduled jobs, one file per job. |
| [env/](env/) | Configuration read from environment variables, with defaults. |
| [dependencycontainer/](dependencycontainer/) | Clients for the internal data services, plus token refresh. |
| [flows/](flows/) | Argo workflow templates, plus shell scripts for trying them out. |
| [Dockerfile](Dockerfile) | Container image that packages the binary with the workflow templates. |

## Technology

Go, Argo Workflows, Google Kubernetes Engine, Pub/Sub, Identity-Aware Proxy, Compute Engine disks and snapshots, cron, and Docker. The internal modules `hise-data-contracts` (service clients and message types) and `shared-utility-library` (logging, cron, Pub/Sub, and Argo helpers) are private.

## Design notes

- **Processors share one shape.** Each processor embeds `baseProcessor`, claims with `claimFlowAction`, and decodes with `decodeMessage[T]`. Adding an action means writing one struct and one `Process` method, then registering it.
- **Bad messages are not retried.** A message that fails to decode is acked and logged, because redelivering it cannot help. It is never processed as an empty request.
- **Optimistic concurrency.** IDE records are updated with ETags. When two writers collide, the service gets a `412 Precondition Failed` and nacks the message, so it is retried against fresh data. See `handleETagError` in [handler/common.go](handler/common.go).
- **Workflows as templates.** Cluster operations live in YAML under [flows/](flows/), each with a script for submitting it by hand. Changing a workflow rarely needs a Go change.
- **Graceful shutdown.** On SIGTERM the service stops taking new work and nacks incoming messages with exponential backoff, while in-flight messages finish. The root context is deliberately not cancelled then, so running workflows can complete.
- **Cleanup that tolerates partial failure.** Deleting volumes and snapshots ignores resources that are already gone, so a run can be retried safely.
- **Environment-driven configuration.** All deployment-specific values come from environment variables. Hostnames, project IDs, and client IDs have no defaults and must be set at deploy time.

## Running the tests

The unit tests use no cloud access, but the module still needs its private dependencies to resolve.

## Configuration

Key environment variables, all read in [env/Env.go](env/Env.go):

| Variable | Meaning |
| --- | --- |
| `GCP_PROJECT` | Project that hosts the IDE cluster. Required. |
| `AUTH_CLIENT_ID` | OAuth client ID used as the IAP audience. Required. |
| `IDE_PROC_ENDPOINT` | Hostname the IDEs are served from. Required. |
| `SUPPORT_EMAIL` | Address copied on user-facing notification emails. Required. |
| `PORT` | Port the service listens on. Required. |
| `GCP_ZONE`, `COMPUTE_GCP_ZONE` | Zones for the IDE cluster and compute resources. Required. |
| `KUBERNETES_NAMESPACE` | Namespace for IDE pods. Default `default`. |
| `KUBERNETES_POD_TIMEOUT`, `KUBERNETES_POD_PENDING_TIMEOUT` | Pod and pending-pod timeouts, in minutes. Defaults `15` and `360`. |
| `POD_TIMEOUT` | Timeout passed to workflows, in seconds. Default `1200`. |
| `DEFAULT_USER_DISK_SIZE` | Default user disk size. Default `108Gi`. |
| `COMPLETED_WORKFLOWS_TOPIC` | Topic for completed workflows. Required. |
| `VULNERABILITIES_SUBSCRIPTION` | Subscription for package scan results. Required. |
| `IDE_SERVICE_ACCOUNT` | Service account that submits workflows. Required. |
| `*_CRON_EXPRESSION` | Schedule for each cron job. Each has a default in [env/Env.go](env/Env.go). |
