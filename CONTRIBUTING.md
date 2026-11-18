# Contributing

Thanks for your interest in this project.

This repository is a sanitized copy of an internal service, shared as a code sample. **It depends on private libraries and cloud infrastructure, so it cannot be built or deployed outside the organization that owns it.** Because of that, outside contributions are limited. Feedback, questions and suggestions are welcome, and small fixes may be accepted at the maintainers' discretion.

## Before you start

Confirm that your change does not add real project names, hostnames, bucket names, email addresses, tokens, or other internal details. Use placeholders such as `example.org` and `example-project`.


## Project layout

See the layout table in the [README](README.md). In short:

- `main.go` starts the cron jobs and Pub/Sub listeners.
- `pubsub/` receives messages and `process/` offers each one to the registered processors.
- `handler/` holds the processors, one per action, plus the helpers they share.
- `cron/` holds the scheduled jobs, one file per job.
- `env/` is the only place configuration is read.
- `flows/` holds the Argo workflow templates.

## Code guidelines

- Write a new processor by embedding `baseProcessor`, claiming with `claimFlowAction` or `claimMessage`, and decoding with `decodeMessage[T]`. Then register it in [process/Runner.go](process/Runner.go). Do not copy another processor's decode and claim code.
- Accept a `context.Context` in any function that calls Kubernetes, Pub/Sub, or another service, and pass the one you were given. Use `context.Background()` only at the program's entry point.
- Update IDE records with the ETag methods, and route failures through `handleETagError` so a conflict is retried and not dropped.

## Tests
The unit tests use no cloud access, but the module still needs its private dependencies to resolve.

## Workflows and scripts

Workflow templates live in [flows/](flows/). Keep parameters explicit rather than hardcoding names, and use placeholder values in any `-test.sh` script.

## License

By contributing you agree your work is licensed under the repository's BSD-3-Clause license.

## Contributions

| Contributor | Contribution |
| --- | --- |
| Aldan Beaubien | Added dashboard configuration and the dashboard health check, including a response-content check to prevent misuse. Updated the health-check and IAP workflow templates. |
| Autumn Kelsey | Moved Argo workflow monitoring into this service and added resolved machine info on IDE records. Fixed PVC scaling and user-disk reverts, raised retry limits on the HTTP status check, updated the Dockerfile and shared dependencies, and made IDE start fields update before the IAP flow. |
| James Harvey | Added the listener for failed conda pack builds, including status updates, failure emails that name the user's conda environment, and polling for workflow completion with a longer wait timeout. |
| Madeline Ambrose | Built most of the processors, cron jobs, and Argo workflows: bad-state detection and recovery emails, timeout and stale-IDE cleanup, temporary user disks, monthly quota refresh, package vulnerability notices, the HTTP route health check, and IAP policy setup. Added ETag-based optimistic concurrency, the shared processor helpers, and context plumbing. |
| Nicole Howard | Contributed the early health check. |
| Paul Mariz | Added handling for IDE commit failures and improved debug handling. |
| Sathya Subramanian | Created the repository and its early structure. Wrote the original Pub/Sub listener and processor wiring, SIGTERM handling, message locking, and the first workflow templates for pod describe and clone. |
| Stark Pister | Added the deployment automation and hotfixed the IAP flow. Reworked how workflows read secrets and environment variables and cleaned up formatting across handlers and cron jobs. |