# VPS CI setup

GitHub Actions schedules these jobs; the `r1cco-ci` runner executes them on the VPS. GitHub artifact storage and external deployment or package services remain separate.

Before enabling this routing, register a runner for this repository under its own unprivileged Linux account and home directory. Use `--no-default-labels --labels r1cco-ci`; do not add `r1cco-ci` to an existing production deployment runner. The custom-only label keeps generic `self-hosted` deployment jobs on their existing runners.

Give the account a writable tool cache and only its own workspace. Do not add it to the host Docker or sudo groups. For Docker jobs, provision an isolated rootless daemon and its socket for that account. Set `DOCKER_HOST` in the runner service environment and verify `docker info` reports rootless operation from that service account; job containers and services start before workflow steps, so a shell-only Docker context is insufficient. Bound aggregate CI CPU, memory, concurrency, and disk usage before enabling additional repository runners on the shared VPS.

For public repositories, require workflow approval for all external contributors and do not approve fork runs on this VPS. Workflow conditions reject fork PR code; trusted comment automation may inspect PR metadata before refusing an external head. Repository administrators must preserve these controls when changing workflows.

## Repository prerequisites and coverage

- Docker Compose is also required by `make integration` (Postgres, Kafka, and Debezium). Use one runner for this repository: the harness uses the fixed project `postie-integration` and ports 25432, 25433, 39092, and 28083. Configure the dedicated rootless daemon so all Compose port publications bind only to loopback, and verify that binding before activation. The workflow always removes that exact Compose project and its volumes after integration tests, including failures.
- Rootless Docker with Buildx, and an isolated builder capable of linux/amd64 and linux/arm64 builds. Provision any emulation administratively; do not give CI the production Docker socket.
- Existing GHCR publication and GitHub release behavior is preserved, but must be validated before activation without publishing a release as a test.

## Activation verification

Keep the migration PR in draft until the dedicated runner is registered, online, and its prerequisites are installed. Run the applicable non-publishing checks on a trusted branch, confirm the runner name in GitHub job details, and inspect the final logs. Release and production deployment jobs require a separate deliberate activation; do not run them merely to test runner registration.
