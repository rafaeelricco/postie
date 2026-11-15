# VPS CI setup

GitHub Actions schedules these jobs; the dedicated `VPS-CI-postie` runner executes them on `r1cco.com`. GitHub artifact storage and external deployment or package services remain separate.

The runner uses the unprivileged `ci-postie` account and private home `/srv/r1cco-ci/postie`. Register it with `--no-default-labels --labels r1cco-ci`; do not add this label to a production deployment runner. The custom-only label keeps generic `self-hosted` deployment jobs on their existing runners. The account has no sudo or production Docker access.

Each account has its own workspace and tool cache. Docker-dependent repositories use a dedicated rootless daemon and service-level `DOCKER_HOST`; verify `docker info` reports rootless operation. Published container ports default to 127.0.0.1. Rootless Docker must retain systemd's canonical `user-UID.slice` hierarchy. That slice bounds the runner and its Docker processes together to 250% CPU and 4 GiB memory.

A root-owned job hook serializes CI across these runners and Nod, holding the slot until Runner.Worker exits after cleanup. Admission requires 6 GiB free disk. The per-repository memory limits and serialization are not a hard aggregate cap; monitor idle services, caches, and production headroom. Inspect and remove abandoned containers only through the relevant CI account's rootless socket.

For public repositories, require workflow approval for all external contributors and do not approve fork runs on this VPS. Workflow conditions reject fork PR code; trusted comment automation may inspect PR metadata before refusing an external head. Repository administrators must preserve these controls when changing workflows.

## Repository prerequisites and coverage

- Docker Compose is also required by `make integration` (Postgres, Kafka, and Debezium). Use one runner for this repository: the harness uses the fixed project `postie-integration` and ports 25432, 25433, 39092, and 28083. Configure the dedicated rootless daemon so all Compose port publications bind only to loopback, and verify that binding before activation. The workflow always removes that exact Compose project and its volumes after integration tests, including failures.
- Rootless Docker with Buildx, and an isolated builder capable of linux/amd64 and linux/arm64 builds. Provision any emulation administratively; do not give CI the production Docker socket.
- Existing GHCR publication and GitHub release behavior is preserved, but must be validated before activation without publishing a release as a test.

## Verification and activation

Run applicable non-publishing checks on the migration branch, confirm `VPS-CI-postie` in GitHub job details, and inspect completed results before merging this PR. Native macOS/Windows coverage and unsupported npm trusted publishing remain disabled where documented above. Release and production deployment jobs require deliberate activation; do not execute them merely to test runner registration.
