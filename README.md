<h1 align="center">Postie</h1>

Postie captures committed rows from append-only PostgreSQL event tables,
retains them in Kafka, and delivers them to HTTP endpoints. It handles capture,
routing, retries, acknowledgements, and delivery state. Your application owns
event schemas, business rules, and projections.

```mermaid
flowchart LR
  PG[PostgreSQL event table] --> D[Debezium]
  D --> K[Kafka event history]
  K --> W[Postie delivery workers]
  W --> H[HTTP endpoint]
  H --> P[Application projection]
```

Replay jobs and administrative repair of blocked streams are not implemented.

## Getting started

You need Go 1.26 or later and Docker with Compose. Run every command from the
repository root. The Compose stack uses roughly 700 MB of memory.

Export the variables the example configuration reads:

```bash
export POSTIE_OPERATOR_TOKEN=change-me POSTIE_CAPTURE_PASSWORD=capture \
  POSTIE_DELIVERY_PASSWORD=delivery \
  POSTIE_DATABASE_URL='postgres://control:control@localhost:15433/control?sslmode=disable'
```

Validate the example configuration. This does not connect to PostgreSQL,
Kafka, or an HTTP destination:

```bash
go run ./cmd/postie config validate --config examples/postie.yaml
```

The sample reports one PostgreSQL source and one HTTP destination. Engine
settings are in [examples/postie.yaml](examples/postie.yaml). Application
sources and destinations are in
[examples/application.yaml](examples/application.yaml). Both files support
`${NAME}` substitution in string and duration values. Numeric fields need
literal numbers, and a missing variable fails validation.

Start the local PostgreSQL, control PostgreSQL, Kafka, Kafka Connect, and
Postie services:

```bash
docker compose --profile postie up --build -d
```

The `postie` profile is required to start the HTTP API and delivery workers.

| Service          | Address           |
| ---------------- | ----------------- |
| Source database  | `localhost:15432` |
| Control database | `localhost:15433` |
| Kafka            | `localhost:29092` |
| Kafka Connect    | `localhost:18083` |

The source database uses `postie` as its user, password, and database name.

**The bundled source database does not create the application database, event
table, capture user, or HTTP receiver that the sample configuration uses.**
Point `examples/application.yaml` at an existing append-only table and receiver
before provisioning. If you use the local source database, create those
resources there first.

Provision the first stream generation from inside the Compose network:

```bash
docker compose run --rm postie provision --config /app/examples/postie.yaml --generation 1
```

Provisioning validates each source table, creates or verifies its Kafka topic,
configures the Debezium connector, and stores the stream identity. It does not
start HTTP delivery. The Postie service consumes generation 1 by default.

Provision the same generation again and it verifies the stored identity, topic,
connector, and replication slot. A missing or changed history boundary blocks
the stream for diagnosis.

Check liveness and readiness:

```bash
curl -fsS http://localhost:8081/health/live
curl -i http://localhost:8081/health/ready
```

Readiness stays at HTTP 503 until capture, Kafka, control state, and workers
are available. Check status with the operator token:

```bash
curl -fsS \
  -H "Authorization: Bearer $POSTIE_OPERATOR_TOKEN" \
  http://localhost:8081/v1/status
```

The operator API also lists subscriptions, pauses or resumes a destination, and
reads recent activity. The routes are in
[Delivery and operations](docs/specification.md#delivery-and-operations).

Stop the development stack when finished:

```bash
docker compose --profile postie down
```

## Use the published image

Every pull request merged into `main` publishes a release for `linux/amd64`
and `linux/arm64`.

The first publication creates a private GHCR package. After that release,
the maintainer must open the `postie` package's **Package settings**, choose
**Change visibility** under **Danger Zone**, and select **Public**.
This one-time setup enables the anonymous pull and Compose examples below.

Once the package is public:

```bash
docker pull ghcr.io/rafaeelricco/postie:latest
```

In another project, mount your own engine and application configuration and
point `--config` at it. Relative paths inside `postie.yaml` resolve against
the container working directory, `/app`:

```yaml
services:
  postie:
    image: ghcr.io/rafaeelricco/postie:0.1.0
    command: ["--config", "/app/config/postie.yaml"]
    volumes:
      - ./postie:/app/config:ro
    environment:
      POSTIE_OPERATOR_TOKEN: ${POSTIE_OPERATOR_TOKEN}
      POSTIE_DATABASE_URL: ${POSTIE_DATABASE_URL}
      POSTIE_CAPTURE_PASSWORD: ${POSTIE_CAPTURE_PASSWORD}
      POSTIE_DELIVERY_PASSWORD: ${POSTIE_DELIVERY_PASSWORD}
    ports: ["8081:8081"]
```

Set `application_config: ./config/application.yaml` in the mounted
`postie.yaml`. Postie still needs Kafka, Kafka Connect with Debezium, and a
control PostgreSQL database; [docker-compose.yml](docker-compose.yml) shows a
working set. Run `postie provision` and `postie config validate` from the same image.

## Delivery contract

Postie sends a Basic-authenticated JSON `POST` with source and destination
identifiers, descriptions, and a JSON payload. What happens next depends on
your response:

| Response                                                                                          | What Postie does                                                |
| ------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| A 2xx containing `{"result":{"success":{}}}`                                                      | Accepts the acknowledgement. Nothing else counts as success.    |
| A `keep_going` error                                                                              | Records a terminal skip.                                        |
| `must_retry`, a malformed acknowledgement, a non-2xx response, a timeout, or a connection failure | Retries with jittered exponential backoff from 1 to 60 seconds. |

Redirects are not followed, and responses above 64 KiB are rejected.

**Delivery is at least once.** A crash after the HTTP acknowledgement but
before the Kafka commit can produce a duplicate. Your receiver should apply an
event and save its idempotency key in the same transaction. Each partition
delivers in order, and a retrying partition does not block other partitions.

See [HTTP delivery](docs/specification.md#http-delivery) for wire fields,
acknowledgements, filters, and supported PostgreSQL values.

## Documentation

| Document                               | Purpose                                                                       |
| -------------------------------------- | ----------------------------------------------------------------------------- |
| [Specification](docs/specification.md) | Capture, configuration, the HTTP wire contract, operations, and architecture. |

## Development

Run the offline checks:

```bash
make lint test
```

Use the focused checks when you change a specific area:

| Check            | Command            | Covers                                                      |
| ---------------- | ------------------ | ----------------------------------------------------------- |
| Lint and tests   | `make lint test`   | Formatting, `go vet`, race tests, and boundary checks.      |
| Coverage         | `make cover`       | Coverage for gated packages; default floor is 90%.          |
| Mutation checks  | `make mutation`    | Mutation efficacy for gated packages; default floor is 90%. |
| Full quality set | `make quality`     | Lint, coverage, and mutation checks.                        |
| Integration      | `make integration` | PostgreSQL, Kafka, Kafka Connect, and HTTP delivery.        |
| Fuzzing          | `make fuzz`        | Acknowledgements, filters, and Debezium decoding.           |

The gated packages are listed in the `GATED` variable of the
[Makefile](Makefile). Kafka, control database, and source database adapters
rely on the integration stack for their external behavior. `internal/app`
composes adapters without owning business rules.

The main code areas are `internal/config`, `internal/protocol`,
`internal/delivery`, `internal/adapters/debezium`, `internal/provision`,
`internal/control`, and `internal/adapters/operator`. There is one binary,
`postie`, built from `cmd/postie`. Run without a subcommand it serves delivery
and the operator API. `postie config validate` and `postie provision` are its
subcommands.

### Where tests belong

- Each package's tests live beside the code in `<pkg>_test.go`, including
  `cmd/postie`. They run with `make test`. Fuzz
  tests cover untrusted protocol values and Debezium decoding and run with
  `make fuzz`.
- Architecture tests are in `tests/architecture`. They reject forbidden
  package dependencies and any direct module outside the allowlist.
- Integration tests are in `tests/integration`. They use the Compose stack for
  behavior that needs real PostgreSQL, Kafka, or Kafka Connect services.

### Rules

- Tests use only the standard library. A new direct module fails
  `make test` until it is added to `allowedModules` in
  `tests/architecture/architecture_test.go`, in the same change, with the
  reason in the commit message.
- When a new package can be tested offline, add it to `GATED` in the Makefile
  in the same change, so coverage and mutation checks stay current.
- For a new bug, add a failing test in that package's test file before
  changing the implementation.

### Integration project

The integration suite runs [docker-compose.yml](docker-compose.yml) as its own
`postie-integration` Compose project on separate ports, so it can run beside
the development stack. Run only one integration suite at a time. Ports and
`POSTIE_KEEP` are in [Development topology](docs/specification.md#development-topology).

### CI and releases

One workflow, `.github/workflows/ci.yml`, verifies pull requests and publishes
releases. Every pull request runs
`make lint cover`, `make integration`, and a multi-architecture image build.
Merging a pull request into `main` runs the lint, coverage, and integration
checks on the merge commit, then tags the next version, creates the GitHub
Release, and pushes `ghcr.io/rafaeelricco/postie`. The version bump is a patch
unless the pull request carries the `release:minor` or `release:major` label.
Direct pushes to `main` do not release.
