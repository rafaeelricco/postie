# Quality procedures

## Checks

| Check               | Command             | Covers                                                      |
| ------------------- | ------------------- | ----------------------------------------------------------- |
| Lint and unit tests | `make lint test`    | Formatting, `go vet`, race-enabled tests.                   |
| Behavior scenarios  | `make bdd`          | Scenario subtests in `tests/bdd`.                           |
| HTTP protocol       | `make contract`     | Synthetic protocol fixtures and the delivery path.          |
| Regressions         | `make regression`   | Acknowledgement cases and fixed regressions.                |
| Package boundaries  | `make architecture` | Allowed dependency direction and direct modules.            |
| Coverage            | `make cover`        | Coverage for gated packages; default floor is 90%.          |
| Mutation checks     | `make mutation`     | Mutation efficacy for gated packages; default floor is 90%. |
| Full quality set    | `make quality`      | Lint, coverage, and mutation checks.                        |
| Integration         | `make integration`  | PostgreSQL, Kafka, Kafka Connect, and HTTP delivery.        |
| Fuzzing             | `make fuzz`         | Acknowledgements, filters, and Debezium decoding.           |

The gated packages are `internal/{config,stream,protocol,provision,delivery,control,activity}` and
`internal/adapters/{operator,httpdelivery,debezium}`. Kafka, control database,
and source database adapters rely on the integration stack for their external
behavior. `internal/app` composes adapters without owning business rules.

## Where tests belong

Unit tests for a package live beside its Go implementation and run with `make
test`. Fuzz tests cover untrusted protocol values and Debezium decoding; they
run with `make fuzz`. Scenario tests in `tests/bdd` cover user-visible
behavior and run with `make bdd`. Contract tests in `tests/contract` compare the
synthetic examples in `tests/fixtures/protocol` with the full decode, format,
and HTTP path; they run with `make contract`. Regression tests in
`tests/regression` cover the acknowledgement corpus and fixed bugs. The
architecture suite rejects forbidden package dependencies and any direct module
outside its allowlist. Integration tests use the Compose stack in
`tests/integration` for behavior that needs real PostgreSQL, Kafka, or Kafka
Connect services.

A scenario is one `t.Run` subtest in `tests/bdd`, named as the sentence a user
would say ("keep_going is a terminal skip"). It builds its own world with
`newConfigWorld`, `newDeliveryWorld` or `newOperatorWorld`, then calls that
world's steps in given, when, then order. Variations of one behavior are rows of
a table inside one subtest. Name the specification section above the Test
function, never a line number. A behavior the specification states for users
needs a scenario even when a unit test already pins it.

Tests use the standard library only. A new direct module fails `make
architecture` until it is added to `allowedModules` in
`tests/architecture/modules_test.go`, in the same change, with the reason in the
commit message.

For a new bug, add a failing regression case before changing the implementation.
Acknowledgement cases belong in `tests/regression/testdata/acks` using the
`<delivered|skipped|retry>__<slug>.json` filename pattern. Other regressions
belong in `tests/regression`.

When a new package can be tested offline, add it to `GATED` in the Makefile in
the same change, so coverage and mutation checks stay current.

## Manual local check

Run the automated checks first:

```sh
make lint test
make quality
make integration
```

The integration suite owns an isolated Compose project named
`postie-integration`, using host ports 25432, 25433, 39092, and 28083. It can
run alongside the development Compose project. Run one integration suite at a
time. Set `POSTIE_KEEP=1` to retain its containers and volumes for inspection.

To start the development stack, copy and load the sample environment file and
start the `postie` profile:

```sh
cp .env.example .env
set -a
. ./.env
set +a
docker compose --profile postie up --build -d
```

Validate the example settings:

```sh
go run ./cmd/postiectl config validate --config examples/postie.yaml
```

Check liveness and readiness:

```sh
curl -i http://localhost:8081/health/live
curl -i http://localhost:8081/health/ready
```

Readiness stays at HTTP 503 until capture has been provisioned and Kafka,
control state, and workers are available. The operator routes require
`POSTIE_OPERATOR_TOKEN`:

```sh
curl -i -H "Authorization: Bearer $POSTIE_OPERATOR_TOKEN" http://localhost:8081/v1/status
curl -i http://localhost:8081/v1/status
curl -i -H "Authorization: Bearer $POSTIE_OPERATOR_TOKEN" http://localhost:8081/v1/subscriptions
```

The first status request should succeed when the service is ready; the request
without a token should return 401. Provisioning is described in the [README](../README.md#run-locally).

Stop the development stack when finished:

```sh
docker compose --profile postie down
```

## Continuous integration and releases

Every pull request runs `make lint cover`, `make integration`, and a
multi-architecture image build. Merging a pull request into `main` runs the
same checks on the merge commit, then tags the next version, creates the
GitHub Release, and pushes `ghcr.io/rafaeelricco/postie`. The version bump is
a patch unless the pull request carries the `release:minor` or
`release:major` label. Direct pushes to `main` do not release.
