# Postie specification

Postie captures committed events from append-only PostgreSQL tables through
Debezium and Kafka, then delivers them to HTTP endpoints. Kafka retains source
records and delivery offsets. Control PostgreSQL stores stream registrations,
subscription state, worker leases, and terminal delivery skips. Recent activity
is held in a bounded process-local buffer and also written to standard output.
Applications own their event schemas, projections, and external side effects.

Postie currently supports PostgreSQL sources and `http-push` destinations.
Replay jobs, public publishing, other source databases, and exactly-once HTTP
effects are not supported.

## Capture contract

Each source names one table in the `public` schema. The configured `columns`
must include `serialColumn` and `partitioningColumn`. The serial column must be
a non-null `int2`, `int4`, or `int8` column with a unique or primary-key index
on that column alone. The partitioning column must be non-null and becomes the
Kafka message key. If `event_id` is included in the configured columns,
Postie uses it as the event identifier for delivery diagnostics and
idempotency.

Supported source column types are `int2`, `int4`, `int8`, `float4`, `float8`,
`bool`, `json`, `bytea`, `timestamp`, `timestamptz`, and `text`. Nullable SQL
`json` values, including SQL `NULL`, are supported. `jsonb` and other
PostgreSQL types are unsupported and fail source contract validation.

Provisioning takes an ascending snapshot and then follows WAL changes. Updates,
deletes, truncations, unsupported schema changes, a missing replication slot,
or unavailable history block the affected source or stream. Postie does not
silently skip these conditions or resnapshot an established generation.

The capture user needs logical replication access and permission to read the
table. By default, Debezium manages a filtered publication, which requires the
capture user to own the table. For a non-owner capture user, the table owner
must create a publication and `sources.<source-id>.publication` in
`examples/postie.yaml` must name it. Naming it there selects the externally
managed publication mode.

A stream generation freezes its table identity, configured columns, key,
partition count, and capture resource names. Changing the frozen identity
requires a new generation. Topics use delete-only retention, so event history
stays available for normal delivery. Topic compaction is not used.

## Configuration

The engine settings live in `examples/postie.yaml`. Source and destination
definitions live in `examples/application.yaml` using the existing YAML keys
`data_sources` and `data_destinations`.

PostgreSQL sources use `host`, `port`, `username`, `password`, `database`,
`table`, `columns`, `serialColumn`, and `partitioningColumn`. HTTP destinations
use `endpoint`, `username`, `password`, `sources`, and an optional `filter`.
Configuration supports the `postgres` source type and `http-push` destination
type. Validation rejects duplicate IDs, unknown source references, incomplete
settings, empty filters, missing configured columns, and unsupported types.

`${NAME}` environment substitution applies to YAML string and duration values.
Missing variables fail loading. Numeric fields require literal numeric values.
Secrets are redacted from logs and stored configuration metadata.

The operator API uses a separate bearer token. Set `POSTIE_OPERATOR_TOKEN` or
configure `operator.token_file`. The Compose environment also uses
`POSTIE_DATABASE_URL`, `POSTIE_CAPTURE_PASSWORD`, and
`POSTIE_DELIVERY_PASSWORD` for local values.

## HTTP delivery

Each request is a Basic-authenticated JSON `POST` containing a Postie envelope.
The envelope has the established keys `data_source_id`, `data_source_description`,
`data_destination_id`, `data_destination_description`, and `payload`. The
`payload` value is the configured source row as a JSON object. The diagnostic
headers use the `X-Postie-*` prefix and include event ID, delivery generation,
replay flag, Kafka topic, partition, and offset. `Idempotency-Key` combines the
event identifier, destination ID, and generation.

The destination acknowledges a delivered event with a non-null `success` object
inside `result`, for example `{"result":{"success":{}}}`. An `error` object
with policy `must_retry` requests a retry. Policy `keep_going`, with `class` and
`description` fields present, records a terminal skip before advancing the
Kafka offset. A valid success takes precedence if both recognized outcomes are
present. Unknown policies, malformed or empty bodies, non-2xx responses,
timeouts, and connection failures retry. Redirects are not followed. Responses
larger than 64 KiB retry. A response of exactly 64 KiB is still evaluated. The
default request timeout is 60 seconds.

Filters compare a configured string field in the JSON payload. A matching value
is delivered and a nonmatching string is skipped. A missing field, non-string
field, or non-object payload passes through without filtering.

Integer values remain JSON numbers with their full integer precision. Floating
point values remain JSON numbers and exponent notation is expanded to a decimal
form. Booleans and SQL `NULL` remain JSON booleans and `null`. Text remains a
JSON string. Non-null `json` values remain JSON text inside a JSON string;
nullable `json` values can be `null`. `bytea` values are base64 strings.
`timestamp` values use SQL-style text without a zone, while `timestamptz` is
normalized to UTC and ends with `+00`. See [the protocol contract](protocol.md)
for the complete field and conversion details.

## Delivery and operations

Each destination and generation has a Kafka consumer group. A worker processes
one request at a time per assigned partition. It commits an offset after a
terminal acknowledgement or audited `keep_going` skip. Other partitions and
destinations can continue while one partition retries.

Delivery is at least once. A receiver can commit its own change before Postie
commits the Kafka offset, so the same request can arrive again. Receivers should
atomically apply the event and record the idempotency key. Failures retry with
jittered exponential backoff between one and 60 seconds. There is no automatic
dead-letter skip.

The control lease lasts 30 seconds and is renewed every five seconds. Losing
control state stops new dispatch. During partition revocation, workers stop
scheduling requests, cancel in-flight work, and commit only completed records
while they still own the partition. A lost owner never commits.

The private operator API provides liveness, readiness, status, subscriptions,
pause/resume, and recent activity. `postiectl` validates configuration and
provisions capture. `postie` runs delivery workers and the operator API.
Administrative repair of blocked generations is not implemented.

## Planned replay (not implemented)

Replay is a planned capability and cannot be created or run in the current
release. The requirements below describe intended behavior, not an available
operator API or runtime guarantee.

Only projection destinations with a configured replay target are eligible for
replay. A replay uses an independent Kafka consumer group and cannot change
normal delivery offsets. Creation validates the stream identity and retained
history, then records starting offsets and exclusive per-partition boundaries.
It also stores the configuration revision and a declaration that the target
starts fresh. Dispatch begins after that.

A bounded replay stops at its recorded boundaries. A follow replay continues
after reaching those initial boundaries. Planned lifecycle states are `created`,
`running`, `following`, `paused`, `blocked`, `completed`, and `cancelled`.
Restart resumes from replay-group Kafka commits; missing history blocks the
replay, and a cancelled replay cannot resume.

Replay targets must use a fresh projection and deduplication database, or a
separate application generation namespace. The application validates the
rebuilt model and switches read traffic. Postie will not provide an atomic
multi-partition cutover while writes continue.

## Development topology and verification

The development Compose project is named `postie`; its API and workers run
under the `postie` profile. Kafka Connect uses the `postie-connect` group
and topic names. Control-state tables and capture resources both use the
`postie_` prefix.

Integration tests use the isolated Compose project `postie-integration` on
ports 25432, 25433, 39092, and 28083. Set `POSTIE_KEEP=1` to retain its
containers and volumes after a run. Protocol examples in
`tests/fixtures/protocol` are reviewed synthetic contract examples. They are
not live captures.
