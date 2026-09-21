# Postie protocol contract

This document defines the HTTP request Postie sends to configured
`http-push` destinations and the acknowledgement it accepts. Contract examples
under `tests/fixtures/protocol` are reviewed synthetic examples. They are not
live captures.

## Request body

Postie sends a JSON `POST` with `Content-Type: application/json` and HTTP
Basic authentication from the destination's configured `username` and
`password`. The body has these established keys:

```json
{
  "data_source_id": "application_events",
  "data_source_description": "Application event store",
  "data_destination_id": "Accounts_Projection",
  "data_destination_description": "Accounts read model",
  "payload": {
    "event_id": "evt_42",
    "recorded_on": "2024-03-01 08:00:00.123456+00",
    "payload": "{\"type\":\"AccountCreated\"}"
  }
}
```

`payload` contains the configured source columns as JSON fields. In the example,
the inner `payload` field is a string that holds JSON text. A PostgreSQL `text`
value stays a JSON string; Postie does not parse it as an object. JSON object
key order is not part of the contract.

Postie adds these diagnostic headers:

| Header                         | Value                                                                                                                             |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `X-Postie-Event-ID`            | Source `event_id`, or a stable SHA-256 identifier derived from Kafka topic, partition, and offset when no event ID is configured. |
| `X-Postie-Delivery-Generation` | Stream generation number.                                                                                                         |
| `X-Postie-Replay`              | Boolean replay marker; current delivery is live, and replay jobs are not implemented.                                             |
| `X-Postie-Topic`               | Kafka topic.                                                                                                                      |
| `X-Postie-Partition`           | Kafka partition number.                                                                                                           |
| `X-Postie-Offset`              | Kafka offset.                                                                                                                     |
| `Idempotency-Key`              | Event identifier, destination ID, and generation joined with colons.                                                              |

The request timeout defaults to 60 seconds. Postie does not follow redirects.
Any non-2xx status is retryable. A response body larger than 64 KiB is rejected.
A body of exactly 64 KiB still goes through acknowledgement decoding.

## Acknowledgements

A destination acknowledges delivery with a non-null object at
`result.success`. The empty object is valid:

```json
{ "result": { "success": {} } }
```

A response can instead return an error object with `policy`, `class`, and
`description` fields:

```json
{
  "result": {
    "error": {
      "policy": "must_retry",
      "class": "temporary",
      "description": "try again"
    }
  }
}
```

`must_retry` retries the current record. `keep_going` records a terminal skip,
then advances the Kafka offset. Unknown policies and malformed error objects
retry. If both a valid `success` and an error are present, success takes
precedence.

Empty or malformed bodies, unknown result shapes, non-2xx responses, timeouts,
and connection failures all retry. Retries use jittered exponential backoff from
1 to 60 seconds and continue until delivery is cancelled. A `keep_going` skip is
saved before its offset is committed.

## Filters

An optional destination filter names a payload column and a list of allowed
string values. A string equal to one of those values is delivered. A different
string is skipped. If the field is missing, is not a string, or the payload is
not a JSON object, the record passes through without filtering.

## PostgreSQL value conversion

The source contract accepts `int2`, `int4`, `int8`, `float4`, `float8`, `bool`,
`json`, `bytea`, `timestamp`, `timestamptz`, and `text`. A configured
`serialColumn` must be a non-null integer type with a unique or primary-key
index on that column alone. The configured `partitioningColumn` must be
non-null. Both columns must be included in the `columns` list.

| PostgreSQL type        | JSON value sent in `payload`                                                                                                          |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `int2`, `int4`, `int8` | JSON number. Integer range is validated and full integer precision is retained.                                                       |
| `float4`, `float8`     | JSON number. Input is range-checked and exponent notation is expanded to decimal form.                                                |
| `bool`                 | JSON `true` or `false`.                                                                                                               |
| SQL `NULL`             | JSON `null`, including nullable `json` columns.                                                                                       |
| `text`                 | JSON string, preserving the text value.                                                                                               |
| `json`                 | A JSON string containing the SQL JSON text; SQL `NULL` becomes JSON `null`.                                                           |
| `bytea`                | Base64 JSON string.                                                                                                                   |
| `timestamp`            | SQL-style string with a space separator and no zone; fractional seconds are emitted only when non-zero, with trailing zeroes trimmed. |
| `timestamptz`          | SQL-style string normalized to UTC, ending in `+00`; fractional seconds follow the same trimming rule.                                |

`jsonb` is unsupported. Unsupported source types fail source contract
validation instead of producing a guessed conversion.

## Contract fixtures

`tests/fixtures/protocol` contains reviewed synthetic request bodies and headers
used by `tests/contract/envelope_test.go`. The suite compares the envelope and
HTTP behavior to the contract. The fixtures are not generated from a separate
service or a live destination.
