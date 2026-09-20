
CREATE TABLE IF NOT EXISTS postie_streams (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    source_id text NOT NULL,
    identity jsonb NOT NULL,
    names jsonb NOT NULL,
    topic_id uuid NOT NULL,
    blocked text NOT NULL DEFAULT '',
    PRIMARY KEY (namespace, environment, generation, source_id)
);
CREATE TABLE IF NOT EXISTS postie_subscriptions (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    destination_id text NOT NULL,
    desired text NOT NULL CHECK (desired IN ('running', 'paused')),
    revision bigint NOT NULL CHECK (revision > 0),
    PRIMARY KEY (namespace, environment, generation, destination_id)
);
CREATE TABLE IF NOT EXISTS postie_worker_leases (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    worker_id text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (namespace, environment, generation, worker_id)
);
CREATE TABLE IF NOT EXISTS postie_worker_subscriptions (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    worker_id text NOT NULL,
    destination_id text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    state text NOT NULL CHECK (state <> ''),
    PRIMARY KEY (namespace, environment, generation, worker_id, destination_id)
);
CREATE TABLE IF NOT EXISTS postie_skips (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    destination_id text NOT NULL,
    source_id text NOT NULL,
    event_id text NOT NULL DEFAULT '',
    topic text NOT NULL,
    partition integer NOT NULL,
    offset_value bigint NOT NULL,
    PRIMARY KEY (namespace, environment, generation, destination_id, topic, partition, offset_value)
);
CREATE TABLE IF NOT EXISTS postie_partition_started (
    namespace text NOT NULL,
    environment text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    destination_id text NOT NULL,
    topic text NOT NULL,
    partition integer NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, environment, generation, destination_id, topic, partition)
);