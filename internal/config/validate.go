package config

import (
	"fmt"
	"slices"
	"time"
)

// Engine defaults applied by Validate to settings left at zero.
const (
	defaultPartitions        = 10
	defaultReplicationFactor = 1
	defaultRequestTimeout    = 60 * time.Second
	defaultDrainTimeout      = 30 * time.Second
	productionReplication    = 3
)

// Validate checks every source, then every destination against the known
// sources, and returns the first problem found.
func (c Application) Validate() error {
	sources, err := validateSources(c.Sources)
	if err != nil {
		return err
	}
	return validateDestinations(c.Destinations, sources)
}

// validateSources checks each source's id, description, and per-source
// rules, rejects a duplicate id, and returns the set of ids seen.
func validateSources(sources []Source) (map[string]bool, error) {
	seen := map[string]bool{}
	for _, s := range sources {
		if s.ID == "" || s.Description == "" {
			return nil, fmt.Errorf("source id and description are required")
		}
		if seen[s.ID] {
			return nil, fmt.Errorf("duplicate source id %q", s.ID)
		}
		if err := validateSource(s); err != nil {
			return nil, err
		}
		seen[s.ID] = true
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("at least one source is required")
	}
	return seen, nil
}

// validateDestinations checks each destination's id, description, and
// per-destination rules against the known source ids, and rejects a
// duplicate id.
func validateDestinations(destinations []Destination, sources map[string]bool) error {
	seen := map[string]bool{}
	for _, d := range destinations {
		if d.ID == "" || d.Description == "" {
			return fmt.Errorf("destination id and description are required")
		}
		if seen[d.ID] {
			return fmt.Errorf("duplicate destination id %q", d.ID)
		}
		seen[d.ID] = true
		if err := validateDestination(d, sources); err != nil {
			return err
		}
	}
	return nil
}

// validateSource checks one source on its own. The caller has already
// checked its ID and description.
func validateSource(s Source) error {
	if s.Type != SourcePostgres {
		return fmt.Errorf("source %q: unsupported type %q; v1 supports postgres", s.ID, s.Type)
	}
	if s.Host == "" || s.Port < 1 || s.Username == "" || s.Database == "" || s.Table == "" {
		return fmt.Errorf("source %q: incomplete PostgreSQL connection", s.ID)
	}
	if s.SerialColumn == "" || s.PartitioningColumn == "" ||
		!slices.Contains(s.Columns, s.SerialColumn) || !slices.Contains(s.Columns, s.PartitioningColumn) {
		return fmt.Errorf("source %q: columns must include serialColumn and partitioningColumn", s.ID)
	}
	return nil
}

// validateDestination checks one destination against the valid source IDs.
func validateDestination(d Destination, sources map[string]bool) error {
	if d.Type != DestinationHTTPPush {
		return fmt.Errorf("destination %q: unsupported type %q; v1 supports http-push", d.ID, d.Type)
	}
	if d.Endpoint == "" || d.Username == "" || len(d.Sources) == 0 {
		return fmt.Errorf("destination %q: incomplete HTTP push configuration", d.ID)
	}
	for _, id := range d.Sources {
		if !sources[id] {
			return fmt.Errorf("destination %q: unknown source %q", d.ID, id)
		}
	}
	if d.Filter != nil && (d.Filter.Column == "" || len(d.Filter.Values) == 0) {
		return fmt.Errorf("destination %q: filter column and values are required", d.ID)
	}
	return nil
}

// Validate applies defaults to settings left at zero and then checks the
// result, which is why it needs a pointer receiver.
//
//	var engine config.Engine // kafka.partitions omitted
//	err := engine.Validate()
//	engine.Kafka.Partitions  // 10
func (c *Engine) Validate() error {
	c.applyDefaults()
	return c.check()
}

// applyDefaults fills settings left at zero with the package's defaults.
func (c *Engine) applyDefaults() {
	if c.Kafka.Partitions == 0 {
		c.Kafka.Partitions = defaultPartitions
	}
	if c.Kafka.ReplicationFactor == 0 {
		c.Kafka.ReplicationFactor = defaultReplicationFactor
	}
	if c.Delivery.RequestTimeout == 0 {
		c.Delivery.RequestTimeout = defaultRequestTimeout
	}
	if c.Delivery.DrainTimeout == 0 {
		c.Delivery.DrainTimeout = defaultDrainTimeout
	}
}

// check runs the engine's validation rules, independent of one another, and
// returns the first one that fails.
func (c Engine) check() error {
	if c.Version != 1 {
		return fmt.Errorf("engine config version must be 1")
	}
	if c.Namespace == "" || c.Environment == "" {
		return fmt.Errorf("namespace and environment are required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("at least one Kafka broker is required")
	}
	if c.Kafka.Partitions < 1 {
		return fmt.Errorf("kafka partitions must be positive")
	}
	if c.Kafka.ReplicationFactor < 1 {
		return fmt.Errorf("kafka replication_factor must be positive")
	}
	if c.Environment == "production" && c.Kafka.ReplicationFactor != productionReplication {
		return fmt.Errorf("kafka replication_factor must be 3 in production")
	}
	if c.Connect.URL == "" || c.Operator.Listen == "" {
		return fmt.Errorf("connect.url and operator.listen are required")
	}
	if c.Control.DatabaseURL == "" {
		return fmt.Errorf("control.database_url is required")
	}
	for id, policy := range c.Destinations {
		if err := validatePolicy(id, policy); err != nil {
			return err
		}
	}
	return nil
}

// validatePolicy rejects an unknown kind, and a replay endpoint on a reaction:
// reactions cause side effects, so they are never replayed.
func validatePolicy(id string, policy DestinationPolicy) error {
	if policy.Kind != KindProjection && policy.Kind != KindReaction {
		return fmt.Errorf("destination %q: kind must be \"projection\" or \"reaction\", got %q", id, policy.Kind)
	}
	if policy.Kind == KindReaction && policy.ReplayEndpoint != "" {
		return fmt.Errorf("destination %q: reaction destinations must not set replay_endpoint", id)
	}
	return nil
}
