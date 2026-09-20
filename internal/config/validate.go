package config

import (
	"fmt"
	"strings"
	"time"
)

func (c Application) Validate() error {
	sources := map[string]Source{}
	for _, s := range c.Sources {
		if s.ID == "" || s.Description == "" {
			return fmt.Errorf("source id and description are required")
		}
		if _, ok := sources[s.ID]; ok {
			return fmt.Errorf("duplicate source id %q", s.ID)
		}
		if s.Type != SourcePostgres {
			return fmt.Errorf("source %q: unsupported type %q; v1 supports postgres", s.ID, s.Type)
		}
		if s.Host == "" || s.Port < 1 || s.Username == "" || s.Database == "" || s.Table == "" {
			return fmt.Errorf("source %q: incomplete PostgreSQL connection", s.ID)
		}
		if s.SerialColumn == "" || s.PartitioningColumn == "" || !contains(s.Columns, s.SerialColumn) || !contains(s.Columns, s.PartitioningColumn) {
			return fmt.Errorf("source %q: columns must include serialColumn and partitioningColumn", s.ID)
		}
		sources[s.ID] = s
	}
	if len(sources) == 0 {
		return fmt.Errorf("at least one source is required")
	}
	destinations := map[string]struct{}{}
	for _, d := range c.Destinations {
		if d.ID == "" || d.Description == "" {
			return fmt.Errorf("destination id and description are required")
		}
		if _, ok := destinations[d.ID]; ok {
			return fmt.Errorf("duplicate destination id %q", d.ID)
		}
		destinations[d.ID] = struct{}{}
		if d.Type != DestinationHTTPPush {
			return fmt.Errorf("destination %q: unsupported type %q; v1 supports http-push", d.ID, d.Type)
		}
		if d.Endpoint == "" || d.Username == "" || len(d.Sources) == 0 {
			return fmt.Errorf("destination %q: incomplete HTTP push configuration", d.ID)
		}
		for _, id := range d.Sources {
			if _, ok := sources[id]; !ok {
				return fmt.Errorf("destination %q: unknown source %q", d.ID, id)
			}
		}
		if d.Filter != nil && (d.Filter.Column == "" || len(d.Filter.Values) == 0) {
			return fmt.Errorf("destination %q: filter column and values are required", d.ID)
		}
	}
	return nil
}

func (c *Engine) Validate() error {
	if c.Version != 1 {
		return fmt.Errorf("engine config version must be 1")
	}
	if c.Namespace == "" || c.Environment == "" {
		return fmt.Errorf("namespace and environment are required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("at least one Kafka broker is required")
	}
	if c.Kafka.Partitions == 0 {
		c.Kafka.Partitions = 10
	}
	if c.Kafka.Partitions < 1 {
		return fmt.Errorf("kafka partitions must be positive")
	}
	if c.Kafka.ReplicationFactor == 0 {
		c.Kafka.ReplicationFactor = 1
	}
	if c.Kafka.ReplicationFactor < 1 {
		return fmt.Errorf("kafka replication_factor must be positive")
	}
	if c.Environment == "production" && c.Kafka.ReplicationFactor != 3 {
		return fmt.Errorf("kafka replication_factor must be 3 in production")
	}
	if c.Connect.URL == "" || c.Operator.Listen == "" {
		return fmt.Errorf("connect.url and operator.listen are required")
	}
	if c.Control.DatabaseURL == "" {
		return fmt.Errorf("control.database_url is required")
	}
	if c.Delivery.RequestTimeout == 0 {
		c.Delivery.RequestTimeout = 60 * time.Second
	}
	if c.Delivery.DrainTimeout == 0 {
		c.Delivery.DrainTimeout = 30 * time.Second
	}
	for id, policy := range c.Destinations {
		if policy.Kind != KindProjection && policy.Kind != KindReaction {
			return fmt.Errorf("destination %q: kind must be \"projection\" or \"reaction\", got %q", id, policy.Kind)
		}
		if policy.Kind == KindReaction && policy.ReplayEndpoint != "" {
			return fmt.Errorf("destination %q: reaction destinations must not set replay_endpoint", id)
		}
	}
	return nil
}

func contains(items []string, item string) bool {
	return strings.Contains("\x00"+strings.Join(items, "\x00")+"\x00", "\x00"+item+"\x00")
}
