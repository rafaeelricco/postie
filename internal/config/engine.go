package config

import "time"

type Engine struct {
	Version           int    `yaml:"version"`
	Namespace         string `yaml:"namespace"`
	Environment       string `yaml:"environment"`
	ApplicationConfig string `yaml:"application_config"`
	Kafka             struct {
		Brokers           []string `yaml:"brokers"`
		Partitions        int32    `yaml:"partitions"`
		ReplicationFactor int16    `yaml:"replication_factor"`
	} `yaml:"kafka"`
	Connect struct {
		URL string `yaml:"url"`
	} `yaml:"connect"`
	Operator struct {
		Listen    string `yaml:"listen"`
		TokenFile string `yaml:"token_file"`
	} `yaml:"operator"`
	Control struct {
		DatabaseURL string `yaml:"database_url"`
	} `yaml:"control"`
	Delivery struct {
		RequestTimeout time.Duration `yaml:"request_timeout"`
		DrainTimeout   time.Duration `yaml:"drain_timeout"`
	} `yaml:"delivery"`
	Destinations map[string]DestinationPolicy `yaml:"destinations"`
	Sources      map[string]SourcePolicy      `yaml:"sources"`
}

// DestinationPolicy carries the engine-only facts about a destination:
// whether it may be replayed, and where a replay should be sent.
type DestinationPolicy struct {
	Kind           DestinationKind `yaml:"kind"`
	ReplayEndpoint string          `yaml:"replay_endpoint"`
}

// SourcePolicy is engine-side configuration for one data source.
type SourcePolicy struct {
	// Publication names a publication the table owner created beforehand.
	// Empty means the engine lets Debezium manage one, which requires the
	// capture user to own the table.
	Publication string `yaml:"publication"`
}

type DestinationKind string

const (
	KindProjection DestinationKind = "projection"
	KindReaction   DestinationKind = "reaction" // the default: never replayable
)

// PolicyFor returns the engine-only policy for a destination. Unlisted
// destinations default to {Kind: "reaction"} and are never replayable.
func (e Engine) PolicyFor(destinationID string) DestinationPolicy {
	if policy, ok := e.Destinations[destinationID]; ok {
		return policy
	}
	return DestinationPolicy{Kind: KindReaction}
}
