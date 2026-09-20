package stream

// Identity is frozen per stream generation (spec §Source and topic
// contract). It is stored as JSONB in the control store, so its fields are
// JSON-tagged.
type Identity struct {
	Table              string   `json:"table"`
	SerialColumn       string   `json:"serialColumn"`
	PartitioningColumn string   `json:"partitioningColumn"`
	EventIDColumn      string   `json:"eventIdColumn"`
	Partitions         int32    `json:"partitions"`
	Columns            []Column `json:"columns"` // in config order; the payload profile input
}

// Names are the deterministic Kafka/Postgres/Connect names for one stream
// generation. TopicPrefix is the Debezium topic.prefix; since Debezium
// names topics "<topic.prefix>.<schema>.<table>", Topic is the full Kafka
// topic name Debezium actually writes to.
type Names struct {
	TopicPrefix string
	Topic       string
	Connector   string
	Slot        string
	Publication string
}

// Scope identifies one deployed stream generation.
type Scope struct {
	Namespace   string
	Environment string
	Generation  Generation
}

// Registration is the control-plane registration for one source stream.
type Registration struct {
	SourceID string
	Identity Identity
	Names    Names
	TopicID  [16]byte
	Blocked  string
}
