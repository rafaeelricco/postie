package config

// Application is the application configuration: source definitions
// and destination definitions that consume their records.
type Application struct {
	Sources      []Source      `yaml:"data_sources"`
	Destinations []Destination `yaml:"data_destinations"`
}

// Source is one captured PostgreSQL table: its connection, and the columns
// Postie tracks for it.
type Source struct {
	ID                 string     `yaml:"id"`
	Description        string     `yaml:"description"`
	Type               SourceType `yaml:"type"`
	Host               string     `yaml:"host"`
	Port               int        `yaml:"port"`
	Username           string     `yaml:"username"`
	Password           string     `yaml:"password"`
	Database           string     `yaml:"database"`
	Table              string     `yaml:"table"`
	Columns            []string   `yaml:"columns"`
	SerialColumn       string     `yaml:"serialColumn"`
	PartitioningColumn string     `yaml:"partitioningColumn"`
}

// SourceType names the kind of system a Source connects to.
type SourceType string

// SourcePostgres is the only SourceType v1 supports.
const SourcePostgres SourceType = "postgres"

// Filter keeps only records whose Column holds one of Values.
type Filter struct {
	Column string   `yaml:"column"`
	Values []string `yaml:"values"`
}

// Destination is one place records are delivered to, and which sources feed it.
type Destination struct {
	ID          string          `yaml:"id"`
	Description string          `yaml:"description"`
	Type        DestinationType `yaml:"type"`
	Endpoint    string          `yaml:"endpoint"`
	Username    string          `yaml:"username"`
	Password    string          `yaml:"password"`
	Sources     []string        `yaml:"sources"`
	Filter      *Filter         `yaml:"filter"`
}

// DestinationType names the kind of system a Destination delivers to.
type DestinationType string

// DestinationHTTPPush is the only DestinationType v1 supports.
const DestinationHTTPPush DestinationType = "http-push"
