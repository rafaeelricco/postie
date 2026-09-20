package config

// Application is the application configuration: source definitions
// and destination definitions that consume their records.
type Application struct {
	Sources      []Source      `yaml:"data_sources"`
	Destinations []Destination `yaml:"data_destinations"`
}

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

type SourceType string

const SourcePostgres SourceType = "postgres"

type Filter struct {
	Column string   `yaml:"column"`
	Values []string `yaml:"values"`
}

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

type DestinationType string

const DestinationHTTPPush DestinationType = "http-push"
