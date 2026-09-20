package protocol

import "encoding/json"

// Envelope is the HTTP payload sent to a Postie destination.
type Envelope struct {
	DataSourceID               string          `json:"data_source_id"`
	DataSourceDescription      string          `json:"data_source_description"`
	DataDestinationID          string          `json:"data_destination_id"`
	DataDestinationDescription string          `json:"data_destination_description"`
	Payload                    json.RawMessage `json:"payload"`
}
