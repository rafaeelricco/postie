package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/rafaeelricco/postie/internal/adapters/httpdelivery"
	"github.com/rafaeelricco/postie/internal/stream"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/delivery"
)

// longNumberLiteral matches a JSON number value at least 10 digits long:
// int8 extremes and the large float literal in the reviewed contract fixtures,
// numbers big enough that decoding into float64 and re-encoding would
// silently change their digits.
var longNumberLiteral = regexp.MustCompile(`:(-?[0-9]{10,})[,}]`)

// contractEnvelope is the envelope described by a reviewed contract fixture.
// Payload stays as raw bytes so its number literals remain unchanged.
type contractEnvelope struct {
	DataSourceID               string          `json:"data_source_id"`
	DataSourceDescription      string          `json:"data_source_description"`
	DataDestinationID          string          `json:"data_destination_id"`
	DataDestinationDescription string          `json:"data_destination_description"`
	Payload                    json.RawMessage `json:"payload"`
}

// TestEnvelopeMatchesContractFixtures replays each reviewed fixture through
// delivery.Sender.Process and checks the resulting envelope, long number
// literals, Content-Type, and Basic Authorization header. Envelope comparison
// is semantic because JSON object key order is not significant.
func TestEnvelopeMatchesContractFixtures(t *testing.T) {
	const dir = "../fixtures/protocol"
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatalf("globbing %s: %v", dir, err)
	}
	if len(files) == 0 {
		t.Fatalf("%s: no reviewed contract fixtures found", dir)
	}
	sort.Strings(files)

	for _, path := range files {
		t.Run(strings.TrimSuffix(filepath.Base(path), ".json"), func(t *testing.T) {
			fixtureRaw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			headersPath := strings.TrimSuffix(path, ".json") + ".headers.txt"
			headers := parseHeaders(t, headersPath)

			var envelope contractEnvelope
			if err := json.Unmarshal(fixtureRaw, &envelope); err != nil {
				t.Fatalf("decoding contract fixture %s: %v", path, err)
			}

			var engineRaw []byte
			var engineContentType, engineAuthorization string
			receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				engineRaw, _ = io.ReadAll(r.Body)
				engineContentType = r.Header.Get("Content-Type")
				engineAuthorization = r.Header.Get("Authorization")
				_, _ = w.Write([]byte(`{"result":{"success":{}}}`))
			}))
			defer receiver.Close()
			destination := config.Destination{
				ID:          envelope.DataDestinationID,
				Description: envelope.DataDestinationDescription,
				Endpoint:    receiver.URL,
				Username:    "contract_http_user",
				Password:    "contract_http_pass",
			}

			sender := delivery.NewProcessor(delivery.Destination{ID: destination.ID, Description: destination.Description}, httpdelivery.NewClient(nil, httpdelivery.Destination{ID: destination.ID, Endpoint: destination.Endpoint, Username: destination.Username, Password: destination.Password}))
			sender.Sleep = func(context.Context, time.Duration) error {
				t.Fatal("unexpected retry sleep")
				return nil
			}

			record := stream.Record{
				Source:     stream.Source{ID: envelope.DataSourceID, Description: envelope.DataSourceDescription},
				Payload:    envelope.Payload,
				Generation: 1,
			}
			outcome, err := sender.Process(context.Background(), record, nil)
			if err != nil || outcome != delivery.Delivered {
				t.Fatalf("Process(%s) = %v, %v; want Delivered, nil", path, outcome, err)
			}

			var fixtureDecoded, engineDecoded any
			fixtureDecoder := json.NewDecoder(bytes.NewReader(fixtureRaw))
			fixtureDecoder.UseNumber()
			if err := fixtureDecoder.Decode(&fixtureDecoded); err != nil {
				t.Fatalf("decoding contract fixture with UseNumber: %v", err)
			}
			engineDecoder := json.NewDecoder(bytes.NewReader(engineRaw))
			engineDecoder.UseNumber()
			if err := engineDecoder.Decode(&engineDecoded); err != nil {
				t.Fatalf("decoding engine request body with UseNumber: %v", err)
			}
			if !reflect.DeepEqual(fixtureDecoded, engineDecoded) {
				t.Errorf("engine envelope differs from the reviewed contract fixture %s:\n fixture: %s\n engine:  %s", path, fixtureRaw, engineRaw)
			}

			for _, m := range longNumberLiteral.FindAllSubmatch(fixtureRaw, -1) {
				literal := m[1]
				if !bytes.Contains(engineRaw, literal) {
					t.Errorf("number literal %q from %s did not survive byte-for-byte in the engine request body: %s", literal, path, engineRaw)
				}
			}

			if engineContentType != headers["Content-Type"] {
				t.Errorf("Content-Type = %q, want %q (from %s)", engineContentType, headers["Content-Type"], headersPath)
			}
			if engineAuthorization != headers["Authorization"] {
				t.Errorf("Authorization = %q, want %q (from %s)", engineAuthorization, headers["Authorization"], headersPath)
			}
		})
	}
}

// parseHeaders reads a reviewed *.headers.txt contract fixture (an HTTP request line
// followed by "Name: value" header lines) into a map. The request line has
// no ": " in it and is skipped along with any blank line.
func parseHeaders(t *testing.T, path string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	headers := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimRight(line, "\r")
		key, value, ok := strings.Cut(line, ": ")
		if !ok {
			continue
		}
		headers[key] = value
	}
	return headers
}
