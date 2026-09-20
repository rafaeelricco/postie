package regression

import (
	"context"
	"errors"
	"github.com/rafaeelricco/postie/internal/adapters/debezium"
	"github.com/rafaeelricco/postie/internal/adapters/httpdelivery"
	"github.com/rafaeelricco/postie/internal/stream"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/config"
	"github.com/rafaeelricco/postie/internal/delivery"
	"github.com/rafaeelricco/postie/internal/provision"
)

func TestRegression_CaptureNamesPreserveIdentity(t *testing.T) {
	type tuple struct {
		namespace   string
		environment string
		sourceID    string
	}
	tuples := []tuple{
		{"ns", "dev", "orders.v1"},
		{"ns", "dev", "orders-v1"},
		{"ns", "dev", "orders_v1"},
		{"ns", "dev", "Orders_v1"},
		{"a.b", "c", "orders"},
		{"a", "b.c", "orders"},
	}
	fields := func(n stream.Names) []string {
		return []string{n.TopicPrefix, n.Topic, n.Connector, n.Slot, n.Publication}
	}

	all := make([][]string, len(tuples))
	for i, tc := range tuples {
		n := provision.NamesFor(tc.namespace, tc.environment, stream.Source{ID: tc.sourceID, Table: "events"}, 1)
		all[i] = fields(n)
		for j, got := range all[i] {
			if got == "" {
				t.Errorf("tuple %d resource field %d is empty", i, j)
			}
		}
	}
	for i := range all {
		for j := i + 1; j < len(all); j++ {
			for k := range fields(provision.NamesFor("ns", "dev", stream.Source{ID: "orders", Table: "events"}, 1)) {
				if all[i][k] == all[j][k] {
					t.Errorf("resource field %d collides for tuples %d and %d: %q", k, i, j, all[i][k])
				}
			}
		}
	}

	longBase := strings.Repeat("source-", 14)
	longA := provision.NamesFor("a-very-long-namespace", "a-very-long-environment", stream.Source{ID: longBase + "alpha", Table: "events"}, 1)
	longB := provision.NamesFor("a-very-long-namespace", "a-very-long-environment", stream.Source{ID: longBase + "bravo", Table: "events"}, 1)
	if longA.Slot == longB.Slot || longA.Publication == longB.Publication {
		t.Fatalf("long source IDs differing only in their suffix collided: A=%+v B=%+v", longA, longB)
	}
	if again := provision.NamesFor("a-very-long-namespace", "a-very-long-environment", stream.Source{ID: longBase + "alpha", Table: "events"}, 1); again != longA {
		t.Fatalf("NamesFor is not deterministic: first=%+v again=%+v", longA, again)
	}
	generationOne := fields(provision.NamesFor("ns", "dev", stream.Source{ID: "orders", Table: "events"}, 1))
	generationTwo := fields(provision.NamesFor("ns", "dev", stream.Source{ID: "orders", Table: "events"}, 2))
	for i := range generationOne {
		if generationOne[i] == generationTwo[i] {
			t.Errorf("resource field %d did not change between generations: %q", i, generationOne[i])
		}
	}

	maxGeneration := stream.Generation(int(^uint(0) >> 1))
	maxNames := provision.NamesFor(strings.Repeat("n", 24), strings.Repeat("e", 24), stream.Source{ID: strings.Repeat("s", 24), Table: "events"}, maxGeneration)
	for label, value := range map[string]string{"slot": maxNames.Slot, "publication": maxNames.Publication} {
		if len(value) > 63 {
			t.Errorf("%s name length = %d, want <= 63: %q", label, len(value), value)
		}
		for _, r := range value {
			if !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') && r != '_' {
				t.Errorf("%s name contains invalid PostgreSQL identifier rune %U: %q", label, r, value)
			}
		}
	}
}

func TestRegression_EnvironmentStringsPreserved(t *testing.T) {
	values := []string{"alpha #beta", "alpha: beta", `embedded "double" quote`, `literal\backslash`, "actual\nnewline", "null", "true", "001", "${OTHER}"}
	styles := map[string]func(string) string{
		"unquoted":      func(value string) string { return "${SECRET}" },
		"single-quoted": func(value string) string { return "'${SECRET}'" },
		"double-quoted": func(value string) string { return `"${SECRET}"` },
	}
	for _, value := range values {
		for style, scalar := range styles {
			t.Run(style+"/"+value, func(t *testing.T) {
				t.Setenv("SECRET", value)
				t.Setenv("OTHER", "must-not-expand")
				app := `
data_sources:
  - id: source
    description: source
    type: postgres
    host: db
    port: 5432
    username: engine
    password: ` + scalar(value) + `
    database: app
    table: events
    columns: [id, correlation_id]
    serialColumn: id
    partitioningColumn: correlation_id
data_destinations: []
`
				loadedApp, err := config.LoadApplication(writeRegressionFixture(t, app))
				if err != nil {
					t.Fatalf("LoadApplication() error = %v", err)
				}
				if got := loadedApp.Sources[0].Password; got != value {
					t.Errorf("LoadApplication() password = %q, want %q", got, value)
				}

				engine := `
version: 1
namespace: ` + scalar(value) + `
environment: development
kafka:
  brokers: [kafka:9092]
connect:
  url: http://connect:8083
operator:
  listen: 0.0.0.0:8081
control:
  database_url: postgres://control
`
				loadedEngine, err := config.LoadEngine(writeRegressionFixture(t, engine))
				if err != nil {
					t.Fatalf("LoadEngine() error = %v", err)
				}
				if got := loadedEngine.Namespace; got != value {
					t.Errorf("LoadEngine() namespace = %q, want %q", got, value)
				}
			})
		}
	}
}

func TestRegression_EnvironmentStructurePreserved(t *testing.T) {
	for _, name := range []string{"SECRET", "REG_COMMENT_MUST_BE_IGNORED", "REG_KEY_MUST_BE_IGNORED", "OTHER"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
	t.Setenv("SECRET", "anchored")
	app := `
# ${REG_COMMENT_MUST_BE_IGNORED}
"${REG_KEY_MUST_BE_IGNORED}": ignored
data_sources:
  - id: source
    description: source
    type: postgres
    host: db
    port: 5432
    username: engine
    database: app
    table: events
    columns: [id, correlation_id]
    serialColumn: id
    partitioningColumn: correlation_id
data_destinations:
  - id: destination
    description: destination
    type: http-push
    endpoint: http://example.test
    username: engine
    sources: [source]
    filter:
      column: value
      values:
        - &regression_anchor ${SECRET}
        - *regression_anchor
`
	loaded, err := config.LoadApplication(writeRegressionFixture(t, app))
	if err != nil {
		t.Fatalf("LoadApplication() structural fixture error = %v", err)
	}
	if got, want := loaded.Destinations[0].Filter.Values, []string{"anchored", "anchored"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("anchor and alias values = %#v, want %#v", got, want)
	}
	engine := `
# ${REG_COMMENT_MUST_BE_IGNORED}
"${REG_KEY_MUST_BE_IGNORED}": ignored
version: 1
namespace: ${SECRET}
environment: development
kafka:
  brokers: [kafka:9092]
connect:
  url: http://connect:8083
operator:
  listen: 0.0.0.0:8081
control:
  database_url: postgres://control
`
	loadedEngine, err := config.LoadEngine(writeRegressionFixture(t, engine))
	if err != nil {
		t.Fatalf("LoadEngine() structural fixture error = %v", err)
	}
	if loadedEngine.Namespace != "anchored" {
		t.Fatalf("LoadEngine() namespace = %q, want %q", loadedEngine.Namespace, "anchored")
	}
}

func TestRegression_EnvironmentTypedFields(t *testing.T) {
	t.Setenv("DURATION", "1500ms")
	t.Setenv("REG_PORT", "5432")
	engine := `
version: 1
namespace: ns
environment: development
kafka:
  brokers: [kafka:9092]
  partitions: 001
  replication_factor: 001
connect:
  url: http://connect:8083
operator:
  listen: 0.0.0.0:8081
control:
  database_url: postgres://control
delivery:
  request_timeout: ${DURATION}
  drain_timeout: 2s
`
	loaded, err := config.LoadEngine(writeRegressionFixture(t, engine))
	if err != nil {
		t.Fatalf("LoadEngine() typed fixture error = %v", err)
	}
	if loaded.Kafka.Partitions != 1 || loaded.Kafka.ReplicationFactor != 1 {
		t.Errorf("literal numeric config fields = partitions %d, replication factor %d; want 1, 1", loaded.Kafka.Partitions, loaded.Kafka.ReplicationFactor)
	}
	if loaded.Delivery.RequestTimeout != 1500*time.Millisecond {
		t.Errorf("request timeout = %s, want 1.5s", loaded.Delivery.RequestTimeout)
	}

	app := `
data_sources:
  - id: source
    description: source
    type: postgres
    host: db
    port: ${REG_PORT}
    username: engine
    database: app
    table: events
    columns: [id, correlation_id]
    serialColumn: id
    partitioningColumn: correlation_id
data_destinations: []
`
	if _, err := config.LoadApplication(writeRegressionFixture(t, app)); err == nil {
		t.Fatal("LoadApplication() accepted an environment placeholder for an integer field")
	}
}

func TestRegression_EnvironmentMissingNamesAggregateAcrossScalars(t *testing.T) {
	for _, name := range []string{"REG_MISSING_A", "REG_MISSING_Z"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
	content := `
data_sources:
  - id: source
    description: source
    type: postgres
    host: db
    port: 5432
    username: ${REG_MISSING_Z}
    password: ${REG_MISSING_A}
    database: ${REG_MISSING_Z}
    table: events
    columns: [id, correlation_id]
    serialColumn: id
    partitioningColumn: correlation_id
data_destinations: []
`
	_, err := config.LoadApplication(writeRegressionFixture(t, content))
	if err == nil {
		t.Fatal("LoadApplication() accepted missing environment variables")
	}
	want := "environment variables REG_MISSING_A, REG_MISSING_Z are not set"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("LoadApplication() error = %q, want sorted and deduplicated names %q", err, want)
	}
}

func TestRegression_RedirectsRetryOriginalPOST(t *testing.T) {
	statuses := []int{http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect}
	for _, status := range statuses {
		t.Run(http.StatusText(status), func(t *testing.T) {
			for _, supplied := range []bool{false, true} {
				t.Run(map[bool]string{false: "default client", true: "supplied client"}[supplied], func(t *testing.T) {
					var originalCount, targetCount int32
					type request struct {
						method, body, username, password, idempotency string
					}
					var requests []request
					var requestsMu sync.Mutex
					target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						atomic.AddInt32(&targetCount, 1)
						_, _ = w.Write([]byte(`{"result":{"success":{}}}`))
					}))
					defer target.Close()
					original := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						atomic.AddInt32(&originalCount, 1)
						body, _ := io.ReadAll(r.Body)
						username, password, _ := r.BasicAuth()
						requestsMu.Lock()
						requests = append(requests, request{r.Method, string(body), username, password, r.Header.Get("Idempotency-Key")})
						requestsMu.Unlock()
						w.Header().Set("Location", target.URL)
						w.WriteHeader(status)
					}))
					defer original.Close()

					var client *http.Client
					var transport *regressionRoundTripper
					var callbackCalls int32
					var jar http.CookieJar
					if supplied {
						cookieJar, err := cookiejar.New(nil)
						if err != nil {
							t.Fatal(err)
						}
						jar = cookieJar
						transport = &regressionRoundTripper{base: http.DefaultTransport}
						client = &http.Client{
							Transport: transport,
							Jar:       jar,
							Timeout:   17 * time.Second,
							CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
								atomic.AddInt32(&callbackCalls, 1)
								return nil
							},
						}
					}
					destination := config.Destination{ID: "destination", Description: "destination", Endpoint: original.URL, Username: "user", Password: "pass"}

					sender := delivery.NewProcessor(delivery.Destination{ID: destination.ID, Description: destination.Description}, httpdelivery.NewClient(client, httpdelivery.Destination{ID: destination.ID, Endpoint: destination.Endpoint, Username: destination.Username, Password: destination.Password}))
					sleeps := 0
					sender.Sleep = func(context.Context, time.Duration) error {
						sleeps++
						if sleeps == 1 {
							return nil
						}
						return errRetried
					}
					record := stream.Record{Source: stream.Source{ID: "source", Description: "source"}, Payload: []byte(`{"value":"payload"}`), Topic: "topic", Partition: 4, Offset: 7, EventID: "event-id", Generation: 3}
					var before http.Client
					if supplied {
						before = *client
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_, err := sender.Process(ctx, record, nil)
					if !errors.Is(err, errRetried) {
						t.Fatalf("Process() error = %v, want errRetried", err)
					}
					if got := atomic.LoadInt32(&originalCount); got != 2 {
						t.Fatalf("original request count = %d, want 2", got)
					}
					if got := atomic.LoadInt32(&targetCount); got != 0 {
						t.Fatalf("redirect target request count = %d, want 0", got)
					}
					requestsMu.Lock()
					gotRequests := append([]request(nil), requests...)
					requestsMu.Unlock()
					if sleeps != 2 || len(gotRequests) != 2 {
						t.Fatalf("sleeps=%d requests=%d, want 2 and 2", sleeps, len(gotRequests))
					}
					wantKey := "event-id:destination:3"
					wantBody := `{"data_source_id":"source","data_source_description":"source","data_destination_id":"destination","data_destination_description":"destination","payload":{"value":"payload"}}`
					for i, got := range gotRequests {
						if got.method != http.MethodPost || got.body != wantBody || got.username != "user" || got.password != "pass" || got.idempotency != wantKey {
							t.Errorf("request %d = %+v, want original POST/body/basic auth/idempotency %q", i, got, wantKey)
						}
					}
					if supplied {
						if got := transport.calls.Load(); got != 2 {
							t.Fatalf("supplied transport calls = %d, want 2", got)
						}
						if client.Timeout != before.Timeout || client.Transport != before.Transport || client.Jar != before.Jar || reflect.ValueOf(client.CheckRedirect).Pointer() != reflect.ValueOf(before.CheckRedirect).Pointer() {
							t.Fatal("supplied http.Client settings changed while sending")
						}
						if got := atomic.LoadInt32(&callbackCalls); got != 0 {
							t.Fatalf("supplied CheckRedirect callback called during sender.Process: %d", got)
						}
						response, err := client.Get(original.URL)
						if err != nil {
							t.Fatalf("independent supplied client request failed: %v", err)
						}
						response.Body.Close()
						if got := atomic.LoadInt32(&targetCount); got != 1 {
							t.Fatalf("independent supplied client target requests = %d, want 1", got)
						}
						if got := atomic.LoadInt32(&callbackCalls); got != 1 {
							t.Fatalf("independent supplied client callback calls = %d, want 1", got)
						}
					}
				})
			}
		})
	}
}

type regressionRoundTripper struct {
	base  http.RoundTripper
	calls atomic.Int32
}

func (t *regressionRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return t.base.RoundTrip(r)
}

func TestRegression_SnapshotTableIsQuoted(t *testing.T) {
	src := stream.Source{Table: "Event_Store", SerialColumn: "id", Columns: []string{"id"}}
	id := stream.Identity{Columns: []stream.Column{{Name: "id", Type: stream.PGInt8}}}
	n := provision.NamesFor("ns", "dev", src, 1)
	cfg := debezium.ConnectorConfig(src, debezium.Connection{}, id, n, provision.PublicationManaged)
	if got, want := cfg["snapshot.select.statement.overrides.public.Event_Store"], `SELECT "id" FROM public."Event_Store" ORDER BY "id" ASC`; got != want {
		t.Fatalf("snapshot override = %q, want %q", got, want)
	}
}

func writeRegressionFixture(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}
