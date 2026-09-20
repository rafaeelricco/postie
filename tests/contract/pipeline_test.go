package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/adapters/debezium"
	"github.com/rafaeelricco/postie/internal/adapters/httpdelivery"
	"github.com/rafaeelricco/postie/internal/delivery"
	"github.com/rafaeelricco/postie/internal/stream"
)

func TestDecodeFormatSendPipeline(t *testing.T) {
	for _, op := range []string{"c", "r"} {
		t.Run(op, func(t *testing.T) {
			source := stream.Source{ID: "events", Description: "Source events", Table: "events", Columns: []string{"id", "event_id", "correlation_id", "recorded_on", "score", "payload", "binary"}, SerialColumn: "id", PartitioningColumn: "correlation_id"}
			identity := stream.Identity{Table: "events", SerialColumn: "id", PartitioningColumn: "correlation_id", EventIDColumn: "event_id", Partitions: 3, Columns: []stream.Column{{Name: "id", Type: stream.PGInt8}, {Name: "event_id", Type: stream.PGText}, {Name: "correlation_id", Type: stream.PGText}, {Name: "recorded_on", Type: stream.PGTimestamptz}, {Name: "score", Type: stream.PGFloat8}, {Name: "payload", Type: stream.PGJSON}, {Name: "binary", Type: stream.PGBytea}}}
			raw := &stream.RawRecord{Topic: "events-topic", Partition: 2, Offset: 41, LeaderEpoch: 7, Key: []byte(`{"correlation_id":"group"}`), Value: []byte(`{"source":{"schema":"public","table":"events"},"op":"` + op + `","after":{"id":9223372036854775807,"event_id":"event-1","correlation_id":"group","recorded_on":"2024-01-02T03:04:05.123456Z","score":1.25e1,"payload":"{\"z\":2,\"a\":1}","binary":"3q2+7w=="}}`)}
			record, err := debezium.Decode(source, identity, 2, raw)
			if err != nil {
				t.Fatal(err)
			}
			if record.LeaderEpoch != 7 {
				t.Fatalf("leader epoch=%d", record.LeaderEpoch)
			}
			type request struct {
				body                   []byte
				header                 http.Header
				method, user, password string
			}
			received := make(chan request, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				user, password, _ := r.BasicAuth()
				received <- request{body, r.Header.Clone(), r.Method, user, password}
				_, _ = io.WriteString(w, `{"result":{"success":{}}}`)
			}))
			defer server.Close()
			sender := httpdelivery.NewClient(nil, httpdelivery.Destination{ID: "projection", Endpoint: server.URL, Username: "user", Password: "secret"})
			processor := delivery.NewProcessor(delivery.Destination{ID: "projection", Description: "Read model"}, sender)
			processor.Sleep = func(context.Context, time.Duration) error { t.Error("unexpected retry"); return context.Canceled }
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			outcome, err := processor.Process(ctx, record, nil)
			if err != nil || outcome != delivery.Delivered {
				t.Fatalf("outcome=%v error=%v", outcome, err)
			}
			got := <-received
			const expected = `{"data_source_id":"events","data_source_description":"Source events","data_destination_id":"projection","data_destination_description":"Read model","payload":{"id":9223372036854775807,"event_id":"event-1","correlation_id":"group","recorded_on":"2024-01-02 03:04:05.123456+00","score":12.5,"payload":"{\"z\":2,\"a\":1}","binary":"3q2+7w=="}}`
			decode := func(raw []byte) any {
				t.Helper()
				var value any
				d := json.NewDecoder(bytes.NewReader(raw))
				d.UseNumber()
				if err := d.Decode(&value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			if !reflect.DeepEqual(decode(got.body), decode([]byte(expected))) {
				t.Fatalf("body=%s, want %s", got.body, expected)
			}
			if got.method != "POST" || got.user != "user" || got.password != "secret" {
				t.Fatalf("request method/credentials=%s %s %s", got.method, got.user, got.password)
			}
			for name, want := range map[string]string{"Content-Type": "application/json", "X-Postie-Event-ID": "event-1", "X-Postie-Delivery-Generation": "2", "X-Postie-Topic": "events-topic", "X-Postie-Partition": "2", "X-Postie-Offset": "41", "Idempotency-Key": "event-1:projection:2"} {
				if got.header.Get(name) != want {
					t.Errorf("%s=%q, want %q", name, got.header.Get(name), want)
				}
			}
		})
	}
}
