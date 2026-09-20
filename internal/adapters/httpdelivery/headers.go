package httpdelivery

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/rafaeelricco/postie/internal/stream"
)

func setHeaders(req *http.Request, destination Destination, record stream.Record) {
	req.SetBasicAuth(destination.Username, destination.Password)
	req.Header.Set("Content-Type", "application/json")
	id := stableID(record)
	req.Header.Set("X-Postie-Event-ID", id)
	req.Header.Set("X-Postie-Delivery-Generation", record.Generation.String())
	req.Header.Set("X-Postie-Replay", fmt.Sprint(record.Replay))
	req.Header.Set("X-Postie-Topic", record.Topic)
	req.Header.Set("X-Postie-Partition", fmt.Sprint(record.Partition))
	req.Header.Set("X-Postie-Offset", fmt.Sprint(record.Offset))
	req.Header.Set("Idempotency-Key", id+":"+destination.ID+":"+record.Generation.String())
}

func stableID(record stream.Record) string {
	if record.EventID != "" {
		return record.EventID
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", record.Topic, record.Partition, record.Offset)))
	return hex.EncodeToString(sum[:])
}
