package provision

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/rafaeelricco/postie/internal/stream"
)

// NamesFor derives every resource name of one stream generation. The names
// are persisted and compared on every health check, so this scheme must never
// change for an existing generation.
//
// The hash covers namespace, environment, source ID, and generation, which
// keeps distinct streams from colliding on a shared Kafka or PostgreSQL.
//
//	n := NamesFor("acme", "production", src, 2)
//	n.TopicPrefix // postie_g2_<28 hex chars>
//	n.Topic       // postie_g2_<28 hex chars>.public.<table>
//	n.Slot        // postie_g2_<28 hex chars>_slot
func NamesFor(namespace, environment string, src stream.Source, generation stream.Generation) stream.Names {
	// A JSON array is an unambiguous encoding: ("a", "bc") never hashes like ("ab", "c").
	identity, _ := json.Marshal([4]string{
		namespace, environment, src.ID, generation.String(),
	})
	sum := sha256.Sum256(identity)
	// Keep the slot name within PostgreSQL's 63-byte limit even at the maximum generation.
	prefix := fmt.Sprintf("postie_g%s_%s", generation.String(), hex.EncodeToString(sum[:14]))
	return stream.Names{
		TopicPrefix: prefix,
		Topic:       prefix + ".public." + src.Table,
		Connector:   prefix + "_connector",
		Slot:        prefix + "_slot",
		Publication: prefix + "_pub",
	}
}
