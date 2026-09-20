package provision

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/rafaeelricco/postie/internal/stream"
)

// NamesFor preserves the current persisted naming scheme for each stream generation.
func NamesFor(namespace, environment string, src stream.Source, generation stream.Generation) stream.Names {
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
