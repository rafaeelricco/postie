package architecture

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// allowedModules is every module the project may require directly. Adding one
// is a deliberate decision: extend this list in the same change and say why.
var allowedModules = []string{
	"github.com/jackc/pgx/v5",
	"github.com/twmb/franz-go",
	"github.com/twmb/franz-go/pkg/kadm",
	"github.com/twmb/franz-go/pkg/kmsg",
	"gopkg.in/yaml.v3",
}

// directRequires returns the module paths a go.mod requires without an
// "// indirect" marker, from both the block and the single-line require form.
func directRequires(gomod string) []string {
	var out []string
	inBlock := false
	for _, line := range strings.Split(gomod, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "require (":
			inBlock = true
			continue
		case line == ")":
			inBlock = false
			continue
		case strings.HasPrefix(line, "require "):
			line = strings.TrimPrefix(line, "require ")
		case !inBlock:
			continue
		}
		if line == "" || strings.HasPrefix(line, "//") || strings.HasSuffix(line, "// indirect") {
			continue
		}
		out = append(out, strings.Fields(line)[0])
	}
	return out
}

// A third-party import from any file, test files included, needs a direct
// require, so pinning go.mod covers the whole tree.
func TestDirectRequiresAreAllowed(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for _, module := range directRequires(string(b)) {
		if !slices.Contains(allowedModules, module) {
			t.Errorf("go.mod requires %s directly; it is not in allowedModules", module)
		}
	}
}

func TestDirectRequiresParsing(t *testing.T) {
	const sample = `module example

go 1.26.0

require github.com/single/line v1.0.0

require github.com/single/indirect v1.0.0 // indirect

require (
	github.com/cucumber/godog v0.16.0
	// a comment inside the block
	github.com/google/uuid v1.6.0 // indirect
)

replace github.com/x/y => ../y
`
	want := []string{"github.com/single/line", "github.com/cucumber/godog"}
	if got := directRequires(sample); !slices.Equal(got, want) {
		t.Fatalf("directRequires = %v, want %v", got, want)
	}
}
