package architecture

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// These boundaries keep production rules independent of configuration and clients.
func TestDependencyBoundaries(t *testing.T) {
	const module = "github.com/rafaeelricco/postie/"
	root := filepath.Join("..", "..")
	allowed := map[string]map[string]bool{
		"stream":    {},
		"protocol":  {"stream": true},
		"activity":  {},
		"provision": {"stream": true},
		"delivery":  {"stream": true, "protocol": true, "activity": true},
		"control":   {"stream": true, "activity": true},
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" || entry.Name() == ".gremlins-tmp" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		production := !strings.HasSuffix(rel, "_test.go")
		parts := strings.Split(rel, "/")
		var core string
		if len(parts) >= 3 && parts[0] == "internal" {
			if _, ok := allowed[parts[1]]; ok {
				core = parts[1]
			}
		}
		for _, im := range file.Imports {
			name, err := strconv.Unquote(im.Path.Value)
			if err != nil {
				return err
			}
			for _, removed := range []string{"pkg/", "internal/runtime", "internal/operator"} {
				if strings.HasPrefix(name, module+removed) {
					t.Errorf("%s imports removed package %s", rel, name)
				}
			}
			if !production {
				continue
			}
			driver := strings.HasPrefix(name, "github.com/jackc/") || strings.HasPrefix(name, "github.com/twmb/")
			if strings.HasPrefix(rel, "cmd/") && driver {
				t.Errorf("%s constructs infrastructure directly: %s", rel, name)
			}
			if core == "" {
				continue
			}
			if strings.HasPrefix(name, module) {
				dependency := strings.TrimPrefix(name, module+"internal/")
				if !allowed[core][dependency] {
					t.Errorf("%s crosses the %s boundary by importing %s", rel, core, name)
				}
			} else if driver || strings.Contains(name, "yaml") || name == "net/http" || name == "database/sql" || name == "os/exec" {
				t.Errorf("%s imports external-system implementation %s", rel, name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

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
