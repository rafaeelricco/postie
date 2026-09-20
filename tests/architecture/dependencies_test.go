package architecture

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
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
