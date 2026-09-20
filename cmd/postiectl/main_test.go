package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// repoRoot returns the repository root, two levels above this test file
// (cmd/postiectl).
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(filepath.Join(wd, "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestUsageOnUnknownSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"bogus"}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "config validate") || !strings.Contains(stderr.String(), "provision") {
		t.Fatalf("stderr = %q, want usage listing both subcommands", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestConfigValidate(t *testing.T) {
	t.Setenv("POSTIE_DATABASE_URL", "postgres://control")
	t.Setenv("POSTIE_CAPTURE_PASSWORD", "capture-pass")
	t.Setenv("POSTIE_DELIVERY_PASSWORD", "delivery-pass")

	// examples/postie.yaml's application_config is a relative path
	// ("./examples/application.yaml"), interpreted relative to the
	// process's working directory, so run it from the repo root.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(repoRoot(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(wd); err != nil {
			t.Fatal(err)
		}
	})

	var stdout, stderr bytes.Buffer
	code := run([]string{"config", "validate", "--config", "examples/postie.yaml"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr = %s", code, stderr.String())
	}
	const want = "valid: 1 PostgreSQL sources, 1 HTTP destinations\n"
	if stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestConfigValidateReportsError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.yaml")

	var stdout, stderr bytes.Buffer
	code := run([]string{"config", "validate", "--config", missing}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if stderr.Len() == 0 {
		t.Fatal("expected an error message on stderr")
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestProvisionFailsCleanlyWithoutInfrastructure(t *testing.T) {
	dir := t.TempDir()

	const password = "super-secret-password"
	appPath := filepath.Join(dir, "app.yaml")
	appConfig := `
data_sources:
  - id: closed_source
    description: closed
    type: postgres
    host: 127.0.0.1
    port: 1
    username: u
    password: ` + password + `
    database: d
    table: t
    columns: [id, correlation_id]
    serialColumn: id
    partitioningColumn: correlation_id
data_destinations:
  - id: dest
    description: dest
    type: http-push
    endpoint: http://127.0.0.1:1/dest
    username: u
    sources: [closed_source]
`
	if err := os.WriteFile(appPath, []byte(appConfig), 0o600); err != nil {
		t.Fatal(err)
	}

	enginePath := filepath.Join(dir, "postie.yaml")
	engineConfig := fmt.Sprintf(`
version: 1
namespace: ns
environment: development
application_config: %q
kafka:
  brokers: [127.0.0.1:1]
  partitions: 1
  replication_factor: 1
connect:
  url: http://127.0.0.1:1
operator:
  listen: 0.0.0.0:8081
control:
  database_url: postgres://control
`, appPath)
	if err := os.WriteFile(enginePath, []byte(engineConfig), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	start := time.Now()
	code := run([]string{"provision", "--config", enginePath}, &stdout, &stderr)
	elapsed := time.Since(start)

	if elapsed > 15*time.Second {
		t.Fatalf("provision took %s, want under 15s", elapsed)
	}
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "closed_source") {
		t.Fatalf("stderr = %q, want it to name the source id", stderr.String())
	}
	if strings.Contains(stderr.String(), password) {
		t.Fatalf("stderr leaked the password: %q", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty (nothing provisioned)", stdout.String())
	}
}

// TestProvisionRejectsGenerationZero asserts --generation is validated
// before any config is even loaded: an invalid generation exits 2 with a
// plain explanation, the same as a flag-parsing failure.
func TestProvisionRejectsGenerationZero(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"provision", "--config", "postie.yaml", "--generation", "0"}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "generation must be 1 or greater") {
		t.Fatalf("stderr = %q, want it to explain generation must be 1 or greater", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}
