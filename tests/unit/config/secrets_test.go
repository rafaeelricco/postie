package config_test

import (
	"errors"
	"testing"

	"github.com/rafaeelricco/postie/internal/config"
)

func TestOperatorToken(t *testing.T) {
	notCalled := func(string) ([]byte, error) {
		t.Fatal("readFile should not be called when file is empty")
		return nil, nil
	}

	t.Run("file wins over env", func(t *testing.T) {
		readFile := func(path string) ([]byte, error) {
			if path != "token.txt" {
				t.Fatalf("unexpected path %q", path)
			}
			return []byte("file-token"), nil
		}
		token, err := config.OperatorToken("env-token", "token.txt", readFile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if token != "file-token" {
			t.Fatalf("got %q, want %q", token, "file-token")
		}
	})

	t.Run("trailing newline trimmed", func(t *testing.T) {
		readFile := func(string) ([]byte, error) {
			return []byte("file-token\n"), nil
		}
		token, err := config.OperatorToken("", "token.txt", readFile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if token != "file-token" {
			t.Fatalf("got %q, want %q", token, "file-token")
		}
	})

	t.Run("unreadable file is an error, not a fallback", func(t *testing.T) {
		readFile := func(string) ([]byte, error) {
			return nil, errors.New("permission denied")
		}
		token, err := config.OperatorToken("env-token", "token.txt", readFile)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if token != "" {
			t.Fatalf("expected empty token on error, got %q", token)
		}
	})

	t.Run("empty env with no file is an error", func(t *testing.T) {
		token, err := config.OperatorToken("", "", notCalled)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if token != "" {
			t.Fatalf("expected empty token on error, got %q", token)
		}
	})

	t.Run("whitespace-only file is an error", func(t *testing.T) {
		readFile := func(string) ([]byte, error) {
			return []byte("   \n\t"), nil
		}
		token, err := config.OperatorToken("env-token", "token.txt", readFile)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if token != "" {
			t.Fatalf("expected empty token on error, got %q", token)
		}
	})
}
