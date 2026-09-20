package delivery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/stream"
)

func TestCompleteRecordDoesNotResendAfterAcknowledgement(t *testing.T) {
	commits := 0
	err := completeRecord(context.Background(), time.Millisecond, recordActions{
		skipped: func(context.Context) (bool, error) { return false, nil },
		send:    func(context.Context) (Outcome, error) { return Delivered, nil },
		commit: func(context.Context) error {
			commits++
			if commits == 1 {
				return errors.New("broker down")
			}
			return nil
		},
	})
	if err != nil || commits != 2 {
		t.Fatalf("commits=%d err=%v", commits, err)
	}
}

func TestCompleteRecordAuditsSkipBeforeCommit(t *testing.T) {
	audits, commits := 0, 0
	err := completeRecord(context.Background(), time.Millisecond, recordActions{
		skipped: func(context.Context) (bool, error) { return false, nil },
		send:    func(context.Context) (Outcome, error) { return Skipped, nil },
		audit:   func(context.Context) error { audits++; return nil },
		commit:  func(context.Context) error { commits++; return nil },
	})
	if err != nil || audits != 1 || commits != 1 {
		t.Fatalf("audit=%d commit=%d err=%v", audits, commits, err)
	}
}

type skipStore struct {
	has   bool
	saves int
}

func (s *skipStore) HasSkip(context.Context, stream.Scope, string, stream.Record) (bool, error) {
	return s.has, nil
}
func (s *skipStore) SaveSkip(context.Context, stream.Scope, string, stream.Record) error {
	s.saves++
	return nil
}

func TestWorkerPreviouslyAuditedSkipOnlyCommits(t *testing.T) {
	store := &skipStore{has: true}
	worker := Worker{Store: store, Scope: stream.Scope{Generation: 1}, Destination: "d", Decode: func(*stream.RawRecord) (stream.Record, error) { return stream.Record{}, nil }, Processor: &Processor{}}
	commits := 0
	err := worker.Process(context.Background(), &stream.RawRecord{}, func(context.Context, *stream.RawRecord) error { commits++; return nil })
	if err != nil || commits != 1 || store.saves != 0 {
		t.Fatalf("err=%v commits=%d saves=%d", err, commits, store.saves)
	}
}
