package delivery

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTerminalOutcomeBeforeCommit(t *testing.T) {
	for _, outcome := range []Outcome{Delivered, Filtered, Skipped} {
		t.Run(string(rune('0'+outcome)), func(t *testing.T) {
			processed, audited, committed := 0, 0, 0
			err := completeRecord(context.Background(), time.Millisecond, recordActions{
				skipped: func(context.Context) (bool, error) { return false, nil },
				send:    func(context.Context) (Outcome, error) { processed++; return outcome, nil },
				audit: func(context.Context) error {
					audited++
					if committed != 0 {
						t.Fatal("commit before audit")
					}
					if audited == 1 {
						return errors.New("store temporarily down")
					}
					return nil
				},
				commit: func(context.Context) error {
					committed++
					if outcome == Skipped && audited < 2 {
						t.Fatal("missing durable audit")
					}
					if committed == 1 {
						return errors.New("broker temporarily down")
					}
					return nil
				},
			})
			if err != nil || processed != 1 || committed != 2 {
				t.Fatalf("process=%d commit=%d err=%v", processed, committed, err)
			}
			if outcome == Skipped && audited != 2 {
				t.Fatal("audit was not retried")
			}
			if outcome != Skipped && audited != 0 {
				t.Fatal("non-skip audited")
			}
		})
	}
}
func TestPreviouslyAuditedSkipOnlyCommits(t *testing.T) {
	commits := 0
	err := completeRecord(context.Background(), time.Millisecond, recordActions{
		skipped: func(context.Context) (bool, error) { return true, nil },
		send:    func(context.Context) (Outcome, error) { t.Fatal("skip was redelivered"); return 0, nil },
		audit:   func(context.Context) error { t.Fatal("skip was re-audited"); return nil },
		commit:  func(context.Context) error { commits++; return nil },
	})
	if err != nil || commits != 1 {
		t.Fatalf("commits=%d err=%v", commits, err)
	}
}
func TestCancellationNeverCommitsIncompleteRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := completeRecord(ctx, time.Millisecond, recordActions{
		skipped: func(context.Context) (bool, error) { return false, nil },
		send:    func(context.Context) (Outcome, error) { cancel(); return 0, context.Canceled },
		commit:  func(context.Context) error { t.Fatal("committed retrying record"); return nil },
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	checks := 0
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = completeRecord(ctx, time.Millisecond, recordActions{
		skipped: func(context.Context) (bool, error) { checks++; return false, errors.New("store down") },
		send:    func(context.Context) (Outcome, error) { t.Fatal("sent without skip check"); return 0, nil },
	})
	if !errors.Is(err, context.DeadlineExceeded) || checks < 2 {
		t.Fatalf("checks=%d err=%v", checks, err)
	}
}
