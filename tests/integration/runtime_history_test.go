//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rafaeelricco/postie/internal/adapters/controlpg"
	"github.com/rafaeelricco/postie/internal/adapters/kafka"
	engineapp "github.com/rafaeelricco/postie/internal/app"
	provisioning "github.com/rafaeelricco/postie/internal/provision"
	streams "github.com/rafaeelricco/postie/internal/stream"
	"github.com/twmb/franz-go/pkg/kadm"
)

func TestRuntimeMissingSlotBlocksEvenWhenConnectorIsUnavailable(t *testing.T) {
	engine, app, _, table := runtimeFixture(t)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" })

	if err := Stop("connect"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := Start("connect"); err != nil {
			t.Error(err)
		}
	})
	dropSlot(t, names.Slot)

	waitRuntime(t, func() bool { return historySourceBlocked(runtime, table) })
	assertStoredBlock(t, table, "replication slot")
	stop()
}

func TestRuntimeRecreatedTopicUUIDBlocksEstablishedStream(t *testing.T) {
	engine, app, _, table := runtimeFixture(t)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" })
	stop()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	adm := Admin(t)
	oldID := historyTopicID(t, adm, names.Topic)
	if _, err := adm.DeleteTopic(ctx, names.Topic); err != nil {
		t.Fatalf("delete established topic: %v", err)
	}
	waitTopicAbsent(t, adm, names.Topic)
	newID, err := (&kafka.Admin{Client: adm}).EnsureTopic(ctx, names, partitions, replication)
	if err != nil {
		t.Fatalf("recreate topic: %v", err)
	}
	if newID == oldID {
		t.Fatalf("recreated topic kept established UUID %x", oldID)
	}

	runtime, _ = startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return historySourceBlocked(runtime, table) })
	assertStoredBlock(t, table, "topic")
}

func TestRuntimeMissingCommittedOffsetBlocksStartedPartition(t *testing.T) {
	engine, app, _, table := runtimeFixture(t)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" })
	stop()

	partition := historyBusyPartition(t, names.Topic)
	group := kafka.GroupID(streams.Scope{Namespace: namespace, Environment: environment, Generation: 1}, app.Destinations[0].ID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var topics kadm.TopicsSet
	topics.Add(names.Topic, partition)
	if result, err := Admin(t).DeleteOffsets(ctx, group, topics); err != nil || result.Error() != nil {
		t.Fatalf("delete committed offset: result=%v err=%v", result, err)
	}

	runtime, _ = startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return historySourceBlocked(runtime, table) })
	assertStoredBlock(t, table, "offset")
}

func TestRuntimeOutOfRangeCommittedOffsetBlocksAfterRetention(t *testing.T) {
	engine, app, _, table := runtimeFixture(t)
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return runtime.Status().Status == "ready" })
	stop()

	partition := historyBusyPartition(t, names.Topic)
	group := kafka.GroupID(streams.Scope{Namespace: namespace, Environment: environment, Generation: 1}, app.Destinations[0].ID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	adm := Admin(t)
	commits := kadm.Offsets{}
	commits.AddOffset(names.Topic, partition, 0, -1)
	if result, err := adm.CommitOffsets(ctx, group, commits); err != nil || result.Error() != nil {
		t.Fatalf("reset committed offset: result=%v err=%v", result, err)
	}
	deleteAt := kadm.Offsets{}
	deleteAt.AddOffset(names.Topic, partition, 1, -1)
	if result, err := adm.DeleteRecords(ctx, deleteAt); err != nil || result.Error() != nil {
		t.Fatalf("advance retention boundary: result=%v err=%v", result, err)
	}
	historyWaitStartOffset(t, adm, names.Topic, partition, 1)

	runtime, _ = startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return historySourceBlocked(runtime, table) })
	assertStoredBlock(t, table, "history")
}

func TestRuntimeAuditedSkipSurvivesBackwardOffsetReset(t *testing.T) {
	engine, app, receiver, table := runtimeFixture(t)
	receiver.SetReply(func(ReceivedRequest) (int, string) {
		return 200, `{"result":{"error":{"policy":"keep_going","class":"history","description":"audit"}}}`
	})
	names := provisioning.NamesFor(namespace, environment, engineapp.Source(HostSource(table)), 1)
	runtime, stop := startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return historyCommittedCount(runtime) == 2 })
	before := len(receiver.Received())
	stop()

	partition := historyBusyPartition(t, names.Topic)
	group := kafka.GroupID(streams.Scope{Namespace: namespace, Environment: environment, Generation: 1}, app.Destinations[0].ID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	offsets := kadm.Offsets{}
	offsets.AddOffset(names.Topic, partition, 0, -1)
	if result, err := Admin(t).CommitOffsets(ctx, group, offsets); err != nil || result.Error() != nil {
		t.Fatalf("reset audited partition offset: result=%v err=%v", result, err)
	}

	runtime, _ = startRuntime(t, engine, app)
	waitRuntime(t, func() bool { return historyCommittedCount(runtime) == 2 })
	if got := len(receiver.Received()); got != before {
		t.Fatalf("audited skip was redelivered after offset reset: before=%d after=%d", before, got)
	}
}

func dropSlot(t *testing.T, slot string) {
	t.Helper()
	conn := SourceDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, slot); err != nil {
		t.Fatalf("drop slot %q: %v", slot, err)
	}
}

func historySourceBlocked(runtime *engineapp.App, sourceID string) bool {
	for _, source := range runtime.Status().Sources {
		if source.ID == sourceID {
			return source.State == "blocked"
		}
	}
	return false
}

func historyCommittedCount(runtime *engineapp.App) int {
	page, _ := runtime.Logs("")
	count := 0
	for _, entry := range page.Entries {
		if entry.Message == "committed" {
			count++
		}
	}
	return count
}

func assertStoredBlock(t *testing.T, sourceID, want string) {
	t.Helper()
	store, err := controlpg.Open(context.Background(), controlConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stream, found, err := store.GetStream(context.Background(), streams.Scope{Namespace: namespace, Environment: environment, Generation: 1}, sourceID)
	if err != nil || !found {
		t.Fatalf("stored stream found=%v err=%v", found, err)
	}
	if !strings.Contains(stream.Blocked, want) {
		t.Fatalf("stored block reason %q does not contain %q", stream.Blocked, want)
	}
}

func historyTopicID(t *testing.T, adm *kadm.Client, topic string) [16]byte {
	t.Helper()
	details, err := adm.ListTopics(context.Background(), topic)
	if err != nil {
		t.Fatal(err)
	}
	detail, ok := details[topic]
	if !ok || detail.Err != nil {
		t.Fatalf("topic %q unavailable: %v", topic, detail.Err)
	}
	return [16]byte(detail.ID)
}

func waitTopicAbsent(t *testing.T, adm *kadm.Client, topic string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		details, err := adm.ListTopics(context.Background(), topic)
		if err == nil {
			detail, found := details[topic]
			if !found || detail.Err != nil {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %q did not disappear", topic)
}

func historyBusyPartition(t *testing.T, topic string) int32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	end, err := Admin(t).ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	for partition := int32(0); partition < partitions; partition++ {
		offset, ok := end.Lookup(topic, partition)
		if ok && offset.Offset > 0 {
			return partition
		}
	}
	t.Fatalf("topic %q has no partition with captured records", topic)
	return -1
}

func historyWaitStartOffset(t *testing.T, adm *kadm.Client, topic string, partition int32, want int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		start, err := adm.ListStartOffsets(context.Background(), topic)
		if err == nil {
			if offset, ok := start.Lookup(topic, partition); ok && offset.Offset >= want {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %q[%d] did not reach start offset %d", topic, partition, want)
}
