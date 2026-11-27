package gateway

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// handOverInject stops r's gateway and starts a fresh one on the same bus,
// as a failover or a rollout hands the inject door to the other replica: the
// session records and the task streams survive, the door's memory does not.
// whileDown runs with no gateway serving, so nothing rewrites what it writes.
func handOverInject(t *testing.T, r *injectRig, whileDown ...func()) *injectRig {
	t.Helper()
	r.stop()
	select {
	case <-r.ran:
	case <-time.After(10 * time.Second):
		t.Fatal("the first gateway did not stop")
	}
	for _, f := range whileDown {
		f()
	}
	return startInjectRigOn(t, r.url, nil, nil)
}

// completeInject finishes a task the way an executor does: working, the
// result artifact, then completed.
func completeInject(t *testing.T, r *injectRig, origin *lib.Envelope, result string) {
	t.Helper()
	exec := r.execFor(t, origin, "platform")
	ctx := context.Background()
	if err := exec.PublishStatus(ctx, lib.StateWorking, false); err != nil {
		t.Fatal(err)
	}
	if err := exec.PublishArtifact(ctx, lib.Artifact{
		Name:  lib.ArtifactResult,
		Parts: []lib.Part{{Kind: "text", Text: result}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := exec.PublishStatus(ctx, lib.StateCompleted, true); err != nil {
		t.Fatal(err)
	}
}

// finishedConversation runs one turn to its terminal on r and returns the
// POST's reply and the conversation's last sequence once the relay has
// released the record.
func finishedConversation(t *testing.T, r *injectRig, conversation, text, messageID, result string) (injectResponse, int) {
	t.Helper()
	reply := r.injectWithID(t, conversation, injectTestAuthor, text, messageID)
	if reply.TaskID == "" {
		t.Fatalf("POST started nothing: %+v", reply)
	}
	completeInject(t, r, r.awaitTask(t, "platform"), result)
	r.waitForTerminal(t, reply.Conversation, reply.TaskID)
	r.awaitRecordRelease(t, reply.Conversation)
	_, last, _ := r.adapter.snapshot(reply.Conversation, 0, "")
	return reply, last
}

// A conversation that finished before the handover is read on from the
// cursor the caller carried: the next gateway rebuilds it numbered above
// that cursor, with the result and the terminal (#7456).
func TestAnInjectConversationFinishedBeforeAHandoverIsReadOnFromTheOldCursor(t *testing.T) {
	t.Parallel()
	before := startInjectRig(t)
	reply, cursor := finishedConversation(t, before, "case-handover-done", "how is the fleet?", "", "all green")
	var firstIDs []string
	old, _, _ := before.adapter.snapshot(reply.Conversation, 0, "")
	for _, entry := range old {
		if entry.MessageID != "" {
			firstIDs = append(firstIDs, entry.MessageID)
		}
	}

	after := handOverInject(t, before)
	page := after.conversation(t, reply.Conversation, cursor, reply.TaskID, 0)
	if page.Terminal != string(lib.StateCompleted) {
		t.Errorf("terminal = %q, want completed", page.Terminal)
	}
	if len(page.Entries) == 0 {
		t.Fatalf("a GET from the old cursor %d read nothing: %+v", cursor, page)
	}
	for _, entry := range page.Entries {
		if entry.Seq <= cursor {
			t.Errorf("entry %+v is numbered at or below the old cursor %d", entry, cursor)
		}
		if !entry.Recovered {
			t.Errorf("entry %+v is not marked recovered", entry)
		}
	}
	if got := entryTexts(page.Entries, InjectEntryPost); !slices.Equal(got, []string{"all green"}) {
		t.Errorf("posts = %q, want the result", got)
	}
	// No message id the first gateway minted comes back: a caller that saw
	// one edited (the rolling line) would take the result for it.
	for _, entry := range page.Entries {
		if entry.MessageID != "" && slices.Contains(firstIDs, entry.MessageID) {
			t.Errorf("the rebuilt %s reuses message id %s from the first gateway", entry.Kind, entry.MessageID)
		}
	}
	terms := terminalEntries(page.Entries)
	if len(terms) != 1 || terms[0].TaskID != reply.TaskID || terms[0].State != string(lib.StateCompleted) {
		t.Errorf("terminals = %+v, want the task's completed terminal", terms)
	}
	if page.LastSeq <= cursor {
		t.Errorf("lastSeq = %d, want above the old cursor %d", page.LastSeq, cursor)
	}
}

// A conversation whose task was running at the handover is rebuilt with
// the task running, and the next gateway's relay renders its end above the
// old cursor.
func TestAnInjectConversationRunningAtAHandoverIsReadOnToItsEnd(t *testing.T) {
	t.Parallel()
	before := startInjectRig(t)
	reply := before.inject(t, "case-handover-running", injectTestAuthor, "how is the fleet?")
	if reply.TaskID == "" {
		t.Fatalf("POST started nothing: %+v", reply)
	}
	origin := before.awaitTask(t, "platform")
	before.awaitRecordedTask(t, reply.Conversation)
	_, cursor, _ := before.adapter.snapshot(reply.Conversation, 0, "")

	after := handOverInject(t, before)
	page := after.conversation(t, reply.Conversation, cursor, "", 0)
	if len(page.Entries) != 1 || page.Entries[0].Kind != InjectEntryTask || page.Entries[0].TaskID != reply.TaskID {
		t.Fatalf("entries = %+v, want the running task's entry alone", page.Entries)
	}
	if page.Entries[0].Seq <= cursor {
		t.Errorf("the task entry is numbered %d, at or below the old cursor %d", page.Entries[0].Seq, cursor)
	}

	completeInject(t, after, origin, "all green")
	end := after.conversation(t, reply.Conversation, page.LastSeq, reply.TaskID, injectTestWait)
	waitFor(t, "the next gateway's relay to render the terminal", func() bool {
		end = after.conversation(t, reply.Conversation, page.LastSeq, reply.TaskID, 1)
		return end.Terminal != ""
	})
	if end.Terminal != string(lib.StateCompleted) {
		t.Errorf("terminal = %q, want completed", end.Terminal)
	}
}

// A same-messageId POST to the next gateway starts nothing: it is answered
// with the first attempt's task, from the record (#7456).
func TestARepeatedInjectPostIsAnsweredWithItsFirstTaskAcrossAHandover(t *testing.T) {
	t.Parallel()
	before := startInjectRig(t)
	const messageID = "run-1/case-durable-dedupe/1"
	first, _ := finishedConversation(t, before, "case-durable-dedupe", "how is the fleet?", messageID, "all green")

	after := handOverInject(t, before)
	retry := after.injectWithID(t, "case-durable-dedupe", injectTestAuthor, "how is the fleet?", messageID)
	if retry.TaskID != first.TaskID || !retry.Accepted || !retry.Deduplicated {
		t.Fatalf("retry = %+v, want the first task %s, accepted and deduplicated", retry, first.TaskID)
	}
	if n := len(inSubjectEnvelopes(t, after.url, "platform")); n != 1 {
		t.Fatalf("%d envelopes on the in subject, want the first submission alone: the retry was routed", n)
	}
	if terms := terminalEntries(retry.Entries); len(terms) != 1 || terms[0].TaskID != first.TaskID {
		t.Errorf("the retry's entries carry terminals %+v, want the first task's", terms)
	}
}

// Two conversations sending the same text under the same messageId never
// collide: the key hashes the conversation, so the second starts its own
// task on the next gateway.
func TestTheSameInjectMessageIDOnAnotherConversationIsNotARepeatAcrossAHandover(t *testing.T) {
	t.Parallel()
	before := startInjectRig(t)
	const messageID = "run-1/shared-id"
	first, _ := finishedConversation(t, before, "case-collide-a", "how is the fleet?", messageID, "all green")

	after := handOverInject(t, before)
	other := after.injectWithID(t, "case-collide-b", injectTestAuthor, "how is the fleet?", messageID)
	if other.Deduplicated || other.TaskID == "" || other.TaskID == first.TaskID {
		t.Fatalf("other conversation = %+v, want a task of its own (first was %s)", other, first.TaskID)
	}
}

func TestInjectSubmissionKeyKeepsConversationsIDsAndTextsApart(t *testing.T) {
	t.Parallel()
	base := injectSubmissionKey("inject:a", "m-1", "", "", "text")
	if base == "" || len(base) != 32 {
		t.Fatalf("key = %q, want 32 hex digits", base)
	}
	if again := injectSubmissionKey("inject:a", "m-1", "", "", "text"); again != base {
		t.Errorf("the key isn't stable: %q then %q", base, again)
	}
	for name, other := range map[string]string{
		"another conversation": injectSubmissionKey("inject:b", "m-1", "", "", "text"),
		"another id":           injectSubmissionKey("inject:a", "m-2", "", "", "text"),
		"another text":         injectSubmissionKey("inject:a", "m-1", "", "", "other"),
		"another intent":       injectSubmissionKey("inject:a", "m-1", IntentConfirm, "", "text"),
		"another profile":      injectSubmissionKey("inject:a", "m-1", "", "cluster", "text"),
		"the A2A door's key":   a2aSubmissionKey("inject:a", "m-1", "text"),
	} {
		if other == base {
			t.Errorf("%s gives the same key", name)
		}
	}
	if got := injectSubmissionKey("inject:a", "", "", "", "text"); got != "" {
		t.Errorf("a POST with no messageId has key %q, want none", got)
	}
}

// A task the record lists with no terminal on its stream and no longer
// active has an outcome the gateway doesn't know: the rebuild leaves it out
// rather than showing it running forever.
func TestARebuiltInjectTaskWithNoTerminalAndNotActiveIsLeftOut(t *testing.T) {
	t.Parallel()
	r := startInjectRig(t)
	ctx := context.Background()
	key := injectKeyPrefix + "case-unknown-outcome"
	rec := doorRecord(key, "task-00000000000000bb")
	rec.Kind = injectConversationKind
	if err := r.g.reg.Put(ctx, rec); err != nil {
		t.Fatal(err)
	}
	page := r.conversation(t, key, 0, "", 0)
	if len(page.Entries) != 0 || page.LastSeq != 0 {
		t.Errorf("page = %+v, want nothing shown for a task whose outcome is unknown", page)
	}
}

// A key with no record reads exactly as before: a 200 with no entries, and
// no conversation minted.
func TestAnInjectKeyWithNoRecordReadsEmptyAndMintsNothing(t *testing.T) {
	t.Parallel()
	r := startInjectRig(t)
	key := injectKeyPrefix + "case-never-seen"
	page := r.conversation(t, key, 0, "", 0)
	if len(page.Entries) != 0 || page.LastSeq != 0 || page.Terminal != "" {
		t.Errorf("page = %+v, want an empty answer", page)
	}
	r.adapter.mu.Lock()
	_, minted := r.adapter.conversations[key]
	r.adapter.mu.Unlock()
	if minted {
		t.Error("a read of a key with no record minted a conversation")
	}
}

// Two reads racing on one conversation the door doesn't hold rebuild it
// once.
func TestRacingReadsRebuildAnInjectConversationOnce(t *testing.T) {
	t.Parallel()
	before := startInjectRig(t)
	reply, _ := finishedConversation(t, before, "case-racing-reads", "how is the fleet?", "", "all green")

	after := handOverInject(t, before)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			after.conversation(t, reply.Conversation, 0, "", 0)
		}()
	}
	wg.Wait()
	entries, _, _ := after.adapter.snapshot(reply.Conversation, 0, "")
	var tasks, terms int
	for _, entry := range entries {
		switch entry.Kind {
		case InjectEntryTask:
			tasks++
		case InjectEntryTerminal:
			terms++
		}
	}
	if tasks != 1 || terms != 1 {
		t.Errorf("entries = %+v, want one task entry and one terminal", entries)
	}
}

// ConversationTasks answers only a door session's own record, and only its
// chain roots: a chat conversation's record, a child and a wake never reach
// a door through it. The inject door's own check (the inject: prefix, and
// the key is the conversation asked for) is the counterpart of the A2A
// door's caller check on a recovered task.
func TestConversationTasksAnswersOnlyADoorSessionsOwnRoots(t *testing.T) {
	t.Parallel()
	r := startInjectRig(t)
	ctx := context.Background()

	chat := doorRecord("discord:chan-1", "task-chat-root")
	chat.Kind = "channel"
	if err := r.g.reg.Put(ctx, chat); err != nil {
		t.Fatal(err)
	}
	if got, err := r.g.conversationTasks(ctx, "discord:chan-1"); err != nil || len(got) != 0 {
		t.Errorf("a chat conversation's record = %v, %v; want nothing", got, err)
	}

	key := injectKeyPrefix + "case-roots"
	rec := doorRecord(key, "task-roots-1")
	rec.Kind = injectConversationKind
	rec.Tasks = append(rec.Tasks,
		TaskRef{ID: "task-roots-child", Addressee: "platform", ParentTaskID: "task-roots-1", RootTaskID: "task-roots-1"},
		TaskRef{ID: "task-roots-wake", Addressee: "platform", RootTaskID: "task-roots-1"},
		TaskRef{ID: "task-roots-2", Addressee: "platform"})
	if err := r.g.reg.Put(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, err := r.g.conversationTasks(ctx, key)
	if err != nil || !slices.Equal(got, []string{"task-roots-1", "task-roots-2"}) {
		t.Errorf("ConversationTasks = %v, %v; want the two roots in order", got, err)
	}
	if got, err := r.g.conversationTasks(ctx, injectKeyPrefix+"case-elsewhere"); err != nil || len(got) != 0 {
		t.Errorf("another conversation's key = %v, %v; want nothing", got, err)
	}
	// The A2A door's records stay its own: the inject door only ever asks
	// for an inject: key, and a key under the A2A prefix can't be named
	// through it (injectConversationKey refuses the colon).
	if _, err := injectConversationKey(a2aConversationKey(a2aTestCaller, "ctx-1")); err == nil {
		t.Error("the inject door accepted an A2A conversation's key")
	}
	if r.adapter.recoverConversation(ctx, a2aConversationKey(a2aTestCaller, "ctx-1")) {
		t.Error("the inject door's rebuild took an A2A conversation's key")
	}
}
