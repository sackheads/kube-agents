package gateway

import (
	"context"
	"testing"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// submissionsOn counts the task submissions on an addressee's .in subjects.
func submissionsOn(t *testing.T, r *a2aRig, addressee string) int {
	t.Helper()
	n := 0
	for _, env := range inSubjectEnvelopes(t, r.url, addressee) {
		if env.Kind == lib.KindMessage {
			n++
		}
	}
	return n
}

// A send retried with the same messageId after a handover is answered with
// the task the first attempt started, and starts nothing: with no contextId
// (the door derives the conversation from the caller and the messageId, so
// the retry reaches the same record) and with one (#7455).
func TestARetriedSendAfterAHandoverAnswersTheFirstTask(t *testing.T) {
	t.Parallel()
	for _, contextID := range []string{"", "ctx-client-chosen"} {
		before := startA2ADoorRig(t)
		first := taskOf(t, before.rpc(t, a2aTestCaller, a2aMethodSend, sendParams("do it once", "m-once", contextID, false)))
		before.awaitTask(t, "platform")

		after := handOver(t, before)
		again := taskOf(t, after.rpc(t, a2aTestCaller, a2aMethodSend, sendParams("do it once", "m-once", contextID, false)))
		if again.ID != first.ID || again.ContextID != first.ContextID {
			t.Errorf("contextId %q: the retry answered %s/%s, want the first task %s/%s", contextID, again.ID, again.ContextID, first.ID, first.ContextID)
		}
		if n := submissionsOn(t, after, "platform"); n != 1 {
			t.Errorf("contextId %q: %d submissions on the bus, want the first one alone", contextID, n)
		}
	}
}

// Two callers using the same messageId never collide: the key holds the
// caller, so the second caller's send starts a task of its own, and the
// answer carries nothing of the first caller's.
func TestTwoCallersReusingAMessageIDGetTwoTasks(t *testing.T) {
	t.Parallel()
	before := startA2ADoorRig(t)
	mine := taskOf(t, before.rpc(t, a2aTestCaller, a2aMethodSend, sendParams("first caller", "m-shared", "", false)))
	before.awaitTask(t, "platform")

	after := handOver(t, before)
	theirs := taskOf(t, after.rpc(t, a2aTestOtherCaller, a2aMethodSend, sendParams("second caller", "m-shared", "", false)))
	if theirs.ID == mine.ID || theirs.ContextID == mine.ContextID {
		t.Errorf("the second caller got %s/%s, the first caller's task is %s/%s: want a task of its own", theirs.ID, theirs.ContextID, mine.ID, mine.ContextID)
	}
	if n := submissionsOn(t, after, "platform"); n != 2 {
		t.Errorf("%d submissions, want two", n)
	}
}

// A record an older gateway rewrote has no submission key on its tasks: a
// retry then misses and starts the task again, which is today's behaviour,
// not an error.
func TestARetryAgainstARecordWithoutSubmissionKeysStartsANewTask(t *testing.T) {
	t.Parallel()
	before := startA2ADoorRig(t)
	first := taskOf(t, before.rpc(t, a2aTestCaller, a2aMethodSend, sendParams("rolled back", "m-rollback", "", false)))
	// Ended, so the retry is a new turn rather than a steer of a running task.
	before.complete(t, before.awaitTask(t, "platform"), "done")
	before.getUntil(t, a2aTestCaller, first.ID, "the terminal", func(o a2aTaskObject) bool { return o.Status.State == lib.StateCompleted })
	// Stripped while no gateway runs: the first one's relay may still be
	// writing the record after the terminal reaches the door.
	after := handOver(t, before, func() {
		ctx := context.Background()
		key := a2aConversationKey(a2aTestCaller, first.ContextID)
		rec, err := before.g.reg.Get(ctx, key)
		if err != nil || rec == nil {
			t.Errorf("record %s: %v %v", key, rec, err)
			return
		}
		for i := range rec.Tasks {
			rec.Tasks[i].SubmissionKey = ""
		}
		if err := before.g.reg.Put(ctx, rec); err != nil {
			t.Error(err)
		}
	})
	again := taskOf(t, after.rpc(t, a2aTestCaller, a2aMethodSend, sendParams("rolled back", "m-rollback", "", false)))
	if again.ID == first.ID {
		t.Errorf("the retry was deduplicated against a record with no keys")
	}
}

// The submission key holds the caller: the same messageId under two callers
// gives two keys, and the derived contextId differs from the key.
func TestTheSubmissionKeyHoldsTheCaller(t *testing.T) {
	t.Parallel()
	a, b := a2aSubmissionKey(a2aTestCaller, "m-1", "hi"), a2aSubmissionKey(a2aTestOtherCaller, "m-1", "hi")
	if a == "" || a == b {
		t.Errorf("keys %q and %q, want two distinct keys", a, b)
	}
	if a == a2aSubmissionKey(a2aTestCaller, "m-1", "something else") {
		t.Error("the same messageId with other text has the same key: a reused id would be answered with the old task")
	}
	if a2aSubmissionKey(a2aTestCaller, "", "hi") != "" {
		t.Error("a send with no messageId has a key")
	}
	ctx := a2aDerivedContextID(a2aTestCaller, "m-1", "hi")
	if ctx == a2aDerivedContextID(a2aTestOtherCaller, "m-1", "hi") || ctx == a2aDerivedContextID(a2aTestCaller, "m-1", "something else") || ctx[len(a2aContextIDPrefix):] == a {
		t.Errorf("derived contextId %q collides", ctx)
	}
}

// A messageId reused for a different message (a client numbering its ids
// per process) is not a retry: after a handover it starts a task of its
// own, rather than being answered with the old one.
func TestAMessageIDReusedForOtherTextAfterAHandoverStartsANewTask(t *testing.T) {
	t.Parallel()
	before := startA2ADoorRig(t)
	first := taskOf(t, before.rpc(t, a2aTestCaller, a2aMethodSend, sendParams("first question", "1", "", false)))
	before.complete(t, before.awaitTask(t, "platform"), "first answer")
	before.getUntil(t, a2aTestCaller, first.ID, "the terminal", func(o a2aTaskObject) bool { return o.Status.State == lib.StateCompleted })

	after := handOver(t, before)
	second := taskOf(t, after.rpc(t, a2aTestCaller, a2aMethodSend, sendParams("an unrelated question", "1", "", false)))
	if second.ID == first.ID {
		t.Errorf("a reused messageId with other text was answered with the old task %s", first.ID)
	}
}

// A matched submission whose task can't be answered (its publish failed, or
// its end has left the stream) is no match: the turn goes on as it would
// have without the key, rather than ending on an error every retry repeats.
func TestAMatchedSubmissionWhoseTaskCantBeReadStartsANewTask(t *testing.T) {
	t.Parallel()
	r := startA2ADoorRig(t)
	ctx := context.Background()
	const text, messageID = "never published", "m-unread"
	contextID := a2aDerivedContextID(a2aTestCaller, messageID, text)
	key := a2aConversationKey(a2aTestCaller, contextID)
	const ghost = "task-00000000000000cc"
	if err := r.g.reg.Put(ctx, &SessionRecord{Key: key, ContextID: "ctx-bus-unread", Addressee: "platform", Kind: a2aConversationKind,
		Tasks: []TaskRef{{ID: ghost, Addressee: "platform", SubmissionKey: a2aSubmissionKey(a2aTestCaller, messageID, text)}}}); err != nil {
		t.Fatal(err)
	}
	got := taskOf(t, r.rpc(t, a2aTestCaller, a2aMethodSend, sendParams(text, messageID, "", false)))
	if got.ID == ghost {
		t.Fatalf("answered with the unreadable task %s", ghost)
	}
	if n := submissionsOn(t, r, "platform"); n != 1 {
		t.Errorf("%d submissions, want the new task's", n)
	}
}

// Two first attempts racing on one gateway start one task: the second is
// answered with the first's.
func TestTwoRacingFirstAttemptsStartOneTask(t *testing.T) {
	t.Parallel()
	r := startA2ADoorRig(t)
	a := r.sendAsync(a2aTestCaller, sendParams("only once", "m-race", "", false))
	b := r.sendAsync(a2aTestCaller, sendParams("only once", "m-race", "", false))
	ta, tb := taskOf(t, collect(t, a)), taskOf(t, collect(t, b))
	if ta.ID != tb.ID {
		t.Errorf("two tasks %s and %s for one messageId", ta.ID, tb.ID)
	}
	if n := submissionsOn(t, r, "platform"); n != 1 {
		t.Errorf("%d submissions, want one", n)
	}
}
