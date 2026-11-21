package gateway

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// TestAProfileParentStoppedAtAWriteRefusalCancelsItsChild: the worker ends a
// profile task it stopped at a definitive write refusal with a final
// `rejected` status (reason write-refused, a2a/worker-adapter/refusal.go).
// That is the task's own end, so its live child is cancelled, with its stop
// record, as for any other end of a profile parent.
func TestAProfileParentStoppedAtAWriteRefusalCancelsItsChild(t *testing.T) {
	t.Parallel()
	r := startProfileRig(t)
	r.g.profiles.put("chat", &lib.AgentCard{Name: "chat"})
	ctx := context.Background()
	conv := "console:tab-profile-parent-write-refused"
	turn := profileTurn(conv, "chat", nil)
	turn.Backend, turn.AuthorID = consoleBackend, consoleAuthor
	r.adapter.inbox <- turn
	origin := submissionOn(t, r, "chat")
	parent := r.execFor(t, origin, "chat")
	if err := parent.PublishStatus(ctx, lib.StateWorking, false); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(lib.DelegateRequest{Addressee: "cluster", Text: "look at a", Parameters: targetParams("p1", "us-east1", "a")})
	if err != nil {
		t.Fatal(err)
	}
	if err := parent.PublishArtifact(ctx, lib.Artifact{ArtifactID: "artifact-refused-" + lib.ArtifactDelegate, Name: lib.ArtifactDelegate,
		Parts: []lib.Part{{Kind: "data", Data: data}}}); err != nil {
		t.Fatal(err)
	}
	child := submissionOn(t, r, "cluster")
	waitForRecord(t, r, conv, "the child holding the conversation", func(rec *SessionRecord) bool {
		return rec.ActiveTask != nil && rec.ActiveTask.TaskID == child.TaskID
	})
	publishFinal(t, r, origin, "chat", lib.StateRejected,
		"reason: write-refused - the task stopped after a write refusal: the command policy's kubernetes.read-only rule refused the verb scale.")
	waitFor(t, "the live child cancelled with its stop record", func() bool {
		cancelled := false
		for _, e := range streamEnvelopes(t, r.url, lib.TaskInSubject("cluster", child.TaskID)) {
			cancelled = cancelled || e.env.Kind == lib.KindCancel
		}
		_, stop := dispatchStopFor(t, r.url, "cluster", child.TaskID)
		return cancelled && stop
	})
}
