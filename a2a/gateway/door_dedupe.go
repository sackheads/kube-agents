package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

// a2aSubmissionKey is the A2A door's dedupe key for a send that names a
// messageId: a hash over the caller, the id and the text, so two callers
// reusing one id never collide, an id a client reuses for another message
// isn't taken for a retry of the first, and the stored key says none of
// them (#7455). "" for a send with no messageId, which can't repeat.
func a2aSubmissionKey(caller, messageID, text string) string {
	if messageID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("a2a-submission\x00" + caller + "\x00" + messageID + "\x00" + text))
	return hex.EncodeToString(sum[:16])
}

// a2aDerivedContextID is the contextId the door gives a send that names a
// messageId and no contextId, in place of a drawn one: a retry of the send
// then reaches the same conversation, whose record holds the first
// attempt's task. Over the same inputs as the key, so a reused id with other
// text gets a conversation of its own, as a drawn id would. Shaped like a
// drawn one (the prefix and the width), and hashed under its own label, so
// it isn't the submission key.
func a2aDerivedContextID(caller, messageID, text string) string {
	sum := sha256.Sum256([]byte("a2a-context\x00" + caller + "\x00" + messageID + "\x00" + text))
	return a2aContextIDPrefix + hex.EncodeToString(sum[:messageIDHexWidth])
}

// SubmissionMatched (SubmissionObserver): the gateway found the turn's
// submission on the conversation's record, task taskID, which it has read as
// answerable, and started nothing. The door reads that task back for the
// conversation's caller and files it as the turn's task, so the send that is
// waiting answers with it. ctx is the turn's.
func (d *A2ADoor) SubmissionMatched(ctx context.Context, conversation, taskID string) {
	d.mu.Lock()
	caller := d.conversationLocked(conversation).caller
	d.mu.Unlock()
	if caller == "" {
		return
	}
	if _, held := d.taskFor(taskID, caller); !held && !d.recoverTask(ctx, taskID, caller) {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	conv := d.conversationLocked(conversation)
	conv.tasks.add(taskID)
	d.wakeLocked()
}
