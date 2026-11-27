package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// a2aSubmissionKey is the A2A door's dedupe key for a send that names a
// messageId: a hash over the caller and the id, so two callers reusing one
// id never collide, and the stored key says neither (#7455). "" for a send
// with no messageId, which can't repeat.
func a2aSubmissionKey(caller, messageID string) string {
	if messageID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("a2a-submission\x00" + caller + "\x00" + messageID))
	return hex.EncodeToString(sum[:16])
}

// a2aDerivedContextID is the contextId the door gives a send that names a
// messageId and no contextId, in place of a drawn one: a retry of the send
// then reaches the same conversation, whose record holds the first
// attempt's task. Shaped like a drawn one (the prefix and the width), and
// hashed under its own label, so it isn't the submission key.
func a2aDerivedContextID(caller, messageID string) string {
	sum := sha256.Sum256([]byte("a2a-context\x00" + caller + "\x00" + messageID))
	return a2aContextIDPrefix + hex.EncodeToString(sum[:messageIDHexWidth])
}

// a2aMatchReadBound bounds the read SubmissionMatched makes on the gateway's
// turn goroutine.
const a2aMatchReadBound = 10 * time.Second

// SubmissionMatched (SubmissionObserver): the gateway found the turn's
// submission on the conversation's record, task taskID, and started nothing.
// The door reads that task back for the conversation's caller and files it as
// the turn's task, so the send that is waiting answers with it.
func (d *A2ADoor) SubmissionMatched(conversation, taskID string) {
	d.mu.Lock()
	caller := d.conversationLocked(conversation).caller
	d.mu.Unlock()
	if caller == "" {
		return
	}
	if _, held := d.taskFor(taskID, caller); !held {
		ctx, cancel := context.WithTimeout(context.Background(), a2aMatchReadBound)
		defer cancel()
		if !d.recoverTask(ctx, taskID, caller) {
			return
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	conv := d.conversationLocked(conversation)
	conv.tasks.add(taskID)
	d.wakeLocked()
}
