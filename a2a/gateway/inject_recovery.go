package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/gke-labs/kube-agents/a2a/lib"
)

// injectMatchedNote is the note on a POST the gateway answered with an
// earlier attempt's task, found on the session record by its submission key
// (SubmissionMatched): the dedupe that outlasts this door's memory.
const injectMatchedNote = "this message id was already accepted on this conversation; " +
	"answered with the task it started, and nothing was started for this one"

// injectSubmissionKey is the inject door's durable dedupe key for a POST
// that names a messageId: a hash over the conversation, the id, the intent,
// the profile and the text, under its own label. The conversation stands in
// for the caller the A2A door hashes (a2aSubmissionKey): this door has no
// per-caller identity, and its key is what names one conversation. Two
// conversations reusing one id never collide, an id reused for another
// message isn't taken for a retry of the first, and the stored key says
// none of them (#7456). "" for a POST with no messageId, which can't repeat.
func injectSubmissionKey(conversation, messageID, intent, profile, text string) string {
	if messageID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("inject-submission\x00" + conversation + "\x00" + messageID +
		"\x00" + intent + "\x00" + profile + "\x00" + text))
	return hex.EncodeToString(sum[:16])
}

// SetConversationTasks receives the gateway's ConversationTasks
// (ConversationTasksSink). Called once from New, before Run, and read
// without a lock for the reason SetProbe gives.
func (a *InjectAdapter) SetConversationTasks(tasks ConversationTasks) {
	a.tasks = tasks
}

// SubmissionMatched (SubmissionObserver): the gateway found the turn's
// submission on the conversation's record, task taskID, and started
// nothing. The POST waiting on the turn answers with taskID (awaitTurn).
// The POST rebuilt the conversation before it handed the turn over, so the
// task's entries are already in the transcript.
func (a *InjectAdapter) SubmissionMatched(_ context.Context, conversation, taskID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	conv := a.conversationLocked(conversation)
	conv.matched.add(taskID)
	close(a.notify)
	a.notify = make(chan struct{})
}

// injectRecoveredTask is one task a rebuild found answerable, with what the
// probe read for it.
type injectRecoveredTask struct {
	id string
	st ConversationState
}

// recoverConversation rebuilds a conversation this door doesn't hold from
// the session record and the task streams, as after a gateway handover
// (#7456): one task entry per chain root the record lists, and for each
// that has ended, its result as a post and its terminal. The state rule is
// the A2A door's (recoverTask): a terminal on the stream gives that
// terminal; with none, a task is running only while it is the record's
// active task; anything else is left out, as unknown, never as running.
// Posts and edits the earlier gateway made aren't rebuilt: the stream holds
// none of them.
//
// A pure read of the bus, outside a.mu, and once per minting of the key:
// a conversation already rebuilt, or minted by a POST that had just
// rebuilt it, is left alone, and a request racing another's rebuild of the
// key waits for it. A conversation this door minted meanwhile (the relay
// posting into it) is kept, and the rebuild adds only what it lacks.
//
// True when the key needs nothing more: rebuilt, or no record to rebuild
// from. False when the read failed or the door has no source, so a later
// request tries again.
func (a *InjectAdapter) recoverConversation(ctx context.Context, key string) bool {
	if !strings.HasPrefix(key, injectKeyPrefix) {
		return false
	}
	a.mu.Lock()
	if conv, ok := a.conversations[key]; ok && conv.recovered {
		a.mu.Unlock()
		return true
	}
	if busy, ok := a.recovering[key]; ok {
		a.mu.Unlock()
		select {
		case <-busy:
		case <-ctx.Done():
		}
		return false
	}
	tasks, probe := a.tasks, a.probe
	if tasks == nil || probe == nil {
		a.mu.Unlock()
		return false
	}
	done := make(chan struct{})
	a.recovering[key] = done
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.recovering, key)
		a.mu.Unlock()
		close(done)
	}()

	ids, err := tasks(ctx, key)
	if err != nil {
		a.log.Warn("inject: could not read the session record of a conversation the door doesn't hold",
			"conversation", key, "err", err)
		return false
	}
	var found []injectRecoveredTask
	for _, id := range ids {
		st, err := probe(ctx, key, id)
		if err != nil {
			a.log.Warn("inject: could not read a task of a conversation the door doesn't hold",
				"conversation", key, "taskId", id, "err", err)
			return false
		}
		if st.Final || (st.Active && st.TaskID == id) {
			found = append(found, injectRecoveredTask{id: id, st: st})
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	conv, held := a.conversations[key]
	if !held && len(found) == 0 {
		// Nothing to show: no conversation is minted, and the read answers
		// as it always has for a key the door doesn't hold.
		return true
	}
	if !held {
		conv = a.conversationLocked(key)
	}
	if conv.recovered {
		return true
	}
	conv.recovered = true
	for _, task := range found {
		a.restoreTaskLocked(conv, task)
	}
	if len(found) > 0 {
		a.log.Info("inject: rebuilt a conversation the door didn't hold from its session record",
			"conversation", key, "tasks", len(found))
	}
	return true
}

// restoreTaskLocked appends what the transcript lacks of one recovered task:
// its task entry, unless the door has already seen it start or end, and, for
// a task that has ended and whose end the door hasn't recorded, the result
// and the terminal. Caller holds a.mu.
func (a *InjectAdapter) restoreTaskLocked(conv *injectConversation, task injectRecoveredTask) {
	_, ended := conv.terminals[task.id]
	if !ended && !conv.hasTaskEntry(task.id) {
		a.appendLocked(conv, InjectEntry{Kind: InjectEntryTask, TaskID: task.id, Recovered: true})
	}
	if ended || !task.st.Final {
		return
	}
	state := task.st.ExecutorState
	if state == lib.StateCompleted && task.st.Result != "" {
		a.nextID++
		a.appendLocked(conv, InjectEntry{
			Kind: InjectEntryPost, Text: task.st.Result,
			MessageID: injectMessageIDPrefix + strconv.Itoa(a.nextID), Recovered: true,
		})
	}
	if len(conv.terminals) >= injectMaxEntries {
		conv.terminals = map[string]string{}
	}
	conv.terminals[task.id] = string(state)
	a.appendLocked(conv, InjectEntry{
		Kind: InjectEntryTerminal, TaskID: task.id, State: string(state),
		Source: string(task.st.TerminalSource), Reason: truncateRunes(task.st.Reason, injectMaxEntryBytes),
		Recovered: true,
	})
}

// hasTaskEntry reports whether the transcript still holds a task entry for
// taskID.
func (conv *injectConversation) hasTaskEntry(taskID string) bool {
	for _, entry := range conv.entries {
		if entry.Kind == InjectEntryTask && entry.TaskID == taskID {
			return true
		}
	}
	return false
}
