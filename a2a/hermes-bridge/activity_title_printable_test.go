package hermesbridge

import (
	"strings"
	"testing"
	"unicode"
)

// printableOnly reports the first non-printing rune in s other than a plain
// space, or -1 when there is none.
func printableOnly(s string) rune {
	for _, r := range s {
		if r != ' ' && !unicode.IsPrint(r) {
			return r
		}
	}
	return -1
}

// A step title built from a call's arguments carries none of their
// non-printing characters: line breaks, escape sequences, bidirectional
// overrides and zero-width characters are dropped from a file path, a
// cluster, a search query, a skill name and a card title alike, and an
// ordinary value reads as before.
func TestAStepTitleDropsAnArgumentsNonPrintingCharacters(t *testing.T) {
	for _, c := range []struct {
		tool string
		args map[string]any
		want string
	}{
		{"read_file", map[string]any{"path": "/scratch/out/deploy\n.yaml"}, "Reading out/deploy.yaml."},
		{"read_file", map[string]any{"path": "/scratch/invoice\u202egpj.exe"}, "Reading scratch/invoicegpj.exe."},
		{"write_file", map[string]any{"path": "/scratch/\x1b[31mred\x1b[0m.txt"}, "Writing scratch/[31mred[0m.txt."},
		{"search_files", map[string]any{"pattern": "*.ya\u200bml"}, "Inspecting workspace files (*.yaml)."},
		{"tool_search", map[string]any{"query": "list\r\nclusters"}, "Searching available tools (listclusters)."},
		{"skill_view", map[string]any{"name": "gke-\u2066triage"}, "Loading skill gke-triage."},
		{"mcp__gke__get_cluster", map[string]any{"name": "seeded\u200b-a"}, "Inspecting GKE cluster seeded-a."},
		{kanbanCreateTool, map[string]any{"title": "Check\tnodes for seeded-a"}, "Delegating specialist task: seeded-a."},
		// Ordinary values, as before.
		{"read_file", map[string]any{"path": "/scratch/clusters/seeded-a/deploy.yaml"}, "Reading seeded-a/deploy.yaml."},
		{"tool_search", map[string]any{"query": "list clusters"}, "Searching available tools (list clusters)."},
	} {
		if got := describePlannedTool(c.tool, c.args); got != c.want {
			t.Errorf("describePlannedTool(%s, %q) = %q, want %q", c.tool, c.args, got, c.want)
		}
	}
}

// A space character in an argument becomes a plain space rather than going
// with the other non-printing characters, so a value written with a
// no-break or ideographic space keeps its gap. A value that is only
// non-printing characters names no subject.
func TestAStepTitleKeepsSpacesAndDropsAnEmptiedValue(t *testing.T) {
	if got, want := describePlannedTool("tool_search", map[string]any{"query": "list\u00a0clusters"}), "Searching available tools (list clusters)."; got != want {
		t.Errorf("no-break space: got %q, want %q", got, want)
	}
	if got, want := describePlannedTool("read_file", map[string]any{"path": "\n\u202e\u200b"}), "Reading file in the workspace."; got != want {
		t.Errorf("emptied path: got %q, want %q", got, want)
	}
}

// A card's title reaches its step and the card's synthesis line with its
// non-printing characters dropped, from kanban.db as from kanban_create.
func TestACardTitleDropsItsNonPrintingCharacters(t *testing.T) {
	row := kanbanCardRow{ID: "t_1", Title: "Inspect\nnodes\u202e on seeded-a", Status: "done"}
	step := cardStep(row, 1, 1, "task-1")
	for name, text := range map[string]string{"step title": step.Title, "subtask title": step.Subtask.Title} {
		if r := printableOnly(text); r != -1 {
			t.Errorf("%s %q carries %U", name, text, r)
		}
	}
	if !strings.Contains(step.Subtask.Title, "Inspectnodes on seeded-a") {
		t.Errorf("subtask title %q, want the title with its non-printing characters dropped", step.Subtask.Title)
	}
	a := &activityState{sessionID: "s"}
	a.SyncKanbanCard("t_1", row.Title, "")
	if got := a.trace.cardTitles["t_1"]; printableOnly(got) != -1 || got == "" {
		t.Errorf("recorded card title %q, want it printable and non-empty", got)
	}
}

// A secret split by a zero-width character is still found, since the
// characters go before the scrub, and one crossing a title's cap is
// scrubbed whole before the cap cuts it.
func TestAStepTitleScrubsBeforeItCaps(t *testing.T) {
	const token = "ghp_" + "abcdefghijklmnopqrstuvwxyz0123456789"
	head := token[len("ghp_") : len("ghp_")+8]
	for name, query := range map[string]string{
		"split":    "ghp_\u200b" + token[len("ghp_"):],
		"over cap": strings.Repeat("q", toolSearchQueryCap-len("ghp_")-8) + token,
	} {
		got := describePlannedTool("tool_search", map[string]any{"query": query})
		if strings.Contains(got, head) {
			t.Errorf("%s: title %q carries part of the token", name, got)
		}
	}
}
