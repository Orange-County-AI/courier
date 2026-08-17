package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The fixtures are verbatim herdr pane.read (source=visible) captures taken from
// live omp and Claude Code panes in a scratch herdr 0.8.0 session, including the
// screens that reproduced the clobber this guard prevents.
func readScreenFixture(t *testing.T, name string) string {
	t.Helper()
	screen, err := os.ReadFile(filepath.Join("testdata", "screens", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return string(screen)
}

func TestDetectComposerOnLiveHarnessScreens(t *testing.T) {
	for _, test := range []struct {
		fixture string
		kind    string
		want    ComposerState
		why     string
	}{
		{"omp-empty.txt", "omp", ComposerEmpty, "idle omp with nothing typed"},
		{"omp-empty-working.txt", "omp", ComposerEmpty, "omp mid-turn with nothing typed"},
		{"omp-draft.txt", "omp", ComposerDraft, "one unsent line"},
		{"omp-draft-wrapped.txt", "omp", ComposerDraft, "draft wrapped across the box body"},
		{"omp-draft-palette.txt", "omp", ComposerDraft, "slash palette renders below the composer"},
		{"omp-draft-working.txt", "omp", ComposerDraft, "unsent steering text mid-turn"},
		{"claude-empty.txt", "claude", ComposerEmpty, "idle Claude Code with nothing typed"},
		{"claude-empty-working.txt", "claude", ComposerEmpty, "Claude Code mid-turn with nothing typed"},
		{"claude-draft.txt", "claude", ComposerDraft, "one unsent line"},
		{"claude-draft-working.txt", "claude", ComposerDraft, "unsent steering text mid-turn"},
		// The operator's shell prompt uses the same ❯ glyph as Claude's
		// composer, so an unfenced marker row must not read as a draft.
		{"shell.txt", "claude", ComposerUnknown, "plain shell pane"},
		{"shell.txt", "omp", ComposerUnknown, "plain shell pane"},
		// A harness with no detector dispatches exactly as it did before the
		// guard, whatever is on its screen.
		{"omp-draft.txt", "codex", ComposerUnknown, "unsupported harness"},
		{"claude-draft.txt", "", ComposerUnknown, "unknown harness"},
		{"claude-draft.txt", "omp", ComposerUnknown, "detector is not applied across harnesses"},
		{"omp-draft.txt", "claude", ComposerUnknown, "detector is not applied across harnesses"},
	} {
		t.Run(test.fixture+"/"+test.kind, func(t *testing.T) {
			got := DetectComposer(test.kind, readScreenFixture(t, test.fixture))
			if got != test.want {
				t.Fatalf("DetectComposer(%q, %s) = %s, want %s (%s)", test.kind, test.fixture, got, test.want, test.why)
			}
		})
	}
}

func TestDetectComposerDiscriminatesBoxesFromComposers(t *testing.T) {
	for _, test := range []struct {
		name   string
		kind   string
		screen string
		want   ComposerState
	}{
		{
			name: "omp plain box border is not a composer",
			kind: "omp",
			screen: strings.Join([]string{
				"╭────────────────┬──────────────╮",
				"│ Claude Opus 5  │ no LSP       │",
				"╰────────────────┴──────────────╯",
			}, "\n"),
			want: ComposerUnknown,
		},
		{
			name: "omp draft whose last row is blank still holds",
			kind: "omp",
			screen: strings.Join([]string{
				"╭──   Opus 5 ───╮",
				"│  first line   │",
				"╰─              ─╯",
			}, "\n"),
			want: ComposerDraft,
		},
		{
			name: "omp empty box body stays empty",
			kind: "omp",
			screen: strings.Join([]string{
				"╭──   Opus 5 ───╮",
				"│               │",
				"╰─              ─╯",
			}, "\n"),
			want: ComposerEmpty,
		},
		{
			name: "claude transcript echo is not the composer",
			kind: "claude",
			screen: strings.Join([]string{
				"❯ an earlier prompt I already submitted",
				"",
				"  reply text",
			}, "\n"),
			want: ComposerUnknown,
		},
		{
			name: "claude continuation row holds",
			kind: "claude",
			screen: strings.Join([]string{
				"──────────────────────",
				"❯",
				"  wrapped second row",
				"──────────────────────",
			}, "\n"),
			want: ComposerDraft,
		},
		{
			name:   "empty screen is never a draft",
			kind:   "omp",
			screen: "   \n\n",
			want:   ComposerUnknown,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := DetectComposer(test.kind, test.screen); got != test.want {
				t.Fatalf("DetectComposer = %s, want %s", got, test.want)
			}
		})
	}
}
