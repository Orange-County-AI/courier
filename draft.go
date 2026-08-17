package main

import "strings"

// Draft detection exists because herdr's agent.prompt is not a queue. It writes
// the prompt bytes at the pane's cursor and presses Enter 300 ms later (herdr
// 0.8.0, src/app/api/agents.rs handle_agent_prompt), so a pane whose composer
// already holds a human's unsent keystrokes submits draft+message as a single
// prompt: the half-typed sentence is sent to the human's own agent, and the
// delivered message is corrupted by whatever preceded it. Reproduced against
// herdr 0.8.0 with both omp and Claude Code.
//
// The rendered screen is the only evidence herdr exposes; pane.read returns
// text, never the harness input buffer. Detection is therefore per-harness and
// deliberately one-sided: only a composer this file positively recognizes as
// non-empty holds a delivery back. An unfamiliar harness, a hidden composer, or
// an unreadable screen dispatches exactly as it did before the guard, because
// starving a durable queue on an unparsed screen is worse than the clobber the
// guard prevents.
type ComposerState int

const (
	// ComposerUnknown means no composer this build can read was on the screen.
	ComposerUnknown ComposerState = iota
	// ComposerEmpty means the composer was located and holds no input.
	ComposerEmpty
	// ComposerDraft means the composer holds unsent input.
	ComposerDraft
)

func (state ComposerState) String() string {
	switch state {
	case ComposerEmpty:
		return "empty"
	case ComposerDraft:
		return "draft"
	default:
		return "unknown"
	}
}

// DetectComposer reads the harness composer out of a rendered pane screen.
// agentKind is herdr's AgentInfo.agent label.
func DetectComposer(agentKind, screen string) ComposerState {
	if strings.TrimSpace(screen) == "" {
		return ComposerUnknown
	}
	switch strings.ToLower(strings.TrimSpace(agentKind)) {
	case "omp", "pi":
		return detectOmpComposer(screen)
	case "claude":
		return detectClaudeComposer(screen)
	default:
		return ComposerUnknown
	}
}

// omp draws the composer as a rounded box whose closing border carries the last
// visual row of the input: "╰─ text …─╯" over "│  wrapped text  │" rows under a
// "╭── status ──╮" opener. The closing border is the anchor because omp renders
// the slash-command palette and completion list *below* the box, so the
// composer is not necessarily the last thing on the screen.
func detectOmpComposer(screen string) ComposerState {
	lines := strings.Split(screen, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		interior, ok := ompComposerFooter(lines[index])
		if !ok {
			continue
		}
		if strings.TrimSpace(interior) != "" {
			return ComposerDraft
		}
		// A draft whose last visual row is empty (trailing newline) still shows
		// its earlier rows in the box body above the closing border.
		for above := index - 1; above >= 0; above-- {
			body, ok := ompComposerBody(lines[above])
			if !ok {
				// The opening border, or anything that is not a body row, ends
				// the composer block.
				break
			}
			if strings.TrimSpace(body) != "" {
				return ComposerDraft
			}
		}
		return ComposerEmpty
	}
	return ComposerUnknown
}

// ompComposerFooter matches the composer's closing border and returns the input
// text it carries. A plain box border ("╰────┴────╯") is rejected: the composer
// always pads its text with a space, so an interior that starts with a box rule
// belongs to some other frame, such as the startup splash.
func ompComposerFooter(line string) (string, bool) {
	trimmed := strings.TrimRight(line, " \t\r")
	interior, ok := strings.CutPrefix(trimmed, "╰─")
	if !ok {
		return "", false
	}
	interior, ok = strings.CutSuffix(interior, "─╯")
	if !ok || !strings.HasPrefix(interior, " ") {
		return "", false
	}
	return interior, true
}

func ompComposerBody(line string) (string, bool) {
	trimmed := strings.TrimRight(line, " \t\r")
	interior, ok := strings.CutPrefix(trimmed, "│")
	if !ok {
		return "", false
	}
	return strings.CutSuffix(interior, "│")
}

// Claude Code draws the composer as a "❯ text" row fenced by two horizontal
// rules. The fence is required: Claude echoes every submitted prompt in the
// transcript with the same ❯ marker, and the operator's shell prompt can use it
// too, so an unfenced marker row is not the composer.
func detectClaudeComposer(screen string) ComposerState {
	lines := strings.Split(screen, "\n")
	for index := len(lines) - 1; index >= 1; index-- {
		text, ok := strings.CutPrefix(strings.TrimSpace(lines[index]), "❯")
		if !ok || !claudeComposerRule(lines[index-1]) {
			continue
		}
		if strings.TrimSpace(text) != "" {
			return ComposerDraft
		}
		for below := index + 1; below < len(lines); below++ {
			row := strings.TrimSpace(lines[below])
			if claudeComposerRule(row) {
				break
			}
			if row != "" {
				return ComposerDraft
			}
		}
		return ComposerEmpty
	}
	return ComposerUnknown
}

// claudeComposerRule reports a solid horizontal rule. The minimum length keeps
// a one-glyph box fragment in transcript output from passing as the fence.
func claudeComposerRule(line string) bool {
	trimmed := strings.TrimSpace(line)
	if len([]rune(trimmed)) < 8 {
		return false
	}
	for _, glyph := range trimmed {
		if glyph != '─' {
			return false
		}
	}
	return true
}
