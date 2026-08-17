package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	inboxDefaultWidth = 100
	inboxClearScreen  = "\x1b[2J\x1b[H"
)

// runInbox is the plugin pane program: render the open queue, block on a line
// read, act, re-render. No raw terminal mode, no background ticker and no TUI
// dependency — the pane's behavior is a pure function of what it was told, which
// is also what makes it testable with a stubbed reader.
func runInbox(ctx context.Context, opts clientOptions, in io.Reader, out io.Writer) error {
	reader := bufio.NewReader(in)
	status := ""
	for {
		inbox, err := clientFetchInbox(ctx, opts)
		if err != nil {
			// A restarted or briefly unreachable daemon must not kill the pane.
			inbox = clientInbox{Target: inbox.Target}
			status = "error: " + err.Error()
		}
		inboxRender(out, inbox, status)

		line, readErr := reader.ReadString('\n')
		command := strings.TrimSpace(line)
		if readErr != nil && command == "" {
			return nil
		}
		switch command {
		case "":
			status = ""
		case "q", "quit":
			return nil
		case "d":
			status = inboxKick(ctx, opts, inbox)
		case "p":
			status = inboxTogglePause(ctx, opts, inbox)
		default:
			status = "unknown command: " + command
		}
		if readErr != nil {
			return nil
		}
	}
}

func inboxKick(ctx context.Context, opts clientOptions, inbox clientInbox) string {
	result, err := clientKickNow(ctx, opts)
	if err != nil {
		return "error: " + err.Error()
	}
	if result.Busy {
		return "busy — another delivery is in flight"
	}
	if result.Outcomes > 0 {
		return fmt.Sprintf("delivered %d", result.Outcomes)
	}
	// Nothing moved. The draft guard is the usual reason, and saying so is the
	// difference between a broken button and a working one.
	if inbox.DraftHold != nil {
		return "still held — clear your composer, then it delivers on its own"
	}
	return "nothing to deliver"
}

func inboxTogglePause(ctx context.Context, opts clientOptions, inbox clientInbox) string {
	result, err := clientSetPaused(ctx, opts, !inbox.Paused)
	if err != nil {
		return "error: " + err.Error()
	}
	if result.Paused {
		return "paused"
	}
	return "resumed"
}

func inboxRender(out io.Writer, inbox clientInbox, status string) {
	width := inboxWidth()
	fmt.Fprint(out, inboxClearScreen)
	fmt.Fprintln(out, inboxHeader(inbox))
	fmt.Fprintln(out)
	if len(inbox.Rows) == 0 {
		fmt.Fprintln(out, "no messages waiting")
	} else {
		fmt.Fprintln(out, inboxRowLine("#", "age", "connector", "from", "state", "message", width))
		now := time.Now().UnixMilli()
		for index, row := range inbox.Rows {
			from := row.User
			if from == "" {
				from = "—"
			}
			fmt.Fprintln(out, inboxRowLine(
				strconv.Itoa(index+1),
				inboxAge(now-row.CreatedAt),
				row.Connector,
				from,
				row.Status,
				row.Preview,
				width,
			))
		}
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, status)
	fmt.Fprintln(out, "[enter] refresh   [d] deliver now   [p] pause/resume   [q] quit")
}

func inboxHeader(inbox clientInbox) string {
	var header strings.Builder
	header.WriteString("courier inbox — ")
	header.WriteString(inbox.Target)
	if inbox.DraftHold != nil {
		header.WriteString(fmt.Sprintf(
			" — held: your composer has unsent input (%s, pane %s)",
			inbox.DraftHold.Agent, inbox.DraftHold.PaneID,
		))
	} else {
		header.WriteString(" — delivering")
	}
	if inbox.Paused {
		header.WriteString("  [paused]")
	}
	return header.String()
}

// inboxWidth trusts $COLUMNS when the pane exports it and otherwise assumes a
// conventional width: this program never queries the terminal, so there is no
// ioctl to disagree with.
func inboxWidth() int {
	if raw, ok := os.LookupEnv("COLUMNS"); ok {
		if columns, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && columns > 0 {
			return columns
		}
	}
	return inboxDefaultWidth
}

func inboxRowLine(index, age, connector, from, state, message string, width int) string {
	var line strings.Builder
	line.WriteString(inboxPadLeft(index, 3))
	line.WriteString("  ")
	line.WriteString(inboxPadRight(age, 6))
	line.WriteString("  ")
	line.WriteString(inboxPadRight(connector, 11))
	line.WriteString("  ")
	line.WriteString(inboxPadRight(from, 8))
	line.WriteString("  ")
	line.WriteString(inboxPadRight(state, 11))
	line.WriteString("  ")
	line.WriteString(message)
	return inboxClip(line.String(), width)
}

func inboxPadLeft(value string, size int) string {
	if pad := size - utf8.RuneCountInString(value); pad > 0 {
		return strings.Repeat(" ", pad) + value
	}
	return value
}

func inboxPadRight(value string, size int) string {
	if pad := size - utf8.RuneCountInString(value); pad > 0 {
		return value + strings.Repeat(" ", pad)
	}
	return value
}

func inboxClip(value string, width int) string {
	if width <= 1 || utf8.RuneCountInString(value) <= width {
		return value
	}
	return clipRunes(value, width-1) + "…"
}

// inboxAge reports the largest whole unit, because a queue age is read at a
// glance and "2m" answers the question "is this new?" better than "137s".
func inboxAge(ms int64) string {
	if ms < 0 {
		ms = 0
	}
	seconds := ms / 1000
	switch {
	case seconds < 60:
		return strconv.FormatInt(seconds, 10) + "s"
	case seconds < 3600:
		return strconv.FormatInt(seconds/60, 10) + "m"
	case seconds < 86400:
		return strconv.FormatInt(seconds/3600, 10) + "h"
	default:
		return strconv.FormatInt(seconds/86400, 10) + "d"
	}
}

func runInboxCommand(args []string, stdout io.Writer) error {
	if len(args) != 0 {
		return fmt.Errorf("inbox takes no arguments")
	}
	opts, err := loadClientOptions(os.LookupEnv)
	if err != nil {
		return err
	}
	return runInbox(context.Background(), opts, os.Stdin, stdout)
}
