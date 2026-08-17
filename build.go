package main

import (
	"runtime/debug"
	"strings"
)

// A semver constant alone cannot answer "what is running". Three courier
// binaries deployed on this fleet all self-report 0.1.0 and are demonstrably
// different builds — one of them wrote schema user_version=2, which no 0.1.0
// build did — so an md5 of the file was the only reliable handle on a running
// copy. That is a poor tool for a gateway that owns a customer conversation.
//
// So every version surface carries the commit the binary was built from, read
// from the stamp the Go toolchain already embeds from the VCS tree. A plain
// `go build` gets it right: no ldflags to remember, no build task to keep in
// sync, and nothing a hand-built copy can silently omit.
func buildRevision() (revision string, modified bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	return revision, modified
}

// buildVersion is what the CLI, the MCP manifest and /health all report.
func buildVersion() string {
	revision, modified := buildRevision()
	if revision == "" {
		// Built outside a VCS tree (vendored source, or `go run`). Say so rather
		// than implying a provenance this binary does not have.
		return courierVersion + " (unknown revision)"
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	var reported strings.Builder
	reported.WriteString(courierVersion)
	reported.WriteString(" (")
	reported.WriteString(revision)
	if modified {
		reported.WriteString("-dirty")
	}
	reported.WriteByte(')')
	return reported.String()
}
