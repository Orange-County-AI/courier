package main

import "fmt"

const ShadowRefusal = "refused in SHADOW MODE"

type ShadowMode struct {
	Enabled bool
}

func NewShadowMode(enabled bool) ShadowMode {
	return ShadowMode{Enabled: enabled}
}

// Refuse returns an error rather than quietly succeeding. PostReply resolving
// means the human saw the message; a silent shadow no-op would mark handled
// having posted nothing.
func (s ShadowMode) Refuse(action string) error {
	if !s.Enabled {
		return nil
	}
	return fmt.Errorf(
		"%s %s: this host is observing only (CHANNEL_SHADOW=1) and shares its credentials with the live bridge, so an outbound effect would reach production.",
		action,
		ShadowRefusal,
	)
}

func (s ShadowMode) Refusal(action string) *ToolResult {
	if !s.Enabled {
		return nil
	}
	return &ToolResult{
		Text: action + " " + ShadowRefusal + ". The host is running as an ingest-only observer beside the live " +
			"bridge; nothing you send from here would be delivered, and anything that WAS delivered would " +
			"reach the same humans twice. Read-only tools still work.",
		IsError: true,
		Status:  503,
	}
}

func (s ShadowMode) Suppressed() bool {
	return s.Enabled
}
