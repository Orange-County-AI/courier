package main

import (
	"context"
	"strings"
	"testing"
)

func TestShadowParsing(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		raw  string
		want bool
	}{
		{raw: "", want: false},
		{raw: "0", want: false},
		{raw: "false", want: false},
		{raw: " FALSE ", want: false},
		{raw: "no", want: false},
		{raw: "NO", want: false},
		{raw: "1", want: true},
		{raw: "true", want: true},
		{raw: "off", want: true},
		{raw: "typo", want: true},
	} {
		if got := parseShadow(test.raw); got != test.want {
			t.Errorf("parseShadow(%q) = %t, want %t", test.raw, got, test.want)
		}
	}
}

func TestShadowRefuseThrowsEquivalentError(t *testing.T) {
	t.Parallel()
	shadow := NewShadowMode(true)
	err := shadow.Refuse("mattermost post")
	if err == nil || !strings.HasPrefix(err.Error(), "mattermost post "+ShadowRefusal) {
		t.Fatalf("Refuse() error = %v", err)
	}
	if !strings.Contains(err.Error(), "CHANNEL_SHADOW=1") {
		t.Fatalf("Refuse() error lacks operator context: %v", err)
	}
	if err := NewShadowMode(false).Refuse("mattermost post"); err != nil {
		t.Fatalf("live Refuse() = %v", err)
	}
}

func TestShadowToolRefusal(t *testing.T) {
	t.Parallel()
	result := NewShadowMode(true).Refusal("gmail_send")
	if result == nil {
		t.Fatal("Refusal() = nil in shadow mode")
	}
	if result.Status != 503 || !result.IsError || !strings.HasPrefix(result.Text, "gmail_send "+ShadowRefusal) {
		t.Fatalf("Refusal() = %#v", result)
	}
	if result := NewShadowMode(false).Refusal("gmail_send"); result != nil {
		t.Fatalf("live Refusal() = %#v", result)
	}
}

type registryTestConnector struct {
	name  string
	tools []ToolDef
}

func (c registryTestConnector) Name() string             { return c.name }
func (c registryTestConnector) ManifestTools() []ToolDef { return c.tools }
func (registryTestConnector) Instructions() string       { return "" }
func (registryTestConnector) CallTool(context.Context, string, map[string]any) (ToolResult, error) {
	return ToolResult{}, nil
}
func (registryTestConnector) PostReply(context.Context, DeliveryContext, string) error { return nil }
func (registryTestConnector) Start(context.Context) error                              { return nil }
func (registryTestConnector) Stop(context.Context) error                               { return nil }

func TestRegistryRefusesDuplicateToolNames(t *testing.T) {
	t.Parallel()
	registry := NewRegistry()
	first := registryTestConnector{name: "first", tools: []ToolDef{{Name: "shared"}}}
	second := registryTestConnector{name: "second", tools: []ToolDef{{Name: "shared"}, {Name: "unique"}}}
	if err := registry.Register(first); err != nil {
		t.Fatal(err)
	}
	err := registry.Register(second)
	if err == nil || !strings.Contains(err.Error(), `tool named "shared"`) || !strings.Contains(err.Error(), "two are not allowed to collide") {
		t.Fatalf("Register() error = %v", err)
	}
	if registry.Get("second") != nil {
		t.Fatal("failed registration mutated connector index")
	}
	if got := len(registry.All()); got != 1 {
		t.Fatalf("registry size = %d, want 1", got)
	}
}

func TestRegistryRefusesDuplicateConnectorNames(t *testing.T) {
	t.Parallel()
	registry := NewRegistry()
	if err := registry.Register(registryTestConnector{name: "same"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(registryTestConnector{name: "same"}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate connector error = %v", err)
	}
}
