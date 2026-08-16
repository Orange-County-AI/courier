package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestManifestProtocolAndToolOrder(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	manifest, err := BuildManifest(BuildManifestOptions{
		Name:       "courier-test",
		Connectors: h.registry.All(),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantNames := append(append([]string(nil), HostToolNames...), "test_action")
	gotNames := make([]string, len(manifest.Tools))
	for i, tool := range manifest.Tools {
		gotNames[i] = tool.Name
	}
	if ManifestProtocol != 2 || manifest.Protocol != 2 || !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("manifest protocol/order = %d %v, want 2 %v", manifest.Protocol, gotNames, wantNames)
	}
	if !strings.Contains(manifest.Instructions, "chat_reply") || !strings.Contains(manifest.Instructions, "mark_handled") {
		t.Fatalf("manifest instructions missing host contract: %q", manifest.Instructions)
	}
}

func TestManifestIncludesReadMessageAsPointerTarget(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	manifest, err := BuildManifest(BuildManifestOptions{Name: "courier-test", Connectors: h.registry.All()})
	if err != nil {
		t.Fatal(err)
	}
	var readTool *ToolDef
	for i := range manifest.Tools {
		if manifest.Tools[i].Name == "read_message" {
			readTool = &manifest.Tools[i]
			break
		}
	}
	if readTool == nil || !strings.Contains(strings.ToLower(readTool.Description), "pointer") {
		t.Fatalf("read_message missing or not described as pointer: %#v", readTool)
	}
	required, ok := readTool.InputSchema["required"].([]string)
	if !ok || !reflect.DeepEqual(required, []string{"delivery_id"}) {
		t.Fatalf("read_message required = %#v", readTool.InputSchema["required"])
	}
}

func TestManifestInstructionsAreUpdatedCourierPointerContract(t *testing.T) {
	h := newHosttoolsHarness(t, false)
	manifest, err := BuildManifest(BuildManifestOptions{Name: "courier-test", Connectors: h.registry.All()})
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"<msg>",
		"read_message",
		"POINTER",
		"your judgment",
		"thread chatter that is not addressed to you",
		"Both settle the message equally",
		"A human may be reading your terminal",
		"only what you pass to chat_reply reaches the sender",
		SchemaSkillPath,
		"\n\n" + h.connector.instructions,
	} {
		if !strings.Contains(manifest.Instructions, fragment) {
			t.Errorf("instructions missing %q: %q", fragment, manifest.Instructions)
		}
	}
	for _, stale := range []string{"<chat_message>", "<todo>", "CANNOT see your terminal", "/opt/skills/legacy-envelope/"} {
		if strings.Contains(manifest.Instructions, stale) {
			t.Errorf("instructions contain stale %q: %q", stale, manifest.Instructions)
		}
	}
}

func TestManifestRefusesConnectorToolShadowingHostTool(t *testing.T) {
	bad := &hosttoolsFakeConnector{name: "shadow", tools: []ToolDef{{Name: "chat_reply"}}}
	_, err := BuildManifest(BuildManifestOptions{Name: "x", Connectors: []Connector{bad}})
	if err == nil || !strings.Contains(err.Error(), "already") || !strings.Contains(err.Error(), "chat_reply") {
		t.Fatalf("host-tool collision error = %v", err)
	}
}

func TestRegistryRefusesDuplicateConnectorAndUnknownRequirement(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(&hosttoolsFakeConnector{name: "mm"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(&hosttoolsFakeConnector{name: "mm"}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate connector error = %v", err)
	}
	if got := registry.Get("nope"); got != nil {
		t.Fatalf("unknown connector = %#v", got)
	}
	if _, err := registry.Require("nope"); err == nil || !strings.Contains(err.Error(), "no connector registered") {
		t.Fatalf("unknown require error = %v", err)
	}
}

func TestManifestRefusesConnectorToConnectorToolCollision(t *testing.T) {
	first := &hosttoolsFakeConnector{name: "first", tools: []ToolDef{{Name: "same"}}}
	second := &hosttoolsFakeConnector{name: "second", tools: []ToolDef{{Name: "same"}}}
	_, err := BuildManifest(BuildManifestOptions{Name: "x", Connectors: []Connector{first, second}})
	if err == nil || !strings.Contains(err.Error(), "same") || !strings.Contains(err.Error(), "already") {
		t.Fatalf("connector collision error = %v", err)
	}
}
