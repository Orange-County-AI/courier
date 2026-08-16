package main

import "fmt"

const ManifestProtocol = 2

// BaseInstructions is the standing half of the pointer contract. courier/1 has
// no per-envelope trailer or <todo>, so read-then-judge must be explicit here
// and remain available mid-turn through the absolute schema skill path.
const BaseInstructions = "You are connected to a chat application through courier. Incoming messages arrive in your " +
	"session as <msg> blocks, and each one is a POINTER, not the message: it carries the ids as " +
	"attributes, and usually a one-line preview. The full text lives in courier, so ALWAYS call read_message " +
	"with the delivery_id first — the preview is truncated, never enough to answer from, and not guaranteed " +
	"to be there at all. The full element schema is " + SchemaSkillPath + ". " +
	"Then it is your judgment what to do. Call chat_reply, at most once, passing the delivery_id and " +
	"conversation_id back unchanged, when a reply serves the person who wrote — or call mark_handled when " +
	"none is warranted, which is the normal answer for thread chatter that is not addressed to you. Both " +
	"settle the message equally and neither is the default; a reply nobody needed is as wrong as a silence " +
	"that leaves somebody waiting. " +
	"A human may be reading your terminal, but only what you pass to chat_reply reaches the sender. A " +
	"message you neither reply to nor mark is delivered to you again, so the one thing never to do is fall " +
	"silent without settling it."

type Manifest struct {
	Protocol     int       `json:"protocol"`
	Name         string    `json:"name"`
	Version      string    `json:"version,omitempty"`
	Instructions string    `json:"instructions,omitempty"`
	Tools        []ToolDef `json:"tools"`
}

type BuildManifestOptions struct {
	Name         string
	Version      string
	Connectors   []Connector
	Instructions string
}

// BuildManifest keeps host tools first, followed by connectors in registration
// order. Names are the shim's only dispatch key, so a collision is refused
// rather than resolved by whichever plausible implementation happens to win.
func BuildManifest(opts BuildManifestOptions) (Manifest, error) {
	tools := append([]ToolDef(nil), HostToolDefs...)
	seen := make(map[string]string, len(tools))
	for _, tool := range tools {
		seen[tool.Name] = "host"
	}

	extraInstructions := make([]string, 0, len(opts.Connectors))
	for _, connector := range opts.Connectors {
		if connector == nil {
			return Manifest{}, fmt.Errorf("cannot build a manifest with a nil connector")
		}
		for _, tool := range connector.ManifestTools() {
			if owner, exists := seen[tool.Name]; exists {
				return Manifest{}, fmt.Errorf(
					"connector %s declares a tool named %q, which is already taken by %s. Tool names are the only thing the shim resolves by, so two are not allowed to collide.",
					connector.Name(),
					tool.Name,
					owner,
				)
			}
			seen[tool.Name] = "connector " + connector.Name()
			tools = append(tools, tool)
		}
		if instructions := connector.Instructions(); instructions != "" {
			extraInstructions = append(extraInstructions, instructions)
		}
	}

	instructions := opts.Instructions
	if instructions == "" {
		instructions = BaseInstructions
	}
	for _, extra := range extraInstructions {
		instructions += "\n\n" + extra
	}
	return Manifest{
		Protocol:     ManifestProtocol,
		Name:         opts.Name,
		Version:      opts.Version,
		Instructions: instructions,
		Tools:        tools,
	}, nil
}
