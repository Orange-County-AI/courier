package main

import (
	"context"
	"fmt"
	"sync"
)

// ToolDef is passed through to the MCP manifest without schema interpretation.
type ToolDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	InputSchema map[string]any `json:"inputSchema,omitempty"`
}

// ToolResult carries an HTTP status for ipc.go; Status is response metadata,
// not part of the body the MCP shim sees.
type ToolResult struct {
	Status  int    `json:"-"`
	Text    string `json:"text"`
	IsError bool   `json:"is_error,omitempty"`
}

// DeliveryContext contains the durable rows that identify where a reply goes.
type DeliveryContext struct {
	Delivery       Delivery
	Event          Event
	ConversationID string
}

// Connector is the one seam between courier's durable core and a transport.
// PostReply must return nil only after the remote system confirms the post;
// optimistic success converts "the human never saw it" into "answered".
type Connector interface {
	Name() string
	ManifestTools() []ToolDef
	Instructions() string
	CallTool(ctx context.Context, name string, args map[string]any) (ToolResult, error)
	PostReply(ctx context.Context, dc DeliveryContext, message string) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// unavailableNotifier is optional. A connector implements it only when its
// transport has a user-visible way to explain that a durable delivery is
// queued while the herdr target is unavailable.
type unavailableNotifier interface {
	NotifyUnavailable(context.Context, DeliveryContext) error
}

type Registry struct {
	mu         sync.RWMutex
	byName     map[string]Connector
	connectors []Connector
	toolOwner  map[string]string
}

func NewRegistry() *Registry {
	return &Registry{
		byName:    make(map[string]Connector),
		toolOwner: make(map[string]string),
	}
}

func (r *Registry) Register(connector Connector) error {
	if connector == nil {
		return fmt.Errorf("cannot register a nil connector")
	}
	name := connector.Name()
	if name == "" {
		return fmt.Errorf("cannot register a connector with an empty name")
	}
	tools := connector.ManifestTools()

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byName[name]; exists {
		return fmt.Errorf("connector %s is already registered", name)
	}
	for _, tool := range tools {
		if owner, exists := r.toolOwner[tool.Name]; exists {
			return fmt.Errorf(
				"connector %s declares a tool named %q, which is already declared by connector %s; tool names are the only thing the shim resolves by, so two are not allowed to collide",
				name,
				tool.Name,
				owner,
			)
		}
	}
	// Validate the complete declaration before mutating either index. A failed
	// registration must not reserve half of a connector's tools.
	declared := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		if _, exists := declared[tool.Name]; exists {
			return fmt.Errorf("connector %s declares tool %q more than once", name, tool.Name)
		}
		declared[tool.Name] = struct{}{}
	}

	r.byName[name] = connector
	r.connectors = append(r.connectors, connector)
	for _, tool := range tools {
		r.toolOwner[tool.Name] = name
	}
	return nil
}

func (r *Registry) Get(name string) Connector {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byName[name]
}

func (r *Registry) Require(name string) (Connector, error) {
	connector := r.Get(name)
	if connector == nil {
		return nil, fmt.Errorf("no connector registered under the name %q", name)
	}
	return connector, nil
}

func (r *Registry) All() []Connector {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]Connector(nil), r.connectors...)
}
