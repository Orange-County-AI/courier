package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type HealthState struct {
	Org             string
	Target          string
	Shadow          bool
	Connectors      []string
	HostTools       []string
	Reconcile       *string
	ReconcileAt     *int64
	ReconcileSource *string
	DraftHoldPane   *string
	DraftHoldAt     *int64
}

type healthResponse struct {
	OK               bool     `json:"ok"`
	Events           int64    `json:"events"`
	UnpostedReplies  int      `json:"unposted_replies"`
	Org              string   `json:"org"`
	Target           string   `json:"target"`
	Shadow           bool     `json:"shadow"`
	Connectors       []string `json:"connectors"`
	HostTools        []string `json:"host_tools"`
	Reconcile        *string  `json:"reconcile"`
	ReconcileAt      *int64   `json:"reconcile_at"`
	ReconcileSource  *string  `json:"reconcile_source"`
	DraftHoldPane    *string  `json:"draft_hold_pane"`
	DraftHoldAt      *int64   `json:"draft_hold_at"`
	Unread           int64    `json:"unread"`
	OldestUnreadAgeS *int64   `json:"oldest_unread_age_s"`
	ReadUnconfirmed  int64    `json:"read_unconfirmed"`
}

type IPCOptions struct {
	Store    *Store
	Manifest func() (Manifest, error)
	CallTool func(context.Context, string, map[string]any) (ToolResult, error)
	Health   func() HealthState
	Now      func() int64
	Log      func(...any)
}

type IPCListenOptions struct {
	Bind   string
	Port   int
	Socket string
}

func NewIPCHandler(opts IPCOptions) (http.Handler, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("IPC requires a store")
	}
	if opts.Manifest == nil {
		return nil, fmt.Errorf("IPC requires a manifest provider")
	}
	if opts.CallTool == nil {
		return nil, fmt.Errorf("IPC requires a tool dispatcher")
	}
	now := opts.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	log := opts.Log
	if log == nil {
		log = func(...any) {}
	}
	health := opts.Health
	if health == nil {
		health = func() HealthState { return HealthState{} }
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		state := health()
		events, err := opts.Store.CountEvents()
		if err != nil {
			writeIPCError(w, http.StatusInternalServerError, err)
			return
		}
		unposted, err := opts.Store.UnpostedReplies()
		if err != nil {
			writeIPCError(w, http.StatusInternalServerError, err)
			return
		}
		stats, err := opts.Store.DeliveryStats(state.Target, now())
		if err != nil {
			writeIPCError(w, http.StatusInternalServerError, err)
			return
		}
		connectors := state.Connectors
		if connectors == nil {
			connectors = []string{}
		}
		hostTools := state.HostTools
		if hostTools == nil {
			hostTools = append([]string(nil), HostToolNames...)
		}
		writeJSON(w, http.StatusOK, healthResponse{
			OK:               true,
			Events:           events,
			UnpostedReplies:  len(unposted),
			Org:              state.Org,
			Target:           state.Target,
			Shadow:           state.Shadow,
			Connectors:       connectors,
			HostTools:        hostTools,
			Reconcile:        state.Reconcile,
			ReconcileAt:      state.ReconcileAt,
			ReconcileSource:  state.ReconcileSource,
			DraftHoldPane:    state.DraftHoldPane,
			DraftHoldAt:      state.DraftHoldAt,
			Unread:           stats.Unread,
			OldestUnreadAgeS: stats.OldestUnreadAgeS,
			ReadUnconfirmed:  stats.ReadUnconfirmed,
		})
	})

	mux.HandleFunc("GET /manifest", func(w http.ResponseWriter, r *http.Request) {
		manifest, err := opts.Manifest()
		if err != nil {
			writeIPCError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, manifest)
	})

	mux.HandleFunc("POST /tool/", func(w http.ResponseWriter, r *http.Request) {
		encodedName := strings.TrimPrefix(r.URL.EscapedPath(), "/tool/")
		name, err := url.PathUnescape(encodedName)
		if err != nil {
			writeIPCError(w, http.StatusBadRequest, fmt.Errorf("invalid tool name: %w", err))
			return
		}
		args := make(map[string]any)
		if err := decodeJSON(r, &args); err != nil {
			writeIPCError(w, http.StatusBadRequest, err)
			return
		}
		result, err := opts.CallTool(r.Context(), name, args)
		if err != nil {
			log("tool threw:", name, err.Error())
			writeJSON(w, http.StatusInternalServerError, ToolResult{Text: "error: " + err.Error(), IsError: true})
			return
		}
		status := result.Status
		if status == 0 {
			status = http.StatusOK
		}
		writeJSON(w, status, result)
	})

	// Kept separate from mark_handled so an operator can retire a stuck row
	// without impersonating an agent. This is still the sanctioned Store door;
	// IPC writes no settlement SQL of its own.
	mux.HandleFunc("POST /handled", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			DeliveryID string `json:"delivery_id"`
			EventID    *int64 `json:"event_id"`
		}
		if err := decodeJSON(r, &body); err != nil {
			writeIPCError(w, http.StatusBadRequest, err)
			return
		}
		if body.DeliveryID == "" && body.EventID == nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "delivery_id or event_id is required"})
			return
		}
		ok := opts.Store.MarkHandled(MarkHandledArgs{DeliveryID: body.DeliveryID, EventID: body.EventID}, now())
		writeJSON(w, http.StatusOK, map[string]any{"ok": ok})
	})

	return mux, nil
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	var extra any
	switch err := decoder.Decode(&extra); {
	case errors.Is(err, io.EOF):
		return nil
	case err != nil:
		return fmt.Errorf("invalid JSON body: %w", err)
	default:
		return fmt.Errorf("invalid JSON body: multiple values")
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeIPCError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

// ListenIPC selects a unix socket when Socket is set, otherwise a TCP bind.
// Only a stale socket inode is removed; a regular file at that path is a loud
// configuration error rather than collateral damage during startup.
func ListenIPC(opts IPCListenOptions) (net.Listener, error) {
	if opts.Socket != "" {
		info, err := os.Lstat(opts.Socket)
		switch {
		case err == nil && info.Mode()&os.ModeSocket == 0:
			return nil, fmt.Errorf("IPC socket path %s exists and is not a socket", opts.Socket)
		case err == nil:
			if err := os.Remove(opts.Socket); err != nil {
				return nil, fmt.Errorf("remove stale IPC socket %s: %w", opts.Socket, err)
			}
		case !errors.Is(err, os.ErrNotExist):
			return nil, fmt.Errorf("inspect IPC socket %s: %w", opts.Socket, err)
		}
		listener, err := net.Listen("unix", opts.Socket)
		if err != nil {
			return nil, fmt.Errorf("listen on unix socket %s: %w", opts.Socket, err)
		}
		return listener, nil
	}

	bind := opts.Bind
	if bind == "" {
		bind = "127.0.0.1"
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(bind, strconv.Itoa(opts.Port)))
	if err != nil {
		return nil, fmt.Errorf("listen on %s:%d: %w", bind, opts.Port, err)
	}
	return listener, nil
}
