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
	Paused          bool
	Connectors      []string
	HostTools       []string
	Reconcile       *string
	ReconcileAt     *int64
	ReconcileSource *string
	DraftHoldPane   *string
	DraftHoldAt     *int64
}

// InboxState is the open queue as a human needs to see it: what is waiting, why
// it is waiting, and whether delivery is paused. It never settles anything —
// listing a message must not be able to dismiss it.
type InboxState struct {
	Target    string     `json:"target"`
	Paused    bool       `json:"paused"`
	DraftHold *DraftHold `json:"draft_hold"`
	Rows      []InboxRow `json:"rows"`
}

type InboxRow struct {
	DeliveryID     string  `json:"delivery_id"`
	EventID        int64   `json:"event_id"`
	Status         string  `json:"status"`
	Connector      string  `json:"connector"`
	ConversationID string  `json:"conversation_id"`
	User           string  `json:"user"`
	AttemptCount   int     `json:"attempt_count"`
	Read           bool    `json:"read"`
	CreatedAt      int64   `json:"created_at"`
	LastError      *string `json:"last_error"`
	Preview        string  `json:"preview"`
}

// KickResult reports one on-demand tick. Busy is not a failure: another tick or
// drain already holds the serialization lock, so the work is in flight.
type KickResult struct {
	Busy     bool `json:"busy"`
	Outcomes int  `json:"outcomes"`
}

type inboxResponse struct {
	OK bool `json:"ok"`
	InboxState
}

type kickResponse struct {
	OK bool `json:"ok"`
	KickResult
}

type healthResponse struct {
	OK               bool     `json:"ok"`
	Events           int64    `json:"events"`
	UnpostedReplies  int      `json:"unposted_replies"`
	Org              string   `json:"org"`
	Target           string   `json:"target"`
	Shadow           bool     `json:"shadow"`
	Paused           bool     `json:"paused"`
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
	Inbox    func(context.Context) (InboxState, error)
	Kick     func(context.Context) (KickResult, error)
	Pause    func(context.Context, bool) (bool, error)
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
	inbox := opts.Inbox
	if inbox == nil {
		inbox = func(context.Context) (InboxState, error) { return InboxState{}, nil }
	}
	kick := opts.Kick
	if kick == nil {
		kick = func(context.Context) (KickResult, error) { return KickResult{}, nil }
	}
	pause := opts.Pause
	if pause == nil {
		pause = func(_ context.Context, paused bool) (bool, error) { return paused, nil }
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
			Paused:           state.Paused,
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

	// The inbox is read-only by construction: it reports the open queue and the
	// reason it is held, and offers no way to settle a message. An unanswered
	// message must not be dismissable from a list.
	mux.HandleFunc("GET /inbox", func(w http.ResponseWriter, r *http.Request) {
		state, err := inbox(r.Context())
		if err != nil {
			writeIPCError(w, http.StatusInternalServerError, err)
			return
		}
		if state.Rows == nil {
			state.Rows = []InboxRow{}
		}
		writeJSON(w, http.StatusOK, inboxResponse{OK: true, InboxState: state})
	})

	// No body is required: a kick carries no arguments, and a plugin action that
	// has to synthesize `{}` to ask for a tick is a worse interface.
	mux.HandleFunc("POST /kick", func(w http.ResponseWriter, r *http.Request) {
		result, err := kick(r.Context())
		if err != nil {
			writeIPCError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, kickResponse{OK: true, KickResult: result})
	})

	mux.HandleFunc("POST /pause", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Paused json.RawMessage `json:"paused"`
		}
		if err := decodeJSON(r, &body); err != nil {
			writeIPCError(w, http.StatusBadRequest, err)
			return
		}
		var paused bool
		if len(body.Paused) == 0 || json.Unmarshal(body.Paused, &paused) != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "paused must be a boolean"})
			return
		}
		resulting, err := pause(r.Context(), paused)
		if err != nil {
			writeIPCError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "paused": resulting})
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
