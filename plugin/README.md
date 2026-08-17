# Courier Inbox — herdr plugin

The companion surface for courier's draft guard. courier already refuses to inject a message while
the target pane's composer holds unsent human input — the delivery stays queued and is retried. This
plugin is what makes that hold visible and controllable:

- a **toast** when a delivery is first held (`notification.show`, position from herdr's `[ui.toast]`),
- an **inbox pane** listing what is waiting, why, and for how long,
- **deliver-now** and **pause/resume** actions, and
- an **event hook** on `pane.agent_status_changed` so a held message lands about a second after you
  submit your own prompt instead of up to `COURIER_TICK_MS` (default 15 s) later.

The plugin gets no privileged access: plugin v1 is "the CLI is the plugin API". The guard inside the
daemon remains the mechanism that protects your draft; this is only the human surface. Listing a
message can never settle it, so an unanswered message cannot be dismissed from the UI.

## Install

```sh
herdr plugin link ./plugin
herdr plugin list                       # courier.inbox, enabled
herdr plugin action list --plugin courier.inbox
```

`courier` must be on the **herdr server's** `PATH` (`~/.local/bin/courier` after `mise run build`).
Every manifest entry shells out to that binary; if it is not on `PATH` for the process that started
the herdr server, replace `"courier"` in `plugin/herdr-plugin.toml` with an absolute path.

Point the commands at a non-default IPC bind with `COURIER_HOST_URL` (or `CHANNEL_HOST_URL`);
the default is `http://127.0.0.1:8788`.

## Keybinding

herdr binds plugin *actions*, not panes, which is why `open-inbox` exists. Paste into
`~/.config/herdr/config.toml`, then `herdr server reload-config`:

```toml
[[keys.command]]
key = "prefix+i"
type = "plugin_action"
command = "courier.inbox.open-inbox"
description = "courier inbox"
```

## The pane

`prefix+i` opens a session-modal popup (80% wide, 20 rows) over the layout without disturbing the
tiled panes or your draft; closing it restores focus and your draft exactly as it was.

```
courier inbox — my-agent — held: your composer has unsent input (omp, pane w7V:p1)

  #  age     connector    from      state        message
  1  2m      mattermost   Dana      pending      Can you check whether the Friday batch went out…

still held — clear your composer, then it delivers on its own
[enter] refresh   [d] deliver now   [p] pause/resume   [q] quit
```

`[d]` is `POST /kick`, `[p]` is `POST /pause` — the same calls as the `deliver-now` and
`toggle-pause` actions. While paused the header carries `[paused]` and nothing is delivered until you
resume; pause is process-lifetime state, so restarting courier resumes delivery.

## Debugging

`herdr plugin log list --plugin courier.inbox` is the debug surface: action and hook stdout/stderr
and exit codes land there. Notes on what you will see:

- `plugin.pane.open` answers `ui_busy` while Settings, Copy mode, or another modal is open.
- `courier kick --if-pane-matches` exits 0 silently unless the daemon's hold names the pane whose
  status just changed — an unset event payload, a different pane, or an unreachable daemon are all
  quiet successes, so the per-session hook storm stays cheap.
- `courier plugin-probe` (the startup hook) also always exits 0; an unreachable daemon is reported as
  a notification, because a failing startup hook must not stop the herdr server.
- A headless session answers `no_foreground_client` to `notification.show`. That is correct, not an
  error: there is no client to draw a toast.
