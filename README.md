# Agent Lens

A lightweight, **read-only** web GUI that sits *beside* your Claude Code / Codex
terminal workflow and separates the two things that get tangled in a raw terminal:

- **Discussion** — what the agent said / what you asked (prose only)
- **Code & file changes** — every tool call (edits, writes, commands) **and** the
  live `git diff` of the working directory (ground truth)

It works by *observing* the structured JSONL transcript each agent already writes
(`~/.claude/projects/.../*.jsonl`, `~/.codex/sessions/.../*.jsonl`) plus `git` —
it never drives the agent and never writes to your repos. So it runs safely
alongside your existing terminal; nothing about how you use Claude/Codex changes.

This is **Option A (observer)** of a larger idea: a unified GUI over both agents.
Start here, evaluate, then optionally grow toward a fully interactive front-end.

## Three panes

| Pane | Source | Shows |
|---|---|---|
| 💬 Discussion | transcript `text` blocks | user prompts + assistant replies; `thinking` collapsible; tool calls as slim clickable markers |
| 🔧 Activity | transcript `tool_use`/`tool_result` | every Edit/Write/Bash/… with inputs, diffs, and output |
| ± Git diff | live `git` | `git status` + unified diff of the session's `cwd`, incl. new (untracked) files; auto-refreshing |

Each agent line records its own `cwd`, so the Git pane auto-targets whatever
directory the session is working in. Why a separate Git pane? Because the
authoritative record of *what changed in files* is the working tree, not the
agent's narration — it's correct regardless of which agent or how it edited.

## Run

```bash
# uses tornado from an existing env; any python with tornado works
# defaults to localhost-only (safe):
~/miniforge3/envs/webterminal/bin/python server.py

# to reach it from your phone/iPad on the LAN, expose it WITH auth:
AGENTLENS_BIND=0.0.0.0 AGENTLENS_AUTH=me:secret \
  ~/miniforge3/envs/webterminal/bin/python server.py
```

Open `http://<host>:7703`. Pick a session from the dropdown (auto-sorted by most
recent); it tails live as the agent works.

| Env | Default | Meaning |
|---|---|---|
| `AGENTLENS_PORT` | `7703` | listen port |
| `AGENTLENS_BIND` | `127.0.0.1` | bind address; set `0.0.0.0` for LAN access |
| `AGENTLENS_AUTH` | *(disabled)* | optional HTTP Basic Auth `user:pass` |

> [!WARNING]
> This exposes your **agent conversation transcripts and source diffs**. It reads
> files under `~/.claude/projects` and `~/.codex/sessions` and runs `git diff`
> under `$HOME`. Do not expose it on an untrusted network without `AGENTLENS_AUTH`
> and a trusted boundary (VPN / SSH tunnel / reverse proxy + TLS).

## Console — interactive (`/console`)

The observer (`/`) *watches* a session you run in a terminal. The **Console**
(`/console`) instead *drives* Claude Code from the browser: a Claude-Code-style
chat with an input box, where **code/file changes render as collapsed cards**
(`✏️ Edit server.py  +12 −3`) you expand on demand — chat and changes separated,
exactly the point of this project.

It spawns the real `claude` CLI in headless stream-json mode
(`claude -p --input-format stream-json --output-format stream-json --verbose`),
keeps one process alive per chat, feeds your messages on stdin, and renders the
typed event stream back. Pick the **project dir**, **model** (default/opus/
sonnet/haiku), and **permission mode** in the header; tap **± Changes** for the
live `git diff` drawer.

- Permission mode defaults to `acceptEdits` (tools run without prompts — smooth,
  appropriate on your own machine over a private mesh). `plan` / `default` /
  `bypass` are selectable.
- Each browser connection is **one ephemeral session** (disconnect ends the
  `claude` process). Resume across reloads is a planned addition.
- The console can read/write files and run commands in the chosen directory —
  that's the point, but keep it mesh-only / behind auth.

## Status

- ✅ **Observer**: Claude + Codex transcript tail, live git diff, mobile tabs
- ✅ **Console**: interactive Claude Code driver (stream-json), collapsed change
  cards, model/permission pickers, live-diff drawer
- ⏭️ Next: **Codex console** (drive `codex` too), per-action approve/deny UI,
  session resume across reloads, interrupt mid-turn, Codex `apply_patch` diffs,
  inotify push instead of diff polling.

## How it fits

This is sibling to the author's other local services (`7700` web-terminal,
`7701` web-file-manager, `7702` local-cdn). It's intentionally a single
`server.py` with inline HTML — no build step — matching that style.
