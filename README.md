# Claude Console

An interactive, browser-driven GUI for Claude Code that separates the two things
that get tangled together in a raw terminal:

- **Discussion** — what the agent said / what you asked (prose only)
- **Code & file changes** — every tool call (edits, writes, commands) rendered as
  **collapsed cards** (`✏️ Edit server.py  +12 −3`) you expand on demand, plus the
  live `git diff` of the working directory (ground truth)

It spawns the real `claude` CLI in headless stream-json mode
(`claude -p --input-format stream-json --output-format stream-json --verbose`),
keeps one process alive per chat, feeds your messages on stdin, and renders the
typed event stream back — a Claude-Code-style chat where code and conversation
stay visually separated, exactly the point of this project.

Pick the **project dir**, **model** (default/opus/sonnet/haiku), and **permission
mode** in the header; tap **± Changes** for the live `git diff` drawer.

- Permission mode defaults to `acceptEdits` (tools run without prompts — smooth,
  appropriate on your own machine over a private mesh). `plan` / `default` /
  `bypass` are selectable.
- Sessions are **resumable**: reopen a project and pick up a previous Claude Code
  conversation (transcripts under `~/.claude/projects` are read to restore
  history and target the right `cwd`).
- The console can read/write files and run commands in the chosen directory —
  that's the point, but keep it mesh-only / behind auth.

Why a separate change view? Because the authoritative record of *what changed in
files* is the working tree, not the agent's narration — the `git diff` drawer is
correct regardless of how the agent edited.

## Run

```bash
# uses tornado from an existing env; any python with tornado works
# defaults to localhost-only (safe):
~/miniforge3/envs/webterminal/bin/python server.py

# to reach it from your phone/iPad on the LAN, expose it WITH auth:
CLAUDE_CONSOLE_BIND=0.0.0.0 CLAUDE_CONSOLE_AUTH=me:secret \
  ~/miniforge3/envs/webterminal/bin/python server.py
```

Open `http://<host>:7703`. Pick a project dir and start chatting; the change
cards and the `± Changes` drawer update live as the agent works.

| Env | Default | Meaning |
|---|---|---|
| `CLAUDE_CONSOLE_PORT` | `7703` | listen port |
| `CLAUDE_CONSOLE_BIND` | `127.0.0.1` | bind address; set `0.0.0.0` for LAN access |
| `CLAUDE_CONSOLE_AUTH` | *(disabled)* | optional HTTP Basic Auth `user:pass` |

The legacy `AGENTLENS_*` names are still honored as a fallback.

> [!WARNING]
> This **drives Claude Code** in the directory you pick — it can read/write files
> and run commands there — and it exposes your **chat history and source diffs**.
> It reads transcripts under `~/.claude/projects` and runs `git diff` under
> `$HOME`. Do not expose it on an untrusted network without `CLAUDE_CONSOLE_AUTH`
> and a trusted boundary (VPN / SSH tunnel / reverse proxy + TLS).

## Status

- ✅ **Console**: interactive Claude Code driver (stream-json), collapsed change
  cards, model/permission pickers, live-diff drawer, folder-scoped session resume
- ✅ **Interactive round-trips**: per-action Approve/Deny prompts (in 🔐 Approve
  mode) and in-browser **AskUserQuestion** (the agent's multiple-choice questions
  render as clickable cards; your pick is fed back to the agent)
- ⏭️ Next: **Codex console** (drive `codex` too), interrupt mid-turn, Codex
  `apply_patch` diffs, inotify push instead of diff polling.

## How it fits

This is sibling to the author's other local services (`7700` web-terminal,
`7701` web-file-manager, `7702` local-cdn). It's intentionally a single
`server.py` with inline HTML — no build step — matching that style.
