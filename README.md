# Claude Console

English | [简体中文](README.zh-CN.md)

An interactive, browser-driven GUI for **Claude Code** that separates the two
things a raw terminal tangles together:

- **Discussion** — what you asked / what the agent said (prose only)
- **Code & file changes** — every tool call (edits, writes, commands) rendered as
  **collapsed cards** (`✏️ Edit observables.py  +12 −3`) you expand on demand, plus the
  live `git diff` of the working directory (ground truth)

<p align="center">
  <img src="figs/chat-plan-math-links.png" width="49%"
       alt="Claude Console — the plan pinned above the chat, KaTeX math, and file paths as links">
  <img src="figs/choices-and-attachments.png" width="49%"
       alt="Claude Console — an AskUserQuestion card with options, and a file waiting in the composer">
</p>
<p align="center">
  <sub>Left: the plan stays pinned while the answer scrolls under it; math renders with KaTeX and
  file paths become links that open in a web file manager. Right: the agent asking <em>you</em> a
  question, with the plan folded to just the active task and a file attached to the next message.
  Both in the One Light theme.</sub>
</p>

It drives Claude Code through the **Claude Agent SDK** (which runs the real
`claude` CLI in headless stream-json mode), keeps one process alive per chat,
feeds your messages on stdin, and renders the typed event stream back — a
Claude-Code-style chat where code and conversation stay visually separated,
which is the whole point.

## Features

### Reading what it is doing

- **Chat / code split** — prose in the stream; tool calls as collapsible change
  cards. Click **see Changes** on any edit to open the drawer focused on *just
  that file*, or switch to the live **Git diff** tab for the whole working tree.
- **Streaming answers** — replies appear as they are written. The CLI emits in
  bursts, so the text is played out of a small jitter buffer at a steady rate
  instead of arriving a sentence and a half at a time; formatting and math are
  typeset once, when the block completes, so nothing reflows mid-sentence.
  Scrolling up while the answer is being written keeps the view where you put
  it — the tail is followed until you leave it, and again once you come back. A
  `↓` button at the bottom right of the chat, shown whenever the view is off the
  tail, jumps to the latest message and resumes following.
- **Folded tool runs** — consecutive calls to the *same* tool collapse into one
  row (`▶ Bash ×5  ruff check .  1 failed`) that expands to the individual
  cards. Anything in between — an answer, an edit, an approval — ends the run.
- **Delegated work** — when the agent hands a task to a subagent (the `Agent`
  tool), everything the subagent says and does stays inside that one card,
  collapsed, with its tool-call count and token usage in the header; the report
  it returns is the card's result.
- **Plan dock** — a task list pinned above the chat with the current step, a
  progress count, and a fold to just the active task. A finished plan retires
  two seconds after its last step, or at once with its `read` button, and a
  plan once retired does not come back on a switch or a reload. The console
  supplies the tool behind it to the session and asks for a plan only for work
  with several distinct steps; a turn that ends normally closes whatever the
  plan still has open.
- **Plan history** — the dock's `history` button, or the `☑ plans` pill at the
  top right, lists every plan of the session, newest first, with its steps, start time, duration and a one- or
  two-sentence summary written by a cheap model once the turn that finished the
  plan ends. The list is kept per session and survives a resume; a resumed
  session also gets the plans in its transcript from before the console kept a
  history, and their summaries are written when the panel is opened. Each entry
  has a `✕` and a checkbox; `Select all` and `Delete selected` remove several at
  once. A deleted plan stays deleted across a resume.
- **LaTeX rendering** — `$…$`, `$$…$$`, `\(…\)`, `\[…\]` in replies render with
  KaTeX (vendored, offline).
- **Clickable file paths** — a path in a reply (`~/work/lattice-qmc/observables.py`)
  renders as a link; clicking it opens **that file in a web file manager** in a new
  tab, so you can look at what the agent is talking about without leaving the
  conversation. Point `CLAUDE_CONSOLE_WEBFM_URL` at yours (defaults to this host
  on `:7701`); markdown links to local paths work the same way.

### Working across sessions

- **Session tabs** — the sessions you have open, above the chat. Switching is a
  swap, not a reload: the rendered view is kept and the server sends only what
  happened while you were away. **Closing a tab never ends the session** — the
  sidebar's LIVE list is what is running, the tabs are what you have open. Drag
  a tab to reorder the strip (on a phone, hold it first; a quick swipe scrolls
  the strip). The order is saved with the tabs.
- **Multi-session sidebar** — projects grouped by folder, in Live / Favorites /
  Recent sections (each collapsible); a folder row expands to the sessions
  whose working directory is that folder. A project's own button opens
  a new session there, browses the folder, favorites or renames it, or opens
  **Manage sessions** to clean up several at once; a session's `⋮` covers
  configure, rename, export and end (or delete to trash, for one on disk). Drag
  the right edge to resize, double-click to reset.
- **Resumable sessions** — reopen a project and pick up a previous Claude Code
  conversation (transcripts under `~/.claude/projects` restore history and the
  right `cwd`).
- **Full-text history search** (`⌘/Ctrl+K`) — search everything you and Claude
  ever said, plus the files and commands it touched, scoped to all history / this
  folder / this conversation. Results open in a read-only viewer, so finding
  something never disturbs the session you are in. LaTeX in the results and in
  the viewer is typeset; a match inside a formula marks the whole formula, and
  tool output stays literal.
- **Export / import** — take a conversation to another machine as `.jsonl`, or
  choose a destination folder and import one here; **Import folder** pins a
  directory as a project before it has any history.
- **Session recap** — come back to a session that has been idle and it opens with
  a short summary of where it was left.
- **Per-session drafts** — half-typed messages stay with their session and
  survive a reload.

### Talking to it

- **Interactive round-trips** — per-action approval with three choices
  (**Approve** / **Approve & don't ask again this session** / **Deny**) in 🔐
  Approve mode, plus in-browser **AskUserQuestion** cards; your pick is fed back
  to the agent. Sessions start in ⏩ Full auto unless another mode is chosen,
  and the chosen mode is the one in effect. An approval card shows the CLI's
  reason for asking, which is the only explanation when it asks in a mode that
  normally does not.
- **Message queue** — type while the agent is busy and the message queues, then
  sends when the turn ends. Click a queued chip (or press ↑) to withdraw it back
  into the editor, or hit its **⚡** to steer it into the *running* turn. Steering
  is per-message and opt-in because the CLI shows a mid-turn message to the model
  as a reminder to carry on with what it was doing — it is delivered reliably,
  but a visible reply is not guaranteed.
- **Attachments** — 📎 in the composer, or drag-and-drop, or paste. Images go as
  multimodal blocks; other files are **saved into the session's working
  directory** and named in the message, so Claude can read, edit and *run* them
  rather than only look at them. Works from a phone, which pasting did not.
- **Images in the conversation** — each image you send is also kept as a file in
  a private temporary folder (`/tmp/claude-console-<uid>/images`), and the message
  tells Claude its path. The conversation shows it as a thumbnail, also after a
  reload, with **Save** (into the session's working directory) and **Save as…**
  (any folder in your home folder, with a name of your choice); a save never
  replaces an existing file. The temporary copy does not survive a restart of
  the computer, and the thumbnail then says so.
- **Notifications for a phone** — `GET /api/notify/stream` is a server-sent
  event stream across all live sessions: a snapshot on connect, then each
  approval or question that starts waiting, each that is answered (anywhere),
  each finished turn with the start of the reply, and each session that ends.
  `POST /api/notify/approve` (`{"session", "aid", "allow", "always"}`) answers an
  approval without opening the session. The Android app turns these into
  notifications with Approve / Deny buttons.
- **Voice input** — a 🎙 button beside 📎 (or **Alt+M**) records, transcribes on
  *your* machine with [faster-whisper](https://github.com/SYSTRAN/faster-whisper)
  or on another machine that keeps a speech model loaded,
  and shows a revisable draft while you speak. Pressing the mic prewarms the model;
  confirmed words and punctuation stay fixed, the latest 10 Chinese characters
  remain revisable, and stopping re-runs the whole recording to correct whatever
  was not confirmed yet. It never sends by itself,
  and the text stays with the session that started the recording.
- **Punctuation from the pauses you actually made** — speech models return a wall
  of words, which is unreadable in Chinese and tiring in English. Silences are
  measured against the word timings and become punctuation: half a second is a
  comma, over a second is a full stop, and the ending takes a question mark when
  the wording asks something (`是否…`, `…吗`, `what/why/how…`). Both thresholds
  are yours to set, because where a comma belongs is a matter of ear.
- **Chinese comes out Simplified** — optionally normalized with OpenCC, which
  converts vocabulary and not just characters (`軟體` → `软件`, `記憶體` → `内存`),
  so Taiwanese dictation does not leave Taiwanese terms in the text.
- **Model** — a `🤖` pill showing the live session's model, one click to change
  it (from the next turn, nothing relaunches). A single message can go to another
  model: start it with `@haiku …`, `@sonnet …`, `@opus …` or `@fable …` and that
  turn runs there, then the session switches back. Typing `@` opens a list of
  what each token resolves to (↑/↓ pick, Tab or Enter insert, Esc closes). Any
  fragment of a model id works and is matched against the live list, newest
  first; the long-window `[1m]` variant is used when one exists, and an exact id
  is used as typed. A token that names no model is sent as ordinary text, with a
  note saying so.
- **Thinking effort** — a `🧠` pill (low / medium / high / xhigh / max) to set
  reasoning depth, switchable on the fly without relaunching the session. A
  sixth setting,
  **ultracode**, runs at xhigh and opts every message into multi-agent
  orchestration.
- **New session dialog** — browse or create a folder, then choose the model,
  thinking effort and permission mode (⏩ Full auto, the default / ⚡ Auto-accept / 🔐 Approve / 📋 Plan).
  The model list can be refreshed inside the dialog. Active-session controls remain available.

### Everything else

- **13 color themes** — light & dark (Dark, Dracula, Nord, Tokyo Night,
  Catppuccin, Gruvbox, Light, Solarized Light, Rosé Pine Dawn, One Light, Ayu
  Light, …), switchable from the sidebar; choice persists per device.
- **At-a-glance status** — context-window, rolling 5-hour and weekly usage meters
  in the header, including any per-model weekly quota the account carries (Fable
  on Max), each with its reset time on hover; a floating status pill (ready / working timer / live token counts) plus
  the model and effort pills above a full-width composer. Past 60% of the window
  the empty composer reads *update your snapshot*.

## Run

```bash
# any python with tornado + claude-agent-sdk works; defaults to localhost-only (safe):
python claude_console.py

# to reach it from your phone/iPad on the LAN, expose it WITH auth:
CLAUDE_CONSOLE_BIND=0.0.0.0 CLAUDE_CONSOLE_AUTH=me:secret python claude_console.py
```

Open `http://<host>:7703`, pick a project dir, and start chatting; the change
cards and the **Git diff** drawer update live as the agent works.

Voice input is optional and off by default. To turn it on, install
`faster-whisper` (and `opencc`, if you want Chinese script conversion) into
*some* Python — it does not have to be the one running the console — and point
`CLAUDE_CONSOLE_TRANSCRIBE_PYTHON` at it:

```bash
CLAUDE_CONSOLE_TRANSCRIBE=1 \
CLAUDE_CONSOLE_TRANSCRIBE_PYTHON=/path/to/whisper-env/bin/python \
CLAUDE_CONSOLE_TRANSCRIBE_MODEL=~/models/faster-whisper-large-v3-turbo \
CLAUDE_CONSOLE_TRANSCRIBE_DEVICE=cuda CLAUDE_CONSOLE_TRANSCRIBE_COMPUTE_TYPE=float16 \
CLAUDE_CONSOLE_TRANSCRIBE_PAUSE_PUNCTUATION=1 \
CLAUDE_CONSOLE_TRANSCRIBE_CHINESE_CONVERSION=tw2sp \
python claude_console.py
```

`PAUSE_PUNCTUATION` and `CHINESE_CONVERSION` are both off by default and both
worth turning on. Without the first, a paragraph of dictation arrives as one
unbroken run of words.

The model can also live on another machine that keeps it loaded. Set
`CLAUDE_CONSOLE_TRANSCRIBE_REMOTE` to a command that reaches it, typically
`ssh <host> <relay>`; it then replaces the local worker, and needs only `ffmpeg`
on the console's machine. The console writes `{"kind": "asr"}` and waits for
`{"ready": true}`. Each request is one JSON line, `{"fmt": "flac", "bytes": N,
"context": "..."}`, followed by N bytes of 16 kHz mono 16-bit FLAC, and is answered
by one line, `{"text": "...", "language": "..."}` or `{"error": "..."}`. A
`{"ping": 1}` line every ten seconds keeps the connection while you dictate, and
it closes after `CLAUDE_CONSOLE_TRANSCRIBE_IDLE_SEC`. A model that punctuates by
itself, such as Qwen3-ASR, needs neither `PAUSE_PUNCTUATION` nor
`CHINESE_CONVERSION`, and those apply to the local worker only.

Browsers only expose a microphone in a secure context, so serve the console over
HTTPS (a reverse proxy, or `tailscale serve`) unless you open it on `localhost`.

| Env | Default | Meaning |
|---|---|---|
| `CLAUDE_CONSOLE_PORT` | `7703` | listen port |
| `CLAUDE_CONSOLE_BIND` | `127.0.0.1` | bind address; set `0.0.0.0` for LAN access |
| `CLAUDE_CONSOLE_AUTH` | *(disabled)* | optional HTTP Basic Auth `user:pass` |
| `CLAUDE_CONSOLE_WEBFM_URL` | this host on `:7701` | web file manager to open clicked file paths in |
| `CLAUDE_CONSOLE_CLAUDE` | first on `PATH` | which `claude` binary to drive; point it at a specific version to pin one |
| `CLAUDE_CONSOLE_RECAP` | `1` | summarize a session you return to after it has been idle |
| `CLAUDE_CONSOLE_RECAP_IDLE_SEC` | `300` | idle seconds before a recap is due |
| `CLAUDE_CONSOLE_RECAP_MODEL` | `haiku` | model that writes the recap |
| `CLAUDE_CONSOLE_PLAN_SUMMARY_MODEL` | `haiku` | model that writes the plan-history summaries |
| `CLAUDE_CONSOLE_PLAN_LANG` | *(unset)* | language of plan step titles and history summaries, as a name for the prompts (`Chinese`); unset, both follow the language you write in |
| `CLAUDE_CONSOLE_MAX_BUFFER_MB` | `64` | ceiling on one CLI message |
| `CLAUDE_CONSOLE_IMAGE_DIR` | `/tmp/claude-console-<uid>/images` | where copies of sent images are kept for the conversation's thumbnails and Save / Save as |
| `CLAUDE_CONSOLE_IMPORT_MAX_MB` | `1024` | ceiling on an imported transcript |
| `CLAUDE_CONSOLE_TRANSCRIBE` | `0` | enable local voice input |
| `CLAUDE_CONSOLE_TRANSCRIBE_PYTHON` | current python | interpreter that has `faster-whisper` |
| `CLAUDE_CONSOLE_TRANSCRIBE_MODEL` | *(unset)* | CTranslate2 model directory, or a model name to fetch |
| `CLAUDE_CONSOLE_TRANSCRIBE_DEVICE` | `auto` | `cpu` / `cuda` |
| `CLAUDE_CONSOLE_TRANSCRIBE_DEVICE_INDEX` | `0` | which GPU |
| `CLAUDE_CONSOLE_TRANSCRIBE_COMPUTE_TYPE` | `default` | e.g. `float16`, `int8` |
| `CLAUDE_CONSOLE_TRANSCRIBE_LANGUAGE` | *(auto-detect)* | pin the spoken language |
| `CLAUDE_CONSOLE_TRANSCRIBE_CHINESE_CONVERSION` | `none` | OpenCC: `t2s` converts characters, `tw2sp` also converts Taiwanese vocabulary |
| `CLAUDE_CONSOLE_TRANSCRIBE_PAUSE_PUNCTUATION` | `0` | punctuate from the pauses, and close the sentence |
| `CLAUDE_CONSOLE_TRANSCRIBE_COMMA_GAP` | `0.5` | silence (seconds) that reads as a comma |
| `CLAUDE_CONSOLE_TRANSCRIBE_PERIOD_GAP` | `1.2` | silence (seconds) that reads as a full stop |
| `CLAUDE_CONSOLE_TRANSCRIBE_LD_LIBRARY_PATH` | *(unset)* | extra CUDA library dirs for the worker |
| `CLAUDE_CONSOLE_TRANSCRIBE_MAX_MB` | `16` | audio upload ceiling |
| `CLAUDE_CONSOLE_TRANSCRIBE_MAX_SEC` | `120` | recording length cap |
| `CLAUDE_CONSOLE_TRANSCRIBE_TIMEOUT_SEC` | `180` | give up on a transcription after this |
| `CLAUDE_CONSOLE_TRANSCRIBE_IDLE_SEC` | `600` | worker exits after this, releasing the model's memory; a remote connection closes after this |
| `CLAUDE_CONSOLE_TRANSCRIBE_REMOTE` | *(unset)* | command that reaches a model on another machine; replaces the local worker |
| `CLAUDE_CONSOLE_TRANSCRIBE_CONTEXT` | *(unset)* | words the remote model should expect, sent with each request |
| `CLAUDE_CONSOLE_TRANSCRIBE_FFMPEG` | first on `PATH` | `ffmpeg` that converts the recording for the remote model |

The legacy `AGENTLENS_*` names are still honored as a fallback.

> [!WARNING]
> This **drives Claude Code** in the directory you pick — it can read/write files
> and run commands there — and it exposes your **chat history and source diffs**.
> Do not expose it on an untrusted network without `CLAUDE_CONSOLE_AUTH` and a
> trusted boundary (VPN / SSH tunnel / reverse proxy + TLS).

## Notes

- Intentionally a single `claude_console.py` with inline HTML/CSS/JS — **no build step**.
  The one exception is `faster_whisper_worker.py`, because speech models want a
  different Python than the console runs in, and a separate process is also how
  the model's memory gets released — it exits on its own when idle.
- [KaTeX](https://katex.org/) is vendored under `static/katex/` (MIT) for offline
  math rendering.
- Non-image attachments are written to `.claude-console/uploads/` inside the
  session's working directory, so the agent can open and run them. That folder
  ignores itself in git (it ships a `.gitignore` containing `*`), and nothing is
  written for a queued message you withdraw before it sends.
- Every live preview and final correction uses a `0600` temp file that is deleted
  when the request finishes, including when it fails. The transcript reaches
  Claude only when you send the draft you edited.
- Voice icon from Microsoft's [Fluent Emoji](https://github.com/microsoft/fluentui-emoji)
  (MIT), under `static/icons/`.
- The page carries the hash of the code it was served with and reloads itself
  when the server reports a different one; drafts are saved first.
- Very long conversations keep a bounded window of *rendered* messages per tab;
  older ones fold into a marker that links to the history search. Nothing is
  deleted — the full transcript stays on disk under `~/.claude/projects`.

## License

[MIT](LICENSE) © 2026 BoZhen
