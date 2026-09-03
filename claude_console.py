#!/usr/bin/env python3
"""Claude Console — an interactive browser GUI that drives Claude Code while
keeping the *discussion* and the *code/file changes* visually separated.

It spawns the real `claude` CLI (via the Claude Agent SDK) per chat, renders the
typed event stream, shows code/file changes as collapsed cards plus a live
`git diff` drawer, and supports per-action approval and in-band AskUserQuestion.

Env (legacy AGENTLENS_* names still accepted as a fallback):
  CLAUDE_CONSOLE_PORT   listen port (default 7703)
  CLAUDE_CONSOLE_BIND   bind address (default 127.0.0.1; set 0.0.0.0 for LAN)
  CLAUDE_CONSOLE_AUTH   optional HTTP Basic Auth "user:pass" (default disabled)
  CLAUDE_CONSOLE_WEBFM_URL  web-file-manager base URL — makes file paths in chat
                        clickable (opens the file there). Default: this host on :7701
  CLAUDE_CONSOLE_TRANSCRIBE  set to 1 to enable local voice input (see the README
                        for the full CLAUDE_CONSOLE_TRANSCRIBE_* set)
"""

import asyncio
import base64
import glob
import hashlib
import io
import json
import os
import re
import secrets
import select
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request

import tornado.ioloop
import tornado.iostream
import tornado.process
import tornado.web
import tornado.websocket

try:
    from claude_agent_sdk import (
        query, ClaudeSDKClient, ClaudeAgentOptions, AssistantMessage, UserMessage,
        SystemMessage, ResultMessage, StreamEvent, TextBlock, ThinkingBlock,
        ToolUseBlock, ToolResultBlock, PermissionResultAllow, PermissionResultDeny,
        tool, create_sdk_mcp_server, HookMatcher)
    HAVE_SDK = True
except Exception:
    HAVE_SDK = False

def _env(name, default=""):
    """Read CLAUDE_CONSOLE_<name>, falling back to the legacy AGENTLENS_<name>."""
    return (os.environ.get("CLAUDE_CONSOLE_" + name)
            or os.environ.get("AGENTLENS_" + name) or default)

PORT = int(_env("PORT", "7703"))
AUTH = _env("AUTH", "")
# Base URL of a web-file-manager so file paths in chat become links that open the
# file there (new tab). Empty → the frontend falls back to this host on :7701.
WEBFM_URL = _env("WEBFM_URL", "").rstrip("/")
# Default to loopback: this serves ALL your agent transcripts + home-wide git
# diffs, so it must not land on the network by accident. Set CLAUDE_CONSOLE_BIND=
# 0.0.0.0 (ideally with CLAUDE_CONSOLE_AUTH) to reach it from another device.
BIND = _env("BIND", "127.0.0.1")
# The SDK aborts the whole stream if one CLI stdout NDJSON line exceeds this
# (SDK default 1 MB) — a large tool result / diff / image message can. Raise it
# generously so a big message doesn't kill the session. Override via env.
MAX_BUFFER = int(_env("MAX_BUFFER_MB", "64") or "64") * 1024 * 1024
# upload ceiling for /api/import; tornado's 100 MB default rejects big transcripts
IMPORT_MAX = int(_env("IMPORT_MAX_MB", "1024") or "1024") * 1024 * 1024
HOME = os.path.expanduser("~")
CLAUDE_ROOT = os.path.join(HOME, ".claude", "projects")
CODEX_ROOT = os.path.join(HOME, ".codex", "sessions")
# Interactive console drives the real `claude` CLI in headless stream-json mode.
CLAUDE_BIN = (_env("CLAUDE") or shutil.which("claude")
              or os.path.expanduser("~/.local/bin/claude"))

CAP = 12000          # cap per long string field sent to the browser
RESULT_CAP = 6000    # cap per tool_result body
POLL_MS = 800        # transcript tail interval

# ── Session recap ("away summary") — a one-line Haiku summary of where an idle
# session stands, shown when you return to it, like the CLI's awaySummary feature.
RECAP_ENABLED  = (_env("RECAP", "1") or "1").lower() not in ("0", "false", "no", "off")
RECAP_IDLE_SEC = int(_env("RECAP_IDLE_SEC", "300") or "300")   # idle seconds before a recap is due
RECAP_MODEL    = _env("RECAP_MODEL", "haiku") or "haiku"       # fast + cheap; overridable
RECAP_SYS = ("You write a one-line \"recap\" for a developer who stepped away from an "
             "in-progress coding session and just came back. Given the transcript, reply with a "
             "SINGLE plain-text line (no markdown, no quotes, under 160 characters) stating what "
             "is being worked on and the immediate next step. "
             "Example: Working on the auth refactor — just split the login handler; next: wire up the session cookie.")
# turn-complete verbs — the CLI's curated past-tense set (independent of the live
# spinner's present-participle word); one is stamped onto each finished turn.
DONE_PAST = ["Baked", "Brewed", "Churned", "Cogitated", "Cooked", "Crunched", "Sautéed", "Worked"]


# ───────────────────────── normalization ─────────────────────────
# Both adapters emit the same event shape so the frontend is source-agnostic:
#   {kind, ts, id, ...}
#   kind="user_text"|"assistant_text"   -> Discussion   (+ role, text)
#   kind="thinking"                     -> Discussion (muted, collapsible) (+ text)
#   kind="tool_use"                     -> Activity     (+ tool, input, toolId)
#   kind="tool_result"                  -> Activity (attached by toolId) (+ content, isError)

def _txt(x):
    """Flatten arbitrary content (str | list of blocks | dict) to text."""
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, list):
        out = []
        for b in x:
            if isinstance(b, dict):
                if "text" in b:
                    out.append(str(b.get("text", "")))
                else:
                    out.append(json.dumps(b, ensure_ascii=False))
            else:
                out.append(str(b))
        return "\n".join(out)
    if isinstance(x, dict):
        if "text" in x:
            return str(x.get("text", ""))
        return json.dumps(x, ensure_ascii=False)
    return str(x)


def _cap(s, n=CAP):
    s = s or ""
    if len(s) > n:
        return s[:n] + "\n…[truncated %d chars]" % (len(s) - n)
    return s


def _cap_input(inp):
    if isinstance(inp, dict):
        return {k: (_cap(v) if isinstance(v, str) else v) for k, v in inp.items()}
    if isinstance(inp, str):
        return _cap(inp)
    return inp


_PLUMBING_TAGS = ("<command-name>", "<command-message>", "<command-args>",
                  "<local-command-stdout>", "<local-command-caveat>", "<system-reminder>")
def _is_plumbing(s):
    """CLI-injected user content (slash-command markup, local-command stdout/caveats,
    system reminders) — recorded in transcripts but never real chat to display."""
    return isinstance(s, str) and s.lstrip().startswith(_PLUMBING_TAGS)


_INJECTED_RE = re.compile(r"\s*<task-notification>.*?</task-notification>\s*", re.S)
def _strip_injected(s):
    """Remove harness-appended blocks (e.g. a background-task completion notice) that get
    tacked onto an otherwise-real user message in the transcript. On resume-from-disk we
    then render the user's actual text only, not the plumbing block."""
    return _INJECTED_RE.sub("", s) if isinstance(s, str) else s


# The plan the console pins above the chat. The CLI used to expose a task list
# (TaskCreate/TaskUpdate); 2.1.258 dropped it, so the console now brings its own
# tool into the session over the SDK's in-process MCP server. One tool, one
# shape: the FULL list of steps on every call, so a plan event is always a
# snapshot and there is no id bookkeeping. The old CLI traffic is still folded,
# for transcripts that carry it.
#
# A tool the model can see is not a tool the model uses. Measured on one
# multi-step task: the description alone, zero calls; the protocol appended to
# the system prompt, the model quotes it back when asked and still zero calls;
# the same text beside the prompt as a UserPromptSubmit reminder, a plan in one
# run of two; the same text again as a PostToolUse reminder on the FIRST tool
# call of the turn, and the model stops and writes the plan. That is the moment
# it is deciding how many steps there are, so that is where the reminder rides.
# Keeping the plan CURRENT takes two more: one after a run of tool calls with a
# step still open, and one at Stop, where an open step blocks the stop once so
# the panel never lingers half-done.
PLAN_TOOL = "mcp__console__plan"
PLAN_PROMPT = (
    "# Progress panel\n"
    "The console pins a progress panel above the chat, fed by the `mcp__console__plan` "
    "tool. Whenever a request takes more than one step, call it first with the full "
    "list of steps (all pending), again as each step starts (in_progress) and finishes "
    "(completed), and again if the steps change. Always send the complete list. Keep "
    "exactly one step in_progress while you work. A single-step request needs no plan.")
PLAN_NUDGE_EVERY = 4     # tool calls with a step still open before a reminder rides along
PLAN_SCHEMA = {
    "type": "object", "required": ["steps"],
    "properties": {"steps": {"type": "array", "items": {
        "type": "object", "required": ["title", "status"],
        "properties": {"title": {"type": "string"},
                       "status": {"type": "string", "enum": ["pending", "in_progress", "completed"]}}}}}}
# the CLI's own task list, for transcripts recorded while it still had one:
# TaskCreate {subject, description, activeForm} then TaskUpdate {taskId, status}.
# TaskCreate never reports the id it was given; that comes back only in the tool
# RESULT text, "Task #3 created successfully: Fix lint".
TASK_ID_RE = re.compile(r"[Tt]ask #(\d+)")
TASK_FIELDS = ("subject", "description", "activeForm", "status")


# Attachments land on disk under the session's own working directory rather than
# being inlined into the message. Inlining is what a console without tools has to
# do; Claude has Read, Edit and Bash, so a file that exists is a file it can patch
# and run — and the phone case this exists for ("here is my script, fix it") is
# exactly the case where handing back a whole rewritten copy is the wrong answer.
UPLOAD_DIR = ".claude-console/uploads"
MAX_UPLOADS = 10
MAX_UPLOAD_BYTES = 2 * 1024 * 1024        # per file
MAX_UPLOAD_TOTAL = 10 * 1024 * 1024       # per message
_UNSAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]")


def _human_bytes(n):
    return "%.1f MB" % (n / 1048576.0) if n >= 1048576 else (
        "%.1f KB" % (n / 1024.0) if n >= 1024 else "%d B" % n)


def safe_upload_name(name):
    """A name that cannot escape the uploads directory, whatever the client sent.

    basename() alone is not enough: a leading dot would hide the file, and the
    stray characters a phone puts in a filename make later shell use painful."""
    name = os.path.basename((name or "").replace("\\", "/"))
    name = _UNSAFE_NAME_RE.sub("_", name).lstrip(".")
    return (name or "upload")[:100]


def save_uploads(cwd, files):
    """Write attachments under cwd. Returns (saved, errors), saved as [(rel, size)].

    Refusals are reported, never silent — an attachment that quietly vanished
    would have the user asking about a file the model never received."""
    saved, errors = [], []
    if not files:
        return saved, errors
    root = os.path.join(cwd, *UPLOAD_DIR.split("/"))
    total = 0
    if len(files) > MAX_UPLOADS:
        errors.append("only the first %d files were attached" % MAX_UPLOADS)
    for f in files[:MAX_UPLOADS]:
        raw_name = (f or {}).get("name") or "upload"
        try:
            raw = base64.b64decode(((f or {}).get("data") or "").encode())
        except Exception:
            errors.append("%s: unreadable upload" % raw_name)
            continue
        if len(raw) > MAX_UPLOAD_BYTES:
            errors.append("%s: %s is over the %s limit"
                          % (raw_name, _human_bytes(len(raw)),
                             _human_bytes(MAX_UPLOAD_BYTES)))
            continue
        if total + len(raw) > MAX_UPLOAD_TOTAL:
            errors.append("%s: message would exceed the %s total"
                          % (raw_name, _human_bytes(MAX_UPLOAD_TOTAL)))
            break
        try:
            os.makedirs(root, exist_ok=True)
            # the console's own directory ignores itself, so an upload can never
            # ride into a commit on the back of `git add .`
            ign = os.path.join(cwd, ".claude-console", ".gitignore")
            if not os.path.exists(ign):
                with open(ign, "w") as fh:
                    fh.write("*\n")
            name = safe_upload_name(raw_name)
            stem, ext = os.path.splitext(name)
            path, n = os.path.join(root, name), 2
            while os.path.exists(path):    # never clobber an earlier upload
                name = "%s-%d%s" % (stem, n, ext)
                path = os.path.join(root, name)
                n += 1
            with open(path, "wb") as fh:
                fh.write(raw)
            os.chmod(path, 0o600)
        except Exception as ex:
            errors.append("%s: %s" % (raw_name, ex))
            continue
        total += len(raw)
        saved.append((UPLOAD_DIR + "/" + name, len(raw)))
    return saved, errors


def uploads_note(saved):
    """The line appended to the outgoing message so the model knows what arrived.
    Viewers never see it — they see the file chips instead."""
    if not saved:
        return ""
    lines = ["", "(%d file%s attached, saved under this session's working directory:"
             % (len(saved), "" if len(saved) == 1 else "s")]
    lines += ["    %s  —  %s" % (rel, _human_bytes(n)) for rel, n in saved]
    lines.append("You can read, edit and run them in place.)")
    return "\n".join(lines)


def parse_claude(rec, idx):
    t = rec.get("type")
    base = {"ts": rec.get("timestamp"), "id": rec.get("uuid") or ("L%d" % idx)}
    msg = rec.get("message") or {}
    evs = []
    if t == "assistant":
        content = msg.get("content")
        if isinstance(content, str):
            if content.strip():
                evs.append({**base, "kind": "assistant_text", "role": "assistant", "text": _cap(content)})
        elif isinstance(content, list):
            for b in content:
                if not isinstance(b, dict):
                    continue
                bt = b.get("type")
                if bt == "text" and b.get("text", "").strip():
                    evs.append({**base, "kind": "assistant_text", "role": "assistant", "text": _cap(b["text"])})
                elif bt == "thinking":
                    th = b.get("thinking", "")
                    if th.strip():
                        evs.append({**base, "kind": "thinking", "text": _cap(th)})
                elif bt == "tool_use":
                    evs.append({**base, "kind": "tool_use", "tool": b.get("name", ""),
                                "input": _cap_input(b.get("input")), "toolId": b.get("id", "")})
    elif t == "user":
        if rec.get("isMeta") or rec.get("isCompactSummary"):
            return []          # harness/CLI-injected, never real chat: "Continue from where
                               # you left off." + tool-error retries + caveats (isMeta), and
                               # the post-compaction summary continuation (isCompactSummary)
        content = msg.get("content")
        if isinstance(content, str):
            content = _strip_injected(content)
            if content.strip() and not _is_plumbing(content):
                evs.append({**base, "kind": "user_text", "role": "user", "text": _cap(content)})
        elif isinstance(content, list):
            for b in content:
                if not isinstance(b, dict):
                    continue
                bt = b.get("type")
                if bt == "text":
                    txt = _strip_injected(b.get("text", ""))
                    if txt.strip() and not _is_plumbing(txt):
                        evs.append({**base, "kind": "user_text", "role": "user", "text": _cap(txt)})
                elif bt == "tool_result":
                    evs.append({**base, "kind": "tool_result", "toolId": b.get("tool_use_id", ""),
                                "content": _cap(_txt(b.get("content")), RESULT_CAP),
                                "isError": bool(b.get("is_error"))})
    return evs


def parse_codex(rec, idx):
    base = {"ts": rec.get("timestamp"), "id": "L%d" % idx}
    if rec.get("type") != "response_item":
        return []
    p = rec.get("payload") or {}
    pt = p.get("type")
    evs = []
    if pt == "message":
        role = p.get("role", "assistant")
        text = _txt(p.get("content"))
        if text.strip():
            kind = "user_text" if role == "user" else "assistant_text"
            evs.append({**base, "kind": kind, "role": role, "text": _cap(text)})
    elif pt == "reasoning":
        text = _txt(p.get("summary") or p.get("content"))
        if text.strip():
            evs.append({**base, "kind": "thinking", "text": _cap(text)})
    elif pt == "function_call":
        args = p.get("arguments")
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except Exception:
                pass
        evs.append({**base, "kind": "tool_use", "tool": p.get("name", ""),
                    "input": _cap_input(args), "toolId": p.get("call_id", "")})
    elif pt == "function_call_output":
        out = p.get("output")
        if isinstance(out, str):
            try:
                j = json.loads(out)
                if isinstance(j, dict) and "output" in j:
                    out = j["output"]
            except Exception:
                pass
        evs.append({**base, "kind": "tool_result", "toolId": p.get("call_id", ""),
                    "content": _cap(_txt(out), RESULT_CAP), "isError": False})
    return evs


def parse_line(line, idx, source):
    try:
        rec = json.loads(line)
    except Exception:
        return []
    try:
        return parse_claude(rec, idx) if source == "claude" else parse_codex(rec, idx)
    except Exception:
        return []


def normalize_cc(rec):
    """Normalize one `claude --output-format stream-json` event for the console UI."""
    t = rec.get("type")
    evs = []
    if t == "system":
        if rec.get("subtype") == "init":
            evs.append({"kind": "ready", "session_id": rec.get("session_id"),
                        "model": rec.get("model"), "cwd": rec.get("cwd"),
                        "tools": rec.get("tools"), "permissionMode": rec.get("permissionMode")})
    elif t == "assistant":
        for b in (rec.get("message") or {}).get("content") or []:
            if not isinstance(b, dict):
                continue
            bt = b.get("type")
            if bt == "text" and b.get("text", "").strip():
                evs.append({"kind": "assistant_text", "text": _cap(b["text"])})
            elif bt == "thinking" and b.get("thinking", "").strip():
                evs.append({"kind": "thinking", "text": _cap(b["thinking"])})
            elif bt == "tool_use":
                evs.append({"kind": "tool_use", "tool": b.get("name", ""),
                            "input": _cap_input(b.get("input")), "toolId": b.get("id", "")})
    elif t == "user":
        for b in (rec.get("message") or {}).get("content") or []:
            if isinstance(b, dict) and b.get("type") == "tool_result":
                evs.append({"kind": "tool_result", "toolId": b.get("tool_use_id", ""),
                            "content": _cap(_txt(b.get("content")), RESULT_CAP),
                            "isError": bool(b.get("is_error"))})
    elif t == "result":
        evs.append({"kind": "turn_done", "subtype": rec.get("subtype"),
                    "isError": bool(rec.get("is_error")), "numTurns": rec.get("num_turns"),
                    "cost": rec.get("total_cost_usd")})
    elif t == "rate_limit_event":
        evs.append({"kind": "notice", "text": "rate limit update"})
    return evs


# ───────────────────────── session discovery ─────────────────────────
def _peek_claude(path):
    cwd, branch, title = "", "", ""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                if i > 150:
                    break
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if not cwd:
                    cwd = rec.get("cwd", "") or cwd
                    branch = rec.get("gitBranch", "") or branch
                if not title and rec.get("type") == "user" and not rec.get("isCompactSummary"):
                    msg = (rec.get("message") or {}).get("content")
                    s = msg if isinstance(msg, str) else _txt(msg)
                    s = (s or "").strip()
                    if s.startswith("Transcript so far:") and s.endswith("Recap:"):
                        return "__recap__", "", ""   # claude-console recap-query artifact → hide it
                    if s and not s.startswith("<") and "tool_result" not in s[:40]:
                        title = s.replace("\n", " ")[:100]
                if cwd and title:
                    break
    except Exception:
        pass
    return cwd, branch, title


def _peek_codex(path):
    cwd, branch, title = "", "", ""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                if i > 150:
                    break
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                p = rec.get("payload") or {}
                if rec.get("type") == "session_meta":
                    cwd = p.get("cwd", "") or cwd
                    g = p.get("git") or {}
                    branch = g.get("branch", "") or branch
                if not title and rec.get("type") == "response_item" and p.get("type") == "message" \
                        and p.get("role") == "user":
                    s = _txt(p.get("content")).strip()
                    if s and not s.startswith("<"):
                        title = s.replace("\n", " ")[:100]
                if cwd and title:
                    break
    except Exception:
        pass
    return cwd, branch, title


def list_sessions(limit=50):
    items = []
    claude_files = glob.glob(os.path.join(CLAUDE_ROOT, "*", "*.jsonl"))
    codex_files = glob.glob(os.path.join(CODEX_ROOT, "**", "*.jsonl"), recursive=True)
    paths = ([(p, "claude") for p in claude_files] +
             [(p, "codex") for p in codex_files])
    try:
        paths.sort(key=lambda pc: os.path.getmtime(pc[0]), reverse=True)
    except Exception:
        pass
    for path, source in paths[:limit]:
        try:
            st = os.stat(path)
        except OSError:
            continue
        cwd, branch, title = (_peek_claude if source == "claude" else _peek_codex)(path)
        if cwd == "__recap__":   # recap-query artifact, not a real session
            continue
        items.append({
            "id": path, "source": source, "cwd": cwd, "branch": branch,
            "title": title or os.path.basename(path),
            "mtime": st.st_mtime, "size": st.st_size,
        })
    return items


def list_projects():
    """Real project dirs for the console picker: recent session cwds (filtered).
    Excludes /tmp and runtime/cache dirs. Any other path can be typed in the
    picker's custom-path box."""
    junk = [os.path.realpath(p) for p in (
        "/tmp", os.path.join(HOME, ".cache"), os.path.join(HOME, ".claude-mem"),
        os.path.join(HOME, ".claude"), os.path.join(HOME, ".codex"),
        os.path.join(HOME, ".config"))]

    def is_junk(p):
        rp = os.path.realpath(p)
        return any(rp == j or rp.startswith(j + os.sep) for j in junk)

    out, seen = [], set()
    for s in list_sessions(60):
        cwd = s.get("cwd")
        if cwd and cwd not in seen and os.path.isdir(cwd) and not is_junk(cwd):
            seen.add(cwd)
            out.append({"path": cwd, "recent": True,
                        "git": os.path.isdir(os.path.join(cwd, ".git"))})
    return out


def _valid_cc(cc):
    return bool(cc) and 6 <= len(cc) <= 64 and all(c.isalnum() or c in "-_" for c in cc)


_JUNK_ROOTS = None
def _is_junk(p):
    global _JUNK_ROOTS
    if _JUNK_ROOTS is None:
        _JUNK_ROOTS = [os.path.realpath(x) for x in (
            "/tmp", os.path.join(HOME, ".cache"), os.path.join(HOME, ".claude-mem"),
            os.path.join(HOME, ".claude"), os.path.join(HOME, ".codex"),
            os.path.join(HOME, ".config"))]
    rp = os.path.realpath(p)
    return any(rp == j or rp.startswith(j + os.sep) for j in _JUNK_ROOTS)


def find_transcript(cc):
    """Locate claude's on-disk transcript for a session id (filename == session id)."""
    if not _valid_cc(cc):
        return None
    hits = glob.glob(os.path.join(CLAUDE_ROOT, "*", cc + ".jsonl"))
    return hits[0] if hits else None


def proj_folder(cwd):
    """The ~/.claude/projects/<folder> name the CLI derives from a working dir:
    every non-alphanumeric char becomes '-'. (Verified against 26 real transcripts,
    0 mismatches.) This matters for import: `claude --resume <id>` searches ONLY the
    folder matching the cwd it is launched from, so a transcript has to land here to
    be found. The cwd recorded *inside* the file is not used for lookup, which is why
    a transcript moves between machines and paths untouched."""
    return re.sub(r"[^A-Za-z0-9]", "-", os.path.abspath(cwd))


def transcript_session_id(body):
    """The session id a transcript claims for itself, read from the first line that
    carries one. Preferred over the uploaded filename (which a user may have renamed)
    — the file is written back as '<sessionId>.jsonl' so name and content agree."""
    for line in body[: 1 << 20].decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except Exception:
            continue
        if isinstance(d, dict) and d.get("sessionId"):
            return str(d["sessionId"])
    return ""


def trash_transcript(cc):
    """Move a claude session's on-disk transcript to the trash (reversible) — the
    sidebar 🗑 uses this to clean resumable sessions out of a folder. Prefers
    `gio trash`; falls back to an in-place rename if gio is unavailable."""
    if not _valid_cc(cc):
        return {"ok": False, "error": "invalid session id"}
    path = find_transcript(cc)
    if not path:
        return {"ok": False, "error": "transcript not found"}
    try:
        subprocess.run(["gio", "trash", path], check=True,
                       capture_output=True, timeout=10)
        return {"ok": True}
    except FileNotFoundError:
        pass  # gio not installed — fall through to the rename fallback
    except subprocess.CalledProcessError as ex:
        err = (ex.stderr or b"").decode("utf-8", "replace").strip()
        return {"ok": False, "error": err or "gio trash failed"}
    except Exception as ex:
        return {"ok": False, "error": str(ex)}
    try:
        os.rename(path, "%s.trashed-%d" % (path, int(time.time())))
        return {"ok": True}
    except Exception as ex:
        return {"ok": False, "error": str(ex)}


NAMES_FILE = os.path.join(HOME, ".claude", "console-names.json")
_names_cache = {"mtime": -1.0, "v": {}}

def load_names():
    """Custom session labels {claude_session_id: name} that override the auto
    title. Cached; invalidated by the file's mtime."""
    try:
        m = os.path.getmtime(NAMES_FILE)
    except OSError:
        _names_cache["mtime"], _names_cache["v"] = -1.0, {}
        return _names_cache["v"]
    if m != _names_cache["mtime"]:
        try:
            with open(NAMES_FILE, encoding="utf-8") as f:
                d = json.load(f)
            _names_cache["v"] = d if isinstance(d, dict) else {}
        except Exception:
            _names_cache["v"] = {}
        _names_cache["mtime"] = m
    return _names_cache["v"]


def set_name(cc, name):
    """Set (or, when name is blank, clear) the custom label for a session."""
    if not _valid_cc(cc):
        return False
    names = dict(load_names())
    name = (name or "").strip()[:120]
    if name:
        names[cc] = name
    else:
        names.pop(cc, None)
    try:
        os.makedirs(os.path.dirname(NAMES_FILE), exist_ok=True)
        tmp = NAMES_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(names, f, ensure_ascii=False)
        os.replace(tmp, NAMES_FILE)
        _names_cache["mtime"] = -1.0
        return True
    except Exception:
        return False


PREFS_FILE = os.path.join(HOME, ".claude", "console-prefs.json")
_prefs_cache = {"mtime": -1.0, "v": {}}

def load_prefs():
    """Per-session UI prefs {claude_session_id: {mode, model}} so a resumed
    session restores its own permission mode / model instead of reverting to the
    picker defaults. Cached; invalidated by mtime."""
    try:
        m = os.path.getmtime(PREFS_FILE)
    except OSError:
        _prefs_cache["mtime"], _prefs_cache["v"] = -1.0, {}
        return _prefs_cache["v"]
    if m != _prefs_cache["mtime"]:
        try:
            with open(PREFS_FILE, encoding="utf-8") as f:
                d = json.load(f)
            _prefs_cache["v"] = d if isinstance(d, dict) else {}
        except Exception:
            _prefs_cache["v"] = {}
        _prefs_cache["mtime"] = m
    return _prefs_cache["v"]


def save_pref(cc, mode=None, model=None, effort=None, fav=None):
    """Persist a session's mode/model/effort/favorite. No-op write when unchanged.
    fav: a dict {cwd,name,title,ts} stars the session; a falsy value unstars it."""
    if not _valid_cc(cc):
        return
    prefs = load_prefs()
    cur = dict(prefs.get(cc) or {})
    if mode is not None:
        cur["mode"] = mode
    if model is not None:
        cur["model"] = model
    if effort is not None:
        cur["effort"] = effort
    if fav is not None:
        if fav:
            cur["fav"] = fav
        else:
            cur.pop("fav", None)
    if cur == prefs.get(cc):
        return
    prefs = dict(prefs)
    if cur:
        prefs[cc] = cur
    else:
        prefs.pop(cc, None)   # don't leave an empty entry behind
    try:
        os.makedirs(os.path.dirname(PREFS_FILE), exist_ok=True)
        tmp = PREFS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(prefs, f, ensure_ascii=False)
        os.replace(tmp, PREFS_FILE)
        _prefs_cache["mtime"] = -1.0
    except Exception:
        pass


def list_favorites():
    """All starred sessions across every device, newest-starred first.
    Title tracks the live custom label (load_names) so a rename shows up here too;
    falls back to the snapshot captured at star time when no custom name is set."""
    out = []
    names = load_names()
    for cc, p in load_prefs().items():
        f = p.get("fav") if isinstance(p, dict) else None
        if isinstance(f, dict):
            out.append({"cc": cc, "cwd": f.get("cwd", ""), "name": f.get("name", ""),
                        "title": names.get(cc) or f.get("title", ""), "ts": f.get("ts", 0)})
    out.sort(key=lambda x: x.get("ts", 0), reverse=True)
    for x in out:
        x.pop("ts", None)
    return out


# ─────────────────── projects (the sidebar groups by folder) ───────────────────
#
# A "project" is one folder. Its sessions are the transcripts whose cwd is EXACTLY
# that folder — a subfolder is a separate project, so opening several sessions in
# one directory collects them under one heading instead of scattering them
# through a flat recency list. Folders reach the sidebar two ways: they already
# hold a session, or they were pinned through Import folder.

PROJECTS_FILE = os.path.join(HOME, ".claude", "console-projects.json")
_projmeta_cache = {"mtime": -1.0, "v": {}}


def load_project_meta():
    """Per-folder metadata {abs_path: {name, fav, pinned, ts}}. Cached by mtime."""
    try:
        m = os.path.getmtime(PROJECTS_FILE)
    except OSError:
        _projmeta_cache["mtime"], _projmeta_cache["v"] = -1.0, {}
        return _projmeta_cache["v"]
    if m != _projmeta_cache["mtime"]:
        try:
            with open(PROJECTS_FILE, encoding="utf-8") as f:
                d = json.load(f)
            _projmeta_cache["v"] = d if isinstance(d, dict) else {}
        except Exception:
            _projmeta_cache["v"] = {}
        _projmeta_cache["mtime"] = m
    return _projmeta_cache["v"]


def save_project_meta(path, name=None, fav=None, pinned=None):
    """Persist one folder's label / star / pin. Dropping every flag removes the
    entry, so an unpinned, unstarred, unrenamed folder leaves no residue."""
    path = _norm_dir(path)
    if not path:
        return False
    meta = load_project_meta()
    cur = meta.get(path)
    cur = dict(cur) if isinstance(cur, dict) else {}
    if name is not None:
        name = (name or "").strip()[:120]
        if name:
            cur["name"] = name
        else:
            cur.pop("name", None)
    if fav is not None:
        if fav:
            cur["fav"] = True
            cur.setdefault("ts", time.time())
        else:
            cur.pop("fav", None)
    if pinned is not None:
        if pinned:
            cur["pinned"] = True
            cur.setdefault("ts", time.time())
        else:
            cur.pop("pinned", None)
    if not cur.get("fav") and not cur.get("pinned"):
        cur.pop("ts", None)
    if cur == meta.get(path):
        return True
    meta = dict(meta)
    if cur:
        meta[path] = cur
    else:
        meta.pop(path, None)
    return _write_project_meta(meta)


def _write_project_meta(meta):
    try:
        os.makedirs(os.path.dirname(PROJECTS_FILE), exist_ok=True)
        tmp = PROJECTS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False)
        os.replace(tmp, PROJECTS_FILE)
        _projmeta_cache["mtime"] = -1.0
        return True
    except Exception:
        return False


FAV_MIGRATION_KEY = "#migrated_session_favs"


def migrate_session_favs_to_projects():
    """One-off: favorites used to be per session, they are per folder now. Lift
    every existing session star onto the folder that session ran in, so the
    Favorites list survives the change instead of starting empty. A marker in the
    same file makes this idempotent, so a folder the user later unstars is never
    resurrected on the next start."""
    meta = load_project_meta()
    if meta.get(FAV_MIGRATION_KEY):
        return 0
    moved = set()
    for f in list_favorites():
        path = _norm_dir(f.get("cwd"))
        if path and not _is_junk(path):
            moved.add(path)
    for path in moved:
        save_project_meta(path, fav=True)
    meta = dict(load_project_meta())
    meta[FAV_MIGRATION_KEY] = True
    _write_project_meta(meta)
    return len(moved)


def _norm_dir(p):
    """Absolute, symlink-resolved directory path, or "" when it isn't one.
    Blank input returns "" rather than the process cwd, which is what
    os.path.realpath("") would hand back."""
    p = (p or "").strip()
    if not p:
        return ""
    try:
        p = os.path.realpath(os.path.expanduser(p))
    except Exception:
        return ""
    return p if p and os.path.isdir(p) else ""


def short_path(p):
    """The subtitle shown under a project name: parent/current, never the full
    path. $HOME collapses to ~ so a session opened in the home dir reads as ~."""
    if p == HOME:
        return "~"
    rel = p[len(HOME) + 1:] if p.startswith(HOME + os.sep) else p.lstrip(os.sep)
    parts = [x for x in rel.split(os.sep) if x]
    return os.sep.join(parts[-2:]) if parts else p


_scan_cache = {"t": 0.0, "v": {}}


def _sessions_by_folder(force=False):
    """{folder: [session, ...]} over every claude transcript, newest first.
    Grouping is by exact folder. ~0.25s over 875 transcripts, and it runs on every
    sidebar refresh, so it is cached for a few seconds. _is_junk drops the
    ~/.claude-mem observer artifacts, which are the large majority of the files."""
    now = time.monotonic()
    if not force and _scan_cache["v"] and now - _scan_cache["t"] < 6:
        return _scan_cache["v"]
    names = load_names()
    out = {}
    for s in list_sessions(2000):
        if s.get("source") != "claude":
            continue
        folder = _norm_dir(s.get("cwd"))
        if not folder or _is_junk(folder):
            continue
        cc = os.path.splitext(os.path.basename(s["id"]))[0]
        if not cc:
            continue
        out.setdefault(folder, []).append({
            "cc": cc, "cwd": s.get("cwd") or folder,
            "title": names.get(cc) or s.get("title", ""),
            "mtime": s.get("mtime", 0),
        })
    _scan_cache["v"], _scan_cache["t"] = out, now
    return out


def project_tree(force=False):
    """The sidebar payload. One entry per folder, most recently touched first.
    Pinned and starred folders appear even with no sessions on disk yet."""
    by_folder = _sessions_by_folder(force)
    meta = load_project_meta()
    folders = set(by_folder)
    for path, m in meta.items():
        if not isinstance(m, dict):
            continue          # the migration marker, not a folder
        if (m.get("fav") or m.get("pinned")) and os.path.isdir(path):
            folders.add(path)
    out = []
    for path in folders:
        sessions = by_folder.get(path, [])
        m = meta.get(path)
        m = m if isinstance(m, dict) else {}
        out.append({
            "path": path,
            "name": m.get("name") or os.path.basename(path) or path,
            "sub": short_path(path),
            "fav": bool(m.get("fav")),
            "pinned": bool(m.get("pinned")),
            "renamed": bool(m.get("name")),
            "mtime": max([x["mtime"] for x in sessions], default=m.get("ts", 0)),
            "sessions": sessions[:60],
            "n": len(sessions),
        })
    out.sort(key=lambda x: x.get("mtime", 0), reverse=True)
    return out


_usage_cache = {"t": 0.0, "v": None}

def fetch_usage():
    """The rolling 5h / 7d usage limits, from Claude's OAuth usage endpoint — the
    same numbers the CLI shows. Reads the local OAuth token; cached ~5 min (the
    endpoint is itself rate-limited). Blocking (run it off the IO loop). Returns a
    dict {window: {utilization, resets_at}} plus, when the account has per-model
    weekly quotas, "model_scoped": [{label, utilization, resets_at, severity}].
    Returns the last good value on a transient error, or None when never
    available (no token)."""
    now = time.monotonic()
    if _usage_cache["v"] is not None and now - _usage_cache["t"] < 300:
        return _usage_cache["v"]
    try:
        with open(os.path.join(HOME, ".claude", ".credentials.json"), encoding="utf-8") as f:
            tok = (json.load(f).get("claudeAiOauth") or {}).get("accessToken")
        if not tok:
            return None
        req = urllib.request.Request(
            "https://api.anthropic.com/api/oauth/usage",
            headers={"Authorization": "Bearer " + tok,
                     "anthropic-beta": "oauth-2025-04-20",
                     "User-Agent": "claude-console"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.load(r)
        out = {}
        for k in ("five_hour", "seven_day", "seven_day_opus", "seven_day_sonnet"):
            v = data.get(k)
            if isinstance(v, dict) and v.get("utilization") is not None:
                out[k] = {"utilization": v.get("utilization"), "resets_at": v.get("resets_at")}
        # Per-model weekly windows (currently Fable on Max) exist only inside
        # limits[], never as a top-level key, and the top-level names rotate as
        # unreleased buckets ship. limits[] is self-describing, so take the label
        # off the entry rather than matching on a model name. Note the field
        # names differ from the top-level buckets: percent (int), not utilization.
        scoped = []
        for it in (data.get("limits") or []):
            if not isinstance(it, dict) or it.get("kind") != "weekly_scoped":
                continue
            name = ((it.get("scope") or {}).get("model") or {}).get("display_name")
            if not name or it.get("percent") is None:
                continue
            scoped.append({"label": name, "utilization": it["percent"],
                           "resets_at": it.get("resets_at"), "severity": it.get("severity")})
        if scoped:
            out["model_scoped"] = scoped
        _usage_cache["t"], _usage_cache["v"] = now, out
        return out
    except Exception:
        # transient failure (rate-limited 429 / network): keep serving the last
        # good value so the indicator doesn't blink out. Don't touch the timer,
        # so a real refresh is retried on the next poll.
        return _usage_cache["v"]


# Above this input window, the CLI can be asked for the long-context beta with a
# `[1m]` model suffix. At or below it the request is refused, so the picker must
# not offer the variant.
LONG_CONTEXT_FLOOR = 200000
_models_cache = {"t": 0.0, "v": None}

def fetch_models(force=False):
    """Live model list from the API's /v1/models (id + display name), so the
    picker self-updates when Anthropic ships/retires models (e.g. fable).
    Same OAuth token as fetch_usage; cached ~1h — force=True bypasses the cache
    (the picker's manual ↻ refresh). Blocking (run off the IO loop).
    Returns [{id, name}], the last good value on a transient error, or None."""
    now = time.monotonic()
    if not force and _models_cache["v"] is not None and now - _models_cache["t"] < 3600:
        return _models_cache["v"]
    try:
        with open(os.path.join(HOME, ".claude", ".credentials.json"), encoding="utf-8") as f:
            tok = (json.load(f).get("claudeAiOauth") or {}).get("accessToken")
        if not tok:
            return None
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/models?limit=100",
            headers={"Authorization": "Bearer " + tok,
                     "anthropic-version": "2023-06-01",
                     "anthropic-beta": "oauth-2025-04-20",
                     "User-Agent": "claude-console"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.load(r)
        out = []
        for m in (data.get("data") or []):
            if not isinstance(m, dict) or not m.get("id"):
                continue
            mid = m["id"]
            name = m.get("display_name") or mid
            out.append({"id": mid, "name": name})
            # A model supporting a million tokens is not the same as the CLI
            # using them. The window is opt-in per launch through the `[1m]`
            # suffix, which is what turns on the context-1m beta; without it a
            # long --resume dies on "Prompt is too long" even though the model
            # itself would have fit it. /v1/models only ever returns the bare
            # id, so the variant has to be offered here or it cannot be picked.
            # Which models get one is read off the API rather than hardcoded:
            # asking for [1m] on a 200K model is a 400 from the server.
            if int(m.get("max_input_tokens") or 0) > LONG_CONTEXT_FLOOR:
                out.append({"id": mid + "[1m]", "name": name + " (1M)"})
        if out:
            _models_cache["t"], _models_cache["v"] = now, out
        return _models_cache["v"]
    except Exception:
        return _models_cache["v"]


def load_transcript_events(cc, cap=1000):
    """Parse a saved transcript into console events, for preload on --resume."""
    path = find_transcript(cc)
    if not path:
        return []
    evs = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if line:
                    evs.extend(parse_line(line, i, "claude"))
    except Exception:
        pass
    return evs[-cap:] if len(evs) > cap else evs


def list_resumable(cwd=None, limit=20):
    """Recent claude sessions that can be continued via --resume. If `cwd` is
    given, restrict to sessions whose working dir is exactly that folder
    (scan deeper, since one folder's sessions may be far down the global list)."""
    target = os.path.realpath(cwd) if cwd else None
    out = []
    for s in list_sessions(500 if target else 60):
        if s.get("source") != "claude":
            continue
        scwd = s.get("cwd")
        if not scwd or not os.path.isdir(scwd) or _is_junk(scwd):
            continue
        # match the folder AND everything under it (sessions usually live in a
        # project subdir, not the container folder itself)
        rscwd = os.path.realpath(scwd)
        if target and not (rscwd == target or rscwd.startswith(target + os.sep)):
            continue
        cc = os.path.splitext(os.path.basename(s["id"]))[0]
        out.append({"cc": cc, "cwd": scwd, "name": os.path.basename(scwd) or scwd,
                    "title": load_names().get(cc) or s.get("title", ""),
                    "mtime": s.get("mtime", 0)})
        if len(out) >= limit:
            break
    return out


_proj_cache = {"t": 0.0, "v": []}
def _projects_cached():
    now = time.monotonic()
    if now - _proj_cache["t"] > 8 or not _proj_cache["v"]:
        _proj_cache["v"] = list_projects()
        _proj_cache["t"] = now
    return _proj_cache["v"]


def dir_complete(q, limit=500):
    """Directory autocomplete for the console path box. If the typed path IS an
    existing directory, list its children (so you don't need a trailing '/');
    otherwise complete the last segment within its parent. A bare name fragment
    (no '/') also fuzzy-matches known projects. Restricted to $HOME. Returns
    (dirs, more) where `more` flags that results were capped."""
    q = (q or "").strip()
    home = os.path.realpath(HOME)
    out, seen = [], set()

    def add(p):
        rp = os.path.realpath(os.path.expanduser(p))
        if rp in seen or not os.path.isdir(rp):
            return
        if not (rp == home or rp.startswith(home + os.sep)):
            return
        if _is_junk(rp):
            return
        seen.add(rp)
        out.append(rp)

    def fs_complete():
        cand = os.path.expanduser(q) if q else HOME
        if cand and not cand.endswith(os.sep) and os.path.isdir(cand):
            base, frag = cand, ""           # typed path is a dir → list its children
        elif cand.endswith(os.sep):
            base, frag = cand, ""
        else:
            base, frag = os.path.split(cand)
        base = base or HOME
        try:
            names = sorted(os.listdir(base), key=str.lower)
        except Exception:
            return
        fl = frag.lower()
        for name in names:
            if name.startswith(".") and not frag.startswith("."):
                continue
            if not fl or name.lower().startswith(fl):
                add(os.path.join(base, name))

    def fuzzy():
        ql = q.lower()
        for proj in _projects_cached():
            p = proj["path"]
            if not ql or ql in p.lower() or ql in os.path.basename(p).lower():
                add(p)

    if ("/" in q) or q.startswith("~"):
        fs_complete()                       # explicit path → filesystem browse only
    else:
        fuzzy(); fs_complete()              # bare name → fuzzy projects + home entries
    return out[:limit], len(out) > limit


# ───────────────────────── git diff (ground truth) ─────────────────────────
def _git(cwd, args, timeout=6):
    try:
        r = subprocess.run(["git", "-C", cwd, *args], capture_output=True,
                           text=True, timeout=timeout)
        return r.stdout
    except Exception:
        return ""


def git_snapshot(cwd):
    rp = os.path.realpath(cwd)
    if not (rp == os.path.realpath(HOME) or rp.startswith(os.path.realpath(HOME) + os.sep)):
        return {"ok": False, "error": "path outside home"}
    if not os.path.isdir(rp):
        return {"ok": False, "error": "not a directory"}
    inside = _git(rp, ["rev-parse", "--is-inside-work-tree"]).strip()
    if inside != "true":
        return {"ok": False, "error": "not a git repo", "cwd": rp}
    branch = _git(rp, ["rev-parse", "--abbrev-ref", "HEAD"]).strip()
    porcelain = _git(rp, ["status", "--porcelain"])
    files = []
    untracked = []
    for line in porcelain.splitlines():
        if len(line) < 4:
            continue
        code, name = line[:2], line[3:]
        files.append({"status": code.strip() or "?", "path": name})
        if code == "??":
            untracked.append(name)
    diff = _git(rp, ["-c", "core.pager=cat", "diff"])
    staged = _git(rp, ["-c", "core.pager=cat", "diff", "--cached"])
    chunks = []
    if staged.strip():
        chunks.append("# ── staged ──\n" + staged)
    if diff.strip():
        chunks.append(diff)
    # synthesize a +diff for untracked (small text) files so new files are visible
    for name in untracked[:40]:
        fp = os.path.join(rp, name)
        try:
            if os.path.isfile(fp) and os.path.getsize(fp) < 80000:
                with open(fp, "r", encoding="utf-8", errors="replace") as fh:
                    body = fh.read()
                if "\x00" in body:
                    continue
                lines = body.splitlines()
                head = ["diff --git a/%s b/%s" % (name, name), "new file (untracked)",
                        "--- /dev/null", "+++ b/%s" % name]
                chunks.append("\n".join(head + ["+" + ln for ln in lines]))
        except Exception:
            continue
    full = "\n".join(chunks)
    if len(full) > 260000:
        full = full[:260000] + "\n…[diff truncated]"
    return {"ok": True, "cwd": rp, "branch": branch, "files": files, "diff": full}


# ───────────────────────── auth ─────────────────────────
class AuthMixin:
    def _ok_auth(self):
        if not AUTH:
            return True
        header = self.request.headers.get("Authorization", "")
        if header.startswith("Basic "):
            try:
                if secrets.compare_digest(base64.b64decode(header[6:]).decode(), AUTH):
                    return True
            except Exception:
                pass
        self.set_status(401)
        self.set_header("WWW-Authenticate", 'Basic realm="claude-console"')
        self.finish()
        return False



# ───────────────────── voice input (optional, local) ─────────────────────
#
# Dictation, done locally. The browser records; this transcribes with
# faster-whisper in a separate process and hands the text back as an *editable
# draft*. Nothing is sent to the model until the user presses send, and no audio
# leaves the machine.
#
# It stays off unless CLAUDE_CONSOLE_TRANSCRIBE=1 and a model is configured, so
# the console still runs with nothing installed beyond tornado.
TRANSCRIBE_ENABLED = (_env("TRANSCRIBE", "0") or "0").lower() not in (
    "0", "false", "no", "off")
# The console itself runs in whatever env has the SDK; faster-whisper and its
# CUDA wheels usually live somewhere else, so the worker gets its own interpreter.
TRANSCRIBE_PYTHON = os.path.expanduser(_env("TRANSCRIBE_PYTHON", sys.executable))
TRANSCRIBE_MODEL = os.path.expanduser(_env("TRANSCRIBE_MODEL", ""))
TRANSCRIBE_DEVICE = _env("TRANSCRIBE_DEVICE", "auto") or "auto"
TRANSCRIBE_DEVICE_INDEX = int(_env("TRANSCRIBE_DEVICE_INDEX", "0") or "0")
TRANSCRIBE_COMPUTE_TYPE = _env("TRANSCRIBE_COMPUTE_TYPE", "default") or "default"
TRANSCRIBE_LANGUAGE = _env("TRANSCRIBE_LANGUAGE", "")
TRANSCRIBE_CHINESE_CONVERSION = (
    _env("TRANSCRIBE_CHINESE_CONVERSION", "none") or "none").lower()
TRANSCRIBE_PAUSE_PUNCTUATION = (
    _env("TRANSCRIBE_PAUSE_PUNCTUATION", "0") or "0").lower() not in (
        "0", "false", "no", "off")
# Silence long enough to read as punctuation. Both are judged by ear, so they are
# configurable rather than baked in: a comma is a breath, a period is a full stop.
TRANSCRIBE_COMMA_GAP = float(_env("TRANSCRIBE_COMMA_GAP", "0.5") or "0.5")
TRANSCRIBE_PERIOD_GAP = float(_env("TRANSCRIBE_PERIOD_GAP", "1.2") or "1.2")
TRANSCRIBE_LD_LIBRARY_PATH = _env("TRANSCRIBE_LD_LIBRARY_PATH", "")
TRANSCRIBE_MAX_BYTES = max(
    1024 * 1024, int(_env("TRANSCRIBE_MAX_MB", "16") or "16") * 1024 * 1024)
TRANSCRIBE_MAX_SECONDS = max(10, int(_env("TRANSCRIBE_MAX_SEC", "120") or "120"))
TRANSCRIBE_TIMEOUT_SECONDS = max(
    30, int(_env("TRANSCRIBE_TIMEOUT_SEC", "180") or "180"))
TRANSCRIBE_IDLE_SECONDS = max(
    30, int(_env("TRANSCRIBE_IDLE_SEC", "600") or "600"))
TRANSCRIBE_PREVIEW_INTERVAL_MS = 1600
TRANSCRIBE_WORKER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "faster_whisper_worker.py")


def _transcribe_status():
    """Why the microphone button is (or is not) live, in the words the UI shows."""
    if not TRANSCRIBE_ENABLED:
        return {"enabled": False, "available": False,
                "reason": "voice transcription is disabled"}
    python_path = (TRANSCRIBE_PYTHON if os.path.isabs(TRANSCRIBE_PYTHON)
                   else shutil.which(TRANSCRIBE_PYTHON))
    if not python_path or not os.path.isfile(python_path):
        return {"enabled": True, "available": False,
                "reason": "transcription runtime is unavailable"}
    if not TRANSCRIBE_MODEL:
        return {"enabled": True, "available": False,
                "reason": "transcription model is not configured"}
    if TRANSCRIBE_CHINESE_CONVERSION not in ("none", "t2s", "tw2sp"):
        return {"enabled": True, "available": False,
                "reason": "Chinese transcription conversion is invalid"}
    if not 0 < TRANSCRIBE_COMMA_GAP <= TRANSCRIBE_PERIOD_GAP:
        return {"enabled": True, "available": False,
                "reason": "transcription pause thresholds are invalid"}
    if (os.path.isabs(TRANSCRIBE_MODEL)
            and not os.path.isfile(os.path.join(TRANSCRIBE_MODEL, "model.bin"))):
        return {"enabled": True, "available": False,
                "reason": "transcription model is unavailable"}
    if not os.path.isfile(TRANSCRIBE_WORKER):
        return {"enabled": True, "available": False,
                "reason": "transcription worker is unavailable"}
    return {"enabled": True, "available": True, "reason": ""}


class TranscriberManager:
    """One lazy worker process, so model VRAM is held only while it is being used.

    The worker loads the model on its first request and exits by itself after
    --idle-seconds of silence; the next request starts a fresh one. That is the
    whole memory policy — no unloading logic here, just a process that goes away.
    """

    def __init__(self, python_path=None, worker_path=None, model=None,
                 device=None, device_index=None, compute_type=None,
                 language=None, chinese_conversion=None,
                 pause_punctuation=None, comma_gap=None, period_gap=None,
                 idle_seconds=None, timeout_seconds=None,
                 max_seconds=None, library_path=None):
        self.python_path = python_path or TRANSCRIBE_PYTHON
        self.worker_path = worker_path or TRANSCRIBE_WORKER
        self.model = model or TRANSCRIBE_MODEL
        self.device = device or TRANSCRIBE_DEVICE
        self.device_index = (TRANSCRIBE_DEVICE_INDEX if device_index is None
                             else int(device_index))
        self.compute_type = compute_type or TRANSCRIBE_COMPUTE_TYPE
        self.language = TRANSCRIBE_LANGUAGE if language is None else language
        self.chinese_conversion = (TRANSCRIBE_CHINESE_CONVERSION
                                   if chinese_conversion is None
                                   else chinese_conversion)
        self.pause_punctuation = (TRANSCRIBE_PAUSE_PUNCTUATION
                                  if pause_punctuation is None
                                  else bool(pause_punctuation))
        self.comma_gap = (TRANSCRIBE_COMMA_GAP if comma_gap is None
                          else float(comma_gap))
        self.period_gap = (TRANSCRIBE_PERIOD_GAP if period_gap is None
                           else float(period_gap))
        self.idle_seconds = idle_seconds or TRANSCRIBE_IDLE_SECONDS
        self.timeout_seconds = timeout_seconds or TRANSCRIBE_TIMEOUT_SECONDS
        self.max_seconds = max_seconds or TRANSCRIBE_MAX_SECONDS
        self.library_path = (TRANSCRIBE_LD_LIBRARY_PATH if library_path is None
                             else library_path)
        self.proc = None
        self._lock = threading.Lock()

    def _start_locked(self):
        if self.proc and self.proc.poll() is None:
            return self.proc
        self._stop_locked()
        python_path = (self.python_path if os.path.isabs(self.python_path)
                       else shutil.which(self.python_path))
        if not python_path:
            raise RuntimeError("transcription runtime is unavailable")
        command = [
            python_path, "-u", self.worker_path,
            "--model", self.model,
            "--device", self.device,
            "--device-index", str(self.device_index),
            "--compute-type", self.compute_type,
            "--idle-seconds", str(self.idle_seconds),
            "--max-seconds", str(self.max_seconds),
        ]
        if self.language:
            command += ["--language", self.language]
        if self.chinese_conversion and self.chinese_conversion != "none":
            command += ["--chinese-conversion", self.chinese_conversion]
        if self.pause_punctuation:
            command += ["--pause-punctuation",
                        "--comma-gap", repr(self.comma_gap),
                        "--period-gap", repr(self.period_gap)]
        env = os.environ.copy()
        if self.library_path:
            env["LD_LIBRARY_PATH"] = self.library_path + (
                (":" + env["LD_LIBRARY_PATH"]) if env.get("LD_LIBRARY_PATH") else "")
        if os.path.isdir(self.model):
            env["HF_HUB_OFFLINE"] = "1"   # a local model directory needs no hub
        self.proc = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, encoding="utf-8", bufsize=1,
            env=env)
        return self.proc

    def _stop_locked(self):
        proc, self.proc = self.proc, None
        if not proc:
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=2)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        for stream in (proc.stdin, proc.stdout):
            try:
                stream.close()
            except Exception:
                pass

    def shutdown(self):
        with self._lock:
            self._stop_locked()

    def _request_locked(self, payload):
        proc = self._start_locked()
        request_id = secrets.token_hex(8)
        payload = {**payload, "id": request_id}
        try:
            proc.stdin.write(json.dumps(payload) + "\n")
            proc.stdin.flush()
        except Exception as exc:
            self._stop_locked()
            raise RuntimeError("transcription worker did not start") from exc
        readable, _, _ = select.select(
            [proc.stdout], [], [], self.timeout_seconds)
        if not readable:
            self._stop_locked()
            raise TimeoutError("transcription timed out")
        line = proc.stdout.readline()
        if not line:
            self._stop_locked()
            raise RuntimeError("transcription worker exited")
        try:
            result = json.loads(line)
        except Exception as exc:
            self._stop_locked()
            raise RuntimeError("invalid transcription worker response") from exc
        if result.get("id") != request_id:
            self._stop_locked()
            raise RuntimeError("mismatched transcription worker response")
        if not result.get("ok"):
            error = RuntimeError(result.get("error") or "transcription failed")
            error.code = result.get("code") or ""
            raise error
        return result

    def warmup(self):
        with self._lock:
            return self._request_locked({"type": "warmup"})

    def transcribe(self, path, final=True, stream_id="", duration_hint=None):
        """Blocking; call it off the IOLoop. One request at a time, by the lock."""
        with self._lock:
            return self._request_locked({
                "type": "transcribe", "path": path,
                "final": bool(final), "stream_id": str(stream_id or "")[:120],
                "duration_hint": duration_hint,
            })


_TRANSCRIBER = TranscriberManager()
# The GPU fits one transcription at a time; a second caller is told to wait
# rather than queued behind a lock holding an open request.
_TRANSCRIBER_GATE = threading.BoundedSemaphore(1)
_AUDIO_TYPES = {
    "audio/webm": ".webm", "video/webm": ".webm",
    "audio/mp4": ".m4a", "video/mp4": ".m4a",
    "audio/ogg": ".ogg", "audio/wav": ".wav", "audio/x-wav": ".wav",
    "application/octet-stream": ".audio",
}


class TranscribeWarmupHandler(AuthMixin, tornado.web.RequestHandler):
    async def post(self):
        if not self._ok_auth():
            return
        status = _transcribe_status()
        if not status["available"]:
            raise tornado.web.HTTPError(503, reason=status["reason"])
        self.set_header("Content-Type", "application/json")
        self.set_header("Cache-Control", "no-store")
        if not _TRANSCRIBER_GATE.acquire(blocking=False):
            self.set_status(202)
            self.write(json.dumps({"ok": True, "warming": True}))
            return
        try:
            loop = tornado.ioloop.IOLoop.current()
            result = await loop.run_in_executor(None, _TRANSCRIBER.warmup)
        except TimeoutError as exc:
            raise tornado.web.HTTPError(504, reason=str(exc))
        except Exception as exc:
            raise tornado.web.HTTPError(
                500, reason=(str(exc) or "transcription warmup failed")[:1000])
        finally:
            _TRANSCRIBER_GATE.release()
        self.write(json.dumps({
            "ok": True,
            "warmed": True,
            "elapsed": result.get("elapsed"),
        }))


@tornado.web.stream_request_body
class TranscribeHandler(AuthMixin, tornado.web.RequestHandler):
    """GET reports what voice input can do here; POST takes the audio and
    returns text. The upload streams to a 0600 temp file that on_finish always
    deletes, so a recording exists on disk only for the length of one request."""

    def initialize(self):
        self._upload = None
        self._upload_path = ""
        self._upload_bytes = 0

    def _json(self, payload, status=200):
        self.set_status(status)
        self.set_header("Content-Type", "application/json")
        self.set_header("Cache-Control", "no-store")
        self.write(json.dumps(payload, ensure_ascii=False))

    def write_error(self, status_code, **kwargs):
        exc_info = kwargs.get("exc_info")
        reason = exc_info[1] if exc_info else None
        message = getattr(reason, "reason", "") or self._reason or "request failed"
        if not self._finished:
            self._json({"ok": False, "error": message}, status_code)

    def prepare(self):
        # Runs before any body arrives: auth, size and type are settled while
        # refusing still costs nothing.
        if not self._ok_auth() or self.request.method != "POST":
            return
        status = _transcribe_status()
        if not status["available"]:
            raise tornado.web.HTTPError(503, reason=status["reason"])
        content_length = self.request.headers.get("Content-Length", "")
        try:
            if content_length and int(content_length) > TRANSCRIBE_MAX_BYTES:
                raise tornado.web.HTTPError(413, reason="audio exceeds the upload limit")
        except ValueError:
            raise tornado.web.HTTPError(400, reason="invalid content length")
        media_type = self.request.headers.get("Content-Type", "").split(";", 1)[0].lower()
        suffix = _AUDIO_TYPES.get(media_type)
        if not suffix:
            raise tornado.web.HTTPError(415, reason="unsupported audio type")
        upload = tempfile.NamedTemporaryFile(
            prefix="claude-console-voice-", suffix=suffix, delete=False)
        os.chmod(upload.name, 0o600)
        self._upload = upload
        self._upload_path = upload.name

    def data_received(self, chunk):
        if not self._upload:
            return
        self._upload_bytes += len(chunk)
        # Content-Length can lie, so the cap is enforced again on what arrives.
        if self._upload_bytes > TRANSCRIBE_MAX_BYTES:
            self._upload.close()
            self._upload = None
            raise tornado.web.HTTPError(413, reason="audio exceeds the upload limit")
        self._upload.write(chunk)

    def get(self):
        if not self._ok_auth():
            return
        self._json({
            "ok": True, **_transcribe_status(),
            "maxBytes": TRANSCRIBE_MAX_BYTES,
            "maxSeconds": TRANSCRIBE_MAX_SECONDS,
            "livePreview": True,
            "previewIntervalMs": TRANSCRIBE_PREVIEW_INTERVAL_MS,
        })

    async def post(self):
        if not self._upload or not self._upload_bytes:
            raise tornado.web.HTTPError(400, reason="empty audio")
        self._upload.close()
        self._upload = None
        try:
            duration_ms = int(self.request.headers.get("X-Audio-Duration-Ms", "0") or "0")
        except ValueError:
            duration_ms = 0
        if duration_ms > (TRANSCRIBE_MAX_SECONDS + 2) * 1000:
            raise tornado.web.HTTPError(413, reason="recording is too long")
        partial = self.get_query_argument("partial", "0").lower() in (
            "1", "true", "yes", "on")
        stream_id = re.sub(
            r"[^A-Za-z0-9_-]", "",
            self.request.headers.get("X-Transcription-Stream", ""))[:120]
        if not _TRANSCRIBER_GATE.acquire(blocking=False):
            raise tornado.web.HTTPError(429, reason="another transcription is running")
        try:
            loop = tornado.ioloop.IOLoop.current()
            result = await loop.run_in_executor(
                None, lambda: _TRANSCRIBER.transcribe(
                    self._upload_path, final=not partial,
                    stream_id=stream_id, duration_hint=duration_ms / 1000.0))
        except TimeoutError as exc:
            raise tornado.web.HTTPError(504, reason=str(exc))
        except Exception as exc:
            if getattr(exc, "code", "") == "too_long":
                raise tornado.web.HTTPError(413, reason=str(exc))
            raise tornado.web.HTTPError(500, reason=(str(exc) or "transcription failed")[:1000])
        finally:
            _TRANSCRIBER_GATE.release()
        if not (result.get("text") or "").strip():
            raise tornado.web.HTTPError(422, reason="no speech detected")
        self._json({
            "ok": True,
            "text": result.get("text") or "",
            "stableText": result.get("stable_text") or "",
            "partial": partial,
            "language": result.get("language") or "",
            "duration": result.get("duration"),
            "elapsed": result.get("elapsed"),
        })

    def on_finish(self):
        # Every exit path lands here, including the raised HTTPErrors above, so
        # this is the one place the recording has to be removed.
        if self._upload:
            try:
                self._upload.close()
            except Exception:
                pass
            self._upload = None
        if self._upload_path:
            try:
                os.unlink(self._upload_path)
            except FileNotFoundError:
                pass
            except Exception:
                pass
            self._upload_path = ""


# ─────────────────── history search index ───────────────────
#
# Searching the transcripts directly is the obvious approach and the wrong one:
# measured on a real corpus, 2.4 GB of .jsonl holds only ~250 MB of anything a
# person said — the other 98.6% is tool output, file dumps and base64 images, which
# would bury every real hit and cost seconds per query to scan. So this keeps a
# small SQLite side-table of just the conversation: user/assistant prose plus the
# file paths and commands tools acted on.
#
# Substring matching (LIKE), not FTS. FTS5's default tokenizer scores ZERO hits on
# Chinese text, and the trigram tokenizer that fixes that still misses every
# two-character word (重启, 冲突) while costing ~1 GB of index. LIKE treats every
# language and code fragment alike for ~2 s on the full corpus, and is instant once
# a scope narrows it. A trigram layer can be added later as pure acceleration.

INDEX_DB = os.path.join(HOME, ".cache", "claude-console", "history.db")
CJK_RE = re.compile(r"[㐀-鿿぀-ヿ가-힯]")
INDEX_REFRESH_SEC = 30   # incremental catch-up cadence, checked on search
_index_lock = threading.Lock()
_index_state = {"t": 0.0, "err": ""}


INDEX_SCHEMA = 3      # bump to force a rebuild when the layout below changes


def _index_conn():
    os.makedirs(os.path.dirname(INDEX_DB), exist_ok=True)
    db = sqlite3.connect(INDEX_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")     # readers never block the indexer
    db.execute("PRAGMA synchronous=NORMAL")
    if db.execute("PRAGMA user_version").fetchone()[0] != INDEX_SCHEMA:
        db.executescript("DROP TABLE IF EXISTS msgs; DROP TABLE IF EXISTS files;")
        db.execute("PRAGMA user_version=%d" % INDEX_SCHEMA)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS files(
            fid INTEGER PRIMARY KEY, path TEXT UNIQUE,
            off INTEGER DEFAULT 0, cc TEXT, cwd TEXT);
        CREATE TABLE IF NOT EXISTS msgs(
            fid INTEGER, uid TEXT, ts TEXT, role TEXT, txt TEXT);
        CREATE INDEX IF NOT EXISTS msgs_fid ON msgs(fid);
        CREATE INDEX IF NOT EXISTS files_cc ON files(cc);
        CREATE INDEX IF NOT EXISTS files_cwd ON files(cwd);
        -- resuming a session makes the CLI re-append the whole prior history to the
        -- same .jsonl, so one message can sit in the file three times over with an
        -- identical uuid. Scoped per file, not globally: an imported copy of a
        -- session is its own transcript and has to stay searchable in its own
        -- folder. NULLs stay exempt, and SQLite allows many of those.
        CREATE UNIQUE INDEX IF NOT EXISTS msgs_uid ON msgs(fid, uid);
    """)
    return db


def _searchable(d):
    """The searchable text of one transcript line, as (role, text) pairs. Tool
    *output* is deliberately excluded — see the note above."""
    m = d.get("message")
    if not isinstance(m, dict):
        return
    role = m.get("role")
    if role not in ("user", "assistant"):
        return
    c = m.get("content")
    if isinstance(c, str):
        if c.strip():
            yield role, c
        return
    if not isinstance(c, list):
        return
    for b in c:
        if not isinstance(b, dict):
            continue
        t = b.get("type")
        if t == "text" and isinstance(b.get("text"), str) and b["text"].strip():
            yield role, b["text"]
        elif t == "tool_use":
            i = b.get("input") or {}
            v = i.get("file_path") or i.get("command") or i.get("pattern") or ""
            if isinstance(v, str) and v.strip():
                yield "tool", v[:400]


# Folders under CLAUDE_ROOT that hold machine-written transcripts *about* your
# conversations rather than conversations themselves. Both quote your content back,
# so indexing them returns every hit two or three times over:
#   claude-console-recap — this console's own recap prompt is "Transcript so far:"
#                          followed by a full copy of the session it summarised.
#   claude-mem           — the memory plugin's observer logs each tool call as an
#                          <observed_from_primary_session> XML blob (~80 k chars,
#                          16% of everything indexed here) carrying file contents
#                          and tool parameters verbatim.
INDEX_SKIP_MARKS = ("claude-console-recap", "claude-mem")


def _index_skip(path):
    folder = os.path.basename(os.path.dirname(path))
    return any(m in folder for m in INDEX_SKIP_MARKS)


def reindex():
    """Fold new transcript lines into the index. Transcripts are append-only (checked
    against a 405 MB file spanning a month), so each pass seeks to where the last one
    stopped and reads only the tail — keeping a huge session current costs nothing.
    A file that shrank was replaced rather than appended to, so it is rebuilt."""
    if not _index_lock.acquire(blocking=False):
        return {"skipped": "already running"}
    try:
        db = _index_conn()
        known = {p: (fid, off) for fid, p, off
                 in db.execute("SELECT fid, path, off FROM files")}
        added = 0
        alive = set()
        for path in glob.glob(os.path.join(CLAUDE_ROOT, "*", "*.jsonl")):
            if _index_skip(path):
                continue      # left out of `alive`, so any rows already stored go
            alive.add(path)
            try:
                sz = os.path.getsize(path)
            except OSError:
                continue
            fid, off = known.get(path, (None, 0))
            if fid is not None and sz < off:      # truncated/replaced → rebuild
                db.execute("DELETE FROM msgs WHERE fid=?", (fid,))
                off = 0
            if fid is not None and sz == off:
                continue
            try:
                with open(path, "rb") as f:
                    f.seek(off)
                    data = f.read()
            except OSError:
                continue
            cut = data.rfind(b"\n")               # whole lines only
            if cut < 0:
                continue
            chunk, consumed = data[:cut + 1], cut + 1
            cc = cwd = ""
            rows = []
            for line in chunk.decode("utf-8", "replace").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                if not isinstance(d, dict):
                    continue
                cc = cc or (d.get("sessionId") or "")
                cwd = cwd or (d.get("cwd") or "")
                ts = d.get("timestamp") or ""
                u = d.get("uuid") or ""
                for seq, (role, txt) in enumerate(_searchable(d)):
                    # one line can yield several texts, so the key carries the slot
                    rows.append(("%s:%d" % (u, seq) if u else None, ts, role, txt))
            if fid is None:
                cur = db.execute(
                    "INSERT INTO files(path, off, cc, cwd) VALUES(?,?,?,?)",
                    (path, 0, cc, cwd))
                fid = cur.lastrowid
            elif cc or cwd:                       # backfill ids learned later
                db.execute("UPDATE files SET cc=COALESCE(NULLIF(cc,''),?),"
                           " cwd=COALESCE(NULLIF(cwd,''),?) WHERE fid=?",
                           (cc, cwd, fid))
            if rows:
                before = db.total_changes
                db.executemany(
                    "INSERT OR IGNORE INTO msgs(fid, uid, ts, role, txt) VALUES(?,?,?,?,?)",
                    [(fid, a, b, c, d_) for a, b, c, d_ in rows])
                added += db.total_changes - before   # re-appended history is ignored
            db.execute("UPDATE files SET off=? WHERE fid=?", (off + consumed, fid))
        for path, (fid, _off) in known.items():   # transcripts deleted since
            if path not in alive:
                db.execute("DELETE FROM msgs WHERE fid=?", (fid,))
                db.execute("DELETE FROM files WHERE fid=?", (fid,))
        db.commit()
        n = db.execute("SELECT COUNT(*) FROM msgs").fetchone()[0]
        db.close()
        _index_state["t"] = time.time()
        _index_state["err"] = ""
        return {"added": added, "messages": n}
    except Exception as e:
        _index_state["err"] = str(e)
        return {"error": str(e)}
    finally:
        _index_lock.release()


def min_query_len(q):
    """How short a query is allowed to be, by script. One han character is a whole
    word — 熵, 态, 谱 are all worth searching for on their own — whereas a lone latin
    letter matches nearly every message. A single threshold for both would either
    lock Chinese users out of one-character words or drown Latin ones in noise."""
    return 1 if CJK_RE.search(q or "") else 2


def _snippet(txt, q, span=160):
    """A window of text around the first match, split so the client can highlight
    the hit without re-running the search or trusting a regex."""
    i = txt.lower().find(q.lower())
    if i < 0:
        return {"pre": txt[:span], "hit": "", "post": ""}
    a = max(0, i - span // 2)
    b = min(len(txt), i + len(q) + span)
    return {"pre": ("…" if a else "") + txt[a:i],
            "hit": txt[i:i + len(q)],
            "post": txt[i + len(q):b] + ("…" if b < len(txt) else "")}


def search_history(q, scope="all", cc="", cwd="", limit=200):
    """Substring search over the indexed conversation. Runs an incremental catch-up
    first so a message sent seconds ago is already findable."""
    q = (q or "").strip()
    need = min_query_len(q)
    if len(q) < need:
        return {"results": [], "note": "type at least %d character%s"
                                       % (need, "" if need == 1 else "s")}
    if time.time() - _index_state["t"] > INDEX_REFRESH_SEC:
        reindex()
    try:
        db = _index_conn()
    except Exception as e:
        return {"results": [], "error": str(e)}
    esc = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    where, args = ["m.txt LIKE ? ESCAPE '\\'"], ["%" + esc + "%"]
    if scope == "session" and cc:
        where.append("f.cc=?")
        args.append(cc)
    elif scope == "project" and cwd:
        # the folder AND everything under it — the same rule the sidebar's "In folder"
        # list uses. Sessions usually live in a subdir of the project you pick, so an
        # exact-equality filter would quietly return almost nothing.
        base = os.path.abspath(os.path.expanduser(cwd)).rstrip(os.sep)
        pre = base.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        where.append("(f.cwd=? OR f.cwd LIKE ? ESCAPE '\\')")
        args.extend([base, pre + os.sep + "%"])
    args.append(int(limit) + 1)
    rows = db.execute(
        "SELECT f.cc, f.cwd, m.ts, m.role, m.txt, m.rowid FROM msgs m JOIN files f"
        " ON f.fid=m.fid WHERE " + " AND ".join(where) +
        " ORDER BY m.ts DESC LIMIT ?", args).fetchall()
    db.close()
    more = len(rows) > limit
    names = load_names()
    out = []
    for r_cc, r_cwd, ts, role, txt, mid in rows[:limit]:
        item = {"cc": r_cc, "cwd": r_cwd, "ts": ts, "role": role, "mid": mid,
                "title": names.get(r_cc) or os.path.basename(r_cwd or "") or "session",
                "name": os.path.basename(r_cwd or "")}
        item.update(_snippet(txt, q))
        out.append(item)
    return {"results": out, "more": more}


THREAD_TXT_CAP = 20000    # one pathological message shouldn't blow up the viewer


def load_thread(mid, before=40, after=40):
    """The conversation surrounding one search hit, served straight out of the index.
    Reading it back from the .jsonl would mean touching a file that can be 400 MB to
    show forty messages; the index already holds them in order."""
    try:
        db = _index_conn()
        mid = int(mid)
    except Exception as e:
        return {"error": str(e), "messages": []}
    r = db.execute("SELECT fid FROM msgs WHERE rowid=?", (mid,)).fetchone()
    if not r:
        return {"error": "that message is no longer indexed", "messages": []}
    fid = r[0]
    cc, cwd = db.execute("SELECT cc, cwd FROM files WHERE fid=?", (fid,)).fetchone() or ("", "")
    # rowids are global, but within one file they increase with position, so a
    # window either side of the hit is just two bounded index scans
    pre = db.execute("SELECT rowid, ts, role, txt FROM msgs WHERE fid=? AND rowid<=?"
                     " ORDER BY rowid DESC LIMIT ?", (fid, mid, int(before) + 1)).fetchall()
    post = db.execute("SELECT rowid, ts, role, txt FROM msgs WHERE fid=? AND rowid>?"
                      " ORDER BY rowid LIMIT ?", (fid, mid, int(after))).fetchall()
    lo, hi = db.execute("SELECT MIN(rowid), MAX(rowid) FROM msgs WHERE fid=?", (fid,)).fetchone()
    total = db.execute("SELECT COUNT(*) FROM msgs WHERE fid=?", (fid,)).fetchone()[0]
    db.close()
    rows = list(reversed(pre)) + list(post)
    msgs = []
    for rid, ts, role, txt in rows:
        cut = len(txt) > THREAD_TXT_CAP
        msgs.append({"mid": rid, "ts": ts, "role": role,
                     "txt": txt[:THREAD_TXT_CAP] + ("\n…(truncated)" if cut else "")})
    return {"cc": cc, "cwd": cwd, "total": total, "messages": msgs,
            "title": load_names().get(cc) or os.path.basename(cwd or "") or "session",
            "atStart": bool(rows) and rows[0][0] == lo,
            "atEnd": bool(rows) and rows[-1][0] == hi}


# ───────────────────────── handlers ─────────────────────────


class ProjectsHandler(AuthMixin, tornado.web.RequestHandler):
    def get(self):
        if not self._ok_auth():
            return
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps({"projects": list_projects(), "home": HOME}))


class ResumableHandler(AuthMixin, tornado.web.RequestHandler):
    def get(self):
        if not self._ok_auth():
            return
        self.set_header("Content-Type", "application/json")
        cwd = self.get_argument("cwd", "") or None
        self.write(json.dumps({"resumable": list_resumable(cwd)}))


class TreeHandler(AuthMixin, tornado.web.RequestHandler):
    """The project-grouped sidebar: folders, each with the sessions it owns."""
    def get(self):
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps({"projects": project_tree()}))


class PinFolderHandler(AuthMixin, tornado.web.RequestHandler):
    """Import folder: register a directory as a project so it shows in the
    sidebar before it owns any session."""
    def post(self):
        self.set_header("Content-Type", "application/json")
        try:
            body = json.loads(self.request.body or b"{}")
        except Exception:
            body = {}
        raw = (body.get("path") or "").strip()
        path = _norm_dir(raw)
        if not path:
            self.write(json.dumps({"ok": False, "error": "not a directory: " + (raw or "(empty)")}))
            return
        home = os.path.realpath(HOME)
        if not (path == home or path.startswith(home + os.sep)):
            self.write(json.dumps({"ok": False, "error": "outside $HOME"}))
            return
        if _is_junk(path):
            self.write(json.dumps({"ok": False, "error": "runtime/cache directory"}))
            return
        ok = save_project_meta(path, pinned=True)
        self.write(json.dumps({"ok": bool(ok), "path": path,
                               "name": os.path.basename(path) or path,
                               "sub": short_path(path)}))


class DirCompleteHandler(AuthMixin, tornado.web.RequestHandler):
    def get(self):
        if not self._ok_auth():
            return
        self.set_header("Content-Type", "application/json")
        dirs, more = dir_complete(self.get_argument("q", ""))
        self.write(json.dumps({"dirs": dirs, "more": more}))


class DiffHandler(AuthMixin, tornado.web.RequestHandler):
    def get(self):
        if not self._ok_auth():
            return
        cwd = self.get_argument("cwd", "")
        self.set_header("Content-Type", "application/json")
        if not cwd:
            self.write(json.dumps({"ok": False, "error": "no cwd"}))
            return
        self.write(json.dumps(git_snapshot(cwd)))


class UsageHandler(AuthMixin, tornado.web.RequestHandler):
    async def get(self):
        if not self._ok_auth():
            return
        self.set_header("Content-Type", "application/json")
        loop = tornado.ioloop.IOLoop.current()
        u = await loop.run_in_executor(None, fetch_usage)   # blocking HTTP off-loop
        self.write(json.dumps({"usage": u}))


class ModelsHandler(AuthMixin, tornado.web.RequestHandler):
    async def get(self):
        if not self._ok_auth():
            return
        self.set_header("Content-Type", "application/json")
        loop = tornado.ioloop.IOLoop.current()
        fresh = self.get_argument("fresh", "") == "1"        # manual ↻: skip the 1h cache
        m = await loop.run_in_executor(None, fetch_models, fresh)   # blocking HTTP off-loop
        self.write(json.dumps({"models": m or []}))


class ExportHandler(AuthMixin, tornado.web.RequestHandler):
    """Download a session's raw transcript. Streamed, because these run to hundreds
    of MB. The download keeps the name '<session-id>.jsonl' on purpose: the CLI
    resolves --resume by filename, so a prettier name would break re-import."""
    async def get(self):
        if not self._ok_auth():
            return
        cc = self.get_argument("cc", "")
        path = find_transcript(cc)
        if not path:
            self.set_status(404)
            self.write("no transcript for that session")
            return
        self.set_header("Content-Type", "application/x-ndjson")
        self.set_header("Content-Disposition", 'attachment; filename="%s.jsonl"' % cc)
        self.set_header("Content-Length", str(os.path.getsize(path)))
        with open(path, "rb") as f:
            while True:
                chunk = f.read(1 << 20)
                if not chunk:
                    break
                self.write(chunk)
                await self.flush()


class ImportHandler(AuthMixin, tornado.web.RequestHandler):
    """Adopt a transcript exported from another machine, into the project folder for
    `cwd`.

    The cwd recorded *inside* each line is rewritten to the target. `claude --resume`
    would not care — it finds a session by folder and filename — but this console's
    own sidebar reads that field and drops any session whose directory does not exist
    locally. An import from another machine always carries a foreign path (a different
    user's home, say), so leaving it verbatim files the session away somewhere
    invisible: resumable in principle, unreachable in the UI."""
    def post(self):
        if not self._ok_auth():
            return
        self.set_header("Content-Type", "application/json")

        def fail(msg, code=400):
            self.set_status(code)
            self.write(json.dumps({"ok": False, "error": msg}))

        cwd = (self.get_body_argument("cwd", "") or "").strip()
        if not cwd:
            return fail("pick a project folder first")
        cwd = os.path.abspath(os.path.expanduser(cwd))
        if not os.path.isdir(cwd):
            return fail("not a folder: %s" % cwd)
        files = self.request.files.get("file") or []
        if not files:
            return fail("no file uploaded")
        up = files[0]
        body = up["body"]
        if not body:
            return fail("empty file")

        cc = transcript_session_id(body)
        if not cc:   # fall back to the filename stem when no line carries a sessionId
            cc = os.path.splitext(os.path.basename(up.get("filename") or ""))[0]
        if not _valid_cc(cc):
            return fail("not a claude transcript (no sessionId found)")

        dest_dir = os.path.join(CLAUDE_ROOT, proj_folder(cwd))
        dest = os.path.join(dest_dir, cc + ".jsonl")
        if os.path.exists(dest):
            return fail("this session already exists in that folder", 409)
        rewritten = 0
        try:
            os.makedirs(dest_dir, exist_ok=True)
            tmp = dest + ".part"
            # stream rather than build a second copy: transcripts run to hundreds of MB
            with open(tmp, "wb") as f:
                for raw in io.BytesIO(body):
                    s = raw.strip()
                    if not s:
                        continue
                    try:
                        d = json.loads(s)
                    except Exception:
                        f.write(raw)          # keep unparseable lines untouched
                        continue
                    if isinstance(d, dict) and d.get("cwd") and d["cwd"] != cwd:
                        d["cwd"] = cwd
                        rewritten += 1
                        f.write(json.dumps(d, ensure_ascii=False).encode() + b"\n")
                    else:
                        f.write(raw if raw.endswith(b"\n") else raw + b"\n")
            # transcripts are plaintext and may hold anything a tool printed, so match
            # the 0600 the CLI writes — an upload must not land world-readable.
            os.chmod(tmp, 0o600)
            os.replace(tmp, dest)
        except Exception as e:
            return fail("write failed: %s" % e, 500)
        # the same id living in another folder is fine (verified) — just say so
        elsewhere = [os.path.basename(os.path.dirname(p))
                     for p in glob.glob(os.path.join(CLAUDE_ROOT, "*", cc + ".jsonl"))
                     if os.path.realpath(p) != os.path.realpath(dest)]
        self.write(json.dumps({"ok": True, "cc": cc, "cwd": cwd, "bytes": len(body),
                               "rewritten": rewritten, "also_in": elsewhere}))


class SearchHandler(AuthMixin, tornado.web.RequestHandler):
    """Full-text search across every stored conversation. The scan and the SQLite
    work both run off the IO loop so a 2 s global query never stalls live sessions."""
    async def get(self):
        if not self._ok_auth():
            return
        self.set_header("Content-Type", "application/json")
        q = self.get_argument("q", "")
        scope = self.get_argument("scope", "all")
        cc = self.get_argument("cc", "")
        cwd = self.get_argument("cwd", "")
        loop = tornado.ioloop.IOLoop.current()
        res = await loop.run_in_executor(
            None, search_history, q, scope, cc, cwd, 200)
        self.write(json.dumps(res))


class ThreadHandler(AuthMixin, tornado.web.RequestHandler):
    """Read-only context around a search hit, for the viewer. Deliberately separate
    from resuming: opening a past conversation to read it must never disturb, or take
    over, the session the user is presently talking to."""
    async def get(self):
        if not self._ok_auth():
            return
        self.set_header("Content-Type", "application/json")
        loop = tornado.ioloop.IOLoop.current()
        res = await loop.run_in_executor(
            None, load_thread, self.get_argument("mid", "0"),
            min(400, int(self.get_argument("before", "40") or 40)),
            min(400, int(self.get_argument("after", "40") or 40)))
        self.write(json.dumps(res))


CHAT_SESSIONS = {}  # id -> ChatSession (live, independent of any browser connection)


def safe_cwd(cwd):
    rp = os.path.realpath(cwd or "")
    if not (rp == os.path.realpath(HOME) or rp.startswith(os.path.realpath(HOME) + os.sep)):
        return None
    return rp if os.path.isdir(rp) else None


def _sanitize_mode(m):
    return m if m in ("acceptEdits", "plan", "default", "bypassPermissions") else "acceptEdits"

EFFORTS = ("low", "medium", "high", "xhigh", "max", "ultracode")
_ONCE_RE = re.compile(r"^@([\w.\-\[\]]+)[ \t]+(?=\S)")
CLI_MODEL_ALIASES = ("opus", "sonnet", "haiku")

def resolve_model_token(tok):
    """A CLI alias, an exact id from the model list, or a fragment of one. The
    list is the API's, newest first, so `fable` picks the newest fable. Whatever
    it picks, the long-window `[1m]` variant is used when the list has one: a
    one-off message on another model is usually made from inside a long session,
    and the plain window would only refuse it. An exact id is used as typed, so
    `@claude-sonnet-5` is the way to ask for the plain window. Cached list only:
    this runs on the IO loop at dispatch and must not go to the network."""
    t = tok.lower()
    base = t[:-4] if t.endswith("[1m]") else t
    ids = [m["id"] for m in (_models_cache["v"] or []) if m.get("id")]
    for mid in ids:
        if mid.lower() == t:
            return mid
    def long(mid):
        return mid + "[1m]" if mid + "[1m]" in ids else mid
    for mid in ids:
        if not mid.endswith("[1m]") and base in mid.lower():
            return long(mid)
    if base in CLI_MODEL_ALIASES:
        # no list yet: the CLI resolves opus[1m] / sonnet[1m] itself; haiku has no
        # long window (the API answers 400), so it goes plain
        return base if base == "haiku" else base + "[1m]"
    return None

def split_model_prefix(text):
    """`@haiku fix the typos` → ("haiku", "fix the typos", "haiku"): one message
    on another model without leaving the session or remembering to switch back.
    A token that names no model is left in the text untouched, since "@bob is my
    colleague" is a sentence and a typo must not vanish silently; the token is
    still returned so the caller can say what happened."""
    m = _ONCE_RE.match(text.lstrip())
    if not m:
        return None, text, None
    tok = m.group(1)
    model = resolve_model_token(tok)
    if not model:
        return None, text, tok
    return model, text.lstrip()[m.end():], tok


def _sanitize_effort(e):
    return e if e in EFFORTS else "max"   # default: deepest thinking
# "ultracode" = xhigh thinking + standing multi-agent Workflow orchestration. The
# CLI's session toggle is interactive-only (--effort rejects it; /effort is
# unavailable headless), so we emulate it: launch with --effort xhigh and append
# the "ultracode" keyword trigger to every outgoing user message (each turn
# opts in → standing). Requires the plan's workflow keyword trigger (default-on).


class ChatSession:
    """A persistent Claude Agent SDK client, independent of any browser connection.
    Survives navigation/reload (viewers attach/detach); ends only on explicit end().
    Provides per-action approval (can_use_tool) and interrupt()."""

    def __init__(self, sid, cwd, model, mode, resume_cc=None, effort="max"):
        self.id = sid
        self.cwd = cwd
        self.model = model
        self.mode = mode
        self.effort = _sanitize_effort(effort)   # thinking depth (--effort, launch-time)
        self.client = None
        self.log = []          # full normalized-event history, for replay on reattach
        self.viewers = set()   # currently-attached ChatSockets
        self.busy = False
        self.ended = False
        self.cc_id = resume_cc   # claude's own session_id; preset when resuming
        self.resume_cc = resume_cc
        self._pending = {}       # approval_id -> asyncio.Future
        self._aid = 0
        self.ctx = None          # latest context-window usage (get_context_usage)
        self.queue = []          # messages typed while busy; dispatched on turn_done
        self._qid = 0
        self._control_turn = False   # a /effort turn: settings traffic, not an answer, so no done chip
        self._once = None         # model the CLI was switched to for the running turn only (a @model prefix); put back at turn_done
        self.turn_started = None  # wall time the current turn began (for the timer)
        self.turn_word = 0        # seed for the "thinking" word, stable across reattach
        self.compacting = False   # True while a manual /compact is running
        # live streaming token counters (ephemeral; pushed to the pill, never logged)
        self._tok_up = 0          # input tokens of the latest request (↑)
        self._tok_out = 0         # output tokens so far (↓; estimated, exact at msg end)
        self._tok_chars = 0       # streamed output chars, for the live estimate
        self._tok_exact = False   # True once message_delta gives the real output_tokens
        self._last_tok_emit = 0.0
        self._step = 0            # agentic-step counter; bumps the spinner verb per step
        # session-recap ("away summary") bookkeeping
        self.last_activity = time.time()   # wall time of the last turn boundary (idle clock)
        self.recap_for = None     # the last_activity value already recapped (dedup)
        self.recap_busy = False   # True while a recap generation is in flight
        self._compact_turn = False  # was the in-flight turn a manual /compact? (no "Baked" line)
        self.bg_tasks = {}        # live run_in_background tasks: task_id -> {desc, tool_use_id}
        # the plan, folded out of TaskCreate/TaskUpdate tool traffic (see _fold_tasks)
        self.tasks = {}           # task id -> {subject, description, activeForm, status}
        self._task_open = {}      # TaskCreate tool_use id -> the task, until a result names it
        self._task_seq = 0        # creation ordinal; the fallback id if that text ever changes
        self._since_plan = 0      # tool calls since the plan was last touched (PostToolUse nudge)
        self._agents = {}         # Agent tool_use id -> its subagent's {up, out, steps}, for the card
        self._agent_reqs = {}     # Agent tool_use id -> message ids already counted
        self._agent_tasks = {}    # CLI task id -> Agent tool_use id, for the task_* lifecycle messages
        self.event_seq = 0        # monotonic event number, so a viewer can ask for the gap

    def preload(self):
        """Populate history from the on-disk transcript before resuming."""
        if self.resume_cc:
            self.log = load_transcript_events(self.resume_cc)
            for ev in self.log:
                self.event_seq += 1
                ev["_seq"] = self.event_seq
            self._fold_tasks(self.log)   # the plan the session was left holding

    def title(self):
        """Custom label if the user set one, else the first user message."""
        if self.cc_id:
            nm = load_names().get(self.cc_id)
            if nm:
                return nm
        for e in self.log:
            if e.get("kind") == "user_text" and (e.get("text") or "").strip():
                return e["text"].strip().replace("\n", " ")[:60]
        return ""

    async def start(self):
        if not HAVE_SDK:
            raise RuntimeError("claude-agent-sdk not installed")
        # Launch with --dangerously-skip-permissions so the user can switch to
        # the "Full auto" (bypassPermissions) mode at runtime — the CLI refuses
        # set_permission_mode('bypassPermissions') unless the session was started
        # with that flag. It only UNLOCKS the capability: the session still starts
        # in self.mode and can_use_tool still fires for approvals / AskUserQuestion
        # in default & acceptEdits (verified) — it is not forced bypass.
        opts = ClaudeAgentOptions(
            cwd=self.cwd, permission_mode=self.mode, can_use_tool=self._can_use_tool,
            add_dirs=[self.cwd], cli_path=CLAUDE_BIN, include_partial_messages=True,
            max_buffer_size=MAX_BUFFER,
            extra_args={"dangerously-skip-permissions": None})
        # the console's own plan tool and the three reminders that make it used (PLAN_TOOL)
        opts.mcp_servers = {"console": self._plan_server()}
        opts.allowed_tools = [PLAN_TOOL]
        opts.hooks = {"UserPromptSubmit": [HookMatcher(hooks=[self._hook_prompt])],
                      "PostToolUse": [HookMatcher(hooks=[self._hook_post_tool])],
                      "Stop": [HookMatcher(hooks=[self._hook_stop])]}
        if self.model and self.model != "default":
            opts.model = self.model
        if self.effort:
            # --effort only accepts low..max; ultracode launches at its xhigh base
            opts.effort = "xhigh" if self.effort == "ultracode" else self.effort
        if self.resume_cc:
            opts.resume = self.resume_cc
        self.client = ClaudeSDKClient(options=opts)
        await self.client.connect()
        tornado.ioloop.IOLoop.current().spawn_callback(self._consume)

    def _plan_server(self):
        """The console's plan tool, served in-process. Its result text carries the
        next nudge, since that is the one place the model is certain to read."""
        @tool("plan", "Keep the user's pinned progress panel current. Send the FULL list "
              "of steps every time the plan changes.", PLAN_SCHEMA)
        async def plan(args):
            steps = [x for x in ((args or {}).get("steps") or []) if isinstance(x, dict)]
            done = sum(1 for x in steps if x.get("status") == "completed")
            cur = next((x.get("title") for x in steps if x.get("status") == "in_progress"), None)
            txt = "ok (%d/%d done)" % (done, len(steps))
            if cur:
                txt += " · in progress: %s — call plan again when it finishes" % cur
            return {"content": [{"type": "text", "text": txt}]}
        return create_sdk_mcp_server(name="console", version="1.0.0", tools=[plan])

    def _plan_open(self):
        """The step in progress, else the first not completed; None when there is
        no plan or it is done."""
        ts = self.plan_tasks()
        for t in ts:
            if t.get("status") == "in_progress":
                return t
        for t in ts:
            if t.get("status") != "completed":
                return t
        return None

    async def _hook_prompt(self, input_data, tool_use_id, context):
        """UserPromptSubmit: the plan protocol, beside the prompt (see PLAN_TOOL)."""
        if ((input_data or {}).get("prompt") or "").lstrip().startswith("/"):
            return {}
        return {"hookSpecificOutput": {"hookEventName": "UserPromptSubmit",
                                       "additionalContext": PLAN_PROMPT}}

    async def _hook_post_tool(self, input_data, tool_use_id, context):
        """PostToolUse: after PLAN_NUDGE_EVERY calls with a step still open, remind."""
        if (input_data or {}).get("agent_id"):      # fired inside a subagent: not its plan
            return {}
        if (input_data or {}).get("tool_name") == PLAN_TOOL:
            self._since_plan = 0
            return {}
        self._since_plan += 1
        cur = self._plan_open()
        if not cur:
            # working with no open plan: one nudge, at the first call of the turn,
            # which is the moment the model can still tell how many steps it has
            # (a finished plan from an earlier turn does not count as one)
            if self._since_plan != 1:
                return {}
            return {"hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext":
                    "Progress panel: no plan is on the board. If this request takes more than "
                    "one step, call mcp__console__plan now with the full list of steps."}}
        if self._since_plan < PLAN_NUDGE_EVERY:
            return {}
        self._since_plan = 0
        return {"hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext":
                "Progress panel: '%s' is still %s. If it is finished, or the plan has changed, "
                "call mcp__console__plan with the full list before going on."
                % (cur.get("subject") or "", cur.get("status") or "pending")}}

    async def _hook_stop(self, input_data, tool_use_id, context):
        """Stop: an open step blocks the stop once, so the panel is settled before
        the answer stands. stop_hook_active is the CLI's own loop guard."""
        if (input_data or {}).get("stop_hook_active") or not self._plan_open():
            return {}
        return {"decision": "block", "reason":
                "Before finishing, bring the progress panel up to date: call mcp__console__plan "
                "with the full list, marking finished steps completed and dropping steps you will "
                "not do. Then finish."}

    async def _can_use_tool(self, tool_name, tool_input, context):
        """SDK permission hook. AskUserQuestion is answered in-band: we surface the
        choices, collect the user's picks, and feed them back via
        updated_input['answers'] so the tool resolves with the real answer. Every
        other tool gets a plain Approve/Deny prompt. Both await the browser."""
        if tool_name == PLAN_TOOL:       # ours; the panel is the whole point of it
            return PermissionResultAllow()
        self._aid += 1
        aid = "ap%d" % self._aid
        fut = asyncio.get_running_loop().create_future()
        self._pending[aid] = fut

        if tool_name == "AskUserQuestion":
            qs = (tool_input or {}).get("questions") or []
            self._push([{"kind": "question", "aid": aid, "questions": qs}])
            try:
                picks = await fut          # list indexed by question, or None
            except Exception:
                picks = None
            answers = {}
            if isinstance(picks, list):
                for i, q in enumerate(qs):
                    val = picks[i] if i < len(picks) else None
                    if val:
                        key = (q.get("question") or q.get("header")
                               or "Question %d" % (i + 1))
                        answers[key] = val
            if not answers:
                self._push([{"kind": "question_resolved", "aid": aid, "answers": None}])
                return PermissionResultDeny(message="User dismissed the question")
            self._push([{"kind": "question_resolved", "aid": aid, "answers": answers}])
            new_input = dict(tool_input or {})
            new_input["answers"] = answers
            return PermissionResultAllow(updated_input=new_input)

        # CLI-supplied "don't ask again" rules; enables the 3rd option when present
        sugg = list(getattr(context, "suggestions", None) or [])
        self._push([{"kind": "approval", "aid": aid, "tool": tool_name,
                     "input": _cap_input(tool_input),
                     "toolId": getattr(context, "tool_use_id", None),
                     "always": bool(sugg)}])
        try:
            res = await fut
        except Exception:
            res = {"allow": False}
        allow = bool(res.get("allow")) if isinstance(res, dict) else bool(res)
        always = bool(isinstance(res, dict) and res.get("always") and sugg)
        self._push([{"kind": "approval_resolved", "aid": aid, "allow": allow, "always": always}])
        if not allow:
            return PermissionResultDeny(message="Denied by user")
        if always:
            # allow now AND apply the CLI's suggested rules → won't ask again this session
            return PermissionResultAllow(updated_permissions=sugg)
        return PermissionResultAllow()

    def resolve_approval(self, aid, allow, always=False):
        fut = self._pending.pop(aid, None)
        if fut and not fut.done():
            fut.set_result({"allow": bool(allow), "always": bool(always)})

    def resolve_answer(self, aid, answers):
        fut = self._pending.pop(aid, None)
        if fut and not fut.done():
            fut.set_result(answers if isinstance(answers, list) else None)

    async def _consume(self):
        try:
            async for msg in self.client.receive_messages():
                if isinstance(msg, StreamEvent):
                    self._on_stream_event(msg)   # ephemeral token ticks; not logged
                    continue
                evs = self._normalize(msg)
                for e in evs:
                    if e["kind"] == "turn_done":
                        if not (self._compact_turn or self._control_turn):   # /compact and /effort aren't "Baked" turns
                            e["done_word"] = secrets.choice(DONE_PAST)
                            e["done_at"] = time.time()   # UTC epoch of completion
                            if self.turn_started:
                                e["dur_ms"] = int((time.time() - self.turn_started) * 1000)
                        if self.bg_tasks:   # still-running run_in_background tasks at this boundary
                            e["bg_running"] = [t.get("desc") or "background task"
                                               for t in self.bg_tasks.values()]
                        self._compact_turn = False
                        self._control_turn = False
                        self.busy = False
                        self.turn_started = None
                        self.compacting = False
                        self.last_activity = time.time()   # idle clock starts now
                        if self._once:                     # a @model turn: back to the session's model
                            tornado.ioloop.IOLoop.current().spawn_callback(self._restore_model)
                    elif e["kind"] == "compacted":
                        self.compacting = False
                    elif e["kind"] == "ready":
                        self.cc_id = e.get("session_id") or self.cc_id
                        if self.cc_id:   # record this session's mode/model once
                            save_pref(self.cc_id, mode=self.mode, model=self.model, effort=self.effort)
                if evs:
                    self._push(evs)
                # refresh context-window usage after a turn settles, on init, or
                # after a compaction (which slashes the token count)
                if any(e["kind"] in ("turn_done", "ready", "compacted") for e in evs):
                    await self._refresh_context()
                # a turn just settled → send the next queued message, if any.
                # Queued messages are NO LONGER auto-steered at tool boundaries:
                # the CLI delivers a steered message only as a <system-reminder>
                # aside ("address it as you continue this turn"), which the model
                # is free to note and never answer — measured compliance was ~1/3
                # even though API captures showed 100% delivery. Held messages
                # become first-class turns instead, which always get a reply;
                # steering is still available per message via the chip's ⚡.
                if any(e["kind"] == "turn_done" for e in evs):
                    self._drain_queue()
        except Exception as ex:
            self._emit({"type": "stderr", "text": "stream ended: %r" % ex})
        self.busy = False
        self.ended = True
        self._emit({"type": "exit", "code": 0})

    async def _refresh_context(self):
        """Pull the /context breakdown from the SDK and push it to viewers."""
        if not self.client or self.ended:
            return
        try:
            u = await asyncio.wait_for(self.client.get_context_usage(), 6)
        except Exception:
            return
        if not isinstance(u, dict):
            return
        self.ctx = {"totalTokens": u.get("totalTokens"), "maxTokens": u.get("maxTokens"),
                    "percentage": u.get("percentage"), "model": self._shown_model(u.get("model"))}
        self._emit({"type": "context", "ctx": self.ctx})

    def _live_ctx(self, total):
        """Live context-meter update from a request's input usage mid-turn (input +
        cache_read + cache_creation ≈ the whole window the model sees, images
        included). Needs a maxTokens from a prior get_context_usage; the exact
        /context value snaps back in at the next turn boundary (_refresh_context)."""
        if not total:
            return
        prev = self.ctx or {}
        mx = prev.get("maxTokens")
        if not mx:
            return
        self.ctx = {"totalTokens": total, "maxTokens": mx,
                    "percentage": round(total * 100.0 / mx, 1),
                    "model": prev.get("model")}
        self._emit({"type": "context", "ctx": self.ctx})

    def _recap_transcript(self):
        """Compact text view of the recent conversation for the recap model.
        Returns '' when there's nothing worth recapping yet (no assistant turn)."""
        lines, has_asst = [], False
        for e in self.log:
            k = e.get("kind")
            if k == "user_text":
                t = (e.get("text") or "").strip()
                if t:
                    lines.append("User: " + t)
            elif k == "assistant_text":
                t = (e.get("text") or "").strip()
                if t:
                    lines.append("Assistant: " + t); has_asst = True
            elif k == "tool_use":
                lines.append("[tool: %s]" % (e.get("tool") or "?"))
        if not has_asst:
            return ""
        return "\n".join(lines)[-6000:]   # tail keeps the most recent context

    async def _make_recap(self):
        """Generate a one-line 'away summary' and push it to viewers (logged, so it
        also replays when you return). Isolated Haiku call: no tools, no settings /
        CLAUDE.md, single turn — cheap and side-effect-free."""
        if (not RECAP_ENABLED or self.busy or self.ended or self.compacting
                or self.recap_busy or not self.client
                or self.recap_for == self.last_activity):
            return
        transcript = self._recap_transcript()
        if not transcript:
            return
        self.recap_busy = True
        text = None
        try:
            # isolate the recap query's transcript into a junk dir so it never
            # lands in (and pollutes) a real project's session list. cwd is
            # cosmetic here: no tools, no settings — pure text summarization.
            recap_cwd = os.path.join(HOME, ".cache", "claude-console-recap")
            os.makedirs(recap_cwd, exist_ok=True)
            opts = ClaudeAgentOptions(
                model=RECAP_MODEL, cwd=recap_cwd, cli_path=CLAUDE_BIN,
                system_prompt=RECAP_SYS, allowed_tools=[], max_turns=1,
                max_buffer_size=MAX_BUFFER, setting_sources=[])
            chunks = []
            async def _run():
                async for m in query(
                        prompt="Transcript so far:\n\n" + transcript + "\n\nRecap:",
                        options=opts):
                    if isinstance(m, AssistantMessage):
                        for b in m.content:
                            if isinstance(b, TextBlock):
                                chunks.append(b.text)
            await asyncio.wait_for(_run(), 30)
            line = " ".join(chunks).strip()
            text = line.splitlines()[0].strip() if line else None
        except Exception:
            text = None
        finally:
            self.recap_busy = False
        # a turn may have started while we were generating — don't recap over it
        if text and not self.busy and not self.ended:
            self.recap_for = self.last_activity
            self._push([{"kind": "recap", "text": _cap(text, 400)}])

    def _shown_model(self, reported):
        """The CLI reports the model it resolved. `[1m]` is the CLI's own selector
        rather than a model, and for Fable 5.1 the CLI resolves it away and reports
        the bare id (the window is a million tokens either way; the context report
        for such a session says maxTokens 1000000 next to `claude-fable-5-1`). Opus
        gets echoed back with the suffix. So one picker choice read as two different
        strings in the ready line. When the report is the chosen model minus that
        suffix, show the choice; anything else the CLI says is shown as it said it."""
        chosen = self._once or self.model or ""
        if reported and chosen.endswith("[1m]") and chosen[:-4] == reported:
            return chosen
        return reported

    def _normalize(self, msg):
        evs = []
        if isinstance(msg, SystemMessage):
            if msg.subtype == "init":
                d = msg.data or {}
                evs.append({"kind": "ready", "session_id": d.get("session_id"),
                            "model": self._shown_model(d.get("model")), "cwd": d.get("cwd"),
                            "effort": self.effort})
            elif msg.subtype == "compact_boundary":
                d = msg.data or {}
                # On-disk transcript uses camelCase (compactMetadata/preTokens);
                # the stream-json the SDK delivers uses snake_case — accept both.
                cm = d.get("compactMetadata") or d.get("compact_metadata") or {}
                def g(*ks):
                    for k in ks:
                        v = cm.get(k)
                        if v is None:
                            v = d.get(k)
                        if v is not None:
                            return v
                    return None
                evs.append({"kind": "compacted", "trigger": g("trigger"),
                            "pre": g("preTokens", "pre_tokens"),
                            "post": g("postTokens", "post_tokens"),
                            "ms": g("durationMs", "duration_ms")})
            elif msg.subtype == "task_started":
                # a run_in_background Bash, or a subagent (the CLI backgrounds parallel
                # Agent calls on its own: the Agent tool result is then only a launch
                # notice and the turn ends while the agents still run) → track it as a
                # live background task; a subagent's card is told to stay running
                d = msg.data or {}
                tid, tu = d.get("task_id"), d.get("tool_use_id")
                if tid and tu and d.get("task_type") == "local_agent":
                    self._agent_tasks[tid] = tu
                if tid and (d.get("task_type") == "local_bash" or d.get("is_backgrounded")):
                    self.bg_tasks[tid] = {"desc": d.get("description") or "", "tool_use_id": tu}
                if tid and tu and d.get("is_backgrounded"):
                    evs.append({"kind": "agent_state", "toolId": tu, "state": "background"})
            elif msg.subtype == "task_progress":
                # what the subagent is doing right now; ephemeral, the card shows it
                d = msg.data or {}
                tu = d.get("tool_use_id") or self._agent_tasks.get(d.get("task_id"))
                if tu and d.get("description"):
                    self._emit({"type": "agent_progress", "toolId": tu, "desc": d["description"]})
            elif msg.subtype == "task_notification":
                # authoritative completion notice (carries task_id + status + summary)
                d = msg.data or {}
                self.bg_tasks.pop(d.get("task_id"), None)
                tu = d.get("tool_use_id") or self._agent_tasks.pop(d.get("task_id"), None)
                if tu:
                    self._agent_tasks.pop(d.get("task_id"), None)
                    evs.append({"kind": "agent_done", "toolId": tu, "status": d.get("status") or "completed",
                                "summary": _cap(d.get("summary") or "", RESULT_CAP),
                                "agent": dict(self._agents.get(tu) or {})})
            elif msg.subtype == "task_updated":
                # faster completion path: a status patch reaching a terminal state
                d = msg.data or {}
                st = (d.get("patch") or {}).get("status")
                if st in ("completed", "failed", "killed", "error"):
                    self.bg_tasks.pop(d.get("task_id"), None)
                    # the notification with the summary normally follows at once; if it
                    # never does, the card must still stop saying it is running
                    tu = self._agent_tasks.get(d.get("task_id"))
                    if tu and st != "completed":
                        self._agent_tasks.pop(d.get("task_id"), None)
                        evs.append({"kind": "agent_done", "toolId": tu, "status": st, "summary": "",
                                    "agent": dict(self._agents.get(tu) or {})})
        elif isinstance(msg, AssistantMessage):
            if not self.cc_id and getattr(msg, "session_id", None):
                self.cc_id = msg.session_id
            for b in msg.content:
                if isinstance(b, TextBlock) and b.text.strip():
                    evs.append({"kind": "assistant_text", "text": _cap(b.text)})
                elif isinstance(b, ThinkingBlock) and b.thinking.strip():
                    evs.append({"kind": "thinking", "text": _cap(b.thinking)})
                elif isinstance(b, ToolUseBlock):
                    evs.append({"kind": "tool_use", "tool": b.name,
                                "input": _cap_input(b.input), "toolId": b.id})
        elif isinstance(msg, UserMessage):
            c = msg.content
            if isinstance(c, list):
                for b in c:
                    if isinstance(b, ToolResultBlock):
                        evs.append({"kind": "tool_result", "toolId": b.tool_use_id,
                                    "content": _cap(_txt(b.content), RESULT_CAP),
                                    "isError": bool(b.is_error)})
        elif isinstance(msg, ResultMessage):
            if getattr(msg, "session_id", None) and not self.cc_id:
                self.cc_id = msg.session_id
            evs.append({"kind": "turn_done", "subtype": msg.subtype,
                        "isError": msg.is_error, "numTurns": msg.num_turns,
                        "cost": msg.total_cost_usd})
        # A subagent's messages carry the id of the Agent call that spawned it. The
        # tag is what lets the console keep its run inside that one card instead of
        # spliced into the conversation as if the main agent had done it.
        par = getattr(msg, "parent_tool_use_id", None)
        if par:
            for e in evs:
                e["parent"] = par
            if isinstance(msg, AssistantMessage):
                self._agent_usage(par, msg)
        elif isinstance(msg, UserMessage):
            for e in evs:   # the Agent call's own result: carry the totals, so replay has them
                a = self._agents.get(e.get("toolId")) if e.get("kind") == "tool_result" else None
                if a:
                    e["agent"] = dict(a)
        return evs

    def _on_stream_event(self, msg):
        """Partial-message stream events (include_partial_messages) → live ↑/↓ token
        counts in the status pill. message_start carries the request's input usage;
        content_block_delta lets us estimate output as it streams; message_delta
        snaps it to the exact output_tokens. All ephemeral — emitted straight to
        viewers, never appended to self.log (would flood replay on reattach)."""
        e = getattr(msg, "event", None) or {}
        et = e.get("type")
        if getattr(msg, "parent_tool_use_id", None):
            return                  # a subagent's request: not the pill's numbers, not the context meter's
        if et == "message_start":
            # each model request = one agentic step. Re-pick the spinner verb on
            # every step after the first (the first keeps turn_start's word), so the
            # word changes per tool round like the CLI; pushed to viewers below.
            self._step += 1
            if self._step > 1:
                self.turn_word = secrets.randbelow(100000)
            u = (e.get("message") or {}).get("usage") or {}
            # ↑ = tokens actually sent/processed fresh THIS request: new uncached
            # input + content newly written to cache. EXCLUDES cache_read (the prior
            # context replayed from cache — not re-uploaded). So ↑ stays small each
            # turn/step instead of always showing the whole ~100k cached context
            # (which the header context meter already tracks).
            self._tok_up = ((u.get("input_tokens") or 0)
                            + (u.get("cache_creation_input_tokens") or 0))
            self._tok_out = u.get("output_tokens") or 0
            self._tok_chars = 0
            self._tok_exact = False
            self._emit_tokens(force=True)
            # live context meter: input + cache_read + cache_creation = the full
            # context the model just received (images included) — replaces the stale
            # turn-boundary snapshot; the exact /context value snaps back at turn end.
            self._live_ctx((u.get("input_tokens") or 0)
                           + (u.get("cache_read_input_tokens") or 0)
                           + (u.get("cache_creation_input_tokens") or 0))
        elif et == "content_block_delta":
            d = e.get("delta") or {}
            # live prose → viewers as it arrives. Ephemeral on purpose: the
            # authoritative text lands later as a logged `asst` event, so streaming
            # deltas must never enter self.log or a reattach would replay the answer
            # twice. Only top-level assistant text — thinking has its own lane and a
            # subagent's text belongs to its tool card, not this bubble.
            if d.get("text") and not getattr(msg, "parent_tool_use_id", None):
                self._emit({"type": "events",
                            "events": [{"kind": "stream_text", "text": d["text"]}]})
            if self._tok_exact:
                return
            t = d.get("text") or d.get("thinking") or d.get("partial_json") or ""
            if t:
                self._tok_chars += len(t)
                # ~4 chars/token; snapped to the exact count at message_delta
                self._tok_out = max(self._tok_out, (self._tok_chars + 3) // 4)
                self._emit_tokens()
        elif et == "message_delta":
            ot = (e.get("usage") or {}).get("output_tokens")
            if ot is not None:
                self._tok_out = ot
                self._tok_exact = True
                self._emit_tokens(force=True)

    def _agent_usage(self, par, msg):
        """A subagent's request usage, read off its assistant messages (its stream
        events never reach the SDK client) and shown on its Agent card. Same ↑
        convention as the pill: fresh input plus cache writes, never cache reads.
        One assistant message can arrive in several pieces that all repeat the
        request's usage, so a request counts once, by message id."""
        u = getattr(msg, "usage", None)
        if not isinstance(u, dict):
            return
        a = self._agents.setdefault(par, {"up": 0, "out": 0, "steps": 0})
        seen = self._agent_reqs.setdefault(par, set())
        mid = getattr(msg, "message_id", None) or id(msg)
        if mid in seen:
            return
        seen.add(mid)
        a["up"] += (u.get("input_tokens") or 0) + (u.get("cache_creation_input_tokens") or 0)
        # output_tokens on this message is the request's opening figure, not its
        # final one (the exact count travels in a stream event we never see), so
        # the content itself is the better measure: ~4 chars a token, as the pill
        # estimates mid-stream. Flagged, since it stays an estimate.
        chars = 0
        for b in getattr(msg, "content", None) or []:
            chars += len(getattr(b, "text", "") or "") + len(getattr(b, "thinking", "") or "")
            if getattr(b, "input", None) is not None:
                chars += len(json.dumps(b.input))
        est = (chars + 3) // 4
        rep = u.get("output_tokens") or 0
        a["out"] += max(rep, est)
        if est > rep:
            a["est"] = True
        a["steps"] += 1
        self._emit({"type": "agent_tokens", "parent": par, "up": a["up"], "out": a["out"],
                    "steps": a["steps"], "est": bool(a.get("est"))})

    def _emit_tokens(self, force=False):
        """Throttled ephemeral push of the current ↑/↓ token counts to the pill."""
        now = time.time()
        if not force and (now - self._last_tok_emit) < 0.12:
            return
        self._last_tok_emit = now
        self._emit({"type": "tokens", "up": self._tok_up,
                    "out": self._tok_out, "exact": self._tok_exact,
                    "word": self.turn_word})

    def turn_age(self):
        """Seconds the current turn has been running (0 if idle) — lets a
        re-attaching viewer resume the elapsed-time display instead of resetting."""
        return (time.time() - self.turn_started) if (self.busy and self.turn_started) else 0

    def send_user(self, text, images=None, files=None):
        images = [im for im in (images or []) if im.get("data")]
        files = [f for f in (files or []) if isinstance(f, dict) and f.get("data")]
        if (not text.strip() and not images and not files) or not self.client or self.ended:
            return
        if self.busy:
            # a turn is running — queue it. It's injected into the live turn at
            # the next tool boundary (steering), or dispatched when the turn ends.
            self._qid += 1
            qid = "q%d" % self._qid
            # the bodies ride along in memory until dispatch: nothing is written
            # to disk for a message that might still be withdrawn
            self.queue.append({"qid": qid, "text": text, "images": images,
                               "files": files})
            ev = {"kind": "queued", "qid": qid, "text": _cap(text)}
            if images:
                ev["images"] = len(images)
            if files:
                ev["files"] = [safe_upload_name(f.get("name")) for f in files]
            self._push([ev])
            return
        self._dispatch(text, images, files)

    def _make_payload(self, text, images):
        """A string (text-only) or an Anthropic-format async-iterable (multimodal)
        for client.query(); the string path can't carry image blocks."""
        if not images:
            return text
        content = []
        if text.strip():
            content.append({"type": "text", "text": text})
        for im in images:
            content.append({"type": "image", "source": {
                "type": "base64",
                "media_type": im.get("media_type") or "image/png",
                "data": im["data"]}})
        async def _gen():
            yield {"type": "user", "parent_tool_use_id": None,
                   "message": {"role": "user", "content": content}}
        return _gen()

    def _echo_user(self, text, images, files=None, qid=None, start=False):
        """Push the console events for one outgoing user message."""
        evs = []
        if qid:                       # came off the queue — drop its chip
            evs.append({"kind": "dequeued", "qid": qid})
        if start:                     # begins a fresh turn (vs mid-turn injection)
            evs.append({"kind": "turn_start", "word": self.turn_word})
        ue = {"kind": "user_text", "text": _cap(text)}
        if images:
            ue["images"] = len(images)
        if files:
            ue["files"] = list(files)    # names only: bodies never enter the log
        evs.append(ue)
        self._push(evs)

    def _dispatch(self, text, images, files=None, qid=None, start=True):
        """Send one message to the live client. start=True begins a new turn;
        start=False injects into the running turn without flipping busy."""
        shown = text                  # viewers see what was typed, @prefix included
        once, text, tok = split_model_prefix(text)
        if tok and not once:
            self._push([{"kind": "notice", "text": "ℹ @%s names no model here — sent as text on the session model" % tok}])
        elif once and not start:
            self._push([{"kind": "notice", "text": "ℹ @%s applies to a new turn; a steer stays on the running model" % tok}])
            once = None
        elif once and once == self.model:
            once = None
        if start:
            self.busy = True
            self.turn_started = time.time()
            self.turn_word = secrets.randbelow(100000)
            self._tok_up = self._tok_out = self._tok_chars = 0
            self._tok_exact = False
            self._step = 0
            self._since_plan = 0
            cmd0 = text.strip().split(None, 1)[0] if text.strip() else ""
            self.compacting = (cmd0 == "/compact")
            self._compact_turn = self.compacting
            self._control_turn = (cmd0 == "/effort")
        # written before the echo so the chips show the names that actually landed
        # (a collision renames test.py to test-2.py, and the user should see that)
        saved, up_errs = save_uploads(self.cwd, files)
        self._echo_user(shown, images, files=[rel.rsplit("/", 1)[-1] for rel, _ in saved],
                        qid=qid, start=start)
        for e in up_errs:
            self._push([{"kind": "notice", "text": "⚠ attachment — " + e}])
        if start and self.compacting:   # surface the otherwise-silent long compaction
            self._push([{"kind": "compacting", "word": self.turn_word}])
        client = self.client
        send_text = text + uploads_note(saved)
        if (self.effort == "ultracode" and text.strip()
                and not text.lstrip().startswith("/")
                and "ultracode" not in text.lower()):
            # ultracode mode: opt every turn into Workflow orchestration via the
            # keyword trigger (viewers still see the clean original text above)
            send_text = text.rstrip() + "\n\nultracode"
        payload = self._make_payload(send_text, images)
        async def _q():
            try:
                if once:
                    try:
                        await client.set_model(once)
                        self._once = once
                        self._emit({"type": "events", "events": [{"kind": "model", "model": once, "once": True}]})
                        self._notice("⚙ @%s → %s for this message" % (tok, once))
                    except Exception as ex:
                        self._push([{"kind": "notice", "text": "⚠ @%s refused by the CLI (%r) — sent on the session model" % (once, ex)}])
                elif self._once:
                    # the last @model turn never reported done (interrupted): back first
                    await self._restore_model()
                await client.query(payload)
            except Exception as ex:
                if start:
                    self.busy = False
                self._push([{"kind": "notice", "text": "send failed: %r" % ex}])
                self._drain_queue()    # don't strand the rest of the queue
        tornado.ioloop.IOLoop.current().spawn_callback(_q)

    def steer_now(self, qid):
        """Explicit per-message steering (the queue chip's ⚡). Writes the message
        into the running turn immediately — the CLI absorbs it at its own next
        boundary and shows it to the model as a <system-reminder> beside a tool
        result. Delivery is reliable (verified by API capture); a *visible reply*
        is not guaranteed, because the reminder tells the model to carry on with
        the current turn. That trade-off is why this is opt-in per message."""
        if not self.client or self.ended:
            return
        for i, it in enumerate(self.queue):
            if it["qid"] == qid:
                self.queue.pop(i)
                break
        else:
            return
        if not self.busy:                      # turn already over → normal dispatch
            self._dispatch(it["text"], it["images"], it.get("files"),
                           qid=it["qid"], start=True)
            return
        saved, up_errs = save_uploads(self.cwd, it.get("files"))
        self._echo_user(it["text"], it["images"],
                        files=[rel.rsplit("/", 1)[-1] for rel, _ in saved],
                        qid=it["qid"], start=False)
        for e in up_errs:
            self._push([{"kind": "notice", "text": "⚠ attachment — " + e}])
        self._push([{"kind": "notice",
                     "text": "⚡ steered into the running turn — Claude sees it at its "
                             "next step but may answer only after finishing current work"}])
        payload = self._make_payload(it["text"] + uploads_note(saved), it["images"])
        client = self.client
        async def _q():
            try:
                await client.query(payload)
            except Exception as ex:
                self._push([{"kind": "notice", "text": "steer failed: %r" % ex}])
        tornado.ioloop.IOLoop.current().spawn_callback(_q)

    def _drain_queue(self):
        """Dispatch the next queued message as a fresh turn (used once a turn has
        fully settled and anything still queued needs its own turn)."""
        if self.busy or self.ended or not self.client or not self.queue:
            return
        item = self.queue.pop(0)
        self._dispatch(item["text"], item["images"], item.get("files"),
                       qid=item["qid"], start=True)

    def unqueue(self, qid):
        """Withdraw a still-pending queued message (user is editing it)."""
        for i, it in enumerate(self.queue):
            if it["qid"] == qid:
                self.queue.pop(i)
                self._push([{"kind": "unqueued", "qid": qid}])
                return

    def interrupt(self):
        if self.client and not self.ended:
            if self.queue:        # interrupting also cancels pending queued messages
                self._push([{"kind": "unqueued", "qid": it["qid"]} for it in self.queue])
                self.queue = []
            client = self.client
            async def _i():
                try:
                    await client.interrupt()
                except Exception:
                    pass
            tornado.ioloop.IOLoop.current().spawn_callback(_i)

    async def _restore_model(self):
        """A @model turn is over: put the CLI back on the session's model."""
        self._once = None
        back = self.model or "default"
        try:
            await self.client.set_model(None if back == "default" else back)
            self._emit({"type": "events", "events": [{"kind": "model", "model": back, "once": False}]})
            self._notice("⚙ back to %s" % back)
        except Exception as ex:
            self._notice("model restore failed: %r" % ex)

    def _notice(self, text):
        """Transient note to current viewers (not persisted in the log)."""
        self._emit({"type": "events", "events": [{"kind": "notice", "text": text}]})

    def set_effort(self, eff):
        """Switch thinking depth on the live session by sending `/effort` down the
        stream. The CLI takes it there since 2.1.258; older builds answered
        "/effort isn't available in this environment", which is why this used to
        relaunch the session with a new --effort and replay its history. The
        exchange stays in the chat like any local command. ultracode is xhigh on
        the CLI side plus the keyword on every message."""
        if not self.client or self.ended or eff == self.effort:
            return
        prev, self.effort = self.effort, eff
        if self.cc_id:
            save_pref(self.cc_id, effort=eff)
        self._emit({"type": "events", "events": [{"kind": "effort", "effort": eff}]})
        level = lambda e: "xhigh" if e == "ultracode" else e
        if level(eff) != level(prev):
            self._dispatch("/effort " + level(eff), [], None, start=True)
        else:
            self._notice("⚙ effort → %s (this session)" % eff)

    def set_model(self, model):
        """Switch the model on the live session (SDK set_model). 'default'→None."""
        if not self.client or self.ended:
            return
        m = None if (not model or model == "default") else model
        self.model = model or "default"   # optimistic; reflected in list/attach
        if self.cc_id:
            save_pref(self.cc_id, model=self.model)
        client = self.client
        async def _s():
            try:
                await client.set_model(m)
                self._notice("⚙ model → %s (this session)" % self.model)
                self._emit({"type": "events", "events": [{"kind": "model", "model": self.model, "once": False}]})
            except Exception as ex:
                self._notice("model change failed: %r" % ex)
        tornado.ioloop.IOLoop.current().spawn_callback(_s)

    def set_mode(self, mode):
        """Switch the permission mode on the live session (SDK set_permission_mode)."""
        if not self.client or self.ended:
            return
        mode = _sanitize_mode(mode)
        self.mode = mode
        if self.cc_id:
            save_pref(self.cc_id, mode=mode)
        client = self.client
        async def _s():
            try:
                await client.set_permission_mode(mode)
                self._notice("⚙ permission mode → %s (this session)" % mode)
                if mode == "bypassPermissions":
                    self._notice("⚠ Full auto: tools run without prompts; "
                                 "AskUserQuestion won't be interactive in this mode.")
            except Exception as ex:
                self._notice("mode change failed: %r" % ex)
        tornado.ioloop.IOLoop.current().spawn_callback(_s)

    def attach(self, ws):
        self.viewers.add(ws)

    def detach(self, ws):
        self.viewers.discard(ws)

    def _emit(self, obj):
        data = json.dumps(obj)
        for v in list(self.viewers):
            try:
                v.write_message(data)
            except Exception:
                self.viewers.discard(v)

    def _fold_tasks(self, evs):
        """Fold plan traffic into the current task list.

        The console's own tool sends the whole plan per call, so that branch just
        replaces the list. The CLI's former TaskCreate/TaskUpdate calls, still met
        in older transcripts, have to be rebuilt piecewise. A create is parked under
        its tool_use id until its own result names it, because the id is assigned
        by the CLI and appears nowhere in the input. When that result text does not
        parse, the id falls back to the creation ordinal — which is what the CLI is
        counting anyway, so a reworded message costs ordering at worst, never the plan.

        Returns True if anything moved, so the caller knows to push a snapshot."""
        changed = False
        for e in evs:
            if e.get("parent"):          # a subagent's plan is its own business
                continue
            k = e.get("kind")
            if k == "tool_use":
                inp = e.get("input")
                if not isinstance(inp, dict):
                    continue
                if e.get("tool") == PLAN_TOOL:
                    steps = inp.get("steps")
                    if isinstance(steps, list):    # a snapshot: replaces the list wholesale
                        self.tasks = {str(i + 1): {"subject": str(x.get("title") or ""),
                                                   "status": str(x.get("status") or "pending")}
                                      for i, x in enumerate(steps) if isinstance(x, dict)}
                        self._task_open = {}
                        changed = True
                elif e.get("tool") == "TaskCreate":
                    self._task_open[e.get("toolId") or ""] = {
                        f: inp[f] for f in TASK_FIELDS if inp.get(f)}
                elif e.get("tool") == "TaskUpdate":
                    tid = str(inp.get("taskId") or "").strip()
                    if not tid:
                        continue
                    # an id we never saw created means the create scrolled out of the
                    # window (long session, or a resume that starts mid-plan). Keep the
                    # row anyway: a status with no title still says work is moving.
                    t = self.tasks.setdefault(tid, {})
                    for f in TASK_FIELDS:
                        if inp.get(f):
                            t[f] = inp[f]
                    changed = True
            elif k == "tool_result":
                t = self._task_open.pop(e.get("toolId") or "", None)
                if t is None or e.get("isError"):
                    continue
                m = TASK_ID_RE.search(e.get("content") or "")
                self._task_seq = max(self._task_seq + 1, int(m.group(1)) if m else 0)
                tid = m.group(1) if m else str(self._task_seq)
                t.setdefault("status", "pending")
                self.tasks.setdefault(tid, {}).update(t)
                changed = True
        return changed

    def plan_tasks(self):
        """The task list on the wire, in the CLI's own order (its ids count up)."""
        def key(kv):
            try:
                return (0, int(kv[0]))
            except ValueError:
                return (1, 0)   # unparsable id: keep it after the rest, in arrival order
        return [dict(t, id=tid) for tid, t in sorted(self.tasks.items(), key=key)]

    def _push(self, evs):
        # one snapshot per batch, appended after the calls that caused it, so a
        # replayed history converges on the same plan the live viewers are showing
        if self._fold_tasks(evs):
            evs = list(evs) + [{"kind": "plan", "tasks": self.plan_tasks()}]
        for ev in evs:
            self.event_seq += 1
            ev["_seq"] = self.event_seq
        self.log.extend(evs)
        if len(self.log) > 1500:
            self.log = self.log[-1500:]
        self._emit({"type": "events", "events": evs})

    def events_since(self, after_seq=None):
        """What a viewer has not seen yet, when the window still reaches back that far.

        Returns (events, is_delta). is_delta False means "here is the whole log,
        build it from scratch": the viewer asked for nothing, or asked for a point
        that has since fallen out of the 1500-event window. Otherwise it gets only
        the gap, which is what makes returning to a long session cost the time you
        were away rather than the whole conversation."""
        if after_seq is None:
            return self.log, False
        try:
            after = int(after_seq)
        except (TypeError, ValueError):
            return self.log, False
        if after < 0 or after > self.event_seq:
            return self.log, False    # nonsense, or a sequence from before a restart
        if not self.log:
            return [], True
        if after < int(self.log[0].get("_seq") or 1) - 1:
            return self.log, False    # the gap itself has been trimmed away
        return [ev for ev in self.log if int(ev.get("_seq") or 0) > after], True

    def terminate(self):
        self.ended = True
        for aid, fut in list(self._pending.items()):
            if not fut.done():
                fut.set_result(False)
        self._pending.clear()
        client = self.client
        self.client = None
        if client:
            async def _d():
                try:
                    await client.disconnect()
                except Exception:
                    pass
            tornado.ioloop.IOLoop.current().spawn_callback(_d)


class ChatSocket(AuthMixin, tornado.websocket.WebSocketHandler):
    """Thin attach/detach controller over persistent ChatSessions.
    Closing the socket only DETACHES — it never kills the claude process."""

    def check_origin(self, origin):
        return True

    clients = set()          # every open socket — for cross-device favorite sync

    def open(self):
        if AUTH and not self._ok_auth():
            self.close(4401, "Unauthorized")
            return
        self.session = None
        ChatSocket.clients.add(self)
        self._say({"type": "hello", "stamp": PAGE_STAMP})
        self._say({"type": "favorites", "favorites": list_favorites()})
        self._say({"type": "projects", "projects": project_tree()})

    def _say(self, obj):
        try:
            self.write_message(json.dumps(obj))
        except Exception:
            pass

    def _broadcast_favorites(self):
        favs = list_favorites()
        for ws in list(ChatSocket.clients):
            ws._say({"type": "favorites", "favorites": favs})

    def _broadcast_tree(self, rescan=False):
        """Push the project-grouped sidebar to every device. `rescan` forces a
        disk re-scan; metadata-only edits (star, rename, pin) reuse the cache."""
        tree = project_tree(force=rescan)
        for ws in list(ChatSocket.clients):
            ws._say({"type": "projects", "projects": tree})

    async def on_message(self, raw):
        try:
            msg = json.loads(raw)
        except Exception:
            return
        mt = msg.get("type")
        if mt == "start":
            if not CLAUDE_BIN or not os.path.exists(CLAUDE_BIN):
                self._say({"type": "error", "error": "claude CLI not found"})
                return
            cwd = safe_cwd(msg.get("cwd") or HOME)
            if not cwd:
                self._say({"type": "error", "error": "invalid working directory"})
                return
            sid = secrets.token_hex(6)
            sess = ChatSession(sid, cwd, msg.get("model") or "", _sanitize_mode(msg.get("mode")),
                               effort=_sanitize_effort(msg.get("effort")))
            try:
                await sess.start()
            except Exception as e:
                self._say({"type": "error", "error": "spawn failed: %s" % e})
                return
            CHAT_SESSIONS[sid] = sess
            if self.session and self.session is not sess:
                self.session.detach(self)   # keep it alive, just stop viewing it
            self.session = sess
            sess.attach(self)
            self._say({"type": "started", "id": sid, "cwd": cwd,
                       "name": os.path.basename(cwd) or cwd,
                       "model": sess.model or "default", "mode": sess.mode, "effort": sess.effort})
        elif mt == "attach":
            sess = CHAT_SESSIONS.get(msg.get("id"))
            if not sess:
                self._say({"type": "no_session", "id": msg.get("id")})
                return
            if self.session and self.session is not sess:
                self.session.detach(self)
            self.session = sess
            sess.attach(self)
            events, events_delta = sess.events_since(msg.get("after_seq"))
            self._say({"type": "attached", "id": sess.id, "cwd": sess.cwd,
                       "name": os.path.basename(sess.cwd) or sess.cwd, "cc": sess.cc_id,
                       "title": sess.title(), "ctx": sess.ctx,
                       "model": sess.model or "default", "mode": sess.mode,
                       "busy": sess.busy, "ended": sess.ended, "events": events,
                       "events_delta": events_delta, "event_seq": sess.event_seq,
                       "turn_age": sess.turn_age(), "word": sess.turn_word, "effort": sess.effort,
                       "compacting": sess.compacting, "tasks": sess.plan_tasks()})
            # returning to an idle session that's been quiet a while → recap it now
            # (the periodic sweep may not have ticked yet); guarded against busy/dup
            if (RECAP_ENABLED and not sess.busy and not sess.ended and not sess.compacting
                    and sess.recap_for != sess.last_activity
                    and (time.time() - sess.last_activity) >= RECAP_IDLE_SEC):
                tornado.ioloop.IOLoop.current().spawn_callback(sess._make_recap)
        elif mt == "resume":
            if not CLAUDE_BIN or not os.path.exists(CLAUDE_BIN):
                self._say({"type": "error", "error": "claude CLI not found"})
                return
            cc = msg.get("cc")
            cwd = safe_cwd(msg.get("cwd") or HOME)
            if not cwd or not _valid_cc(cc):
                self._say({"type": "error", "error": "invalid resume target"})
                return
            live = next((s for s in CHAT_SESSIONS.values()
                         if s.cc_id == cc and not s.ended), None)
            if live:
                sess = live
            else:
                pf = load_prefs().get(cc) or {}   # restore this session's own
                r_mode = _sanitize_mode(pf.get("mode") or msg.get("mode"))
                r_model = pf.get("model") or msg.get("model") or ""
                r_effort = _sanitize_effort(pf.get("effort") or msg.get("effort"))
                sess = ChatSession(secrets.token_hex(6), cwd, r_model,
                                   r_mode, resume_cc=cc, effort=r_effort)
                sess.preload()
                try:
                    await sess.start()
                except Exception as e:
                    self._say({"type": "error", "error": "resume spawn failed: %s" % e})
                    return
                CHAT_SESSIONS[sess.id] = sess
            if self.session and self.session is not sess:
                self.session.detach(self)
            self.session = sess
            sess.attach(self)
            self._say({"type": "attached", "id": sess.id, "cwd": sess.cwd,
                       "name": os.path.basename(sess.cwd) or sess.cwd, "cc": sess.cc_id,
                       "title": sess.title(), "ctx": sess.ctx,
                       "model": sess.model or "default", "mode": sess.mode,
                       "busy": sess.busy, "ended": sess.ended, "events": sess.log,
                       "turn_age": sess.turn_age(), "word": sess.turn_word, "effort": sess.effort,
                       "compacting": sess.compacting, "resumed": True, "tasks": sess.plan_tasks(),
                       "events_delta": False, "event_seq": sess.event_seq})
        elif mt == "detach":
            # closing the last tab stops the VIEW, never the session: drop this
            # socket's subscription so a background turn is not rendered into a
            # console that is showing nothing.
            if self.session:
                self.session.detach(self)
                self.session = None
            self._say({"type": "detached"})
        elif mt == "approve" and self.session:
            self.session.resolve_approval(msg.get("aid"), bool(msg.get("allow")), bool(msg.get("always")))
        elif mt == "answer" and self.session:
            self.session.resolve_answer(msg.get("aid"), msg.get("answers"))
        elif mt == "interrupt" and self.session:
            self.session.interrupt()
        elif mt == "set_model" and self.session:
            self.session.set_model(msg.get("model") or "")
        elif mt == "set_mode" and self.session:
            self.session.set_mode(msg.get("mode") or "")
        elif mt == "configure":
            # ⚙ per-session model/permission from the kebab Configure popover,
            # targeting a live session by id (the attached one or any other).
            # Effort is NOT here — it stays on the pill's set_effort relaunch path.
            sess = CHAT_SESSIONS.get(msg.get("id"))
            if sess and not sess.ended:
                if msg.get("model") is not None:
                    sess.set_model(msg.get("model") or "")
                if msg.get("mode") is not None:
                    sess.set_mode(msg.get("mode") or "")
        elif mt == "set_effort" and self.session:
            eff = _sanitize_effort(msg.get("effort"))
            sess = self.session
            if not sess.ended and eff != sess.effort:
                if sess.busy:
                    sess._notice("⚙ finish or interrupt the current turn before changing effort")
                else:
                    sess.set_effort(eff)
        elif mt == "user" and self.session:
            self.session.send_user(msg.get("text", ""), msg.get("images"),
                                   msg.get("files"))
        elif mt == "unqueue" and self.session:
            self.session.unqueue(msg.get("qid"))
        elif mt == "steer" and self.session:
            self.session.steer_now(msg.get("qid"))
        elif mt == "del_resumable":
            # Sidebar 🗑: move a resumable session's transcript to the trash.
            cc = msg.get("cc")
            for s in list(CHAT_SESSIONS.values()):
                if s.cc_id == cc:          # end a live session resumed from it first
                    s.terminate()
                    CHAT_SESSIONS.pop(s.id, None)
            res = trash_transcript(cc)
            save_pref(cc, fav=False)       # a trashed session can't stay starred
            self._say({"type": "resumable_deleted", "cc": cc,
                       "ok": bool(res.get("ok")), "error": res.get("error")})
            self._broadcast_favorites()
            self._broadcast_tree(rescan=True)
        elif mt == "set_favorite":
            # Star/unstar a session; persisted server-side and broadcast so every
            # device shares one favorites list. Starring carries a metadata snapshot.
            cc = msg.get("cc")
            if _valid_cc(cc):
                if msg.get("fav"):
                    save_pref(cc, fav={"cwd": msg.get("cwd") or "",
                                       "name": (msg.get("name") or "")[:120],
                                       "title": (msg.get("title") or "")[:200],
                                       "ts": time.time()})
                else:
                    save_pref(cc, fav=False)
                self._broadcast_favorites()
        elif mt == "proj_fav":
            # Star/unstar a FOLDER. Favorites are project-level now: a starred
            # project keeps every session it owns, instead of pinning one thread.
            if save_project_meta(msg.get("path"), fav=bool(msg.get("fav"))):
                self._broadcast_tree()
        elif mt == "proj_rename":
            # Sidebar ✎ on a project: label the folder. Empty name resets to basename.
            if save_project_meta(msg.get("path"), name=msg.get("name") or ""):
                self._broadcast_tree()
        elif mt == "proj_unpin":
            # Drop an imported folder. Only removes the sidebar entry; a folder
            # that still owns sessions keeps showing up through the disk scan.
            if save_project_meta(msg.get("path"), pinned=False, fav=False):
                self._broadcast_tree()
        elif mt == "proj_refresh":
            self._broadcast_tree(rescan=True)
        elif mt == "rename":
            # Sidebar ✎: set/clear a custom label for a session (by claude id).
            cc = msg.get("cc")
            ok = set_name(cc, msg.get("name") or "")
            self._say({"type": "renamed", "cc": cc,
                       "name": (msg.get("name") or "").strip()[:120], "ok": bool(ok)})
            if ok:
                self._broadcast_favorites()   # a renamed session may be starred → refresh every device's list
                self._broadcast_tree(rescan=True)   # the session title shows in its project too
        elif mt == "end":
            # End a specific session by id (sidebar ✕) or the active one.
            # Ending a background session never disturbs the active stream.
            target = msg.get("id")
            cur = self.session
            if target and (not cur or target != cur.id):
                s = CHAT_SESSIONS.get(target)
                if s:
                    s.terminate()
                    CHAT_SESSIONS.pop(s.id, None)
                self._say({"type": "ended", "id": target})
            elif cur:
                cur.terminate()
                cur.detach(self)
                CHAT_SESSIONS.pop(cur.id, None)
                self.session = None
                self._say({"type": "ended", "id": cur.id})
        elif mt == "list":
            self._say({"type": "sessions", "sessions": [
                {"id": s.id, "cwd": s.cwd, "root": _norm_dir(s.cwd) or s.cwd,
                 "name": os.path.basename(s.cwd) or s.cwd,
                 "cc": s.cc_id, "model": s.model or "default", "mode": s.mode,
                 "effort": s.effort, "title": s.title(),
                 "busy": s.busy, "ended": s.ended, "event_seq": s.event_seq}
                for s in CHAT_SESSIONS.values()]})

    def on_close(self):
        if self.session:
            self.session.detach(self)   # keep the claude process alive
        ChatSocket.clients.discard(self)


class ConsoleHandler(AuthMixin, tornado.web.RequestHandler):
    def get(self):
        if not self._ok_auth():
            return
        self.set_header("Cache-Control", "no-store")
        self.write(CONSOLE_HTML.replace("__CLAUDE_CONSOLE_WEBFM_URL__", json.dumps(WEBFM_URL))
                   .replace("__CLAUDE_CONSOLE_STAMP__", PAGE_STAMP))


CONSOLE_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,interactive-widget=resizes-content">
<title>Claude Console</title>
<script>try{var _t=localStorage.getItem('al_theme');if(_t&&_t!=='dark')document.documentElement.setAttribute('data-theme',_t);}catch(e){}</script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
/* ── theme palettes: 13 base vars each; panels/tints derived below ── */
:root{ /* Dark (default) */
  --bg:#1e1e1e;--bg2:#252526;--bg3:#2d2d2d;--line:#3c3c3c;--fg:#d4d4d4;--mut:#858585;
  --acc:#4fc1ff;--usr:#3794ff;--add:#2ea043;--del:#f85149;--tool:#e0c080;--think:#858585;--onacc:#04121f}
:root[data-theme="light"]{
  --bg:#ffffff;--bg2:#f6f8fa;--bg3:#eaeef2;--line:#d0d7de;--fg:#1f2328;--mut:#656d76;
  --acc:#0969da;--usr:#0969da;--add:#1a7f37;--del:#cf222e;--tool:#9a6700;--think:#767676;--onacc:#ffffff}
:root[data-theme="dracula"]{
  --bg:#282a36;--bg2:#21222c;--bg3:#343746;--line:#44475a;--fg:#f8f8f2;--mut:#8390b7;
  --acc:#bd93f9;--usr:#8be9fd;--add:#50fa7b;--del:#ff5555;--tool:#f1fa8c;--think:#8390b7;--onacc:#282a36}
:root[data-theme="nord"]{
  --bg:#2e3440;--bg2:#2b303b;--bg3:#3b4252;--line:#434c5e;--fg:#d8dee9;--mut:#919cb0;
  --acc:#88c0d0;--usr:#81a1c1;--add:#a3be8c;--del:#bf616a;--tool:#ebcb8b;--think:#929cae;--onacc:#2e3440}
:root[data-theme="solarized-light"]{
  --bg:#fdf6e3;--bg2:#eee8d5;--bg3:#e4ddc8;--line:#d6cfb8;--fg:#586e75;--mut:#657474;
  --acc:#268bd2;--usr:#268bd2;--add:#859900;--del:#dc322f;--tool:#b58900;--think:#657474;--onacc:#fdf6e3}
:root[data-theme="tokyo-night"]{
  --bg:#1a1b26;--bg2:#1f2335;--bg3:#292e42;--line:#3b4261;--fg:#c0caf5;--mut:#7981ab;
  --acc:#7aa2f7;--usr:#7dcfff;--add:#9ece6a;--del:#f7768e;--tool:#e0af68;--think:#7981ab;--onacc:#1a1b26}
:root[data-theme="catppuccin"]{
  --bg:#1e1e2e;--bg2:#181825;--bg3:#313244;--line:#45475a;--fg:#cdd6f4;--mut:#81869d;
  --acc:#89b4fa;--usr:#89dceb;--add:#a6e3a1;--del:#f38ba8;--tool:#f9e2af;--think:#82859a;--onacc:#1e1e2e}
:root[data-theme="gruvbox"]{
  --bg:#282828;--bg2:#1d2021;--bg3:#3c3836;--line:#504945;--fg:#ebdbb2;--mut:#a89984;
  --acc:#83a598;--usr:#8ec07c;--add:#b8bb26;--del:#fb4934;--tool:#fabd2f;--think:#9a8c7e;--onacc:#282828}
:root[data-theme="catppuccin-latte"]{
  --bg:#eff1f5;--bg2:#e6e9ef;--bg3:#dce0e8;--line:#ccd0da;--fg:#4c4f69;--mut:#6a6d82;
  --acc:#1e66f5;--usr:#04a5e5;--add:#40a02b;--del:#d20f39;--tool:#df8e1d;--think:#6a6d82;--onacc:#ffffff}
:root[data-theme="gruvbox-light"]{
  --bg:#fbf1c7;--bg2:#f2e5bc;--bg3:#ebdbb2;--line:#d5c4a1;--fg:#3c3836;--mut:#786b61;
  --acc:#458588;--usr:#689d6a;--add:#98971a;--del:#cc241d;--tool:#b57614;--think:#786b5e;--onacc:#fbf1c7}
:root[data-theme="rose-pine-dawn"]{
  --bg:#faf4ed;--bg2:#fffaf3;--bg3:#f2e9e1;--line:#dfdad9;--fg:#575279;--mut:#736d83;
  --acc:#907aa9;--usr:#286983;--add:#5b8a3a;--del:#b4637a;--tool:#ea9d34;--think:#736d83;--onacc:#faf4ed}
:root[data-theme="one-light"]{
  --bg:#fafafa;--bg2:#f0f0f0;--bg3:#e5e5e6;--line:#d4d4d6;--fg:#383a42;--mut:#72737b;
  --acc:#4078f2;--usr:#0184bc;--add:#50a14f;--del:#e45649;--tool:#c18401;--think:#72737b;--onacc:#ffffff}
:root[data-theme="ayu-light"]{
  --bg:#fcfcfc;--bg2:#f3f4f5;--bg3:#e7e8e9;--line:#dcdde0;--fg:#5c6166;--mut:#70757d;
  --acc:#399ee6;--usr:#55b4d4;--add:#86b300;--del:#e65050;--tool:#f2ae49;--think:#70757d;--onacc:#ffffff}
/* derived panel & accent-text tints — one set of formulas reused by every theme */
:root{
  --codebg:color-mix(in srgb, var(--bg) 88%, #000);
  --sel:color-mix(in srgb, var(--acc) 16%, var(--bg));
  --selln:color-mix(in srgb, var(--acc) 42%, var(--bg));
  --toolbg:color-mix(in srgb, var(--tool) 13%, var(--bg));
  --toolln:color-mix(in srgb, var(--tool) 34%, var(--bg));
  --okbg:color-mix(in srgb, var(--add) 20%, var(--bg));
  --nobg:color-mix(in srgb, var(--del) 20%, var(--bg));
  --infobg:color-mix(in srgb, var(--usr) 14%, var(--bg));
  --infoln:color-mix(in srgb, var(--usr) 40%, var(--bg));
  --addfg:color-mix(in srgb, var(--add) 62%, var(--fg));
  --delfg:color-mix(in srgb, var(--del) 58%, var(--fg));
  --dim:var(--mut);   /* was used in 9 places but never defined -> inherited --fg */
  --fsans:-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,"Noto Sans CJK SC",sans-serif;
  --fmono:ui-monospace,SFMono-Regular,Menlo,"Noto Sans Mono CJK SC",monospace}
/* THEME-END */
html,body{height:100%;background:var(--bg);color:var(--fg);overflow:hidden;
  font-family:var(--fsans);font-size:14px}
body{display:flex;flex-direction:column}
header{display:flex;gap:6px;align-items:center;padding:6px 10px;background:var(--bg2);
  border-bottom:1px solid var(--line);flex-shrink:0;flex-wrap:wrap}
.sb-brand{font-weight:700;color:var(--acc);font-size:15px;padding:11px 12px 9px;border-bottom:1px solid var(--line);white-space:nowrap}
header select,header input{background:var(--bg3);color:var(--fg);border:1px solid var(--line);
  border-radius:5px;padding:4px 7px;font-size:12.5px}
header select#project{flex:1;min-width:120px;max-width:380px}
header input#cwd{flex:1;min-width:120px;display:none}
.status{font-size:11.5px;color:var(--mut);white-space:nowrap;margin-left:auto;display:flex;gap:6px;align-items:center}
.dot{width:8px;height:8px;border-radius:50%;background:var(--mut)}
.dot.on{background:var(--add);box-shadow:0 0 6px var(--add)}
.dot.busy{background:var(--tool);box-shadow:0 0 6px var(--tool);animation:pulse 1s infinite}
@keyframes pulse{50%{opacity:.35}}
/* floating status pill above the composer — a "ready/idle" light, or the
   animated working indicator (glyph + word + elapsed) while a turn runs */
.pillrow{display:flex;flex-wrap:wrap;justify-content:space-between;align-items:flex-end;gap:6px;margin:0 0 7px}
.pills{display:flex;flex:none;gap:6px;max-width:100%}
#effort,#modelpill{flex:none;max-width:100%;padding:3px 11px;background:var(--bg3);border:1px solid var(--line);
  border-radius:9px;box-shadow:0 2px 10px rgba(0,0,0,.28);user-select:none;cursor:pointer;
  font-size:13px;line-height:1.1;color:var(--fg);white-space:nowrap}
#effort:hover,#modelpill:hover{border-color:var(--acc);color:var(--acc)}
#thinking{flex:none;max-width:100%;padding:3px 12px;
  background:var(--bg3);border:1px solid var(--line);border-radius:9px;
  box-shadow:0 2px 10px rgba(0,0,0,.28);user-select:none}
#thinking .twrap{display:flex;align-items:center;gap:8px;font-size:13px;line-height:1.1}
#thinking .dot{flex:none}
#thinking.busy .dot{display:none}
#thinking .glyph{display:none;font-size:13px;color:var(--tool);width:1.1em;text-align:center;
  text-shadow:0 0 8px var(--tool);animation:thinkpulse 1.4s ease-in-out infinite}
#thinking.busy .glyph{display:inline-block}
#thinking.compacting .glyph{width:2.6em;text-align:left;letter-spacing:1px;font-weight:700}
#thinking .meta .mtx{font-family:var(--fmono);letter-spacing:1px}
#thinking .meta .mtx span{text-shadow:0 0 5px currentColor}
#thinking .meta .mtx.lite span{text-shadow:none;font-weight:600}   /* light bg: glow→smear, so drop it & bolden for contrast */
#thinking .word{color:var(--fg);font-weight:600}
#thinking.busy .word::after{content:'…'}
#thinking .meta{color:var(--mut);font-size:12px;font-variant-numeric:tabular-nums}
@keyframes thinkpulse{0%,100%{opacity:.45}50%{opacity:1}}
.btn{background:var(--bg3);color:var(--fg);border:1px solid var(--line);border-radius:5px;
  padding:4px 9px;font-size:12.5px;cursor:pointer;white-space:nowrap}
.btn:hover{background:var(--line)}

#chat{flex:1;overflow-y:auto;overflow-x:hidden;padding:14px;-webkit-overflow-scrolling:touch}
.wrap{max-width:820px;margin:0 auto}
.msg{margin-bottom:14px;line-height:1.5;word-wrap:break-word;overflow-wrap:anywhere}
.msg.user{display:flex;flex-direction:column;align-items:flex-end}
.msg.user .b{background:var(--sel);border:1px solid var(--selln);border-radius:10px;padding:8px 12px;max-width:85%;white-space:pre-wrap}
.msg.asst .b{color:var(--fg);text-wrap:pretty}
/* The streaming bubble inherits the finished bubble's box and typography, so the
   one swap at completion changes inline formatting only and the block does not
   jump. pre-wrap keeps the raw newlines the model emits; text-wrap:pretty is off
   here because rebalancing the last lines on every append is itself a wobble. */
.msg.asst.streaming .b{text-wrap:auto}
.msg.asst.streaming .stxt{white-space:pre-wrap;word-break:break-word}
.msg.asst.streaming .scaret{display:inline-block;width:2px;height:1em;margin-left:1px;
  vertical-align:text-bottom;background:var(--acc);opacity:.8;animation:scaretb 1s steps(2,start) infinite}
@keyframes scaretb{50%{opacity:0}}
/* Plan dock — the current task list, pinned to the top of the chat viewport so it
   stays readable while the answer scrolls under it. The server pushes the whole
   list on every change (the console's own plan tool, see PLAN_TOOL there), so
   this only ever paints a snapshot.
   Opaque background, because content scrolls behind it once it sticks. */
.plandock{position:sticky;top:0;z-index:7;max-width:820px;margin:0 auto 10px;
  padding:0 0 8px;background:var(--bg);
  /* #chat carries 14px of padding, and a scroll container's padding scrolls WITH
     its content — so a sticky child pins below it and messages ride up through
     the gap above the card. The shadow is that gap, painted opaque. Keep it equal
     to #chat's padding-top. */
  box-shadow:0 -14px 0 var(--bg)}
.plandock[hidden]{display:none}
.plandock .plan{margin:0;box-shadow:0 7px 18px rgba(0,0,0,.18)}
.plan{border:1px solid var(--line);border-radius:8px;background:var(--bg2);overflow:hidden}
.plan .ph{display:flex;align-items:center;gap:7px;padding:6px 10px;color:var(--acc);
  font-weight:650;font-size:12.5px}
.plan .ptitle{flex:1;min-width:0}
.plan .pcount{color:var(--mut);font-weight:400;font-family:var(--fmono);font-size:11.5px}
.plan .ptoggle{width:24px;height:22px;flex:none;display:inline-flex;align-items:center;
  justify-content:center;border:0;background:none;color:var(--mut);cursor:pointer;
  border-radius:5px;font-size:11px;line-height:1}
.plan .ptoggle:hover{color:var(--acc);background:var(--bg3)}
.plan .psteps{display:flex;flex-direction:column;border-top:1px solid var(--line)}
.plan .pst{display:grid;grid-template-columns:18px 1fr;gap:6px;align-items:start;
  padding:5px 10px;font-size:13px;line-height:1.35}
.plan .pst + .pst{border-top:1px solid color-mix(in srgb,var(--line) 55%,transparent)}
.plan .pi{font-family:var(--fmono);color:var(--mut);text-align:center}
.plan .psub{color:var(--fg);min-width:0;overflow-wrap:anywhere}
.plan .pst.done .pi{color:var(--add)}
.plan .pst.done .psub{color:var(--mut);text-decoration:line-through}
.plan .pst.active .pi{color:var(--tool)}
.plan .pst.active .psub{font-weight:600}
.plan .pempty{padding:6px 10px;border-top:1px solid var(--line);font-size:12px;color:var(--mut)}
.think{color:var(--think);font-style:italic;font-size:13px;border-left:2px solid var(--line);padding:3px 0 3px 10px;margin-bottom:12px;white-space:pre-wrap}
.think.hide{display:none}
.notice{color:var(--mut);font-size:11.5px;margin:6px 0}
.errline{color:var(--del);font-size:12px;font-family:var(--fmono);margin:4px 0;white-space:pre-wrap}
.recap{color:var(--mut);font-size:12px;margin:7px 0;line-height:1.45}
.recap .rk{font-weight:600;letter-spacing:.2px}
.recap .rt{font-style:italic;opacity:.92}
.doneline{color:var(--mut);font-size:11.5px;margin:6px 0}
.doneline .dg{color:var(--tool)}
.bgline{color:var(--mut);font-size:11.5px;margin:6px 0}
.bgline .bgk{color:var(--tool);font-weight:600}

/* collapsed change/tool cards */
.tool{border:1px solid var(--line);border-radius:8px;margin:6px 0 12px;background:var(--bg2);overflow:hidden}
.tool .th{padding:7px 10px;cursor:pointer;display:flex;gap:8px;align-items:center;user-select:none}
.tool .th:hover{background:var(--bg3)}
.tool .ico{flex-shrink:0}
.tool .tn{color:var(--tool);font-weight:600;font-family:var(--fmono);font-size:12.5px;flex-shrink:0}
.tool .tp{color:var(--mut);font-family:var(--fmono);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1}
.tool .cnt{font-size:11.5px;font-family:var(--fmono);flex-shrink:0}
.tool .cnt .a{color:var(--addfg)}.tool .cnt .d{color:var(--delfg)}
.tool .eye{color:var(--mut);flex-shrink:0;display:inline-flex;align-items:center;transition:color .15s}
.tool .eye svg{width:16px;height:16px;display:block}
.tool .eye .e-open{display:none}            /* collapsed → closed eye */
.tool.open>.th .eye .e-shut{display:none}
.tool.open>.th .eye .e-open{display:block}      /* expanded → open eye; child selectors, or an open Agent card opens every card folded inside it */
.tool .th:hover .eye{color:var(--fg)}
.tool .tb{display:none;border-top:1px solid var(--line);padding:8px 10px}
.tool.open>.tb{display:block}
/* an Agent card folds its subagent's whole run; the header keeps the live count */
.tool .agst{color:var(--mut);font-family:var(--fmono);font-size:11.5px;flex-shrink:0;white-space:nowrap}
.tool.agent.running>.th .agst{color:var(--tool)}
.tool .brief{margin-bottom:8px}
.tool .sub{margin:0 0 8px;padding-left:10px;border-left:2px solid var(--line)}
.tool .sub .tool,.tool .sub .toolgroup{margin:6px 0}
.tool.err>.th .tn{color:var(--del)}
/* Consecutive calls to the SAME tool fold into one row. A run of eight Bash calls
   otherwise pushes the answer off screen, and Claude routinely fires several tool
   calls in a single message, so these runs are common rather than exceptional.
   Folding costs almost nothing: a tool card is already collapsed by default, so
   what disappears is N-1 header rows, not content. The group header carries the
   count and the newest call's argument, so a folded run still says what is
   happening right now. */
.toolgroup{border:1px solid var(--line);border-radius:8px;margin:6px 0 12px;background:var(--bg2);overflow:hidden}
.toolgroup>.tgh{padding:7px 10px;cursor:pointer;display:flex;gap:8px;align-items:center;user-select:none}
.toolgroup>.tgh:hover{background:var(--bg3)}
.toolgroup>.tgh .ico{flex-shrink:0}
.toolgroup>.tgh .tn{color:var(--tool);font-weight:600;font-family:var(--fmono);font-size:12.5px;flex-shrink:0}
.toolgroup>.tgh .tgcount{color:var(--mut);font-family:var(--fmono);font-size:11.5px;flex-shrink:0}
.toolgroup>.tgh .tp{color:var(--mut);font-family:var(--fmono);font-size:12px;white-space:nowrap;
  overflow:hidden;text-overflow:ellipsis;flex:1;min-width:0}
.toolgroup>.tgh .tgstate{font-family:var(--fmono);font-size:11.5px;flex-shrink:0;color:var(--tool)}
.toolgroup.err>.tgh .tgstate{color:var(--del)}
/* the eye is scoped to the group's own header — nested cards keep their own */
.toolgroup>.tgh .eye{color:var(--mut);flex-shrink:0;display:inline-flex;align-items:center;transition:color .15s}
.toolgroup>.tgh .eye svg{width:16px;height:16px;display:block}
.toolgroup>.tgh .eye .e-open{display:none}
.toolgroup.open>.tgh .eye .e-shut{display:none}
.toolgroup.open>.tgh .eye .e-open{display:block}
.toolgroup>.tgh:hover .eye{color:var(--fg)}
.toolgroup>.tgb{display:none;border-top:1px solid var(--line)}
.toolgroup.open>.tgb{display:block}
/* inside a group the cards give up their own frame: the group owns the box */
.toolgroup>.tgb>.tool{border:0;border-radius:0;margin:0;background:transparent}
.toolgroup>.tgb>.tool+.tool{border-top:1px solid var(--line)}
pre{background:var(--codebg);border:1px solid var(--line);border-radius:6px;padding:8px;overflow-x:auto;margin:5px 0;
  font-family:var(--fmono);font-size:12.5px;line-height:1.45}
code{font-family:var(--fmono);font-size:12.5px;background:var(--codebg);border:1px solid var(--line);border-radius:3px;padding:0 4px}
pre code{background:none;border:none;padding:0}
.bubble a{color:var(--acc);text-decoration:underline;text-underline-offset:2px}
.bubble a.filelink{text-decoration-style:dotted}
.bubble h1{font-size:16px;font-weight:700;line-height:1.3;margin:14px 0 5px;text-wrap:balance}
.bubble h2{font-size:15px;font-weight:700;line-height:1.35;margin:12px 0 4px;text-wrap:balance}
.bubble h3{font-size:14px;font-weight:600;line-height:1.4;margin:10px 0 3px;text-wrap:balance}
.bubble h1:first-child,.bubble h2:first-child,.bubble h3:first-child{margin-top:0}
.bubble table{border-collapse:collapse;margin:7px 0;font-size:12px;display:block;overflow-x:auto;max-width:100%}
.bubble th,.bubble td{border:1px solid var(--line);padding:3px 9px;text-align:left}
.bubble thead th{background:var(--bg3);font-weight:600}
.msg.asst ul,.msg.asst ol,.tool .res .b ul,.tool .res .b ol{margin:4px 0 4px 20px}   /* the report box renders markdown too */
.msg.asst a{color:var(--acc)}
.diffline{font-family:var(--fmono);font-size:12px;white-space:pre-wrap;line-height:1.4}
.dl-add{color:var(--addfg)}.dl-del{color:var(--delfg)}.dl-hdr{color:var(--acc)}.dl-ctx{color:var(--mut)}
.reslabel{font-size:11px;color:var(--mut);margin:6px 0 2px}

#composer{flex-shrink:0;border-top:1px solid var(--line);background:var(--bg2);padding:8px 10px;
  padding-bottom:calc(8px + env(safe-area-inset-bottom));display:flex;flex-direction:column;gap:0;align-items:stretch}
#composer .wrap2{width:100%;display:flex;gap:8px;align-items:flex-end;position:relative}
/* @model completion above the composer: same list styling as the folder picker */
#atac{position:absolute;left:0;right:0;bottom:100%;margin-bottom:4px;z-index:50;background:var(--bg3);border:1px solid var(--acc);border-radius:6px;max-height:260px;overflow:auto;display:none;box-shadow:0 8px 18px rgba(0,0,0,.5)}
#atac.on{display:block}
#atac .acitem{display:flex;justify-content:space-between;gap:10px;align-items:baseline}
#atac .acto{font-size:11px;font-family:var(--fmono);color:var(--mut);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.acitem:hover .acto,.acitem.sel .acto{color:var(--onacc)}
#attach{display:none;flex-wrap:wrap;gap:7px;padding:0 0 8px}
#attach.on{display:flex}
#attach .att{position:relative;width:54px;height:54px;border-radius:8px;overflow:hidden;border:1px solid var(--line);background:var(--bg3)}
#attach .att img{width:100%;height:100%;object-fit:cover;display:block}
#attach .att .rm{position:absolute;top:1px;right:1px;width:17px;height:17px;border-radius:50%;border:none;
  background:rgba(0,0,0,.6);color:#fff;cursor:pointer;font-size:12px;line-height:17px;text-align:center;padding:0}
#attach .att .rm:hover{background:var(--del)}
.msg.user .imgs{margin-top:5px;font-size:11.5px;color:var(--mut)}
/* queued messages (typed while the agent is busy) */
#queue{width:100%;display:none;flex-direction:column;gap:5px;padding:0 0 8px}
#queue.on{display:flex}
#queue .qmsg{display:flex;align-items:center;gap:8px;background:var(--bg3);border:1px solid var(--line);border-radius:8px;padding:5px 9px;font-size:12.5px;color:var(--fg);cursor:pointer}
#queue .qmsg:hover{border-color:var(--acc)}
#queue .qmsg .qicon{flex:none;color:var(--tool);font-size:12px}
#queue .qmsg .qtext{flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#queue .qmsg .qx{flex:none;color:var(--mut);font-size:13px;padding:0 2px}
#queue .qmsg:hover .qx{color:var(--del)}
#queue .qmsg .qsteer{flex:none;color:var(--mut);font-size:12px;padding:0 2px;cursor:pointer}
#queue .qmsg:hover .qsteer{color:var(--tool)}
#queue .qmsg .qsteer:hover{filter:brightness(1.25)}
#ta{flex:1;min-width:0;background:var(--bg3);color:var(--fg);border:1px solid var(--line);border-radius:10px;
  padding:9px 12px;font-size:14px;font-family:inherit;resize:none;max-height:160px;line-height:1.4}
#ta:focus{outline:1px solid var(--acc)}
#send{background:var(--acc);color:var(--onacc);border:none;border-radius:10px;width:38px;height:38px;flex:none;display:inline-flex;align-items:center;justify-content:center;font-size:15px;font-weight:700;cursor:pointer}
#send:disabled{background:var(--line);color:var(--mut);cursor:default}
/* Attach button. Pasting was the only way in before, which on a phone means there
   was no way in at all — no clipboard image, no file picker, nothing. One control
   covers both: the native picker offers camera, photos and files, and the same
   handler takes drag-and-drop on the desktop. */
#attachBtn,#voiceBtn{background:var(--bg3);color:var(--fg);border:1px solid var(--line);border-radius:10px;
  width:38px;height:38px;flex:none;display:inline-flex;align-items:center;justify-content:center;
  font-size:16px;cursor:pointer;padding:0}
#attachBtn:hover:not(:disabled),#voiceBtn:hover:not(:disabled){background:var(--line)}
#attachBtn:disabled,#voiceBtn:disabled{color:var(--mut);cursor:default;opacity:.6}
#ta[readonly]{border-color:var(--acc)}
/* Dictate button. The stop glyph is drawn rather than emoji so it inherits the
   recording colour; the mic itself is a flat icon that reads at 24px on a phone. */
#voiceBtn .voice-mic-icon{width:24px;height:24px;display:block;object-fit:contain}
#voiceBtn:disabled .voice-mic-icon{filter:grayscale(1)}
#voiceBtn svg{width:19px;height:19px;display:block;fill:none;stroke:currentColor;
  stroke-width:2.25;stroke-linecap:round;stroke-linejoin:round}
#voiceBtn.recording{color:var(--delfg);border-color:var(--del);background:var(--nobg)}
#voiceBtn.recording:hover:not(:disabled){background:var(--nobg)}
/* status line above the composer: recording clock, then transcribing, then gone */
#voiceBar{height:28px;display:flex;align-items:center;gap:7px;padding:0 2px 7px;
  color:var(--mut);font:11.5px var(--fmono)}
#voiceBar[hidden]{display:none}
#voiceBar .vmark{width:7px;height:7px;border-radius:50%;background:var(--del);flex:none}
#voiceBar.transcribing .vmark{background:var(--acc);animation:pulse 1.1s ease-in-out infinite}
#voiceLabel{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#voiceCancel{width:24px;height:24px;flex:none;border:0;background:transparent;color:var(--mut);
  cursor:pointer;padding:0;font-size:13px;line-height:1}
#voiceCancel:hover{color:var(--del)}
#composer.drop .wrap2{outline:2px dashed var(--acc);outline-offset:4px;border-radius:12px}
/* a file chip is a name, not a thumbnail: there is nothing to preview */
#attach .fatt{display:inline-flex;align-items:center;gap:7px;height:28px;padding:0 6px 0 9px;
  border:1px solid var(--line);border-radius:8px;background:var(--bg3);font-size:12px;max-width:260px}
#attach .fatt .fn{font-family:var(--fmono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}
#attach .fatt .fsz{color:var(--mut);font-family:var(--fmono);font-size:11px;flex:none}
#attach .fatt .rm{width:17px;height:17px;flex:none;border:none;border-radius:50%;background:none;
  color:var(--mut);cursor:pointer;font-size:11px;line-height:17px;text-align:center;padding:0}
#attach .fatt .rm:hover{background:var(--del);color:#fff}
/* the echoed user message names what rode along with it, under the bubble */
.msg.user .files{margin-top:3px;font-size:11.5px;color:var(--mut);font-family:var(--fmono);
  text-align:right;max-width:85%;overflow-wrap:anywhere}

#drawer{position:fixed;top:0;right:0;width:var(--drw,min(560px,92vw));height:100%;background:var(--bg);border-left:1px solid var(--line);
  transform:translateX(100%);transition:transform .34s cubic-bezier(.32,.72,0,1);z-index:20;display:flex;flex-direction:column}
#drawer.open{transform:none}
#drresize{position:absolute;left:0;top:0;width:6px;height:100%;cursor:col-resize;background:transparent;transition:background .12s;z-index:30}
#drresize:hover,#drresize.drag{background:var(--acc)}
#drawer .dh{padding:8px 12px;background:var(--bg2);border-bottom:1px solid var(--line);display:flex;gap:8px;align-items:center}
#drawer .dh .grow{flex:1}
#drawer .dc{flex:1;overflow:auto;padding:10px}
.gfile{font-family:var(--fmono);font-size:12px;padding:1px 0}.gfile .st{display:inline-block;width:24px;color:var(--tool);font-weight:700}
.empty{color:var(--mut);padding:18px;text-align:center}
/* edits-out-of-chat */
.dh .tab{cursor:pointer;padding:3px 9px;border-radius:5px;color:var(--mut);font-size:12.5px;user-select:none}
.dh .tab.on{background:var(--bg3);color:var(--fg)}
.dh .tab span{font-size:10px;opacity:.8}
.emark{font-size:12px;color:var(--tool);background:var(--toolbg);border:1px solid var(--toolln);border-radius:6px;
  padding:3px 9px;margin:2px 0 12px;display:inline-flex;gap:7px;cursor:pointer;font-family:var(--fmono);align-items:center}
.emark:hover{filter:brightness(1.25)}
.emark .a{color:var(--addfg)}.emark .d{color:var(--delfg)}.emark .mut{color:var(--mut)}
.ecard{border:1px solid var(--line);border-radius:8px;margin-bottom:10px;background:var(--bg2);overflow:hidden}
.ecard .eh{padding:7px 9px;display:flex;gap:7px;align-items:center;background:var(--toolbg);border-bottom:1px solid var(--line)}
.ecard .ef{color:var(--tool);font-family:var(--fmono);font-size:12px;font-weight:600;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ecard .cnt{font-size:11px;font-family:var(--fmono)}.ecard .cnt .a{color:var(--addfg)}.ecard .cnt .d{color:var(--delfg)}
.ecard.flash{outline:2px solid var(--acc);outline-offset:-2px}
.efocus{font-size:11.5px;color:var(--mut);margin:0 0 9px;padding:5px 8px;background:var(--bg2);border:1px solid var(--line);border-radius:6px}
.efocus .showall{color:var(--acc);cursor:pointer;text-decoration:underline}
.ecard .ed{max-height:320px;overflow:auto;padding:6px 9px}
/* single-file focus: let the one diff fill the drawer height instead of capping at 320px */
#edits.focusone{display:flex;flex-direction:column;overflow:hidden}
#edits.focusone .ecard{flex:1;min-height:0;display:flex;flex-direction:column;margin-bottom:0}
#edits.focusone .ecard .ed{flex:1;min-height:0;max-height:none}
.ecard .res{padding:0 9px}
/* approval prompts */
.approval{border:1px solid var(--toolln);border-radius:8px;margin:6px 0 14px;background:var(--toolbg);overflow:hidden}
.approval .ah{padding:8px 10px;color:var(--tool);font-weight:600;display:flex;gap:7px;align-items:center}
.approval .ah .tp{color:var(--mut);font-family:var(--fmono);font-weight:400;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.approval .abody{max-height:240px;overflow:auto;padding:4px 10px;border-top:1px solid var(--toolln)}
.approval .abtns{display:flex;gap:8px;padding:8px 10px;border-top:1px solid var(--toolln)}
.approval .abtns button{flex:1;padding:9px;border-radius:6px;cursor:pointer;font-size:14px;font-weight:700}
.approval .appr{background:var(--okbg);color:var(--addfg);border:1px solid var(--add)}
.approval .apprall{background:var(--infobg);color:var(--acc);border:1px solid var(--infoln)}
.approval .deny{background:var(--nobg);color:var(--delfg);border:1px solid var(--del)}
.approval .abtns button{white-space:nowrap}
.approval.done .abtns{opacity:.85}
.approval .ok{color:var(--addfg);font-weight:700}.approval .no{color:var(--delfg);font-weight:700}
/* question prompts (AskUserQuestion) */
.question{border:1px solid var(--infoln);border-radius:8px;margin:6px 0 14px;background:var(--infobg);overflow:hidden}
.question .qh{padding:8px 10px;color:var(--usr);font-weight:600;display:flex;gap:7px;align-items:center}
.question .qblk{padding:8px 10px;border-top:1px solid var(--infoln)}
.question .qtext{font-weight:600;margin-bottom:7px}
.question .qtext .chip{font-weight:600;color:var(--mut);font-size:11px;border:1px solid var(--line);border-radius:4px;padding:1px 5px;margin-right:6px}
.question .qopts{display:flex;flex-direction:column;gap:6px}
.question .qopt{text-align:left;padding:8px 10px;border-radius:6px;cursor:pointer;background:var(--bg3);
  color:var(--fg);border:1px solid var(--line);font-size:13px;line-height:1.35}
.question .qopt:hover{border-color:var(--usr)}
.question .qopt.sel{background:var(--sel);border-color:var(--usr);color:var(--fg)}
.question .qopt .od{display:block;color:var(--mut);font-size:11.5px;margin-top:2px}
.question .qother{margin-top:7px;width:100%;background:var(--bg3);color:var(--fg);
  border:1px solid var(--line);border-radius:6px;padding:7px;font-size:13px}
.question .qbtns{display:flex;gap:8px;padding:8px 10px;border-top:1px solid var(--infoln)}
.question .qbtns button{flex:1;padding:9px;border-radius:6px;cursor:pointer;font-size:14px;font-weight:700;
  background:var(--okbg);color:var(--addfg);border:1px solid var(--add)}
.question .qbtns button:disabled{opacity:.45;cursor:not-allowed}
.question.done .qbtns{display:none}
.question.done .qopt,.question.done .qother{pointer-events:none;opacity:.7}
.question .qdone{padding:8px 10px;border-top:1px solid var(--infoln);color:var(--addfg);font-weight:600}
#stop{background:var(--nobg);color:var(--delfg);border:1px solid var(--del);border-radius:10px;width:38px;height:38px;flex:none;display:inline-flex;align-items:center;justify-content:center;font-size:15px;font-weight:700;cursor:pointer}

/* sessions sidebar + shell layout */
.iconbtn{background:none;border:none;color:var(--fg);font-size:17px;cursor:pointer;padding:2px 5px;line-height:1}
.curname{font-size:13px;color:var(--mut);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1;min-width:0;max-width:46vw}
.ctx{display:none;align-items:center;gap:6px;font-size:12.5px;font-weight:600;color:var(--fg);white-space:nowrap;font-family:var(--fmono)}
.usage{display:none;align-items:center;gap:6px;font-size:12.5px;font-weight:600;color:var(--fg);white-space:nowrap;font-family:var(--fmono);
  border-left:1px solid var(--line);padding-left:11px;margin-left:4px}
.ctx .ulabel,.usage .ulabel{opacity:.7}
.usage .useg{display:inline-flex;align-items:center;gap:5px}
.usage .useg + .useg::before{content:"|";opacity:.3;font-weight:400}   /* divider between 5h | 7d */
/* shared segmented meter: 5 cells × 20%, whole bar coloured by the total % (Context + Usage) */
.cells{display:inline-flex;gap:2px;align-items:center}
.cells .cell{width:7px;height:13px;border-radius:2px;background:var(--bg3);border:1px solid var(--line);box-sizing:border-box;transition:background .25s,box-shadow .25s}
/* Meter signal ramp — FIXED hues, deliberately not the semantic tokens.
   green/amber/orange/red mean the same thing in every theme, and the semantic
   tokens are tuned for text: pushing them to a text contrast ratio turns the
   ramp muddy (a dark olive "yellow"). Light themes get the same hues one notch
   deeper, because a bright yellow on a white ground is otherwise invisible. */
:root{--mg:#2fbf4f;--my:#ecc020;--mo:#ff8c1a;--mr:#f5483b}
:root[data-theme="light"],:root[data-theme="solarized-light"],:root[data-theme="catppuccin-latte"],
:root[data-theme="gruvbox-light"],:root[data-theme="rose-pine-dawn"],:root[data-theme="one-light"],
:root[data-theme="ayu-light"]{--mg:#22a83f;--my:#d9a800;--mo:#f57c00;--mr:#ef3f31}
.cells.lv-g{color:var(--mg)}.cells.lv-y{color:var(--my)}
.cells.lv-o{color:var(--mo)}.cells.lv-r{color:var(--mr)}
.cells .cell.on{background:currentColor;border-color:currentColor;box-shadow:0 0 4px currentColor}
@media(max-width:680px){.usage{display:none!important}}
#shell{flex:1;display:flex;min-height:0;position:relative}
#mainCol{flex:1;display:flex;flex-direction:column;min-width:0}
/* Session tabs — the working set of THIS browser, not the list of what is running.
   LIVE in the sidebar answers "what exists"; this strip answers "what do I have
   open". Closing a tab is a view action and never touches the process, which is
   why the close control says so and sits apart from End session. */
#sessionTabs{flex:0 0 33px;height:33px;display:flex;align-items:stretch;
  overflow-x:auto;overflow-y:hidden;background:var(--bg2);
  border-bottom:1px solid var(--line);scrollbar-width:thin}
#sessionTabs[hidden]{display:none}
.stab{position:relative;display:flex;align-items:center;gap:6px;padding:0 5px 0 10px;
  max-width:210px;min-width:0;border-right:1px solid var(--line);color:var(--mut);
  font-size:12.5px;cursor:pointer;white-space:nowrap;user-select:none}
.stab:hover{background:var(--bg3);color:var(--fg)}
.stab.active{background:var(--bg);color:var(--fg)}
.stab.active::after{content:"";position:absolute;left:0;right:0;bottom:0;height:2px;background:var(--acc)}
.stab:focus-visible{outline:2px solid var(--acc);outline-offset:-2px}
/* state carries a shape as well as a colour, so it survives colour-blindness and themes */
.stab .sdot{flex:none;width:6px;height:6px;border-radius:50%;background:var(--line)}
.stab.busy .sdot{background:var(--acc);animation:stabpulse 1.1s ease-in-out infinite}
.stab.unread .sdot{background:var(--tool);box-shadow:0 0 0 2px color-mix(in srgb,var(--tool) 30%,transparent)}
.stab.ended .sdot{background:transparent;box-shadow:inset 0 0 0 1px var(--mut)}
.stab .stt{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis}
.stab.ended .stt{opacity:.7;text-decoration:line-through}
.stab.unread .stt{color:var(--fg);font-weight:600}
.stab .sx{flex:none;width:17px;height:17px;display:inline-flex;align-items:center;
  justify-content:center;border-radius:4px;font-size:10px;opacity:.4}
.stab .sx:hover{opacity:1;background:var(--del);color:#fff}
@keyframes stabpulse{50%{opacity:.35}}
/* The chat keeps only a recent window of rendered rows; this marks what was dropped
   and points at the one place the rest is still readable — the history index. */
.trimmark{display:flex;align-items:center;gap:8px;margin:0 0 14px;padding:5px 10px;
  border:1px dashed var(--line);border-radius:8px;color:var(--mut);font-size:12px}
.trimmark .tmx{font-family:var(--fmono)}
.trimmark .tmt{flex:1;min-width:0}
.trimmark .tmb{flex:none;background:none;border:0;padding:0;color:var(--acc);
  font-size:12px;cursor:pointer;text-decoration:underline}
.trimmark .tmb:hover{color:var(--fg)}
@media (prefers-reduced-motion:reduce){.stab.busy .sdot{animation:none}}
#sidebar{width:var(--sbw,270px);flex-shrink:0;background:var(--bg2);border-right:1px solid var(--line);display:flex;flex-direction:column;overflow-y:auto}
#sidebar.collapsed{display:none}
/* desktop: drag handle on the sidebar's right edge to resize */
#sbresize{flex-shrink:0;width:5px;cursor:col-resize;background:transparent;transition:background .12s;z-index:5}
#sbresize:hover,#sbresize.drag{background:var(--acc)}
#sidebar.collapsed + #sbresize{display:none}
.sb-new{padding:8px;border-bottom:1px solid var(--line);display:flex;flex-direction:column;gap:6px}
.sb-new select,.sb-new input{width:100%;background:var(--bg3);color:var(--fg);border:1px solid var(--line);border-radius:5px;padding:5px 7px;font-size:12.5px}
.cwdwrap{position:relative;display:none}
.cwdwrap.show{display:block}
#cwdac{position:absolute;left:0;right:0;top:100%;z-index:50;background:var(--bg3);border:1px solid var(--acc);border-top:none;border-radius:0 0 6px 6px;max-height:240px;overflow:auto;display:none;box-shadow:0 8px 18px rgba(0,0,0,.5)}
#cwdac.on{display:block}
.acitem{padding:5px 8px;cursor:pointer;border-bottom:1px solid var(--line)}
.acitem:last-child{border-bottom:none}
.acitem:hover,.acitem.sel{background:var(--acc)}
.acname{font-size:12px;font-family:var(--fmono);color:var(--fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.acpath{font-size:10px;font-family:var(--fmono);color:var(--mut);overflow-wrap:anywhere;line-height:1.3}
.acitem:hover .acname,.acitem.sel .acname,.acitem:hover .acpath,.acitem.sel .acpath{color:var(--onacc)}
.acmore{padding:5px 8px;font-size:10px;color:var(--mut);font-style:italic}
.sb-row2{display:flex;gap:6px}.sb-row2 select{flex:1;min-width:0}
#srchopen{display:flex;align-items:center;justify-content:space-between;gap:8px;width:100%;
  margin:0 0 8px;padding:6px 8px;background:var(--bg3);color:var(--dim);
  border:1px solid var(--line);border-radius:6px;font-size:12px;cursor:pointer}
#srchopen:hover{color:var(--fg);border-color:var(--acc)}
#srchopen kbd{font:inherit;font-size:10.5px;color:var(--dim);border:1px solid var(--line);
  border-radius:3px;padding:0 4px;background:var(--bg2)}
/* history search: a full-screen palette (Ctrl/Cmd+K). Wide on purpose — snippets
   are the point, and a sidebar-width column truncates them into uselessness. */
#srch{position:fixed;inset:0;z-index:60;background:rgba(0,0,0,.55);
  display:flex;justify-content:center;align-items:flex-start;padding:8vh 16px 16px}
#srch[hidden]{display:none}
#srchpanel{width:min(900px,100%);max-height:78vh;display:flex;flex-direction:column;
  background:var(--bg2);border:1px solid var(--line);border-radius:10px;
  box-shadow:0 18px 60px rgba(0,0,0,.5);overflow:hidden}
.srchtop{display:flex;gap:8px;padding:10px;border-bottom:1px solid var(--line);align-items:center}
#srchq{flex:1;min-width:0;background:var(--bg3);color:var(--fg);border:1px solid var(--line);
  border-radius:6px;padding:9px 11px;font-size:14px;outline:none}
#srchq:focus{border-color:var(--acc)}
#srchscope{flex:0 0 auto;background:var(--bg3);color:var(--fg);border:1px solid var(--line);
  border-radius:6px;padding:8px;font-size:12px}
#srchx{flex:0 0 auto;background:none;border:none;color:var(--dim);font-size:16px;cursor:pointer;padding:4px 8px}
#srchx:hover{color:var(--fg)}
#srchmeta{padding:7px 12px;font-size:11.5px;color:var(--dim);border-bottom:1px solid var(--line)}
#srchres{overflow:auto;padding:4px 0}
.shit{padding:9px 12px;border-bottom:1px solid var(--line);cursor:pointer}
.shit:hover{background:var(--bg3)}
.shit .sh1{display:flex;gap:8px;align-items:baseline;font-size:11.5px;color:var(--dim);margin-bottom:3px}
.shit .sh1 b{color:var(--fg);font-weight:600;font-size:12.5px}
.shit .sh1 .role{border:1px solid var(--line);border-radius:3px;padding:0 4px;font-size:10px}
.shit .sh2{font-size:12.5px;line-height:1.5;color:var(--fg);word-break:break-word;
  display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
.shit .sh2 mark{background:var(--acc);color:var(--bg);border-radius:2px;padding:0 1px}
/* read-only transcript viewer — a search hit opens here, never in the live chat */
#srchthread{display:flex;flex-direction:column;min-height:0;overflow:hidden}
#srchthread[hidden]{display:none}
.thhead{display:flex;align-items:center;gap:10px;padding:8px 12px;border-bottom:1px solid var(--line)}
.thhead .thtitle{flex:1;min-width:0;font-size:12.5px;color:var(--fg);font-weight:600;
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.thhead button{flex:0 0 auto;background:var(--bg3);color:var(--fg);border:1px solid var(--line);
  border-radius:5px;padding:4px 9px;font-size:11.5px;cursor:pointer}
.thhead button:hover{border-color:var(--acc);color:var(--acc)}
#thopen{color:var(--acc);border-color:var(--acc)}
#thopen:hover{background:var(--acc);color:var(--onacc)}
#thbody{overflow:auto;padding:6px 0 12px}
.thmsg{padding:7px 14px;border-left:3px solid transparent}
.thmsg .thr{font-size:10.5px;color:var(--dim);margin-bottom:2px;letter-spacing:.03em}
.thmsg .tht{font-size:12.5px;line-height:1.55;color:var(--fg);white-space:pre-wrap;word-break:break-word}
.thmsg.user{border-left-color:var(--acc)}
.thmsg.assistant{border-left-color:var(--line)}
.thmsg.tool .tht{font-family:var(--fmono);font-size:11.5px;color:var(--dim)}
.thmsg.target{background:var(--bg3)}
.thmsg .tht mark{background:var(--acc);color:var(--bg);border-radius:2px;padding:0 1px}
.thmore{display:block;width:calc(100% - 24px);margin:6px 12px;padding:5px;background:var(--bg3);
  color:var(--dim);border:1px dashed var(--line);border-radius:5px;font-size:11.5px;cursor:pointer}
.thmore:hover{color:var(--acc);border-color:var(--acc)}
.thend{text-align:center;font-size:11px;color:var(--mut);padding:6px}
/* Import sits directly under New session and mirrors its geometry, but stays
   outlined rather than solid: two filled accent buttons stacked would read as two
   equally primary actions, and importing is the rarer one. */
.impline{display:flex;flex-direction:column;gap:5px;margin-top:6px}
#impbtn{width:100%;background:var(--bg3);color:var(--acc);font-weight:700;
  border:1px solid var(--acc);border-radius:6px;padding:7px;font-size:13px;cursor:pointer}
#impbtn:hover{background:var(--acc);color:var(--onacc)}
#imphint{font-size:10.5px;color:var(--mut);line-height:1.3;word-break:break-all}
#imphint.bad{color:var(--tool)}
#impmsg{font-size:11px;color:var(--dim);line-height:1.35;word-break:break-word}
#impmsg:empty{display:none}   /* no dead gap when there is nothing to report */
#impmsg.bad{color:var(--delfg)}
#mrefresh{flex:0 0 auto;width:26px;padding:0;background:var(--bg3);color:var(--fg);border:1px solid var(--line);border-radius:5px;font-size:13px;cursor:pointer;line-height:1}
#mrefresh:hover{color:var(--acc);border-color:var(--acc)}
#mrefresh.busy{color:var(--acc);animation:mrspin .8s linear infinite;pointer-events:none}
@keyframes mrspin{to{transform:rotate(360deg)}}
.newbtn{background:var(--acc);color:var(--onacc);font-weight:700;border:none;border-radius:6px;padding:7px;font-size:13px;cursor:pointer}
.newbtn:hover{filter:brightness(1.08)}
.sb-sec{border-bottom:1px solid var(--line);padding:4px 0 6px}
.sb-h{font-size:11px;letter-spacing:.05em;text-transform:uppercase;color:var(--mut);padding:6px 10px 4px;display:flex;align-items:center;gap:6px}
.sb-h .cnt{background:var(--bg3);border-radius:8px;padding:0 6px;font-size:10px;color:var(--mut)}
.sb-h .grow{flex:1}
.sb-ref{cursor:pointer}.sb-ref:hover{color:var(--fg)}
.sb-foot{margin-top:auto;padding:8px 10px;border-top:1px solid var(--line);display:flex;align-items:center;gap:8px}
.sb-foot-l{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--mut);white-space:nowrap}
.sb-foot select{flex:1;min-width:0;background:var(--bg3);color:var(--fg);border:1px solid var(--line);border-radius:5px;padding:4px 6px;font-size:12px}
.sb-empty{color:var(--mut);font-size:12px;padding:5px 10px;line-height:1.4}
.srow{padding:7px 9px;cursor:pointer;display:flex;gap:8px;align-items:center;border-left:2px solid transparent}
.srow:hover{background:var(--bg3)}
.srow.active{background:var(--sel);border-left-color:var(--acc)}
.srow.ended{opacity:.6}
.srow .sdot,.prow .sdot{width:7px;height:7px;border-radius:50%;background:var(--mut);flex-shrink:0}
.srow .sdot.on,.prow .sdot.on{background:var(--add)}
.srow .sdot.busy,.prow .sdot.busy{background:var(--tool);box-shadow:0 0 5px var(--tool);animation:pulse 1s infinite}
.srow .smeta{flex:1;min-width:0}
.srow .sname{font-size:12.5px;color:var(--fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.srow .ssub{font-size:11px;color:var(--mut);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.srow .skebab{flex-shrink:0;font-size:18px;line-height:1;padding:1px 6px;color:var(--fg);cursor:pointer;opacity:.9;border-radius:5px;user-select:none}
.srow:hover .skebab{opacity:1}.srow .skebab:hover{color:var(--acc);background:var(--bg3)}
/* A project's actions button is a horizontal ··· in a padded box: the vertical ⋮
   stays the session affordance, so the two never read as the same control, and
   the 26x24 target is hittable — the bare glyph was a ~10px-wide tap area.
   Drawn as three dots rather than typed, so it can't shift with the font. */
.prow .pkebab{flex-shrink:0;display:flex;align-items:center;justify-content:center;
  gap:2.5px;width:26px;height:24px;padding:0;border-radius:6px;opacity:.55}
.prow .pkebab::before,.prow .pkebab::after,.prow .pkebab>i{content:"";width:3px;height:3px;
  border-radius:50%;background:currentColor}
.prow:hover .pkebab{opacity:1}
.prow .pkebab:hover{color:var(--acc);background:var(--bg3)}
/* project groups — the sidebar is grouped by folder, so a row is a project and
   the sessions it owns nest under it. A project is Live when one of its sessions
   is running, which is why the Live section lists folders, not threads. */
.pgroup{border-left:2px solid transparent}
.prow{padding:6px 9px;cursor:pointer;display:flex;gap:7px;align-items:center;user-select:none}
.prow:hover{background:var(--bg3)}
.prow .caret{display:inline-flex;align-items:center;justify-content:center;flex:none;
  width:15px;height:15px;border-radius:5px;color:var(--fg);opacity:.65;transition:opacity .15s}
.prow .caret::before{content:"";width:6px;height:6px;border-right:2px solid currentColor;
  border-bottom:2px solid currentColor;border-radius:1.5px;
  transform:translate(-2px,0) rotate(-45deg);transition:transform .2s ease}
.pgroup.open .prow .caret::before{transform:translate(-1px,-2px) rotate(45deg)}
.prow:hover .caret{opacity:1}
.prow .pmeta{flex:1;min-width:0}
.prow .pname{font-size:12.5px;font-weight:600;color:var(--fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.prow .pstar{color:var(--tool);font-size:11px}
.prow .psub{font-size:11px;color:var(--mut);font-family:var(--fmono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.prow .pn{flex:none;font-size:10px;color:var(--mut);background:var(--bg3);border:1px solid var(--line);
  border-radius:8px;padding:0 6px;line-height:15px}
.pgroup .plist{display:none}
.pgroup.open .plist{display:block}
.pgroup .plist .srow{padding-left:31px;border-left:none}
.pgroup .plist .srow .sname{font-size:12px}
.pgroup .plist .sb-empty{padding-left:31px;font-size:11px}

/* Manage sessions — a project can accumulate a lot of threads (sub-agents in
   particular), so the ⋮ menu opens a checklist for deleting several at once. */
#pman{position:fixed;inset:0;z-index:60;background:rgba(0,0,0,.55);
  display:flex;justify-content:center;align-items:flex-start;padding:9vh 16px 16px}
#pman[hidden]{display:none}
#pmanpanel{width:min(680px,100%);max-height:78vh;display:flex;flex-direction:column;
  background:var(--bg2);border:1px solid var(--line);border-radius:10px;
  box-shadow:0 18px 60px rgba(0,0,0,.5);overflow:hidden}
#pman .pmsub{flex:1;min-width:0;font-family:var(--fmono);font-size:11.5px;color:var(--mut);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.pmbar{display:flex;align-items:center;gap:10px;padding:7px 12px;
  border-bottom:1px solid var(--line);font-size:12px;color:var(--mut)}
.pmbar label{display:flex;align-items:center;gap:6px;cursor:pointer;color:var(--fg);user-select:none}
.pmbar .grow{flex:1}
#pmanlist{flex:1;min-height:0;overflow:auto}
.pmrow{display:flex;align-items:center;gap:9px;padding:7px 12px;
  border-bottom:1px solid var(--line);cursor:pointer}
.pmrow:hover{background:var(--bg3)}
.pmrow:last-child{border-bottom:none}
.pmrow input{flex:none;cursor:pointer;width:15px;height:15px;accent-color:var(--acc)}
.pmrow .pmmeta{flex:1;min-width:0}
.pmrow .pmt{font-size:12.5px;color:var(--fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.pmrow .pmd{font-size:11px;color:var(--mut);font-family:var(--fmono)}
.pmrow .pmlive{flex:none;font-size:10px;color:var(--addfg);border:1px solid var(--add);
  border-radius:8px;padding:0 6px;line-height:15px}
#pmandel{background:var(--nobg);color:var(--delfg);border:1px solid var(--del)}
#pmandel:disabled{opacity:.45;cursor:not-allowed}

/* Import folder — pick a directory and it becomes a sidebar project even before
   it owns a session. Same dircomplete backend as the custom-path box. */
#fimp{position:fixed;inset:0;z-index:60;background:rgba(0,0,0,.55);
  display:flex;justify-content:center;align-items:flex-start;padding:12vh 16px 16px}
#fimp[hidden]{display:none}
#fimppanel{width:min(620px,100%);max-height:70vh;display:flex;flex-direction:column;
  background:var(--bg2);border:1px solid var(--line);border-radius:10px;
  box-shadow:0 18px 60px rgba(0,0,0,.5);overflow:hidden}
#fimp .fh,#pman .fh{display:flex;gap:10px;align-items:baseline;padding:10px 12px;
  border-bottom:1px solid var(--line)}
#fimp .fh .ft{flex:1;font-size:13px;font-weight:600;color:var(--fg)}
#pman .fh .ft{flex:none;font-size:13px;font-weight:600;color:var(--fg);white-space:nowrap}
#pman .fh .btn{align-self:center}
#fimpq{width:100%;background:var(--bg3);color:var(--fg);border:1px solid var(--line);
  border-radius:6px;padding:9px 11px;font-size:13px;font-family:var(--fmono);outline:none}
#fimpq:focus{border-color:var(--acc)}
#fimp .fbody{padding:10px;display:flex;flex-direction:column;gap:8px;min-height:0}
#fimpac{flex:1;min-height:0;overflow:auto;border:1px solid var(--line);border-radius:6px;background:var(--bg)}
#fimpmsg{font-size:11.5px;color:var(--mut);min-height:1em}
#fimpmsg.bad{color:var(--delfg)}
#fimp .fbtns,#pman .fbtns{display:flex;gap:8px;justify-content:flex-end;padding:10px}
#pman .fbtns{border-top:1px solid var(--line)}
#fimp .fbtns button,#pman .fbtns button{padding:7px 14px;border-radius:6px;font-size:13px;font-weight:700;cursor:pointer}
#fimp .fadd{background:var(--acc);color:var(--onacc);border:none}
#fimp .fcancel,#pman .fcancel{background:var(--bg3);color:var(--fg);border:1px solid var(--line)}
.improw{display:flex;gap:5px}
.improw button{flex:1;min-width:0}
#fimpbtn{background:var(--bg3);color:var(--fg);border:1px solid var(--line);
  border-radius:6px;padding:7px;font-size:13px;cursor:pointer;white-space:nowrap}
#fimpbtn:hover{border-color:var(--acc);color:var(--acc)}

/* collapsible past-session sections */
.sb-h.sb-toggle{cursor:pointer;user-select:none}
.sb-h .caret{display:inline-flex;align-items:center;justify-content:center;flex:none;
  width:18px;height:18px;margin-left:-3px;border-radius:6px;color:var(--fg);opacity:.72;
  transition:background .15s,opacity .15s}
.sb-h .caret::before{content:"";width:7px;height:7px;border-right:2px solid currentColor;
  border-bottom:2px solid currentColor;border-radius:1.5px;
  transform:translate(-1px,-2px) rotate(45deg);transition:transform .2s ease}
.sb-sec.collapsed .caret::before{transform:translate(-2px,0) rotate(-45deg)}
.sb-h.sb-toggle:hover .caret{opacity:1;background:var(--bg3)}
.sb-sec.collapsed .seclist{display:none}
/* project groups expand in place, so the section must not be its own scroll\n   box — a nested scrollbar swallows the sessions you just expanded. The\n   sidebar itself scrolls instead. */\n.seclist{max-height:none}
/* shared per-card action menu (⋯) */
#cardMenu{position:fixed;z-index:60;min-width:152px;background:var(--bg2);border:1px solid var(--line);border-radius:8px;box-shadow:0 8px 26px rgba(0,0,0,.55);padding:4px;display:none}
#cardMenu.on{display:block}
#cardMenu .mi{padding:7px 10px;font-size:12.5px;color:var(--fg);cursor:pointer;border-radius:5px;white-space:nowrap}
#cardMenu .mi:hover{background:var(--bg3)}
#cardMenu .mi.danger{color:var(--delfg)}#cardMenu .mi.danger:hover{background:var(--nobg)}
#cardMenu .cfgrow{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:5px 9px;font-size:12.5px;color:var(--mut)}
#cardMenu .cfgrow .cfgsel{background:var(--bg3);color:var(--fg);border:1px solid var(--line);border-radius:5px;padding:3px 6px;font-size:12px;cursor:pointer}
.fscope{font-size:10px;color:var(--mut);font-family:var(--fmono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:130px}
#sb-backdrop{display:none}
@media(max-width:860px){
  #sidebar{position:fixed;left:0;top:0;bottom:0;z-index:40;transform:translateX(-100%);transition:transform .34s cubic-bezier(.32,.72,0,1);width:min(310px,86vw);box-shadow:2px 0 14px rgba(0,0,0,.5)}
  #sidebar.open{transform:none}
  #sidebar.collapsed{display:flex}
  #sb-backdrop.show{display:block;position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:35}
  #sbresize,#drresize{display:none}
}
.bubble .math.display{display:block;margin:6px 0;overflow-x:auto;overflow-y:hidden;max-width:100%}
.katex-display{margin:.35em 0!important}

/* keyboard focus — every interactive element gets a ring; mouse clicks stay clean */
button:focus-visible,select:focus-visible,input:focus-visible,textarea:focus-visible,
a:focus-visible,[tabindex]:focus-visible{outline:2px solid var(--acc);outline-offset:2px}
/* press feedback on the commit actions only (approve/deny/answer/send/stop/new/import).
   High-frequency toolbar buttons stay instant — motion there charges its cost every click. */
.approval .abtns button,.question .qbtns button,#send,#stop,.newbtn,#impbtn{
  transition:transform .1s cubic-bezier(.2,0,0,1)}
.approval .abtns button:active,.question .qbtns button:active,
#send:not(:disabled):active,#stop:active,.newbtn:active,#impbtn:active{transform:scale(.97)}

/* reduced motion: kill the loops, keep the static cue (busy dot keeps its --tool colour) */
@media(prefers-reduced-motion:reduce){
  *,*::before,*::after{animation-duration:.01ms!important;animation-iteration-count:1!important;
    transition-duration:.01ms!important}
  .dot.busy,.srow .sdot.busy,.prow .sdot.busy,#thinking .glyph,
  .msg.asst.streaming .scaret{animation:none!important;opacity:1!important}
}
</style>
<link rel="stylesheet" href="/static/katex/katex.min.css">
<script src="/static/katex/katex.min.js"></script>
</head>
<body>
<header>
  <button class="iconbtn" id="navtoggle" title="sessions">☰</button>
  <span class="curname" id="curname">— no session —</span>
  <span class="ctx" id="ctx" title="context-window usage"></span>
  <span class="usage" id="usage" title="usage limits (5h / 7d)"></span>
</header>

<div id="shell">
  <aside id="sidebar">
    <div class="sb-brand">⬡ Claude Console</div>
    <button id="srchopen" title="search all conversation history"><span>🔍 Search history</span><kbd>⌘K</kbd></button>
    <div class="sb-new">
      <select id="project" title="working directory for a new session"></select>
      <div class="cwdwrap" id="cwdwrap"><input id="cwd" placeholder="type a path…  ↑↓ to pick" autocomplete="off"><div id="cwdac"></div></div>
      <div class="sb-row2">
        <select id="model" title="model"><option value="opus">model: opus</option><option>sonnet</option><option>haiku</option></select><button id="mrefresh" title="refresh model list from the API">↻</button>
        <select id="mode" title="permission mode"><option value="acceptEdits">⚡ Auto-accept</option><option value="default">🔐 Approve</option><option value="plan">📋 Plan</option><option value="bypassPermissions">⏩ Full auto</option></select>
      </div>
      <button class="newbtn" id="newbtn">＋ New session</button>
      <div class="impline"><div class="improw"><input type="file" id="impfile" accept=".jsonl,application/x-ndjson" hidden><button id="impbtn" title="adopt a .jsonl transcript exported from another machine — it lands in the folder above">Import session</button><button id="fimpbtn" title="add a folder to the sidebar as a project">Import folder</button></div><span id="imphint"></span><span id="impmsg"></span></div>
    </div>
    <div class="sb-sec">
      <div class="sb-h">Live <span id="liveN" class="cnt">0</span></div>
      <div id="liveList"><div class="sb-empty">no active project</div></div>
    </div>
    <div class="sb-sec" id="secFav">
      <div class="sb-h sb-toggle"><span class="caret"></span>★ Favorites <span id="favN" class="cnt">0</span></div>
      <div id="favList" class="seclist"><div class="sb-empty">star a project to pin it here</div></div>
    </div>
    <div class="sb-sec" id="secRecent">
      <div class="sb-h sb-toggle"><span class="caret"></span>🕘 Recent <span class="grow"></span><span class="sb-ref" id="resumeRef" title="rescan">↻</span></div>
      <div id="recentList" class="seclist"><div class="sb-empty">—</div></div>
    </div>
    <div class="sb-foot">
      <span class="sb-foot-l">Theme</span>
      <select id="theme" title="color theme">
        <optgroup label="Dark">
          <option value="dark">Dark</option>
          <option value="dracula">Dracula</option>
          <option value="nord">Nord</option>
          <option value="tokyo-night">Tokyo Night</option>
          <option value="catppuccin">Catppuccin Mocha</option>
          <option value="gruvbox">Gruvbox Dark</option>
        </optgroup>
        <optgroup label="Light">
          <option value="light">Light</option>
          <option value="solarized-light">Solarized Light</option>
          <option value="catppuccin-latte">Catppuccin Latte</option>
          <option value="gruvbox-light">Gruvbox Light</option>
          <option value="rose-pine-dawn">Rosé Pine Dawn</option>
          <option value="one-light">One Light</option>
          <option value="ayu-light">Ayu Light</option>
        </optgroup>
      </select>
    </div>
  </aside>
  <div id="sbresize"></div>
  <div id="sb-backdrop"></div>
  <div id="srch" hidden><div id="srchpanel">
    <div class="srchtop">
      <input id="srchq" type="text" placeholder="search every conversation…" autocomplete="off" spellcheck="false">
      <select id="srchscope" title="how much history to search">
        <option value="all">all history</option>
        <option value="project">this folder + subfolders</option>
        <option value="session">this conversation</option>
      </select>
      <button id="srchx" title="close (Esc)">✕</button>
    </div>
    <div id="srchmeta">Search your full conversation history — what you asked, what Claude answered, and the files and commands it touched.</div>
    <div id="srchres"></div>
    <div id="srchthread" hidden>
      <div class="thhead">
        <button id="thback" title="back to results">← results</button>
        <div class="thtitle"></div>
        <button id="thopen" title="resume this conversation in the chat pane">Open session</button>
      </div>
      <div id="thbody"></div>
    </div>
  </div></div>
  <div id="mainCol">
    <div id="sessionTabs" role="tablist" aria-label="Open sessions" hidden></div>
    <div id="chat"><div class="plandock" id="planDock" hidden></div><div class="wrap" id="stream"></div></div>
    <div id="composer">
      <div class="pillrow">
        <div id="thinking"><div class="twrap"><span class="dot" id="dot"></span><span class="glyph">✶</span><span class="word">idle</span><span class="meta"></span></div></div>
        <span class="pills"><span id="modelpill" title="model — click to change">🤖 model</span><span id="effort" title="thinking effort — click to change">🧠 max</span></span>
      </div>
      <div id="queue"></div>
      <div id="attach"></div>
      <div id="voiceBar" hidden><span class="vmark"></span><span id="voiceLabel"></span><button id="voiceCancel" type="button" title="Cancel voice input" aria-label="Cancel voice input">✕</button></div>
      <div class="wrap2">
      <input type="file" id="filePick" accept="image/png,image/jpeg,image/gif,image/webp,text/*,application/json,application/xml,application/javascript,application/x-yaml,application/pdf,.py,.ipynb,.js,.jsx,.ts,.tsx,.json,.jsonl,.yaml,.yml,.toml,.md,.rst,.tex,.bib,.c,.h,.cpp,.hpp,.cc,.rs,.go,.java,.kt,.sh,.bash,.zsh,.fish,.sql,.html,.css,.scss,.xml,.csv,.tsv,.ini,.cfg,.conf,.log,.diff,.patch,.mk,.m,.jl,.r,.f90,.cu,.pdf" multiple hidden>
      <button id="attachBtn" type="button" title="Attach images or files" aria-label="Attach files" disabled>📎</button>
      <button id="voiceBtn" type="button" title="Voice input" aria-label="Start voice input" disabled><img class="voice-mic-icon" src="/static/icons/fluent-studio-microphone.png" alt=""></button>
      <div id="atac"></div>
      <textarea id="ta" rows="1" placeholder="Type a message…" disabled></textarea>
      <button id="stop" title="interrupt / stop" style="display:none">⏹</button>
      <button id="send" disabled>➤</button>
    </div></div>
  </div>
</div>

<div id="drawer">
  <div id="drresize"></div>
  <div class="dh"><span class="tab on" id="tabEdits">Edits <span id="editN">0</span></span><span class="tab" id="tabGit">Git diff</span><span class="grow"></span><span class="btn" id="grefresh">↻</span><span class="btn" id="dclose">✕</span></div>
  <div class="dc" id="edits"><div class="empty">no file changes yet</div></div>
  <div class="dc" id="gitc" style="display:none"><div class="empty">—</div></div>
</div>
<div id="pman" hidden>
  <div id="pmanpanel">
    <div class="fh"><span class="ft">☑ Manage</span><span id="pmansub" class="pmsub"></span>
      <button class="btn" id="pmanx" title="close">✕</button></div>
    <div class="pmbar">
      <label><input type="checkbox" id="pmanall"> Select all</label>
      <span class="grow"></span><span id="pmancount">0 selected</span>
    </div>
    <div id="pmanlist"></div>
    <div class="fbtns"><button class="fcancel" id="pmanclose">Close</button>
      <button id="pmandel" disabled>🗑 Delete selected</button></div>
  </div>
</div>
<div id="fimp" hidden>
  <div id="fimppanel">
    <div class="fh"><span class="ft">📁 Import folder</span>
      <button class="btn" id="fimpx" title="close">✕</button></div>
    <div class="fbody">
      <input id="fimpq" placeholder="type a path…  ↑↓ to pick, Enter to add" autocomplete="off" spellcheck="false">
      <div id="fimpac"></div>
      <div id="fimpmsg"></div>
      <div class="fbtns"><button class="fcancel" id="fimpcancel">Cancel</button><button class="fadd" id="fimpok">Add project</button></div>
    </div>
  </div>
</div>
<div id="cardMenu"></div>

<script>
const $=s=>document.querySelector(s);
const PAGE_STAMP='__CLAUDE_CONSOLE_STAMP__';   /* hash of the page as served; see the server's PAGE_STAMP */
const stream=$('#stream'), ta=$('#ta'), sendBtn=$('#send'), voiceBtn=$('#voiceBtn');
let sink=stream;   /* where conversation builders append: the stream, or an Agent card's fold while its subagent's events render */
let ws=null, running=false, ready=false, compacting=false, cwd='', tools={};
let tokUp=0,tokOut=0,tokShow=false;   /* live streaming token counts shown in the pill */
let sid=null, curCC=null, editCount=0, pendingStart=false, reconnectT=0;
const EFFORTS=['low','medium','high','xhigh','max','ultracode'];
let curEffort=localStorage.getItem('al_effort')||'max';
let curModel='default', modelOnce='';   /* the live session's model; the model a @prefix put one turn on */
let showThink=false;
let liveSessions=[], projData=[], HOMEDIR='';
/* which project groups are expanded — per device, survives reloads */
let pExp=new Set();try{pExp=new Set(JSON.parse(localStorage.getItem('al_pexp')||'[]'));}catch(e){}
const EDIT_TOOLS=new Set(['Edit','MultiEdit','Write','NotebookEdit']);
const SKEY='al_session';

function esc(s){return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
/* clickable file paths → open in a web-file-manager (new tab). Base from the
   server-injected config, else this host on :7701 (the file manager's default). */
const WEBFM_CONFIG_URL = __CLAUDE_CONSOLE_WEBFM_URL__;
function escAttr(s){return esc(s).replace(/"/g,'&quot;');}
function unescHtml(s){return (s||'').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&');}
function webfmBase(){const cfg=(WEBFM_CONFIG_URL||'').replace(/\/+$/,'');
  return cfg || (location.protocol+'//'+location.hostname+':7701');}
function webfmOpenUrl(path){return webfmBase()+'/?open='+encodeURIComponent(path);}
function cleanLinkTarget(raw){let t=unescHtml((raw||'').trim());
  if(t[0]==='<'&&t[t.length-1]==='>')t=t.slice(1,-1).trim();
  try{t=decodeURI(t);}catch(e){}
  return t;}
function isLocalTarget(t){return t==='~'||t.startsWith('~/')||t.startsWith('/')||t.startsWith('file://');}
function stripLineRef(t){return t.replace(/:(\d+)(?::\d+)?$/,'');}
function protectMarkdownLinks(h,links){
  return h.replace(/\[([^\]\n]+)\]\((?:&lt;([^\n]*?)&gt;|([^)\s]+))\)/g,(m,label,angle,bare)=>{
    const target=cleanLinkTarget(angle!=null?angle:bare);
    let href='';
    if(/^https?:\/\//i.test(target))href=target;
    else if(isLocalTarget(target))href=webfmOpenUrl(stripLineRef(target));
    else return m;
    links.push('<a href="'+escAttr(href)+'" target="_blank" rel="noopener">'+label+'</a>');
    return '%%LK'+(links.length-1)+'%%';
  });}
function linkLocalPaths(h){
  const exts='pdf|png|jpe?g|gif|webp|svg|avif|heic|txt|md|markdown|rst|log|py|pyi|ipynb|js|mjs|ts|tsx|jsx|css|json|ya?ml|toml|ini|cfg|conf|xml|csv|tsv|sh|bash|zsh|fish|c|h|cpp|cc|hpp|rs|go|java|kt|rb|php|pl|lua|sql|tex|bib|m|jl|r|swift|html?';
  const re=new RegExp('(^|[\\s(>])((?:~|/)[^\\n<]*?\\.('+exts+'))(?=$|[\\s.,;:!?)}\\]]|<br>)','gi');
  return h.replace(re,(m,pre,path)=>pre+'<a href="'+escAttr(webfmOpenUrl(unescHtml(path)))+'" target="_blank" rel="noopener" class="filelink">'+path+'</a>');}
function mdTable(h){
  if(h.indexOf('|')<0)return h;
  const L=h.split('\n'),out=[];let i=0;
  const sep=l=>/^[\s:|-]+$/.test(l)&&l.includes('-')&&l.includes('|');
  const row=l=>l.replace(/^\s*\|/,'').replace(/\|\s*$/,'').split('|').map(c=>c.trim());
  while(i<L.length){
    if(L[i].includes('|')&&i+1<L.length&&sep(L[i+1])){
      const head=row(L[i]);i+=2;const rows=[];
      while(i<L.length&&L[i].trim()&&L[i].includes('|')){rows.push(row(L[i]));i++;}
      let t='<table><thead><tr>'+head.map(c=>'<th>'+c+'</th>').join('')+'</tr></thead><tbody>';
      for(const r of rows)t+='<tr>'+head.map((_,j)=>'<td>'+(r[j]==null?'':r[j])+'</td>').join('')+'</tr>';
      out.push(t+'</tbody></table>');
    }else{out.push(L[i]);i++;}
  }
  return out.join('\n');
}
function md(src){
  src=src||''; const bl=[], ml=[];
  src=src.replace(/```(\w*)\n?([\s\S]*?)```/g,(m,l,c)=>{bl.push('<pre><code>'+esc(c.replace(/\n$/,''))+'</code></pre>');return ' %%CB'+(bl.length-1)+'%% ';});
  /* protect LaTeX from the markdown passes; KaTeX renders it after insert.
     display ($$…$$, \[…\]) before inline (\(…\), $…$). */
  const mj=(t,d)=>{ml.push({t:t,d:d});return '%%MJ'+(ml.length-1)+'%%';};
  src=src.replace(/\$\$([\s\S]+?)\$\$/g,(m,c)=>mj(c,1));
  src=src.replace(/\\\[([\s\S]+?)\\\]/g,(m,c)=>mj(c,1));
  src=src.replace(/\\\(([\s\S]+?)\\\)/g,(m,c)=>mj(c,0));
  src=src.replace(/\$(?!\s)([^\n$]*?[^\s$])\$(?!\d)/g,(m,c)=>mj(c,0));
  let h=esc(src);
  h=mdTable(h);
  h=h.replace(/`([^`\n]+)`/g,(m,c)=>'<code>'+c+'</code>');
  const ll=[];
  h=protectMarkdownLinks(h,ll);   /* [label](http|local path) → placeholder; local paths open in web-file-manager */
  h=linkLocalPaths(h);            /* bare ~/… or /… file paths → web-file-manager links */
  h=h.replace(/\*\*([^*\n]+)\*\*/g,'<b>$1</b>');
  h=h.replace(/^### (.*)$/gm,'<h3>$1</h3>').replace(/^## (.*)$/gm,'<h2>$1</h2>').replace(/^# (.*)$/gm,'<h2>$1</h2>');
  h=h.replace(/%%LK(\d+)%%/g,(m,i)=>ll[+i]);   /* restore protected links */
  h=h.replace(/^(\d+)\. (.*)$/gm,'<oli>$2</oli>').replace(/^[\-\*] (.*)$/gm,'<uli>$1</uli>');
  h=h.replace(/(<uli>.*?<\/uli>(?:\n<uli>.*?<\/uli>)*)/g,m=>'<ul>'+m.replace(/\n/g,'').replace(/uli>/g,'li>')+'</ul>');
  h=h.replace(/(<oli>.*?<\/oli>(?:\n<oli>.*?<\/oli>)*)/g,m=>'<ol>'+m.replace(/\n/g,'').replace(/oli>/g,'li>')+'</ol>');
  h=h.replace(/\*(\S[^*\n]*?\S|\S)\*/g,'<i>$1</i>');
  h=h.replace(/\n/g,'<br>').replace(/<br>(<(?:pre|h2|h3|ul|ol|table)>)/g,'$1').replace(/(<\/(?:pre|h2|h3|ul|ol|table)>)<br>/g,'$1');
  h=h.replace(/%%CB(\d+)%%/g,(m,i)=>bl[+i]);
  h=h.replace(/%%MJ(\d+)%%/g,(m,i)=>'<span class="math'+(ml[+i].d?' display':'')+'" data-d="'+ml[+i].d+'">'+esc(ml[+i].t)+'</span>');
  return h;
}
/* render protected LaTeX spans with KaTeX (textContent decodes the escaped TeX) */
/* lazy KaTeX: defer typesetting until a math node scrolls near the viewport — a big
   win when replaying a long, math-heavy transcript on attach/switch (no up-front burst). */
let _mathObs=null;
function _mathObserver(){if(_mathObs)return _mathObs;
  _mathObs=new IntersectionObserver(es=>{es.forEach(en=>{if(en.isIntersecting){_renderMathEl(en.target);_mathObs.unobserve(en.target);}});},
    {root:$('#chat'),rootMargin:'1200px 0px'});return _mathObs;}
function _renderMathEl(el){if(!window.katex||el.dataset.done)return;el.dataset.done='1';
  try{katex.render(el.textContent,el,{displayMode:el.dataset.d==='1',throwOnError:false,errorColor:'#f85149'});}catch(e){}}
function typesetMath(root){if(!window.katex)return;
  root.querySelectorAll('.math:not([data-done])').forEach(el=>_mathObserver().observe(el));}
function diffHtml(t){return t.split('\n').map(l=>{let c='dl-ctx';
  if(l.startsWith('@@')||l.startsWith('diff ')||l.startsWith('+++')||l.startsWith('---'))c='dl-hdr';
  else if(l.startsWith('+'))c='dl-add';else if(l.startsWith('-'))c='dl-del';
  return '<div class="diffline '+c+'">'+esc(l||' ')+'</div>';}).join('');}
let replaying=false;   /* true while bulk-replaying a session log on attach — suppresses
                          per-event atBottom/scroll so we don't force 1000s of reflows;
                          a single scroll-to-bottom runs once at the end instead. */
/* ── following the tail ───────────────────────────────────────────────────────
   Whether the view sticks to the end is a fact about the reader, not about the
   scrollbar. It used to be read off the position, "within 140px of the end", and
   re-read every time text landed, which during streaming is every frame. A wheel
   notch is 100px and Chrome animates it over a few frames, so the reader never got
   140px away before the next frame put them back at the end and cancelled the
   animation. Scrolling up during an answer was impossible with a mouse and worse
   on a trackpad.

   So it is a state now. Any upward move the reader makes leaves the tail: the
   wheel listener catches the intent before the first animated pixel, the scroll
   listener catches drags, keys and touch. Coming back down into the last
   FOLLOW_BAND px rejoins it, and so does anything that jumps to the end on purpose
   (sending a message, an approval card, attaching). autoTop is where we last put
   the scrollbar, so a scroll event that lands above it is the reader's doing.
   Content that shrinks above the viewport (a trimmed window, the provisional bubble
   being swapped out, a collapsed card) also moves scrollTop, but it clamps exactly
   to the end, and a position at the end is never read as leaving. */
let follow=true, autoTop=0;
const FOLLOW_BAND=140;
function atBottom(){return !replaying&&follow;}
function setTop(v,f){const c=$('#chat');c.scrollTop=v;autoTop=c.scrollTop;follow=f;}
function scroll(){if(replaying)return;setTop($('#chat').scrollHeight,true);}
(()=>{const c=$('#chat');let prev=c.scrollTop;
  c.addEventListener('wheel',e=>{if(e.deltaY<0&&c.scrollTop>0)follow=false;},{passive:true});
  c.addEventListener('scroll',()=>{const top=c.scrollTop,gap=c.scrollHeight-top-c.clientHeight;
    if(top<autoTop-1&&gap>2)follow=false;
    else if(top>prev&&gap<FOLLOW_BAND)follow=true;
    prev=top;},{passive:true});})();

const ICON={Edit:'✏️',MultiEdit:'✏️',Write:'📝',Bash:'▶',Read:'📖',Glob:'🔍',Grep:'🔍',Task:'🤖',Agent:'🤖',
  WebFetch:'🌐',WebSearch:'🌐',TodoWrite:'☑️',NotebookEdit:'📓',
  TaskCreate:'☑️',TaskUpdate:'☑️',TaskList:'☑️',TaskGet:'☑️',mcp__console__plan:'☑️'};
function toolLabel(t){return t==='mcp__console__plan'?'plan':t;}   /* the console's own tool, without its MCP prefix */
function primaryArg(i){if(!i)return '';if(typeof i==='string')return i.slice(0,80);
  if(i.prompt!==undefined&&(i.description||i.subagent_type))return [i.description,i.subagent_type,i.model].filter(Boolean).join(' · ').slice(0,90);   /* Agent: what, who, on which model */
  if(Array.isArray(i.steps)){const n=i.steps.length,d=i.steps.filter(x=>x&&x.status==='completed').length,c=i.steps.find(x=>x&&x.status==='in_progress');
    return d+'/'+n+(c?' · '+(''+(c.title||'')).slice(0,60):'');}   /* plan snapshot: progress + the open step */
  if(i.file_path)return i.file_path.split('/').slice(-2).join('/');
  if(i.command)return (''+i.command).split('\n')[0].slice(0,90);
  if(i.subject)return i.subject.slice(0,80);   /* TaskCreate: the task, not its description */
  if(i.taskId!==undefined)return '#'+i.taskId+(i.status?' → '+i.status:'');
  if(i.pattern)return i.pattern;if(i.description)return i.description.slice(0,80);
  if(i.url)return i.url;return '';}
function counts(ev){const i=ev.input||{};
  if(ev.tool==='Edit'&&i.new_string!==undefined)return {a:(i.new_string.match(/\n/g)||[]).length+1,d:(i.old_string.match(/\n/g)||[]).length+1};
  if(ev.tool==='Write'&&i.content!==undefined)return {a:(i.content.match(/\n/g)||[]).length+1,d:0};
  return null;}
function toolBody(ev){const i=ev.input||{},t=ev.tool;
  if(t==='Edit'&&i.old_string!==undefined)return diffHtml(i.old_string.split('\n').map(x=>'-'+x).join('\n')+'\n'+i.new_string.split('\n').map(x=>'+'+x).join('\n'));
  if(t==='Write'&&i.content!==undefined)return '<div class="reslabel">new file content</div>'+diffHtml(i.content.split('\n').map(x=>'+'+x).join('\n'));
  if(t==='Bash'&&i.command)return '<pre><code>'+esc(i.command)+'</code></pre>';
  if(typeof i==='string')return '<pre><code>'+esc(i)+'</code></pre>';
  return '<pre><code>'+esc(JSON.stringify(i,null,2))+'</code></pre>';}

function addUser(text,nImg,files){const s=atBottom();const d=document.createElement('div');d.className='msg user';
  d.innerHTML='<div class="b">'+esc(text)+'</div>'+
    (nImg?'<div class="imgs">🖼 '+nImg+' image'+(nImg>1?'s':'')+' attached</div>':'')+
    ((files&&files.length)?'<div class="files">📎 '+files.map(esc).join(' · ')+'</div>':'');
  stream.appendChild(d);scroll();}
function addAsst(text){const s=atBottom();const d=document.createElement('div');d.className='msg asst';
  d.innerHTML='<div class="b bubble">'+md(text)+'</div>';typesetMath(d);sink.appendChild(d);if(s)scroll();}
/* ── live streaming bubble ────────────────────────────────────────────────────
   Deltas render as PLAIN TEXT, appended to a text node and never re-parsed. That
   is deliberate: running md()+KaTeX on every delta would re-parse the whole answer
   dozens of times a second, destroying and rebuilding each already-typeset formula,
   and half-arrived math ($\frac{a) would typeset as an error and then flip once the
   closing delimiter lands — the text would visibly shiver. Appending to a text node
   can only ever add glyphs, so nothing above the caret moves.

   The finished text arrives separately as the logged `asst` event, which replaces
   this bubble with the real md()+KaTeX render. So each block reflows exactly once,
   at completion, instead of continuously.

   PACING. The CLI does not stream token by token: measured, it emits a median
   137-char chunk every ~316 ms. Painting each chunk on arrival puts a sentence and
   a half on screen three times a second, which reads as stuttering. The first fix
   drained a fixed FRACTION of the backlog per frame, which is worse than it looks.
   A fresh chunk leaves at ~16 chars/frame and its tail at ~2, so the speed swung
   4.7x within every 316 ms cycle: a 3 Hz pulse of fast-then-slow, which is the
   "waits, then spits" feel rather than typing.

   So this is a jitter buffer instead, the same trick audio playback uses. Arriving
   text is held back by STREAM_LAG worth of reading, and played out at a rate that
   is smoothed over about a second (STREAM_ATK/STREAM_REL) rather than recomputed
   from scratch each frame. The backlog is what absorbs the source's silence, so
   the rate only has to track the AVERAGE arrival rate and a 316 ms gap costs
   nothing. Simulated against the real cadence this holds the speed inside
   420-480 chars/s (1.1x, down from 4.7x) with the caret about 200 chars behind.
   That lag is the price: when the `asst` event swaps this bubble out, those ~200
   chars land at once. They land inside the one reflow the swap already causes.

   Genuinely slow sources still show gaps. If the CLI is silent for 1.2 s there is
   nothing to play out, and trickling a pause-free stream anyway would only misreport
   how fast the model is going. */
let streamEl=null, streamBuf='', streamRAF=0, streamRate=0, streamCarry=0, streamPrev=0;
const STREAM_LAG=500;    /* ms of text kept buffered — must exceed the source's gap */
const STREAM_ATK=900;    /* ms time constant for speeding up */
const STREAM_REL=1500;   /* ms for slowing down — slower, so a lull does not stall us */
const STREAM_MAX=3;      /* chars/ms ceiling while catching up */
const STREAM_DUMP=2500;  /* backlog past which this is not typing any more — paste it */
function streamTake(n){
  const L=streamBuf.length;
  if(n>L)n=L;
  /* Hold back a trailing run of newlines. Emitting a line break with nothing after
     it opens a blank line that then sits empty until the next chunk lands, and that
     empty line opening and then filling is the jolt the eye catches at paragraph
     breaks. Held newlines leave the buffer non-empty, so streamFlush must not
     reschedule after an empty take or it would spin at 60fps doing nothing. */
  let keep=L;while(keep>0&&streamBuf.charCodeAt(keep-1)===10)keep--;
  if(!keep)return '';
  if(n>keep)n=keep;
  else{
    while(n<keep&&streamBuf.charCodeAt(n-1)===10)n++;   /* break + next glyphs together */
    const c=streamBuf.charCodeAt(n-1);
    if(c>=0xD800&&c<=0xDBFF&&n<keep)n++;                /* never split a surrogate pair */
  }
  const out=streamBuf.slice(0,n);streamBuf=streamBuf.slice(n);return out;}
function streamFlush(ts){
  streamRAF=0;
  if(!streamBuf)return;
  let dt=streamPrev?ts-streamPrev:16;streamPrev=ts;
  if(!(dt>0))dt=16;else if(dt>120)dt=120;  /* a backgrounded tab must not burst on return */
  let take;
  if(streamBuf.length>=STREAM_DUMP){take=streamBuf;streamBuf='';streamRate=0;streamCarry=0;}
  else{
    const want=streamBuf.length/STREAM_LAG;
    if(!streamRate)streamRate=want;        /* first frame of a block: start up to speed */
    else streamRate+=(want-streamRate)*(1-Math.exp(-dt/(want>streamRate?STREAM_ATK:STREAM_REL)));
    if(streamRate>STREAM_MAX)streamRate=STREAM_MAX;
    streamCarry+=streamRate*dt;
    const n=Math.floor(streamCarry);
    if(n<1){streamRAF=requestAnimationFrame(streamFlush);return;}
    streamCarry-=n;take=streamTake(n);
  }
  if(take){
    if(!streamEl){
      const s0=atBottom();
      const d=document.createElement('div');d.className='msg asst streaming';
      d.innerHTML='<div class="b bubble"><span class="stxt"></span><span class="scaret"></span></div>';
      stream.appendChild(d);streamEl=d.querySelector('.stxt');
      if(s0)scroll();
    }
    const s=atBottom();
    streamEl.appendChild(document.createTextNode(take));
    if(s)scroll();
    if(streamBuf)streamRAF=requestAnimationFrame(streamFlush);
  }
  /* took nothing: only held-back newlines remain — park until more text arrives */}
function addStreamText(t){
  if(!t)return;
  streamBuf+=t;
  /* restarting from parked: forget the stale clock and part-character, keep the rate */
  if(!streamRAF){streamPrev=0;streamCarry=0;streamRAF=requestAnimationFrame(streamFlush);}}
/* the provisional bubble is dropped the moment any authoritative event lands */
function endStream(){
  streamBuf='';streamRate=0;streamCarry=0;streamPrev=0;
  if(streamRAF){cancelAnimationFrame(streamRAF);streamRAF=0;}
  if(streamEl){const w=streamEl.closest('.msg');if(w)w.remove();streamEl=null;}}
function addThink(text){const s=atBottom();const d=document.createElement('div');d.className='think'+(showThink?'':' hide');d.dataset.t=1;
  d.textContent=text;sink.appendChild(d);if(s)scroll();}
function addNotice(t){const d=document.createElement('div');d.className='notice';d.textContent=t;stream.appendChild(d);}
function addRecap(t){const s=atBottom();const d=document.createElement('div');d.className='recap';
  d.innerHTML='<span class="rk">※ recap:</span> <span class="rt"></span>';
  d.querySelector('.rt').textContent=t;stream.appendChild(d);if(s)scroll();}
/* ── plan dock ────────────────────────────────────────────────────────────────
   The plan comes from the console's own tool (PLAN_TOOL on the server), which
   sends the whole list per call, and the server pushes the WHOLE list on every
   change. That is what makes this simple: there is no incremental state here, every
   `plan` event is a complete snapshot, and replaying a history just converges on
   whatever the last one said. The dock is sticky rather than inline because a plan
   is a statement about the present, and inline it would scroll away into history. */
let planTasks=[],planCollapsed=false,planHideT=0;
const PLAN_HIDE_MS=2000;
const PLAN_MARK={completed:['done','●'],in_progress:['active','◐'],pending:['todo','○']};   /* filled, half, hollow */
function planMark(st){return PLAN_MARK[(''+(st||'pending')).toLowerCase()]||PLAN_MARK.pending;}
function planDone(t){return planMark(t.status)[0]==='done';}
function planAllDone(ts){return ts.length>0&&ts.every(planDone);}
function renderPlan(){
  const host=$('#planDock');if(!host)return;
  if(!planTasks.length){host.hidden=true;host.innerHTML='';return;}
  /* collapsed shows only what is being worked on right now — the whole point of a
     pinned plan is the current step, the rest is reference */
  const shown=planCollapsed?planTasks.filter(t=>planMark(t.status)[0]==='active'):planTasks;
  let h='<div class="plan"><div class="ph"><span class="ptitle">☑ Plan</span>'+
    '<span class="pcount">'+planTasks.filter(planDone).length+'/'+planTasks.length+'</span>'+
    '<button type="button" class="ptoggle" aria-expanded="'+(!planCollapsed)+'" title="'+
    (planCollapsed?'Expand plan':'Collapse plan')+'">'+(planCollapsed?'▼':'▲')+'</button></div>';
  if(shown.length){h+='<div class="psteps">';
    shown.forEach(t=>{const mk=planMark(t.status);
      h+='<div class="pst '+mk[0]+'" title="'+escAttr(t.description||'')+'">'+
         '<span class="pi">'+mk[1]+'</span><span class="psub">'+
         esc(t.subject||t.activeForm||('task #'+t.id))+'</span></div>';});
    h+='</div>';}
  else h+='<div class="pempty">no active task</div>';
  host.innerHTML=h+'</div>';host.hidden=false;
  const tg=host.querySelector('.ptoggle');if(tg)tg.onclick=togglePlan;}
function togglePlan(){planCollapsed=!planCollapsed;
  const t=tabById(sid);if(t){t.planCollapsed=planCollapsed;persistTabs();}renderPlan();}
/* A finished plan is just clutter, so it retires itself. Re-checked when the timer
   fires: the session may have been switched, or new work may have reopened it. */
function schedulePlanHide(){
  if(planHideT){clearTimeout(planHideT);planHideT=0;}
  if(replaying||!planAllDone(planTasks))return;
  const owner=sid;
  planHideT=setTimeout(()=>{planHideT=0;
    /* drop the list, not just the dock: keeping a retired plan in memory means the
       next renderPlan (folding it, restoring the tab) brings the finished thing
       back, and folded it reads "no active task" */
    if(sid===owner&&planAllDone(planTasks))clearPlan();},PLAN_HIDE_MS);}
function setPlan(ts){planTasks=Array.isArray(ts)?ts:[];renderPlan();schedulePlanHide();}
function clearPlan(){if(planHideT){clearTimeout(planHideT);planHideT=0;}
  planTasks=[];planCollapsed=false;const h=$('#planDock');if(h){h.hidden=true;h.innerHTML='';}}
/* turn-complete footer line — ✻ {past verb} for {N}s · {YYYY-MM-DD HH:MM:SS} UTC (server-stamped) */
function utcStamp(sec){const d=new Date(sec*1000),p=n=>String(n).padStart(2,'0');
  return d.getUTCFullYear()+'-'+p(d.getUTCMonth()+1)+'-'+p(d.getUTCDate())+' '+
         p(d.getUTCHours())+':'+p(d.getUTCMinutes())+':'+p(d.getUTCSeconds())+' UTC';}
function addDone(word,durMs,atSec){const s=atBottom();const d=document.createElement('div');d.className='doneline';
  d.innerHTML='<span class="dg">✻</span> <span class="dw"></span>';
  let t=word+(durMs>0?(' for '+fmtSecs(durMs)):'');
  if(atSec)t+=' · '+utcStamp(atSec);
  d.querySelector('.dw').textContent=t;
  stream.appendChild(d);if(s)scroll();}
/* turn-complete: if any background tasks are still running, note them right under the ✻ line */
function addBgRunning(list){const s=atBottom();const n=list.length;const d=document.createElement('div');d.className='bgline';
  d.innerHTML='<span class="bgk"></span><span class="bgt"></span>';
  d.querySelector('.bgk').textContent='⊙ '+n+' background task'+(n>1?'s':'')+' running';
  const labels=list.filter(Boolean);
  d.querySelector('.bgt').textContent=labels.length?(' · '+labels.join(' · ')):'';
  stream.appendChild(d);if(s)scroll();}
function addErr(t){const d=document.createElement('div');d.className='errline';d.textContent=t;stream.appendChild(d);if(atBottom())scroll();}
/* ── consecutive same-tool calls fold into one group ──────────────────────────
   Only ADJACENCY groups: the check is against stream.lastElementChild, so any
   answer text, edit marker or approval between two calls ends the run. That is
   the whole semantic — a group means "these happened back to back with nothing
   in between", which is exactly the noise worth folding and exactly the context
   worth keeping. */
function toolGroupKey(ev){if(ev&&(ev.tool==='Agent'||ev.tool==='Task'))return null;   /* each Agent card is its own fold */
  return (''+((ev&&ev.tool)||'')).trim().toLowerCase();}
function groupCards(g){return [...g.querySelectorAll(':scope>.tgb>.tool')];}
function refreshToolGroup(g){
  if(!g)return;
  const cards=groupCards(g);if(!cards.length)return;
  const last=cards[cards.length-1]._ev||{};
  const errs=cards.filter(c=>c.classList.contains('err')).length;
  const open=cards.filter(c=>!c.classList.contains('done')).length;
  g.querySelector('.tgcount').textContent='×'+cards.length;
  /* the newest call's argument: a folded run still shows what it is doing now */
  g.querySelector('.tp').textContent=primaryArg(last.input)||'';
  g.classList.toggle('err',errs>0);
  g.querySelector('.tgstate').textContent=errs?(errs+' failed'):(open?'running':'');}
function makeToolGroup(first,second,key){
  const ev=first._ev||second._ev||{};
  const g=document.createElement('div');g.className='toolgroup';g.dataset.tkey=key;
  g.innerHTML='<div class="tgh"><span class="ico">'+(ICON[ev.tool]||'🔧')+'</span>'+
    '<span class="tn">'+esc(toolLabel(ev.tool||'tool'))+'</span><span class="tgcount"></span>'+
    '<span class="tp"></span><span class="tgstate"></span>'+TOOL_EYE+'</div><div class="tgb"></div>';
  first.replaceWith(g);                      /* the group takes the first card's place */
  const body=g.querySelector('.tgb');
  body.appendChild(first);body.appendChild(second);
  g.querySelector('.tgh').onclick=()=>g.classList.toggle('open');
  /* a card the user had opened must not vanish behind a fold it did not ask for */
  if(first.classList.contains('open'))g.classList.add('open');
  refreshToolGroup(g);return g;}
function placeToolCard(c,ev){
  const key=toolGroupKey(ev),prev=sink.lastElementChild;
  if(prev&&key){
    if(prev.classList.contains('toolgroup')&&prev.dataset.tkey===key){
      prev.querySelector(':scope>.tgb').appendChild(c);refreshToolGroup(prev);return;}
    if(prev.classList.contains('tool')&&toolGroupKey(prev._ev)===key){
      makeToolGroup(prev,c,key);return;}}
  sink.appendChild(c);}
const TOOL_EYE='<span class="eye"><svg class="e-shut" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 11c3 4 7 6 10 6s7-2 10-6"/><line x1="5.5" y1="15.5" x2="4.3" y2="18"/><line x1="12" y1="17.5" x2="12" y2="20"/><line x1="18.5" y1="15.5" x2="19.7" y2="18"/></svg><svg class="e-open" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg></span>';
/* An Agent card is the whole delegated run: the brief it was given, then every
   message and tool call its subagent makes (routed here by the `parent` tag),
   then the report it came back with as the result. Folded by default; the
   header carries a live count and the subagent's own token usage. */
function agentStat(c,d){const a=c._ag;if(!a)return;
  if(d.tools)a.tools+=d.tools;if(d.up!=null)a.up=d.up;if(d.out!=null)a.out=d.out;if(d.est!=null)a.est=!!d.est;
  if(d.doing!==undefined)a.doing=(''+(d.doing||'')).slice(0,48);   /* the brief needs its room too */
  const el=c.querySelector(':scope>.th .agst');if(!el)return;
  const run=c.classList.contains('running');
  el.textContent=(run?(c._bg?'background':'running'):(a.status||'done'))+(run&&a.doing?(' · '+a.doing):'')+
    ' · '+a.tools+' tool'+(a.tools===1?'':'s')+((a.up||a.out)?(' · ↑'+fmtTok(a.up)+' ↓'+(a.est?'~':'')+fmtTok(a.out)):'');}
/* a backgrounded subagent: the Agent tool result is only the launch notice, the
   real report arrives later as a task notification (agent_done) */
function agentLaunched(c){c._bg=true;c.classList.add('running');
  (c.querySelector(':scope>.tb>.res')||{}).innerHTML='<div class="reslabel">running in the background — the report lands here when it finishes</div>';
  agentStat(c,{});}
function agentDone(ev){const c=tools[ev.toolId];if(!c||!c._ag)return;
  c._bg=false;c.classList.remove('running');c.classList.add('done');if(ev.status&&ev.status!=='completed')c.classList.add('err');
  c._ag.status=ev.status&&ev.status!=='completed'?ev.status:'done';c._ag.doing='';
  const r=c.querySelector(':scope>.tb>.res');
  if(r&&ev.summary){r.innerHTML='<div class="reslabel">report ⤵</div><div class="b">'+md(ev.summary)+'</div>';typesetMath(r);}
  agentStat(c,ev.agent||{});}
function addTool(ev){const s=atBottom();const c=document.createElement('div');c.className='tool';
  const agent=ev.tool==='Agent'||ev.tool==='Task';if(agent)c.classList.add('agent','running');
  const cn=counts(ev);const cnt=cn?('<span class="a">+'+cn.a+'</span> <span class="d">−'+cn.d+'</span>'):'';
  c.innerHTML='<div class="th"><span class="ico">'+(ICON[ev.tool]||'🔧')+'</span><span class="tn">'+esc(toolLabel(ev.tool))+'</span>'+
    '<span class="tp">'+esc(primaryArg(ev.input))+'</span>'+(agent?'<span class="agst"></span>':'')+'<span class="cnt">'+cnt+'</span>'+TOOL_EYE+'</div>'+
    '<div class="tb">'+(agent?'<div class="brief"></div><div class="sub"></div>':'')+'<div class="res"></div></div>';
  c._ev=ev;   /* lazy: build the (maybe large) body only on first expand */
  if(agent){c._sub=c.querySelector('.sub');c._ag={tools:0,up:0,out:0};agentStat(c,{});}
  c.querySelector('.th').onclick=()=>{const open=c.classList.toggle('open');
    if(open&&!c._bodyDone){c._bodyDone=1;const b=c.querySelector(':scope>.tb>.brief');
      if(b)b.innerHTML=toolBody(c._ev);else c.querySelector('.res').insertAdjacentHTML('beforebegin',toolBody(c._ev));}};
  placeToolCard(c,ev);if(ev.toolId)tools[ev.toolId]=c;if(s)scroll();}
function addResult(ev){const c=tools[ev.toolId];if(!c)return;if(ev.isError)c.classList.add('err');
  /* the card's OWN result box: an Agent card has more .res inside its fold; an edit card has no .tb at all */
  const b=(ev.content||'').trim();(c.querySelector(':scope>.tb>.res')||c.querySelector('.res')).innerHTML='<div class="reslabel">'+(ev.isError?'error ⤵':'output ⤵')+
    '</div><pre><code>'+esc(b.length>2200?b.slice(0,2200)+'\n…':b)+'</code></pre>';
  if(c._ag&&(c._bg||/^Async agent launched/.test(b))){agentLaunched(c);return;}   /* not a result, a launch notice */
  c.classList.add('done');   /* a card with its result in is no longer in flight */
  if(c._ag){c.classList.remove('running');agentStat(c,ev.agent||{});}
  if(c.closest)refreshToolGroup(c.closest('.toolgroup'));}

function statset(t){const el=$('#thinking');if(!el)return;
  const w=el.querySelector('.word');if(w)w.textContent=t;
  if(!running){const m=el.querySelector('.meta');if(m)m.textContent='';}}
/* CLI-style ↑input / ↓output token counts for the pill (reuses the project fmtTok) */
function tokStr(){return '↑'+fmtTok(tokUp)+' ↓'+fmtTok(tokOut);}
function bindProject(p){if(!p)return;const sel=$('#project');let ok=false;
  for(const o of sel.options){if(o.value===p){ok=true;break;}}
  if(!ok){const o=document.createElement('option');o.value=p;o.textContent='● '+p.split('/').slice(-2).join('/');sel.insertBefore(o,sel.firstChild);}
  sel.value=p;}
function setBusy(b,wordSeed,elapsedMs){running=b;$('#dot').className='dot '+(b?'busy':(ready?'on':''));
  $('#thinking').classList.toggle('busy',b);
  if(!b&&ready)statset('ready');      /* busy: startThinking owns the word + timer */
  ta.disabled=!ready;
  attachOff(!ready);
  sendBtn.disabled=!ready;            /* send stays available while busy → queues */
  $('#stop').style.display=b?'':'none';   /* interrupt button only while busy */
  sendBtn.style.display='';               /* send always visible */
  if(b)startThinking(wordSeed,elapsedMs);else stopThinking();}

/* in-chat "thinking" indicator — animated glyph + cycling word + elapsed timer */
/* spinner verbs — the full Claude Code CLI set (187 present participles, incl. the
   "Clauding" easter egg). A verb is picked per agentic step (see setWord). */
const THINK_WORDS=['Accomplishing','Actioning','Actualizing','Architecting','Baking','Beaming','Beboppin\'','Befuddling','Billowing','Blanching','Bloviating','Boogieing','Boondoggling','Booping','Bootstrapping','Brewing','Bunning','Burrowing','Calculating','Canoodling','Caramelizing','Cascading','Catapulting','Cerebrating','Channeling','Channelling','Choreographing','Churning','Clauding','Coalescing','Cogitating','Combobulating','Composing','Computing','Concocting','Considering','Contemplating','Cooking','Crafting','Creating','Crunching','Crystallizing','Cultivating','Deciphering','Deliberating','Determining','Dilly-dallying','Discombobulating','Doing','Doodling','Drizzling','Ebbing','Effecting','Elucidating','Embellishing','Enchanting','Envisioning','Evaporating','Fermenting','Fiddle-faddling','Finagling','Flambéing','Flibbertigibbeting','Flowing','Flummoxing','Fluttering','Forging','Forming','Frolicking','Frosting','Gallivanting','Galloping','Garnishing','Generating','Gesticulating','Germinating','Gitifying','Grooving','Gusting','Harmonizing','Hashing','Hatching','Herding','Honking','Hullaballooing','Hyperspacing','Ideating','Imagining','Improvising','Incubating','Inferring','Infusing','Ionizing','Jitterbugging','Julienning','Kneading','Leavening','Levitating','Lollygagging','Manifesting','Marinating','Meandering','Metamorphosing','Misting','Moonwalking','Moseying','Mulling','Mustering','Musing','Nebulizing','Nesting','Newspapering','Noodling','Nucleating','Orbiting','Orchestrating','Osmosing','Perambulating','Percolating','Perusing','Philosophising','Photosynthesizing','Pollinating','Pondering','Pontificating','Pouncing','Precipitating','Prestidigitating','Processing','Proofing','Propagating','Puttering','Puzzling','Quantumizing','Razzle-dazzling','Razzmatazzing','Recombobulating','Reticulating','Roosting','Ruminating','Sautéing','Scampering','Schlepping','Scurrying','Seasoning','Shenaniganing','Shimmying','Simmering','Skedaddling','Sketching','Slithering','Smooshing','Sock-hopping','Spelunking','Spinning','Sprouting','Stewing','Sublimating','Swirling','Swooping','Symbioting','Synthesizing','Tempering','Thinking','Thundering','Tinkering','Tomfoolering','Topsy-turvying','Transfiguring','Transmuting','Twisting','Undulating','Unfurling','Unravelling','Vibing','Waddling','Wandering','Warping','Whatchamacalliting','Whirlpooling','Whirring','Whisking','Wibbling','Working','Wrangling','Zesting','Zigzagging'];
const THINK_GLYPHS=['✶','✷','✸','✹','✺','✹','✸','✷'];
const DREAM_GLYPHS=['Zzz','zZz','zzZ','ZzZ'];   /* compacting: a slow breathing Zzz wave */
/* compacting meta: crush-style flowing gradient over scrambling hex (cf. charmbracelet/crush
   internal/ui/anim/anim.go) — runes à la crush (hex+symbols, minus <>&"' for innerHTML safety);
   each cell re-scrambles every frame while a warm<->cool color wave scrolls across the row */
const MATRIX_CHARS='0123456789abcdefABCDEF~!@#$%^*()+=_-?/|';
const MTX_PERIOD=12, MTX_FLOW=0.06;            /* ~1.3 colour sweeps across the row, scrolling ~1cyc/1.1s */
let gradA=[224,192,128], gradB=[79,193,255];   /* [--tool,--acc] RGB, refreshed from the live theme per compaction */
let mtxLite=false;                              /* light theme: darken the wave + drop the glow so the chars stay legible */
let mtxTheme=null;                              /* last theme the palette was built for → re-read on live theme-swap */
function cssRGB(name){const v=getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  const m=/^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(v);
  return m?[parseInt(m[1],16),parseInt(m[2],16),parseInt(m[3],16)]:[224,192,128];}
function mtxLum(c){return (0.299*c[0]+0.587*c[1]+0.114*c[2])/255;}   /* perceived luminance 0..1 */
function mtxAdjust(c,light){    /* light bg only: scale toward black to a max luminance (hue kept); dark path untouched */
  if(!light)return c;
  const L=mtxLum(c),MAXL=0.34;
  if(L<=MAXL)return c;
  const k=MAXL/L;return [Math.round(c[0]*k),Math.round(c[1]*k),Math.round(c[2]*k)];}
function mtxRefresh(){    /* re-read palette from the live theme; cheap no-op until the theme attr actually changes (hot-swap) */
  const th=document.documentElement.getAttribute('data-theme')||'dark';
  if(th===mtxTheme)return;
  mtxTheme=th;
  mtxLite=mtxLum(cssRGB('--bg'))>0.5;
  gradA=mtxAdjust(cssRGB('--tool'),mtxLite);
  gradB=mtxAdjust(cssRGB('--acc'),mtxLite);}
function matrixHTML(n,phase){let s='';for(let i=0;i<n;i++){
    const ch=MATRIX_CHARS[Math.floor(Math.random()*MATRIX_CHARS.length)];
    const t=0.5-0.5*Math.cos(2*Math.PI*(i/MTX_PERIOD+phase*MTX_FLOW)),
          r=Math.round(gradA[0]+(gradB[0]-gradA[0])*t),
          g=Math.round(gradA[1]+(gradB[1]-gradA[1])*t),
          b=Math.round(gradA[2]+(gradB[2]-gradA[2])*t);
    s+='<span style="color:rgb('+r+','+g+','+b+')">'+ch+'</span>';}
  return s;}
let thinkTimer=0,thinkStart=0,thinkGi=0,thinkWi=0,lastWordSeed=null;
/* map a server seed → spinner verb (so the word is stable across reattach and
   changes only when the server re-picks the seed, i.e. once per agentic step) */
function setWord(seed){const el=$('#thinking');if(!el)return;
  thinkWi=seed!=null?((seed%THINK_WORDS.length)+THINK_WORDS.length)%THINK_WORDS.length:Math.floor(Math.random()*THINK_WORDS.length);
  const w=el.querySelector('.word');if(w)w.textContent=THINK_WORDS[thinkWi];}
function startThinking(wordSeed,elapsedMs){const el=$('#thinking');if(!el)return;
  const fresh=!thinkTimer;         /* new turn, or reattaching to a running one */
  if(fresh||elapsedMs!=null)thinkStart=Date.now()-(elapsedMs||0);
  el.classList.toggle('compacting',compacting);
  if(compacting){thinkGi=0;el.querySelector('.word').textContent='Compacting';el.querySelector('.glyph').textContent=DREAM_GLYPHS[0];}
  else if(fresh||wordSeed!=null){  /* server-provided seed keeps the word stable across reattach */
    lastWordSeed=wordSeed;setWord(wordSeed);
  }
  if(fresh&&atBottom())scroll();
  clearInterval(thinkTimer);
  let frame=0;
  const tick=compacting?65:130;    /* compacting shimmer wants ~15fps; the normal spinner stays 130ms */
  if(compacting)mtxRefresh();    /* seed palette from the live theme (re-checked each frame for hot theme-swap) */
  thinkTimer=setInterval(()=>{     /* during the turn only the glyph + timer move */
    frame++;
    const s=Math.floor((Date.now()-thinkStart)/1000);
    if(compacting){
      mtxRefresh();             /* live theme-swap: re-read the palette if the user changes theme mid-compaction */
      if(frame%6===0)thinkGi=(thinkGi+1)%DREAM_GLYPHS.length;   /* slow breathing Zzz (~390ms) */
      el.querySelector('.glyph').textContent=DREAM_GLYPHS[thinkGi];
      el.querySelector('.meta').innerHTML=s+'s · <span class="mtx'+(mtxLite?' lite':'')+'">'+matrixHTML(16,frame)+'</span>';
    }else{
      thinkGi=(thinkGi+1)%THINK_GLYPHS.length;
      el.querySelector('.glyph').textContent=THINK_GLYPHS[thinkGi];
      const tok=tokShow?(tokStr()+' · '):'';
      el.querySelector('.meta').textContent=tok+s+'s';
    }
  },tick);}
function stopThinking(){clearInterval(thinkTimer);thinkTimer=0;const el=$('#thinking');if(el)el.classList.remove('compacting');}
function fmtSecs(ms){const s=Math.max(0,Math.round(ms/1000));if(s<60)return s+'s';const m=Math.floor(s/60);return m+'m '+(s%60)+'s';}
function doInterrupt(){if(!running)return;wsSend({type:'interrupt'});addNotice('⏹ interrupt sent');}
function clearUI(){stream.innerHTML='';$('#edits').innerHTML='<div class="empty">no file changes yet</div>';
  $('#gitc').innerHTML='<div class="empty">—</div>';tools={};editCount=0;updateEditBadge();renderCtx(null);ready=false;
  queued={};renderQueue();stopThinking();clearPlan();trimHidden=0;
  endStream();}   /* also cancels a pending frame, which would else refill the cleared stream */

/* file edits → out of chat, into the Changes drawer */
function updateEditBadge(){$('#editN').textContent=editCount;}
function addEditCard(ev){if(editCount===0)$('#edits').innerHTML='';
  const c=document.createElement('div');c.className='ecard';const cn=counts(ev);
  const cnt=cn?('<span class="cnt"><span class="a">+'+cn.a+'</span> <span class="d">−'+cn.d+'</span></span>'):'';
  c.innerHTML='<div class="eh"><span>'+(ICON[ev.tool]||'✏️')+'</span><span class="ef">'+esc(primaryArg(ev.input)||ev.tool)+'</span>'+cnt+'</div>'+
    '<div class="ed"></div><div class="res"></div>';
  c._ev=ev;   /* lazy: build the diff only when the Changes drawer is actually shown */
  if(drawerOpen()&&!gitTab()){c._bodyDone=1;c.querySelector('.ed').innerHTML=toolBody(ev);}
  $('#edits').appendChild(c);if(ev.toolId)tools[ev.toolId]=c;editCount++;updateEditBadge();
  if(!replaying){const ed=$('#edits');ed.scrollTop=ed.scrollHeight;}}
function buildPendingEdits(){$('#edits').querySelectorAll('.ecard').forEach(c=>{
  if(c._ev&&!c._bodyDone){c._bodyDone=1;c.querySelector('.ed').innerHTML=toolBody(c._ev);}});}
function addMarker(ev){const s=atBottom();const cn=counts(ev);const m=document.createElement('div');m.className='emark';
  m.innerHTML=(ICON[ev.tool]||'✏️')+'<span>'+esc(primaryArg(ev.input)||ev.tool)+'</span>'+
    (cn?'<span><span class="a">+'+cn.a+'</span> <span class="d">−'+cn.d+'</span></span>':'')+'<span class="mut">— see Changes</span>';
  m.onclick=()=>{openDrawer('edits');focusEdit(ev.toolId);};
  sink.appendChild(m);if(s)scroll();}

function addApproval(ev){const c=document.createElement('div');c.className='approval';c.dataset.aid=ev.aid;
  c.innerHTML='<div class="ah">🔐 Approve <b>'+esc(ev.tool)+'</b> <span class="tp">'+esc(primaryArg(ev.input)||'')+'</span></div>'+
    '<div class="abody">'+toolBody(ev)+'</div>'+
    '<div class="abtns"><button class="appr">✓ Approve</button>'+
    (ev.always?'<button class="apprall" title="approve and don\'t ask again this session">✓✓ Always</button>':'')+
    '<button class="deny">✕ Deny</button></div>';
  c.querySelector('.appr').onclick=()=>decide(ev.aid,true,false);
  const aa=c.querySelector('.apprall');if(aa)aa.onclick=()=>decide(ev.aid,true,true);
  c.querySelector('.deny').onclick=()=>decide(ev.aid,false,false);
  stream.appendChild(c);scroll();}
function decide(aid,allow,always){wsSend({type:'approve',aid:aid,allow:allow,always:!!always});resolveApprovalCard(aid,allow,always);}
function resolveApprovalCard(aid,allow,always){const c=stream.querySelector('.approval[data-aid="'+aid+'"]');
  if(c&&!c.classList.contains('done')){c.classList.add('done');
    const bt=c.querySelector('.abtns');if(bt)bt.innerHTML='<span class="'+(allow?'ok':'no')+'">'+(allow?('✓ Approved'+(always?" · won't ask again this session":'')):'✕ Denied')+'</span>';}}
function qval(bl){const other=bl.querySelector('.qother').value.trim();
  if(other)return other;
  return [...bl.querySelectorAll('.qopt.sel')].map(x=>x.dataset.label).join(', ');}
function addQuestion(ev){const c=document.createElement('div');c.className='question';c.dataset.aid=ev.aid;
  const qs=ev.questions||[];let h='<div class="qh">❓ <b>Question'+(qs.length>1?'s':'')+'</b></div>';
  qs.forEach((q,qi)=>{h+='<div class="qblk" data-qi="'+qi+'"'+(q.multiSelect?' data-multi="1"':'')+'>'+
    '<div class="qtext">'+(q.header?'<span class="chip">'+esc(q.header)+'</span>':'')+esc(q.question||('Question '+(qi+1)))+'</div>'+
    '<div class="qopts">';
    (q.options||[]).forEach(o=>{h+='<button class="qopt" data-label="'+esc(o.label)+'">'+esc(o.label)+
      (o.description?'<span class="od">'+esc(o.description)+'</span>':'')+'</button>';});
    h+='</div><input class="qother" placeholder="Other… custom answer'+(q.multiSelect?' (comma-separated)':'')+'"></div>';});
  h+='<div class="qbtns"><button class="qsub" disabled>Submit</button></div>';
  c.innerHTML=h;const sub=c.querySelector('.qsub');
  function refresh(){let ok=qs.length>0;c.querySelectorAll('.qblk').forEach(bl=>{if(!qval(bl))ok=false;});sub.disabled=!ok;}
  c.querySelectorAll('.qblk').forEach(bl=>{const multi=bl.dataset.multi==='1';
    bl.querySelectorAll('.qopt').forEach(b=>{b.onclick=()=>{
      if(multi)b.classList.toggle('sel');
      else bl.querySelectorAll('.qopt').forEach(x=>x.classList.toggle('sel',x===b));
      refresh();};});
    bl.querySelector('.qother').oninput=refresh;});
  sub.onclick=()=>{const picks=qs.map((q,qi)=>qval(c.querySelector('.qblk[data-qi="'+qi+'"]')));
    wsSend({type:'answer',aid:ev.aid,answers:picks});resolveQuestionCard(ev.aid,picks);};
  stream.appendChild(c);scroll();}
function resolveQuestionCard(aid,ans){const c=stream.querySelector('.question[data-aid="'+aid+'"]');
  if(!c||c.classList.contains('done'))return;c.classList.add('done');
  let vals=null;if(Array.isArray(ans))vals=ans.filter(Boolean);
  else if(ans&&typeof ans==='object')vals=Object.values(ans);
  const bt=c.querySelector('.qbtns');const has=vals&&vals.length;
  if(bt)bt.insertAdjacentHTML('afterend','<div class="qdone">'+(has?'✓ '+vals.map(esc).join(' · '):'✕ dismissed')+'</div>');}
function fmtCompacted(ev){let s='🗜 context compacted';
  if(ev.pre!=null||ev.post!=null)s+=' · '+fmtTok(ev.pre)+' → '+fmtTok(ev.post)+' tokens';
  if(ev.trigger)s+=' · '+(ev.trigger==='auto'?'auto':'manual');
  if(ev.ms)s+=' · '+Math.round(ev.ms/1000)+'s';
  return s;}
function route(ev){
  /* the cursor this tab can resume from — every switch back asks for what follows it */
  const _s=Number(ev&&ev._seq||0);
  if(_s){const _t=tabById(sid);
    if(_t){_t.lastSeq=Math.max(Number(_t.lastSeq||0),_s);
           _t.serverSeq=Math.max(Number(_t.serverSeq||0),_t.lastSeq);}}
  /* a subagent's events render inside the Agent card that spawned it, never in
     the conversation; the streaming bubble is the main agent's and stays */
  if(ev.parent){const c=tools[ev.parent];if(!c||!c._sub)return;
    sink=c._sub;try{
      if(ev.kind==='assistant_text')addAsst(ev.text);
      else if(ev.kind==='thinking')addThink(ev.text);
      else if(ev.kind==='tool_use'){if(EDIT_TOOLS.has(ev.tool)){addEditCard(ev);addMarker(ev);}else addTool(ev);agentStat(c,{tools:1});}
      else if(ev.kind==='tool_result')addResult(ev);
    }finally{sink=stream;}
    return;}
  /* if activity resumes while we think we're idle (e.g. the CLI ran an injected
     queued message as its own turn), step back into the busy state */
  if(!running&&(ev.kind==='assistant_text'||ev.kind==='thinking'||ev.kind==='tool_use'))setBusy(true);
  /* every logged event is authoritative, so it supersedes the provisional
     streaming bubble — drop it before rendering whatever really landed */
  if(ev.kind!=='stream_text')endStream();
  if(ev.kind==='stream_text')addStreamText(ev.text);
  else if(ev.kind==='user_text')addUser(ev.text,ev.images,ev.files);
  else if(ev.kind==='ready'){ready=true;cwd=ev.cwd||cwd;curCC=ev.session_id||curCC;if(ev.model)setResolvedModel(ev.model);addNotice('● session ready · '+(ev.model||'')+(ev.effort?' · '+ev.effort+' effort':'')+' · '+(ev.cwd||''));}
  else if(ev.kind==='assistant_text')addAsst(ev.text);
  else if(ev.kind==='thinking')addThink(ev.text);
  else if(ev.kind==='tool_use'){if(EDIT_TOOLS.has(ev.tool)){addEditCard(ev);addMarker(ev);}else addTool(ev);}
  else if(ev.kind==='tool_result')addResult(ev);
  else if(ev.kind==='approval')addApproval(ev);
  else if(ev.kind==='approval_resolved')resolveApprovalCard(ev.aid,ev.allow,ev.always);
  else if(ev.kind==='question')addQuestion(ev);
  else if(ev.kind==='question_resolved')resolveQuestionCard(ev.aid,ev.answers);
  else if(ev.kind==='turn_start'){tokShow=false;setBusy(true,ev.word,0);}
  else if(ev.kind==='compacting'){compacting=true;setBusy(true,ev.word,0);}
  else if(ev.kind==='compacted'){compacting=false;addNotice(fmtCompacted(ev));if(ev.trigger!=='auto')setBusy(false);}
  else if(ev.kind==='turn_done'){compacting=false;setBusy(false);if(ev.done_word)addDone(ev.done_word,ev.dur_ms||0,ev.done_at);if(ev.bg_running&&ev.bg_running.length)addBgRunning(ev.bg_running);if(drawerOpen()&&gitTab())refreshGit();}
  else if(ev.kind==='queued')addQueued(ev);
  else if(ev.kind==='dequeued'||ev.kind==='unqueued')removeQueued(ev.qid);
  else if(ev.kind==='notice')addNotice(ev.text);
  else if(ev.kind==='recap')addRecap(ev.text);
  else if(ev.kind==='model')setModelPill(ev.model,!!ev.once);
  else if(ev.kind==='agent_state'){const c=tools[ev.toolId];if(c&&c._ag&&ev.state==='background')agentLaunched(c);}
  else if(ev.kind==='agent_done')agentDone(ev);
  else if(ev.kind==='effort')setEffortPill(ev.effort);
  else if(ev.kind==='plan')setPlan(ev.tasks);
  if(!replaying&&stream.children.length>CHAT_KEEP_ITEMS+120)trimChatWindow();
}

/* persistent server-side session: attach / reattach / switch */
function markEnded(msg){ready=false;ta.disabled=true;sendBtn.disabled=true;attachOff(true);$('#dot').className='dot';statset('ended');
  localStorage.removeItem(SKEY);if(msg)addNotice(msg);}
function onMsg(e){const m=JSON.parse(e.data);
  if(m.type==='hello'){if(m.stamp&&m.stamp!==PAGE_STAMP){try{saveDraft();}catch(_){}location.reload();}return;}   /* served under older code: pick up the new page */
  if(m.type==='started'){pendingStart=false;sid=m.id;cwd=m.cwd;bindProject(m.cwd);localStorage.setItem(SKEY,sid);loadDraft(sid);
    ready=true;setBusy(false);ta.focus();setCurname(m.name||'session');setModelPill(m.model,false);setEffortPill(m.effort);renderCtx(null);statset('ready');
    const startedTab=ensureTab(m,true);
    if(startedTab){startedTab.lastSeq=0;startedTab.serverSeq=0;persistTabs();renderTabs();}
    restoredViewId=m.id;
    addNotice('new session « '+(m.name||'')+' »'+(m.effort?' · '+m.effort+' effort':'')+' in '+m.cwd+' — type your first message to begin');reqList();loadTree();}
  else if(m.type==='attached'){
    /* a delta is only safe on top of the view it was measured against */
    const incremental=!!m.events_delta&&restoredViewId===m.id,stick=incremental&&atBottom();
    if(!incremental){clearUI();restoredViewId='';}
    pendingStart=false;sid=m.id;curCC=m.cc||null;localStorage.setItem(SKEY,sid);cwd=m.cwd;bindProject(m.cwd);
    ready=!m.ended;setCurname((m.title||m.name||'session')+(m.ended?' · ended':''));setModelPill(m.model,false);setEffortPill(m.effort);renderCtx(m.ctx);statset(m.ended?'ended':'ready');
    const tab=ensureTab(m,true);if(tab&&!incremental)planCollapsed=!!tab.planCollapsed;
    replaying=true;m.events.forEach(route);replaying=false;
    setPlan(m.tasks);   /* authoritative: the log window may predate the plan */
    trimChatWindow();
    compacting=!!m.compacting;setBusy(!!m.busy,m.word,(m.turn_age||0)*1000);loadDraft(sid);
    if(m.ended){markEnded('— this session has ended (history shown · you can resume it from disk) —');}
    else{ta.disabled=false;sendBtn.disabled=false;attachOff(false);
      if(!incremental)addNotice('— '+(m.resumed?'resumed':'reattached to')+' « '+(m.name||'')+' » ('+m.events.length+' events)'+(m.effort?' with '+m.effort+' effort':'')+' —');}
    const at=tabById(m.id);
    if(at){at.lastSeq=Math.max(Number(at.lastSeq||0),Number(m.event_seq||0));
           at.serverSeq=at.lastSeq;at.unread=false;persistTabs();renderTabs();}
    restoredViewId=m.id;
    if(incremental){if(stick){scroll();requestAnimationFrame(scroll);}}
    else{scroll();requestAnimationFrame(scroll);}
    reqList();loadTree();}
  else if(m.type==='detached'){if(!sid){ready=false;ta.disabled=true;sendBtn.disabled=true;attachOff(true);statset('idle');}}
  else if(m.type==='no_session'){localStorage.removeItem(SKEY);sid=null;restoredViewId='';ready=false;setBusy(false);setCurname('');renderCtx(null);statset('idle');
    if(m.id)dropTab(m.id);
    addNotice('that session is no longer running — pick it under “Resume from disk”, or ＋ New.');reqList();loadTree();}
  else if(m.type==='events')m.events.forEach(route);
  else if(m.type==='stderr')addErr(m.text);
  else if(m.type==='error'){pendingStart=false;addErr('⚠ '+m.error);}
  else if(m.type==='exit'){if(!pendingStart){markEnded('session process exited (code '+m.code+')');setCurname('');}reqList();loadTree();}
  else if(m.type==='ended'){dropDraft(m.id);if(m.id&&m.id===sid){sid=null;setCurname('');markEnded('session ended');}reqList();loadTree();}
  else if(m.type==='resumable_deleted'){
    if(pmanBatch){pmanBatch.done++;
      if(m.ok)pmanBatch.ok++;else pmanBatch.err.push(m.error||'?');
      if(pmanBatch.done>=pmanBatch.n){
        addNotice('🗑 '+pmanBatch.ok+'/'+pmanBatch.n+' session'+(pmanBatch.n>1?'s':'')+' moved to trash'+
          (pmanBatch.err.length?(' · '+pmanBatch.err.length+' failed: '+pmanBatch.err[0]):''));
        pmanBatch=null;reqList();loadTree();}}
    else{addNotice(m.ok?'🗑 session moved to trash':('delete failed: '+(m.error||'?')));loadTree();}}
  else if(m.type==='renamed'){if(m.ok){if(m.cc&&m.cc===curCC&&m.name)setCurname(m.name);addNotice('✎ renamed');reqList();loadTree();}else addNotice('rename failed');}
  else if(m.type==='sessions'){renderLive(m.sessions);syncTabs(m.sessions);}
  else if(m.type==='context')renderCtx(m.ctx);
  else if(m.type==='agent_progress'){const c=tools[m.toolId];if(c)agentStat(c,{doing:m.desc});}
  else if(m.type==='agent_tokens'){const c=tools[m.parent];if(c)agentStat(c,{up:m.up,out:m.out,est:m.est});}
  else if(m.type==='tokens'){tokUp=m.up||0;tokOut=m.out||0;tokShow=true;
    if(m.word!=null&&m.word!==lastWordSeed){lastWordSeed=m.word;if(running&&!compacting)setWord(m.word);}}
  else if(m.type==='projects'){projData=m.projects||[];renderSidebar();}
}
function openWs(cb){const proto=location.protocol==='https:'?'wss:':'ws:';
  ws=new WebSocket(proto+'//'+location.host+'/ws/chat');
  ws.onopen=()=>{clearTimeout(reconnectT);$('#dot').className='dot '+(ready?'on':'');reqList();
    if(cb){cb();return;}
    /* a dropped socket left the server with no subscription, so re-attach even
       when sid still looks set — and if the rendered view survived the drop, ask
       for the gap instead of the whole conversation */
    const saved=localStorage.getItem(SKEY);
    if(saved&&!pendingStart){const req={type:'attach',id:saved},t=tabById(saved);
      if(restoredViewId===saved&&t)req.after_seq=Number(t.lastSeq||0);else statset('reattaching…');
      ws.send(JSON.stringify(req));}};
  ws.onclose=()=>{$('#dot').className='dot';statset('disconnected');ta.disabled=true;sendBtn.disabled=true;attachOff(true);
    clearTimeout(reconnectT);reconnectT=setTimeout(()=>openWs(),1800);};
  ws.onmessage=onMsg;}
function wsSend(o){if(ws&&ws.readyState===1)ws.send(JSON.stringify(o));}
function reqList(){wsSend({type:'list'});}
function reltime(ts){const s=(Date.now()/1000)-ts;if(s<60)return Math.round(s)+'s';
  if(s<3600)return Math.round(s/60)+'m';if(s<86400)return Math.round(s/3600)+'h';return Math.round(s/86400)+'d';}
function setCurname(t){$('#curname').textContent=t||'— no session —';}
function fmtTok(n){if(n==null)return '?';
  if(n>=1e6)return (n/1e6).toFixed(1).replace(/\.0$/,'')+'M';
  if(n>=1000)return (n/1000).toFixed(n>=1e4?0:1)+'k';
  return ''+n;}
/* shared segmented meter: 5 cells (20% each), whole bar coloured by the total % */
function meterLvl(p){return p>80?'r':p>=60?'o':p>=40?'y':'g';}
function cellBar(pct){const p=Math.max(0,Math.min(100,pct)),lit=Math.min(5,Math.ceil(p/20));
  let s='<span class="cells lv-'+meterLvl(p)+'">';
  for(let i=1;i<=5;i++)s+='<span class="cell'+(i<=lit?' on':'')+'"></span>';
  return s+'</span>';}
/* Past SNAPSHOT_AT the empty composer says "update your snapshot" instead of
   inviting a message: compaction is on its way, and a snapshot written now is
   what carries the session across it. The meter alone is easy to stop seeing;
   the box you are about to type into is not. */
const TA_HINT='Type a message…', SNAPSHOT_AT=60;
function renderCtx(c){const el=$('#ctx');curCtx=c||null;
  const pct=(c&&c.percentage!=null)?Math.round(c.percentage):null;
  ta.placeholder=(pct!=null&&pct>=SNAPSHOT_AT)?'update your snapshot':TA_HINT;
  if(pct==null){el.style.display='none';return;}
  el.className='ctx';el.style.display='inline-flex';
  el.innerHTML='<span class="ulabel">Context</span>'+cellBar(pct)+'<span>'+pct+'%</span>';
  el.title='context '+(c.totalTokens||'?')+' / '+(c.maxTokens||'?')+' tokens ('+pct+'%)'+(c.model?' · '+c.model:'');
  setResolvedModel(c.model, c.maxTokens);}
/* rolling 5-hour usage limit (Claude-Code-CLI style: Usage ░░░ 2% (4h 38m / 5h)) */
function fmtDur(ms){if(ms==null||ms<=0)return '0m';const m=Math.floor(ms/60000),h=Math.floor(m/60);
  return h>0?(h+'h '+(m%60)+'m'):(m+'m');}
function renderUsage(u){const el=$('#usage');const f=u&&u.five_hour,w=u&&u.seven_day;
  /* per-model weekly windows (e.g. Fable); label comes from the server */
  const sc=(u&&u.model_scoped)||[];
  if((!f||f.utilization==null)&&(!w||w.utilization==null)&&!sc.length){el.style.display='none';return;}
  function seg(o,lbl){if(!o||o.utilization==null)return '';
    const pct=Math.round(o.utilization);
    return '<span class="useg"><span class="ulabel">'+lbl+'</span>'+cellBar(pct)+'<span>'+pct+'%</span></span>';}
  el.className='usage';el.style.display='inline-flex';
  el.innerHTML='<span class="ulabel">Usage</span>'+seg(f,'5h')+seg(w,'7d')+sc.map(o=>seg(o,o.label)).join('');
  let t='';
  function row(lbl,o){if(!o||o.utilization==null)return;
    const rem=o.resets_at?fmtDur(new Date(o.resets_at)-Date.now()):'';
    t+=(t?'\n':'')+lbl+': '+Math.round(o.utilization)+'% used'+(rem?(' · resets in '+rem):'');}
  const all=[['five_hour','5-hour'],['seven_day','7-day'],['seven_day_sonnet','7-day Sonnet'],['seven_day_opus','7-day Opus']];
  all.forEach(([k,lbl])=>row(lbl,u&&u[k]));
  sc.forEach(o=>row('7-day '+o.label,o));
  el.title=t;}
function loadUsage(){fetch('api/usage').then(r=>r.json()).then(j=>renderUsage(j.usage)).catch(()=>{});}
/* dynamic model list from the live /v1/models API: aliases (track the latest of
   each family) stay on top; the full list — incl. temporary models like fable —
   loads into an "all models" group. Static opus/sonnet/haiku = offline fallback. */
const MODEL_ALIASES=[['opus','model: opus'],['sonnet','sonnet'],['haiku','haiku']];
let MODELS=[];   /* [{id,name}] mirror of the API list */
function rebuildModelPicker(){
  if(!MODELS.length)return;
  const sel=$('#model');const want=localStorage.getItem('al_model')||sel.value;
  sel.innerHTML='';
  MODEL_ALIASES.forEach(([v,t])=>{const o=document.createElement('option');o.value=v;o.textContent=t;sel.appendChild(o);});
  const g=document.createElement('optgroup');g.label='all models';
  MODELS.forEach(m=>{const o=document.createElement('option');o.value=m.id;o.textContent=m.name;o.title=m.id;g.appendChild(o);});
  sel.appendChild(g);
  for(const o of sel.options)if(o.value===want){sel.value=want;break;}   /* keep the saved/current pick */
}
function loadModels(fresh){return fetch('api/models'+(fresh?'?fresh=1':'')).then(r=>r.json()).then(j=>{
  if(j.models&&j.models.length&&JSON.stringify(j.models)!==JSON.stringify(MODELS)){
    MODELS=j.models;rebuildModelPicker();}}).catch(()=>{});}
/* thinking effort: a clickable pill on the right of the status row. A live
   session takes the change through `/effort` (no relaunch); the choice is
   remembered for new sessions. */
function setEffortPill(e){if(e)curEffort=e;const el=$('#effort');if(el)el.textContent='🧠 '+curEffort;}
/* model pill: the live session's model, one click to change it (SDK set_model, no
   relaunch, applies from the next turn). A message that starts with `@haiku ` (or
   @sonnet / @opus / @fable / any id fragment) runs that one turn on that model,
   long-window variant when there is one, and the pill says so until the server
   reports the switch back. */
function pillModel(id){if(!id||id==='default')return 'default';return (modelLabel(id)||id)+(/\[1m\]$/i.test(id)?' 1M':'');}
function setModelPill(m,once){
  if(m!==undefined&&m!==null){if(once)modelOnce=m;else{curModel=m||'default';modelOnce='';}}
  const el=$('#modelpill');if(!el)return;
  el.textContent='🤖 '+(modelOnce?pillModel(modelOnce)+' · this turn':pillModel(curModel));
  el.title=(modelOnce?'@'+modelOnce+' for this turn, then back to '+curModel:'model: '+curModel)+
    ' — click to change · start a message with @haiku / @sonnet / @opus / @fable to use it for one turn';}
function pickModel(v){if(v===curModel)return;setModelPill(v,false);
  if(sid&&ready&&ws&&ws.readyState===1)wsSend({type:'set_model',model:v});}
function setEffort(e){if(!EFFORTS.includes(e)||e===curEffort)return;
  if(running){addNotice('⚙ finish or interrupt the current turn before changing effort');return;}
  curEffort=e;localStorage.setItem('al_effort',e);setEffortPill(e);
  if(sid&&ready&&ws&&ws.readyState===1)wsSend({type:'set_effort',effort:e});}   /* live session: /effort down the stream */
/* show what the live session's model actually resolves to in the picker's default
   option, e.g. "model: opus 4.8 [1M]" — family+version from the model id, the
   [ctx] window from maxTokens. Fed by the ready event and the context usage. */
let _rmodel='', _rmax=0;
function modelLabel(real,maxTok){
  if(!real)return '';
  const s=(''+real).toLowerCase();
  const fam=s.includes('opus')?'opus':s.includes('sonnet')?'sonnet':s.includes('haiku')?'haiku':s.includes('fable')?'fable':'';
  if(!fam)return ''+real;
  const m=s.match(new RegExp(fam+'-(\\d{1,2})(?:-(\\d{1,2}))?(?!\\d)'))||s.match(new RegExp('(\\d+)-(\\d+)-'+fam));
  let lbl=fam+(m?(' '+m[1]+(m[2]?'.'+m[2]:'')):'');
  if(maxTok)lbl+='['+fmtTok(maxTok)+']';
  return lbl;}
/* the sidebar #model picker now holds the NEW-session default, so it no longer
   reflects the live session's resolved model (that moved to ⚙ Configure). Kept
   as a no-op shim so existing callers (ready / context) stay valid. */
function setResolvedModel(real,maxTok){if(real)_rmodel=real;if(maxTok)_rmax=maxTok;}

/* sidebar: live sessions (in-RAM) + resume-from-disk (past transcripts) */
/* shared per-card action menu (⋯): a fixed popover anchored to the clicked
   kebab, so it is never clipped by the sidebar's overflow */
function closeCardMenu(){const m=$('#cardMenu');m.classList.remove('on');m.innerHTML='';m._anchor=null;}
/* clicking the same ⋯ again closes the menu (toggle); a different one re-anchors */
function toggleCardMenu(anchor,items){const m=$('#cardMenu');
  if(m.classList.contains('on')&&m._anchor===anchor){closeCardMenu();return;}
  openCardMenu(anchor,items);}
function openCardMenu(anchor,items){const m=$('#cardMenu');m.innerHTML='';m._anchor=anchor;
  items.forEach(it=>{const d=document.createElement('div');d.className='mi'+(it.danger?' danger':'');
    d.textContent=it.label;d.onclick=ev=>{ev.stopPropagation();closeCardMenu();it.fn();};m.appendChild(d);});
  m.classList.add('on');
  const r=anchor.getBoundingClientRect(),mw=m.offsetWidth,mh=m.offsetHeight;
  let left=r.right-mw,top=r.bottom+4;
  if(left<6)left=6;
  if(top+mh>window.innerHeight-6)top=Math.max(6,r.top-mh-4);
  m.style.left=left+'px';m.style.top=top+'px';}
/* ⚙ Configure: change a live session's model / permission in place (hot-swap by
   id — works for any live session) + effort. Effort is routed through the pill's
   setEffort() so this control and the 🧠 pill stay in lockstep. Reuses the
   #cardMenu popover shell (positioning + outside-click close). */
function openConfigure(s,anchor){const m=$('#cardMenu');m.innerHTML='';m._anchor=anchor;
  const mkSel=(opts,cur,fn)=>{const o=document.createElement('select');o.className='cfgsel';
    opts.forEach(([v,t])=>{const x=document.createElement('option');x.value=v;x.textContent=t;if(v===cur)x.selected=true;o.appendChild(x);});
    o.onchange=()=>fn(o.value);return o;};
  const row=(label,sel)=>{const w=document.createElement('div');w.className='cfgrow';
    const l=document.createElement('span');l.textContent=label;w.appendChild(l);w.appendChild(sel);m.appendChild(w);};
  row('Model',mkSel([['opus','opus'],['sonnet','sonnet'],['haiku','haiku'],...MODELS.map(m=>[m.id,m.name])],s.model||'opus',
    v=>{wsSend({type:'configure',id:s.id,model:v});setTimeout(reqList,200);}));
  row('Permission',mkSel([['acceptEdits','⚡ Auto-accept'],['default','🔐 Approve'],['plan','📋 Plan'],['bypassPermissions','⏩ Full auto']],s.mode||'default',
    v=>{wsSend({type:'configure',id:s.id,mode:v});setTimeout(reqList,200);}));
  row('Effort',mkSel(EFFORTS.map(e=>[e,e]),curEffort,v=>{setEffort(v);closeCardMenu();}));
  m.classList.add('on');
  const r=anchor.getBoundingClientRect(),mw=m.offsetWidth,mh=m.offsetHeight;
  let left,top;
  if(anchor.isConnected&&(r.width||r.height)){   /* fresh kebab → anchor to it */
    left=r.right-mw;top=r.bottom+4;
    if(top+mh>window.innerHeight-6)top=r.top-mh-4;
  }else{   /* anchor was detached by a live-list re-render (its rect is all-zeros,
              which would fling the popover to the top-left). Keep the menu where
              it already sat (closeCardMenu leaves style.left/top intact). */
    left=parseFloat(m.style.left)||(window.innerWidth-mw)/2;
    top=parseFloat(m.style.top)||(window.innerHeight-mh)/2;
  }
  left=Math.min(Math.max(6,left),Math.max(6,window.innerWidth-mw-6));
  top=Math.min(Math.max(6,top),Math.max(6,window.innerHeight-mh-6));
  m.style.left=left+'px';m.style.top=top+'px';}

/* collapsible sidebar sections (Favorites / Recent / In folder) */
const SECKEY='al_seccol';
function toggleSec(id){const el=$('#'+id);if(!el)return;el.classList.toggle('collapsed');
  let c={};try{c=JSON.parse(localStorage.getItem(SECKEY)||'{}');}catch(e){}
  c[id]=el.classList.contains('collapsed');localStorage.setItem(SECKEY,JSON.stringify(c));}
function applySecCollapse(){let c={};try{c=JSON.parse(localStorage.getItem(SECKEY)||'{}');}catch(e){}
  ['secFav','secRecent'].forEach(id=>{const el=$('#'+id);if(el)el.classList.toggle('collapsed',!!c[id]);});}

function renderLive(list){liveSessions=list||[];renderSidebar();}

/* ── project-grouped sidebar ──────────────────────────────────────────────
   A sidebar row is a FOLDER, and the sessions whose cwd is exactly that folder
   nest under it (a subfolder is its own project). Live therefore answers "which
   projects am I working in" rather than "which threads are open": a project is
   Live when at least one of its sessions is running. Favorites and Recent are
   likewise folder-level. Server data arrives as `projects`; live sessions arrive
   separately over the socket and are merged in here, because a brand-new session
   has no transcript on disk yet. */

function saveExp(){try{localStorage.setItem('al_pexp',JSON.stringify([...pExp]));}catch(e){}}

function shortPath(p){   /* mirrors the server: parent/current, $HOME as ~ */
  if(!p)return '';
  if(HOMEDIR&&p===HOMEDIR)return '~';
  const rel=(HOMEDIR&&p.indexOf(HOMEDIR+'/')===0)?p.slice(HOMEDIR.length+1):p.replace(/^\/+/,'');
  const parts=rel.split('/').filter(Boolean);
  return parts.slice(-2).join('/')||p;}

function buildProjects(){
  const liveBy={};
  (liveSessions||[]).forEach(s=>{const k=s.root||s.cwd||'';if(k)(liveBy[k]=liveBy[k]||[]).push(s);});
  const by={};
  (projData||[]).forEach(p=>{by[p.path]=Object.assign({},p,{sessions:(p.sessions||[]).slice()});});
  /* a folder the server hasn't indexed yet still needs a row */
  Object.keys(liveBy).forEach(k=>{if(!by[k])by[k]={path:k,
    name:(k.split('/').filter(Boolean).slice(-1)[0]||k),sub:shortPath(k),
    fav:false,pinned:false,mtime:Date.now()/1000,sessions:[]};});
  const now=Date.now()/1000;
  return Object.keys(by).map(k=>{const p=by[k];
    p.liveS=liveBy[k]||[];
    const lcc=new Set(p.liveS.map(x=>x.cc).filter(Boolean));
    p.past=(p.sessions||[]).filter(x=>!lcc.has(x.cc));
    p.isLive=p.liveS.some(x=>!x.ended);
    p.anyBusy=p.liveS.some(x=>x.busy);
    p.total=p.liveS.length+p.past.length;
    p.sortKey=p.liveS.length?now:(p.mtime||0);
    return p;}).sort((a,b)=>(b.sortKey||0)-(a.sortKey||0));}

/* Live is sorted by NAME, not recency. Every live project shares the same
   activity sortKey, so a recency order has nothing to break ties with and the
   rows reshuffle on each refresh — the section you watch while working is the
   one that must hold still. Favorites and Recent stay newest-first: there the
   ordering carries real information. */
const byName=(a,b)=>(a.name||'').localeCompare(b.name||'',undefined,
                                               {numeric:true,sensitivity:'base'});
function renderSidebar(){
  const all=buildProjects();
  const live=all.filter(p=>p.isLive).sort(byName);
  const fav=all.filter(p=>!p.isLive&&p.fav);
  const rest=all.filter(p=>!p.isLive&&!p.fav).slice(0,40);
  $('#liveN').textContent=live.length;
  $('#favN').textContent=fav.length;
  fillSec('#liveList',live,'no active project — pick a folder, ＋ New session');
  fillSec('#favList',fav,'star a project to pin it here');
  fillSec('#recentList',rest,'no projects yet');}

function fillSec(sel,list,empty){const box=$(sel);if(!box)return;
  if(!list.length){box.innerHTML='<div class="sb-empty">'+esc(empty)+'</div>';return;}
  box.innerHTML='';list.forEach(p=>box.appendChild(projGroup(p)));}

function projGroup(p){
  const g=document.createElement('div');
  g.className='pgroup'+(pExp.has(p.path)?' open':'');
  const r=document.createElement('div');r.className='prow';
  r.innerHTML='<span class="caret"></span>'+
    '<div class="pmeta"><div class="pname">'+esc(p.name)+(p.fav?' <span class="pstar">★</span>':'')+'</div>'+
    '<div class="psub">'+esc(p.sub||shortPath(p.path))+'</div></div>'+
    '<span class="pn">'+p.total+'</span>'+
    '<span class="skebab pkebab" title="project actions" aria-label="project actions"><i></i></span>';
  r.onclick=ev=>{if(ev.target.closest('.skebab'))return;
    if(pExp.has(p.path))pExp.delete(p.path);else pExp.add(p.path);
    saveExp();g.classList.toggle('open');};
  r.querySelector('.skebab').onclick=ev=>{ev.stopPropagation();
    const items=[{label:'＋ New session here',fn:()=>newSessionIn(p.path)},
      {label:'☑ Manage sessions…',fn:()=>pmanOpen(p)},
      /* browse the project directory in web-file-manager (same ?open= scheme as
         file links — its openPath() loads a directory as a folder view) */
      {label:'📂 Open folder',fn:()=>window.open(webfmOpenUrl(p.path),'_blank')},
      {label:'✎ Rename project',fn:()=>renameProject(p)},
      {label:p.fav?'★ Unfavorite':'☆ Favorite',fn:()=>wsSend({type:'proj_fav',path:p.path,fav:!p.fav})}];
    if(p.pinned)items.push({label:'✕ Remove from sidebar',danger:true,fn:()=>{
      if(confirm('Remove “'+p.name+'” from the sidebar?\n\nOnly the sidebar entry goes away. '+
                 'No session and no file is deleted.'))wsSend({type:'proj_unpin',path:p.path});}});
    toggleCardMenu(ev.currentTarget,items);};
  g.appendChild(r);
  const list=document.createElement('div');list.className='plist';
  p.liveS.forEach(x=>list.appendChild(liveSessRow(x)));
  p.past.forEach(x=>list.appendChild(pastSessRow(x)));
  if(!list.children.length)list.innerHTML='<div class="sb-empty">no sessions yet</div>';
  g.appendChild(list);
  return g;}

/* a running session: green dot, click to switch to it */
function liveSessRow(s){const r=document.createElement('div');
  r.className='srow'+(s.id===sid?' active':'')+(s.ended?' ended':'');
  const dot=s.busy?'busy':(s.ended?'':'on');
  r.innerHTML='<span class="sdot '+dot+'"></span><div class="smeta">'+
    '<div class="sname">'+esc(s.title||s.name||'new session')+'</div></div>'+
    '<span class="skebab" title="more">⋮</span>';
  r.querySelector('.smeta').onclick=()=>switchSession(s.id);
  r.querySelector('.sdot').onclick=()=>switchSession(s.id);
  r.querySelector('.skebab').onclick=ev=>{ev.stopPropagation();const kebab=ev.currentTarget;
    const items=[{label:'⚙ Configure',fn:()=>openConfigure(s,kebab)},
      {label:'✎ Rename',fn:()=>renameSession(s.cc,s.title||s.name)}];
    if(s.cc)items.push({label:'⤓ Export transcript',fn:()=>exportSession(s.cc)});
    items.push({label:'✕ End session',danger:true,fn:()=>endSessionById(s.id,s.name)});
    toggleCardMenu(kebab,items);};
  return r;}

/* an on-disk session: grey dot, click to resume */
function pastSessRow(s){const r=document.createElement('div');r.className='srow';
  r.innerHTML='<span class="sdot"></span><div class="smeta">'+
    '<div class="sname">'+esc(s.title||'session')+'</div>'+
    '<div class="ssub">↺ '+(s.mtime?esc(reltime(s.mtime)):'resume')+'</div></div>'+
    '<span class="skebab" title="more">⋮</span>';
  r.querySelector('.smeta').onclick=()=>resumeSession(s);
  r.querySelector('.sdot').onclick=()=>resumeSession(s);
  r.querySelector('.skebab').onclick=ev=>{ev.stopPropagation();toggleCardMenu(ev.currentTarget,[
    {label:'✎ Rename',fn:()=>renameSession(s.cc,s.title)},
    {label:'⤓ Export transcript',fn:()=>exportSession(s.cc)},
    {label:'🗑 Delete (to trash)',danger:true,fn:()=>delResumable(s)}]);};
  return r;}

function renameProject(p){
  const nm=prompt('Rename project (leave empty to reset to the folder name):',p.renamed?p.name:'');
  if(nm===null)return;
  wsSend({type:'proj_rename',path:p.path,name:nm});}

/* start a session in a specific folder: route through the custom-path box so the
   picker visibly shows where the new session will run */
function newSessionIn(path){
  $('#project').value='__custom__';
  $('#cwdwrap').classList.add('show');
  $('#cwd').value=path;
  $('#newbtn').click();}

/* ── Manage sessions ──────────────────────────────────────────────────────
   Batch cleanup for one project. Sub-agent threads can pile up dozens deep, and
   deleting them one ⋮ menu at a time is the wrong tool. Deletion goes through the
   same del_resumable path as the single-session ⋮ (gio trash, recoverable), and a
   running session is terminated first by the server. */
let pmanProj=null, pmanSel=new Set(), pmanBatch=null;

function pmanRows(){
  if(!pmanProj)return [];
  return (pmanProj.liveS||[]).map(s=>({cc:s.cc,title:s.title||s.name||'new session',
                                       live:true,busy:s.busy,ended:s.ended,mtime:0}))
    .concat((pmanProj.past||[]).map(s=>({cc:s.cc,title:s.title||'session',
                                         live:false,mtime:s.mtime})));}

function pmanOpen(p){
  pmanProj=p;pmanSel=new Set();
  $('#pmansub').textContent=p.name+'  ·  '+(p.sub||'');
  $('#pman').removeAttribute('hidden');
  pmanRender();}

function pmanClose(){$('#pman').setAttribute('hidden','');pmanProj=null;pmanSel=new Set();}
function pmanOpened(){return !$('#pman').hasAttribute('hidden');}

function pmanRender(){
  const rows=pmanRows(),box=$('#pmanlist');
  if(!rows.length){box.innerHTML='<div class="sb-empty">no sessions in this project</div>';}
  else{box.innerHTML='';rows.forEach(x=>{
    const el=document.createElement('div');el.className='pmrow';
    const dis=x.cc?'':' disabled title="this session has no transcript yet"';
    el.innerHTML='<input type="checkbox"'+dis+(x.cc&&pmanSel.has(x.cc)?' checked':'')+'>'+
      '<div class="pmmeta"><div class="pmt">'+esc(x.title)+'</div>'+
      '<div class="pmd">'+(x.live?(x.busy?'working…':(x.ended?'ended':'running')):
                                  ('↺ '+(x.mtime?esc(reltime(x.mtime)):'on disk')))+'</div></div>'+
      (x.live?'<span class="pmlive">live</span>':'');
    const cb=el.querySelector('input');
    const flip=()=>{if(!x.cc)return;
      if(pmanSel.has(x.cc))pmanSel.delete(x.cc);else pmanSel.add(x.cc);
      cb.checked=pmanSel.has(x.cc);pmanBar();};
    el.onclick=ev=>{if(ev.target!==cb)flip();else{cb.checked=!cb.checked;flip();}};
    box.appendChild(el);});}
  pmanBar();}

function pmanBar(){
  const rows=pmanRows().filter(x=>x.cc),n=pmanSel.size;
  $('#pmancount').textContent=n+' selected';
  $('#pmandel').disabled=!n;
  const all=$('#pmanall');
  all.checked=n>0&&n===rows.length;
  all.indeterminate=n>0&&n<rows.length;}

function pmanToggleAll(){
  const rows=pmanRows().filter(x=>x.cc);
  if(pmanSel.size===rows.length)pmanSel=new Set();
  else pmanSel=new Set(rows.map(x=>x.cc));
  pmanRender();}

function pmanDelete(){
  const ccs=[...pmanSel];if(!ccs.length)return;
  const liveN=pmanRows().filter(x=>x.live&&pmanSel.has(x.cc)).length;
  if(!confirm('Delete '+ccs.length+' session'+(ccs.length>1?'s':'')+' from '+
      (pmanProj?pmanProj.name:'this project')+'?'+
      (liveN?('\n\n'+liveN+' of them '+(liveN>1?'are':'is')+' still running and will be ended first.'):'')+
      '\n\nTranscripts move to the trash (recoverable), they are not erased.'))return;
  pmanBatch={n:ccs.length,done:0,ok:0,err:[]};
  ccs.forEach(cc=>wsSend({type:'del_resumable',cc:cc}));
  pmanClose();}

$('#pmanx').onclick=pmanClose;
$('#pmanclose').onclick=pmanClose;
$('#pmandel').onclick=pmanDelete;
$('#pmanall').onclick=pmanToggleAll;
$('#pman').addEventListener('mousedown',e=>{if(e.target.id==='pman')pmanClose();});

/* export/import: a transcript is a self-contained .jsonl. `claude --resume` finds it
   by FOLDER (derived from the cwd) + FILENAME (the session id) — never by the cwd
   recorded inside — so moving one between machines is a plain file copy. The download
   therefore keeps the '<session-id>.jsonl' name; renaming it would break re-import. */
function exportSession(cc){
  if(!cc){alert('This session has no transcript yet.');return;}
  const a=document.createElement('a');a.href='api/export?cc='+encodeURIComponent(cc);
  a.download=cc+'.jsonl';document.body.appendChild(a);a.click();a.remove();}
/* ── history search (Ctrl/Cmd+K) ────────────────────────────────────────────
   The server does substring matching over an index of conversation text only, so a
   query costs ~2 s across all history and is instant once a scope narrows it. That
   latency is why this debounces and aborts the in-flight request on every keystroke
   — otherwise slow answers land out of order and overwrite fast ones. */
let srchAbort=null, srchT=0, srchSeq=0;
function srchOpen(){return !$('#srch').hasAttribute('hidden');}
function openSearch(){const w=$('#srch');w.removeAttribute('hidden');
  showResults();
  const q=$('#srchq');q.focus();q.select();}
function closeSearch(){$('#srch').setAttribute('hidden','');
  if(srchAbort){srchAbort.abort();srchAbort=null;}}
/* the folder "this folder" means: the open conversation's own working dir when there
   is one, else whatever the sidebar's project picker points at. Keying it off the
   picker alone would be a trap — you'd be chatting in project A while the search
   silently scoped itself to project B. */
function searchFolder(){
  const s=(liveSessions||[]).find(x=>x.id===sid);
  return (s&&s.cwd)||currentFolder()||'';}
function srchScopeArgs(){const sc=$('#srchscope').value;
  if(sc==='session')return curCC?'&scope=session&cc='+encodeURIComponent(curCC):'&scope=all';
  if(sc==='project'){const f=searchFolder();
    return f?'&scope=project&cwd='+encodeURIComponent(f):'&scope=all';}
  return '&scope=all';}
function runSearch(){
  const q=$('#srchq').value.trim(), res=$('#srchres'), meta=$('#srchmeta');
  showResults();          /* a new query always returns from the reading view */
  if(srchAbort){srchAbort.abort();srchAbort=null;}
  /* one han character is a word, one latin letter is not — mirror the server's rule */
  const need=/[㐀-鿿぀-ヿ가-힯]/.test(q)?1:2;
  if(q.length<need){res.innerHTML='';
    meta.textContent='Type at least '+need+' character'+(need===1?'':'s')+'.';return;}
  const seq=++srchSeq;
  meta.textContent='searching…';
  srchAbort=new AbortController();
  const t0=Date.now();
  fetch('api/search?q='+encodeURIComponent(q)+srchScopeArgs(),{signal:srchAbort.signal})
    .then(r=>r.json()).then(j=>{
      if(seq!==srchSeq)return;                 /* a newer query already answered */
      const list=j.results||[];
      meta.textContent=list.length
        ? list.length+(j.more?'+':'')+' match'+(list.length===1?'':'es')+' · '+((Date.now()-t0)/1000).toFixed(1)+'s'
            +(j.more?' · showing the 200 most recent':'')
        : (j.note||j.error||'no matches');
      res.innerHTML='';
      list.forEach(h=>{
        const d=document.createElement('div');d.className='shit';
        const proj=(h.cwd||'').split('/').slice(-2).join('/');
        d.innerHTML='<div class="sh1"><b>'+esc(h.title||'session')+'</b>'+
          '<span class="role">'+esc(h.role)+'</span><span>'+esc(proj)+'</span>'+
          '<span>'+esc((h.ts||'').slice(0,16).replace('T',' '))+'</span></div>'+
          '<div class="sh2">'+esc(h.pre)+'<mark>'+esc(h.hit)+'</mark>'+esc(h.post)+'</div>';
        d.onclick=()=>openThread(h);
        res.appendChild(d);});
    }).catch(e=>{if(e.name!=='AbortError'&&seq===srchSeq)meta.textContent='search failed: '+e;});
}
/* Clicking a hit opens it HERE, read-only. It used to call resumeSession(), which
   was wrong twice over: it commandeered whatever conversation you were in, and when
   the old session's folder no longer existed the server rejected the resume and the
   chat pane sat on "resuming…" forever. Reading and resuming are now separate acts —
   the latter needs the explicit "Open session" button. */
let thState=null;
function showResults(){$('#srchthread').setAttribute('hidden','');
  $('#srchres').removeAttribute('hidden');}
function highlightInto(el,txt,q){
  el.textContent=txt;
  if(!q)return;
  const i=txt.toLowerCase().indexOf(q.toLowerCase());
  if(i<0)return;
  el.textContent='';
  el.appendChild(document.createTextNode(txt.slice(0,i)));
  const m=document.createElement('mark');m.textContent=txt.slice(i,i+q.length);
  el.appendChild(m);
  el.appendChild(document.createTextNode(txt.slice(i+q.length)));}
function renderThread(j,q,targetMid){
  const body=$('#thbody');body.innerHTML='';
  $('.thtitle').textContent=(j.title||'session')+' · '+(j.cwd||'').split('/').slice(-2).join('/')
    +' · '+(j.total||0)+' messages';
  if(!j.atStart){const b=document.createElement('button');b.className='thmore';
    b.textContent='↑ load 60 earlier';
    b.onclick=()=>loadThread(thState.mid,thState.before+60,thState.after,q,targetMid);
    body.appendChild(b);}
  else body.insertAdjacentHTML('beforeend','<div class="thend">— start of conversation —</div>');
  let tgt=null;
  (j.messages||[]).forEach(m=>{
    const d=document.createElement('div');
    d.className='thmsg '+m.role+(m.mid===targetMid?' target':'');
    d.innerHTML='<div class="thr">'+esc(m.role)+' · '+esc((m.ts||'').slice(0,16).replace('T',' '))+'</div>';
    const t=document.createElement('div');t.className='tht';
    highlightInto(t,m.txt||'',m.mid===targetMid?q:'');
    d.appendChild(t);body.appendChild(d);
    if(m.mid===targetMid)tgt=d;});
  if(!j.atEnd){const b=document.createElement('button');b.className='thmore';
    b.textContent='↓ load 60 later';
    b.onclick=()=>loadThread(thState.mid,thState.before,thState.after+60,q,targetMid);
    body.appendChild(b);}
  else body.insertAdjacentHTML('beforeend','<div class="thend">— end of conversation —</div>');
  if(tgt)tgt.scrollIntoView({block:'center'});}
function loadThread(mid,before,after,q,targetMid){
  thState={mid:mid,before:before,after:after,q:q};
  $('#thbody').innerHTML='<div class="thend">loading…</div>';
  fetch('api/thread?mid='+encodeURIComponent(mid)+'&before='+before+'&after='+after)
    .then(r=>r.json()).then(j=>{
      if(j.error){$('#thbody').innerHTML='<div class="thend">'+esc(j.error)+'</div>';return;}
      thState.cc=j.cc;thState.cwd=j.cwd;renderThread(j,q,targetMid);})
    .catch(e=>{$('#thbody').innerHTML='<div class="thend">failed: '+esc(String(e))+'</div>';});}
function openThread(h){
  $('#srchres').setAttribute('hidden','');
  $('#srchthread').removeAttribute('hidden');
  loadThread(h.mid,40,40,$('#srchq').value.trim(),h.mid);}
function wireSearch(){
  const q=$('#srchq');if(!q)return;
  $('#thback').onclick=showResults;
  $('#thopen').onclick=()=>{const s=thState;if(!s||!s.cc)return;
    closeSearch();resumeSession({cc:s.cc,cwd:s.cwd});};
  q.addEventListener('input',()=>{clearTimeout(srchT);srchT=setTimeout(runSearch,260);});
  q.addEventListener('keydown',e=>{if(e.key==='Enter'){clearTimeout(srchT);runSearch();}});
  $('#srchscope').onchange=runSearch;
  $('#srchx').onclick=closeSearch;
  const ob=$('#srchopen');if(ob)ob.onclick=()=>{openSearch();if(window.innerWidth<=860)closeSidebar();};
  $('#srch').onclick=e=>{if(e.target===$('#srch'))closeSearch();};   /* backdrop */
  document.addEventListener('keydown',e=>{
    if((e.ctrlKey||e.metaKey)&&(e.key==='k'||e.key==='K')){e.preventDefault();
      srchOpen()?closeSearch():openSearch();return;}
    if(e.key==='Escape'&&srchOpen()){e.preventDefault();e.stopPropagation();closeSearch();}
  },true);   /* capture: close the palette before Esc reaches the interrupt handler */
}
function impMsg(t,bad){const el=$('#impmsg');if(!el)return;
  el.textContent=t||'';el.classList.toggle('bad',!!bad);
  if(t&&!bad)setTimeout(()=>{if(el.textContent===t)el.textContent='';},6000);}
/* An import's target folder is the one selected above, the same control New session
   uses — NOT the conversation currently open. That choice decides where the session
   will live and resume, and it used to be invisible at the moment of clicking, so it
   is now shown under the button at all times and confirmed before anything uploads. */
function importTarget(){return (($('#cwd').value||'').trim())||$('#project').value||'';}
/* HOMEDIR arrives asynchronously from /api/projects; guard it, because
   ''.replace('','~') would prepend a tilde and show /data/x as ~/data/x. */
function shortPath(p){p=p||'';
  return (HOMEDIR&&p.startsWith(HOMEDIR))?('~'+p.slice(HOMEDIR.length)):p;}
function refreshImportHint(){
  const el=$('#imphint');if(!el)return;
  const d=importTarget();
  el.textContent=d?('→ '+shortPath(d)):'→ pick a project folder above first';
  el.classList.toggle('bad',!d);}
function wireImport(){
  const btn=$('#impbtn'),inp=$('#impfile');if(!btn||!inp)return;
  refreshImportHint();
  ['#project','#cwd'].forEach(s=>{const el=$(s);if(!el)return;
    el.addEventListener('change',refreshImportHint);
    el.addEventListener('input',refreshImportHint);});
  btn.onclick=()=>{if(!importTarget()){impMsg('pick a project folder above first',true);return;}
    inp.click();};
  inp.onchange=()=>{const f=inp.files&&inp.files[0];if(!f){return;}
    const dir=importTarget();
    if(!dir){impMsg('pick a project folder above first',true);inp.value='';return;}
    if(!confirm('Import this conversation into\n\n    '+shortPath(dir)+
                '\n\nfile: '+f.name+'\n\nIt will resume in that folder. To use a different one, '
                +'cancel and change the project selector above.')){
      impMsg('import cancelled');inp.value='';return;}
    const fd=new FormData();fd.append('file',f);fd.append('cwd',dir);
    impMsg('importing '+f.name+' → '+shortPath(dir)+' …');
    fetch('api/import',{method:'POST',body:fd}).then(r=>r.json().then(j=>({ok:r.ok,j})))
      .then(({ok,j})=>{
        if(ok&&j.ok){impMsg('✓ imported into '+shortPath(j.cwd||dir)+' · resume it below');loadTree();}
        else impMsg('✗ '+((j&&j.error)||'import failed'),true);})
      .catch(e=>impMsg('✗ '+e,true))
      .finally(()=>{inp.value='';});};}
function renameSession(cc,cur){
  if(!cc){alert('This session is still starting — try again in a moment.');return;}
  const nm=prompt('Rename session (leave empty to reset to the auto name):',cur||'');
  if(nm===null)return;
  wsSend({type:'rename',cc:cc,name:nm});}
function delResumable(s){
  if(!confirm('Delete this session from disk?\n\n'+(s.title||s.name||s.cc||'session')+
    '\n\nIt is moved to the trash (recoverable), not permanently deleted.'))return;
  wsSend({type:'del_resumable',cc:s.cc});
  /* optimistic: drop it from the cached tree so it vanishes at once */
  projData=(projData||[]).map(p=>Object.assign({},p,
    {sessions:(p.sessions||[]).filter(x=>x.cc!==s.cc)}));
  renderSidebar();}

function currentFolder(){const p=$('#project').value;
  return p==='__custom__'?$('#cwd').value.trim():(p||'');}

/* pull the folder-grouped sidebar. The socket also pushes it on connect and after
   any change, so this is the cold-start and manual-rescan path. */
function loadTree(){
  fetch('api/tree').then(r=>r.json())
    .then(j=>{projData=j.projects||[];renderSidebar();})
    .catch(()=>{});}

/* directory autocomplete for the custom path box (server /api/dircomplete) */
let acItems=[],acSel=-1,acTimer=0;
function acClose(){const b=$('#cwdac');b.classList.remove('on');b.innerHTML='';acItems=[];acSel=-1;}
function acRender(j){const b=$('#cwdac');acItems=(j&&j.dirs)||[];acSel=-1;
  if(!acItems.length){acClose();return;}
  let html=acItems.map((p,i)=>{const base=(p.split('/').filter(Boolean).slice(-1)[0])||p;
    return '<div class="acitem" data-i="'+i+'"><div class="acname">'+esc(base)+'</div><div class="acpath">'+esc(p)+'</div></div>';}).join('');
  if(j&&j.more)html+='<div class="acmore">… more — keep typing to narrow</div>';
  b.innerHTML=html;b.classList.add('on');
  b.querySelectorAll('.acitem').forEach(el=>el.onmousedown=ev=>{ev.preventDefault();acPick(+el.dataset.i);});}
function acPick(i){if(i<0||i>=acItems.length)return;
  $('#cwd').value=acItems[i]+'/';   /* auto-append slash → keep drilling with the mouse, no typing */
  $('#cwd').focus();loadTree();acQuery();}
function acMove(d){const els=$('#cwdac').querySelectorAll('.acitem');if(!els.length)return;
  acSel=(acSel+d+els.length)%els.length;els.forEach((el,i)=>el.classList.toggle('sel',i===acSel));els[acSel].scrollIntoView({block:'nearest'});}
function acQuery(){clearTimeout(acTimer);const q=$('#cwd').value;
  acTimer=setTimeout(()=>fetch('api/dircomplete?q='+encodeURIComponent(q)).then(r=>r.json()).then(acRender).catch(acClose),130);}

/* ── session tabs, cached views, bounded chat window ──────────────────────────
   Switching used to cost a full rebuild: clearUI() threw the rendered chat away
   and the server re-sent its whole log, so returning to the session you talk to
   most was the slowest thing the console did (measured on a real thread: 1000
   events, 1.0 MB of JSON, 180 markdown re-renders, 762 tool cards — every time).
   Nothing about that was ever needed. The session had not gone anywhere: the
   server has always kept it running and only dropped the subscription.

   So two things are kept instead of rebuilt. The rendered DOM goes into a cache
   keyed by session, and comes back as a DocumentFragment; and every event now
   carries a sequence number, so an attach can say "I have through 412" and get
   back only the gap. A switch is then a swap plus whatever happened while away.

   Tabs are the visible half. The sidebar's LIVE list answers "what is running";
   this strip answers "what do I have open", which is a smaller and much more
   stable set. The two must not be confused, hence: closing a tab never ends a
   session, and the close control says so. */
const TABKEY='cc_session_tabs';
let tabState=[],tabsValidated=false,viewCache=new Map(),restoredViewId='',curCtx=null;
try{const sv=JSON.parse(localStorage.getItem(TABKEY)||'[]');
  if(Array.isArray(sv))tabState=sv.filter(t=>t&&typeof t.id==='string').slice(0,24);}catch(e){}
function tabById(id){return tabState.find(t=>t.id===id)||null;}
function tabLabel(t){return t.title||t.name||('session '+String(t.id||'').slice(0,6));}
/* persist the shape explicitly — a tab record also carries live-only junk (scroll
   metrics) that has no business surviving a reload half-updated */
function persistTabs(){try{localStorage.setItem(TABKEY,JSON.stringify(tabState.map(t=>({
    id:t.id,cc:t.cc||null,title:t.title||'',name:t.name||'',cwd:t.cwd||'',
    busy:!!t.busy,ended:!!t.ended,unread:!!t.unread,lastSeq:Number(t.lastSeq||0),
    serverSeq:Number(t.serverSeq||0),scrollTop:Number(t.scrollTop||0),
    atBottom:t.atBottom!==false,planCollapsed:!!t.planCollapsed}))));}catch(e){}}
function renderTabs(){
  const host=$('#sessionTabs');if(!host)return;
  /* before the first session list lands we cannot tell which stored tabs are still
     real, so an unvalidated strip with nothing open stays out of the way */
  host.hidden=!tabState.length||(!tabsValidated&&!sid);
  host.innerHTML='';if(host.hidden)return;
  tabState.forEach(t=>{
    const el=document.createElement('div');
    el.className='stab'+(t.id===sid?' active':'')+(t.busy?' busy':'')+
      (t.unread&&t.id!==sid?' unread':'')+(t.ended?' ended':'');
    el.setAttribute('role','tab');el.setAttribute('aria-selected',t.id===sid?'true':'false');
    el.tabIndex=t.id===sid?0:-1;
    el.title=tabLabel(t)+(t.cwd?'\n'+shortPath(t.cwd):'')+(t.ended?'\nended':'');
    el.innerHTML='<span class="sdot"></span><span class="stt"></span>'+
      '<span class="sx" role="button" tabindex="-1" aria-label="Close tab" '+
      'title="Close tab — the session keeps running">✕</span>';
    el.querySelector('.stt').textContent=tabLabel(t);
    el.onclick=ev=>{if(!ev.target.classList.contains('sx'))switchSession(t.id);};
    el.querySelector('.sx').onclick=ev=>{ev.stopPropagation();closeTab(t.id);};
    el.onauxclick=ev=>{if(ev.button===1){ev.preventDefault();closeTab(t.id);}};
    el.onkeydown=tabKeydown;
    host.appendChild(el);});
  const act=host.querySelector('.stab.active');
  if(act&&act.scrollIntoView)act.scrollIntoView({block:'nearest',inline:'nearest'});}
function tabKeydown(ev){
  if(ev.key==='Enter'||ev.key===' '){ev.preventDefault();ev.currentTarget.click();return;}
  if(!['ArrowLeft','ArrowRight','Home','End'].includes(ev.key))return;
  const tabs=[...$('#sessionTabs').querySelectorAll('.stab')];let i=tabs.indexOf(ev.currentTarget);
  if(i<0||!tabs.length)return;
  if(ev.key==='Home')i=0;else if(ev.key==='End')i=tabs.length-1;
  else i=(i+(ev.key==='ArrowRight'?1:-1)+tabs.length)%tabs.length;
  ev.preventDefault();tabs[i].focus();tabs[i].click();}
/* a tab appears when a session is OPENED here, not merely because it is running */
function ensureTab(s,active){
  if(!s||!s.id)return null;
  let t=tabById(s.id);
  if(!t){t={id:s.id,cc:s.cc||null,title:'',name:'',cwd:'',busy:false,ended:false,unread:false,
    lastSeq:0,serverSeq:0,scrollTop:0,atBottom:true,planCollapsed:false};tabState.push(t);}
  if(s.cc)t.cc=s.cc;if(s.title)t.title=s.title;if(s.name)t.name=s.name;if(s.cwd)t.cwd=s.cwd;
  if(s.busy!==undefined)t.busy=!!s.busy;
  if(s.ended!==undefined)t.ended=!!s.ended;
  const ss=Number(s.event_seq||0);if(ss)t.serverSeq=Math.max(Number(t.serverSeq||0),ss);
  if(active)t.unread=false;
  tabsValidated=true;persistTabs();renderTabs();return t;}
/* the session list is the authority on what still exists: a tab whose process is
   gone is a dead view, and its cached DOM is just memory */
function syncTabs(list){
  const by=new Map((list||[]).map(s=>[s.id,s]));
  tabState.forEach(t=>{if(!by.has(t.id))viewCache.delete(t.id);});
  tabState=tabState.filter(t=>by.has(t.id));
  tabState.forEach(t=>{const s=by.get(t.id);
    t.busy=!!s.busy;t.ended=!!s.ended;
    if(s.cc)t.cc=s.cc;if(s.title)t.title=s.title;if(s.name)t.name=s.name;if(s.cwd)t.cwd=s.cwd;
    const ss=Number(s.event_seq||0);if(ss)t.serverSeq=Math.max(Number(t.serverSeq||0),ss);
    if(t.id!==sid&&Number(t.serverSeq||0)>Number(t.lastSeq||0))t.unread=true;});
  tabsValidated=true;persistTabs();renderTabs();}
function dropTab(id){const i=tabState.findIndex(t=>t.id===id);
  if(i>=0){tabState.splice(i,1);persistTabs();renderTabs();}viewCache.delete(id);}
function closeTab(id){
  const i=tabState.findIndex(t=>t.id===id);if(i<0)return;
  const active=id===sid;
  if(active){saveDraft();rememberTabView();}
  viewCache.delete(id);tabState.splice(i,1);persistTabs();renderTabs();
  if(!active)return;
  const next=tabState[Math.min(i,tabState.length-1)];
  if(next){switchSession(next.id);return;}
  /* last tab closed: stop viewing, do not stop working */
  wsSend({type:'detach'});sid=null;restoredViewId='';localStorage.removeItem(SKEY);
  clearUI();ready=false;setBusy(false);setCurname('');renderCtx(null);statset('idle');
  ta.disabled=true;sendBtn.disabled=true;attachOff(true);
  addNotice('— tab closed · that session is still running · reopen it from Live —');}

/* ── cached views ──────────────────────────────────────────────────────────── */
function takeChildren(el){const f=document.createDocumentFragment();
  if(el)while(el.firstChild)f.appendChild(el.firstChild);return f;}
function restoreChildren(el,frag){if(!el)return;el.innerHTML='';if(frag)el.appendChild(frag);}
function rememberTabView(){const t=tabById(sid),c=$('#chat');if(!t||!c)return;
  t.scrollTop=c.scrollTop;t.atBottom=follow;}
function restoreTabView(id){const t=tabById(id),c=$('#chat');if(!t||!c)return;
  /* after the fragment is in the document but before paint, so it never flashes */
  requestAnimationFrame(()=>{const f=t.atBottom!==false;setTop(f?c.scrollHeight:(t.scrollTop||0),f);});}
function stashView(id){
  if(!id)return false;const t=tabById(id);if(!t)return false;
  endStream();trimChatWindow();rememberTabView();
  if(planHideT){clearTimeout(planHideT);planHideT=0;}
  viewCache.set(id,{stream:takeChildren(stream),plan:takeChildren($('#planDock')),
    planHidden:$('#planDock').hidden,edits:takeChildren($('#edits')),git:takeChildren($('#gitc')),
    tools:tools,editCount:editCount,queued:queued,planTasks:planTasks,planCollapsed:planCollapsed,
    trimHidden:trimHidden,ctx:curCtx,cwd:cwd,cc:curCC,ready:ready,running:running,
    compacting:compacting,word:lastWordSeed,elapsed:running?Math.max(0,Date.now()-thinkStart):0,
    tokUp:tokUp,tokOut:tokOut,tokShow:tokShow,effort:curEffort,model:curModel,modelOnce:modelOnce,
    title:$('#curname').textContent||tabLabel(t)});
  persistTabs();return true;}
function restoreView(id){
  const v=viewCache.get(id);if(!v)return false;
  restoreChildren(stream,v.stream);
  restoreChildren($('#planDock'),v.plan);$('#planDock').hidden=!!v.planHidden;
  restoreChildren($('#edits'),v.edits);restoreChildren($('#gitc'),v.git);
  tools=v.tools||{};editCount=v.editCount||0;queued=v.queued||{};
  planTasks=v.planTasks||[];planCollapsed=!!v.planCollapsed;trimHidden=Number(v.trimHidden||0);
  cwd=v.cwd||'';curCC=v.cc||null;compacting=!!v.compacting;
  tokUp=v.tokUp||0;tokOut=v.tokOut||0;tokShow=!!v.tokShow;
  ready=!!v.ready;                       /* before setBusy: it gates the composer on it */
  bindProject(cwd);setCurname(v.title);renderCtx(v.ctx);curModel=v.model||'default';modelOnce=v.modelOnce||'';setModelPill();setEffortPill(v.effort);
  updateEditBadge();renderQueue();
  running=false;setBusy(!!v.running,v.word,v.elapsed);   /* restarts the timer where it was */
  loadDraft(id);restoreTabView(id);restoredViewId=id;return true;}

/* ── bounded chat window ───────────────────────────────────────────────────────
   Several cached tabs at once is several rendered conversations held in memory,
   so each keeps only a recent window. What falls out is not lost — it is on disk
   and in the history index — so the marker that replaces it links straight there. */
const CHAT_KEEP_ITEMS=400;
const CHAT_KEEP_CHARS=250000;
let trimHidden=0;
/* never drop something the user still has to act on, or the live bubble */
function trimBlocked(n){const c=n.classList;return !!c&&(
  (c.contains('approval')&&!c.contains('done'))||
  (c.contains('question')&&!c.contains('done'))||c.contains('streaming'));}
function trimChatWindow(){
  /* the marker is bookkeeping, not content: keep it out of the candidate list
     entirely, or a trim that has nothing left to drop removes the marker itself
     and then returns early without putting it back */
  const kids=[...stream.children].filter(n=>!n.classList.contains('trimmark'));
  let keep=0,chars=0,i=kids.length-1;
  for(;i>=0;i--){
    keep++;chars+=(kids[i].textContent||'').length;
    if(keep>=CHAT_KEEP_ITEMS||chars>=CHAT_KEEP_CHARS)break;}
  if(i<=0)return;
  let cut=0;for(;cut<i;cut++)if(trimBlocked(kids[cut]))break;
  if(!cut)return;
  for(let k=0;k<cut;k++)kids[k].remove();
  trimHidden+=cut;
  /* tool cards live in this map by id; the pruned ones would be detached forever */
  for(const id in tools)if(!tools[id].isConnected)delete tools[id];
  renderTrimMark();}
function renderTrimMark(){
  let m=stream.querySelector('.trimmark');
  if(!trimHidden){if(m)m.remove();return;}
  if(!m){m=document.createElement('div');m.className='trimmark';
    m.innerHTML='<span class="tmx">⋯</span><span class="tmt"></span>'+
      '<button type="button" class="tmb">search this conversation</button>';
    m.querySelector('.tmb').onclick=()=>{const sc=$('#srchscope');
      if(sc)sc.value='session';openSearch();};}
  m.querySelector('.tmt').textContent=trimHidden+' earlier '+
    (trimHidden===1?'item':'items')+' hidden to keep this tab fast';
  if(stream.firstChild!==m)stream.insertBefore(m,stream.firstChild);}
/* A switch is now: put the old view away, take the new one out, ask only for the
   gap. The uncached path is the old one — a full replay — so a session opened for
   the first time this page-load still works exactly as before, just once. */
function switchSession(id){
  if(!id)return;
  if(id===sid){const cur=tabById(id);
    if(cur&&cur.unread){cur.unread=false;persistTabs();renderTabs();}
    if(window.innerWidth<=860)closeSidebar();return;}
  const live=liveSessions.find(s=>s.id===id);if(live)ensureTab(live,false);
  stashView(sid);saveDraft();clearUI();restoredViewId='';
  sid=id;const t=tabById(id);if(t)t.unread=false;
  localStorage.setItem(SKEY,id);persistTabs();renderTabs();
  const cached=restoreView(id);if(!cached)statset('switching…');
  const go=()=>{const req={type:'attach',id:id};
    if(cached)req.after_seq=Number((tabById(id)||{}).lastSeq||0);wsSend(req);};
  if(ws&&ws.readyState===1)go();else openWs(go);
  if(window.innerWidth<=860)closeSidebar();}
function resumeSession(s){if(!s||!s.cc)return;
  stashView(sid);saveDraft();clearUI();restoredViewId='';pendingStart=true;sid=null;
  renderTabs();statset('resuming…');
  const go=()=>wsSend({type:'resume',cc:s.cc,cwd:s.cwd,model:$('#model').value,mode:$('#mode').value});
  if(ws&&ws.readyState===1)go();else openWs(go);
  if(window.innerWidth<=860)closeSidebar();}
function newSession(){const proj=$('#project').value;const dir=proj==='__custom__'?$('#cwd').value.trim():proj;
  if(!dir){addErr('pick a project directory first');return;}
  /* keep any current session alive in the background — just spin up another */
  stashView(sid);saveDraft();clearUI();restoredViewId='';pendingStart=true;sid=null;renderTabs();
  const start=()=>wsSend({type:'start',cwd:dir,model:$('#model').value,mode:$('#mode').value,effort:curEffort});
  if(ws&&ws.readyState===1)start();else openWs(start);statset('starting…');
  if(window.innerWidth<=860)closeSidebar();}
function endSessionById(id,name){if(!id)return;
  if(!confirm('End session '+(name?'« '+name+' »':'')+'?\nIts claude process stops; you can still resume it from disk later.'))return;
  wsSend({type:'end',id:id});
  if(id===sid){sid=null;restoredViewId='';setCurname('');markEnded('session ended');}
  dropTab(id);reqList();loadTree();}
/* image attachments: paste (Ctrl/Cmd+V) an image into the composer */
let pendingImages=[];
/* ── attachments ──────────────────────────────────────────────────────────────
   Images go to the model as image blocks, unchanged. Everything else is WRITTEN
   TO DISK under the session's working directory and named in the message, which
   is the one place this console should not copy codex: codex has to inline file
   bodies because its app-server has no file input, but Claude has Read, Edit and
   Bash. A file that exists is a file it can patch and run — and the case this
   feature exists for, attaching a script from a phone and asking for a fix, is
   exactly the case where getting a whole rewritten copy back is the wrong answer.

   Bodies are held in memory only. persistDrafts() already stores nothing but
   text, so nothing ever reaches localStorage, and the server writes them only at
   dispatch — a queued message that gets withdrawn leaves no file behind. */
const MAX_FILES=10, MAX_FILE_BYTES=2*1024*1024;
let pendingFiles=[];
/* FileReader lands asynchronously, so the caps have to count what is still in
   flight as well as what has arrived — otherwise picking fourteen files at once
   passes fourteen synchronous "is there room?" checks before any of them lands. */
let filesLoading=0, imgsLoading=0;
function attachOff(off){const b=$('#attachBtn'),locked=voiceLocksComposer();
  if(b)b.disabled=!!off||locked;syncVoiceButton();}
function fmtBytes(n){return n>=1048576?((n/1048576).toFixed(1)+' MB')
  :(n>=1024?((n/1024).toFixed(1)+' KB'):(n+' B'));}
function addUploadFile(file){
  if(pendingFiles.length+filesLoading>=MAX_FILES){addNotice('⚠ up to '+MAX_FILES+' files at once');return;}
  if(file.size>MAX_FILE_BYTES){
    addNotice('⚠ '+file.name+' is too large ('+fmtBytes(file.size)+', max '+fmtBytes(MAX_FILE_BYTES)+')');return;}
  const r=new FileReader();filesLoading++;
  r.onerror=()=>{filesLoading--;addNotice('⚠ could not read '+file.name);};
  r.onload=()=>{filesLoading--;const u=''+r.result;
    pendingFiles.push({name:file.name,size:file.size,data:u.slice(u.indexOf(',')+1)});
    renderAttach();};
  r.readAsDataURL(file);}
/* one entry point for the picker, drag-and-drop and paste: images take the image
   path, everything else takes the upload path */
function takeFiles(list){[...(list||[])].forEach(f=>{
  if(f&&OK_IMG.indexOf(f.type)>=0)addImageFile(f);else if(f)addUploadFile(f);});}
const MAX_IMG=8, MAX_IMG_BYTES=5*1024*1024, OK_IMG=['image/png','image/jpeg','image/gif','image/webp'];
function renderAttach(){const a=$('#attach');
  a.classList.toggle('on',pendingImages.length+pendingFiles.length>0);
  a.innerHTML=pendingImages.map((im,i)=>'<div class="att"><img src="'+im.url+'"><button class="rm" data-i="'+i+'" title="remove">✕</button></div>').join('')
    +pendingFiles.map((f,i)=>'<div class="fatt" title="'+escAttr(f.name)+'"><span class="fn">📎 '+esc(f.name)+
      '</span><span class="fsz">'+fmtBytes(f.size)+'</span><button class="rm" data-f="'+i+'" title="remove">✕</button></div>').join('');
  a.querySelectorAll('.rm').forEach(b=>b.onclick=()=>{
    if(b.dataset.f!==undefined)pendingFiles.splice(+b.dataset.f,1);
    else pendingImages.splice(+b.dataset.i,1);
    renderAttach();});}
function addImageFile(file){
  if(pendingImages.length+imgsLoading>=MAX_IMG){addNotice('⚠ up to '+MAX_IMG+' images at once');return;}
  if(OK_IMG.indexOf(file.type)<0){addNotice('⚠ unsupported image type: '+(file.type||'?'));return;}
  if(file.size>MAX_IMG_BYTES){addNotice('⚠ image too large ('+Math.round(file.size/1048576)+'MB, max 5MB)');return;}
  const r=new FileReader();imgsLoading++;
  r.onerror=()=>{imgsLoading--;addNotice('⚠ could not read '+file.name);};
  r.onload=()=>{imgsLoading--;const url=''+r.result;
    pendingImages.push({media_type:file.type,data:url.split(',')[1]||'',url:url});renderAttach();};
  r.readAsDataURL(file);}
function handlePaste(e){const cd=e.clipboardData;if(!cd)return;
  /* Word / rich-text copies put BOTH the text AND a rendered image on the clipboard.
     If there's any plain text, let the textarea paste it normally (don't grab the
     image); only treat the paste as an image when it's image-only — a real screenshot. */
  if((cd.getData('text/plain')||'').length>0)return;
  const items=cd.items||[];let got=false;
  for(const it of items){if(it.kind==='file'&&it.type.indexOf('image/')===0){const f=it.getAsFile();if(f){addImageFile(f);got=true;}}}
  if(got)e.preventDefault();}
/* per-session composer drafts — each session keeps its own unsent text (+ images),
   so switching sessions swaps the draft with the chat. Text persists across reloads
   via localStorage; attached images are kept in memory only. */
const DKEY='al_drafts';
let drafts={}, _dpT=0;
try{const sv=JSON.parse(localStorage.getItem(DKEY)||'{}');for(const k in sv)drafts[k]={text:sv[k]||'',images:[]};}catch(e){}
function persistDrafts(){const t={};for(const k in drafts){const v=drafts[k];if(v&&v.text&&v.text.trim())t[k]=v.text;}try{localStorage.setItem(DKEY,JSON.stringify(t));}catch(e){}}
function schedulePersist(){clearTimeout(_dpT);_dpT=setTimeout(persistDrafts,500);}
function saveDraft(){if(sid)drafts[sid]={text:ta.value,images:pendingImages.slice(),
  files:pendingFiles.slice()};persistDrafts();}
function loadDraft(id){const d=drafts[id]||{text:'',images:[],files:[]};ta.value=d.text||'';
  ta.style.height='auto';ta.style.height=Math.min(ta.scrollHeight,160)+'px';
  pendingImages=(d.images||[]).slice();pendingFiles=(d.files||[]).slice();renderAttach();syncVoiceComposerState();}
function dropDraft(id){if(id&&drafts[id]){delete drafts[id];persistDrafts();}}

/* ── voice input ───────────────────────────────────────────────────────────────
   Records in the browser, transcribes on this machine (faster-whisper, in its own
   process), and drops the text into the composer as an editable draft. It never
   sends: dictation misreads words, so the turn only starts when you press send.
   The text goes to the session that started the recording — switch tabs while it
   transcribes and it lands in that session's draft, not the one you moved to. */
let voiceCaps={enabled:false,available:false,livePreview:false,previewIntervalMs:1600,
  maxBytes:16*1024*1024,maxSeconds:120,reason:'loading voice input…'};
let voiceJob=null,voiceSeq=0;
const VOICE_MIC_ICON='<img class="voice-mic-icon" src="/static/icons/fluent-studio-microphone.png" alt="">';
const VOICE_STOP_ICON='<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="5" y="5" width="14" height="14" rx="1"></rect></svg>';
function voiceUnavailableReason(){
  /* getUserMedia is gated on a secure context: over plain http the button can
     never work, so say why instead of failing on click. */
  if(!window.isSecureContext)return 'Voice input requires HTTPS';
  if(!navigator.mediaDevices||!navigator.mediaDevices.getUserMedia)return 'Microphone access is unavailable';
  if(typeof MediaRecorder==='undefined')return 'Audio recording is unsupported';
  if(!voiceCaps.available)return voiceCaps.reason||'Voice transcription is unavailable';
  return '';}
function fmtVoiceTime(ms){const s=Math.max(0,Math.floor(ms/1000));return Math.floor(s/60)+':'+String(s%60).padStart(2,'0');}
function voiceLocksComposer(){return !!(voiceJob&&voiceJob.targetSid===sid);}
function syncVoiceComposerState(){const locked=voiceLocksComposer(),enabled=!ta.disabled,b=$('#attachBtn');
  ta.readOnly=locked;sendBtn.disabled=!enabled||locked;if(b)b.disabled=!enabled||locked;
  sendBtn.title=locked?'Stop the recording first — dictation is still being revised':'';}
function syncVoiceButton(){if(typeof voiceBtn==='undefined'||!voiceBtn)return;
  const reason=voiceUnavailableReason(),recording=voiceJob&&voiceJob.mode==='recording';
  voiceBtn.disabled=!!reason||(!ready&&!recording)||!!(voiceJob&&voiceJob.mode!=='recording');
  voiceBtn.classList.toggle('recording',!!recording);voiceBtn.innerHTML=recording?VOICE_STOP_ICON:VOICE_MIC_ICON;
  voiceBtn.title=reason||((recording?'Stop and transcribe':'Voice input')+' · Alt+M');
  voiceBtn.setAttribute('aria-label',reason||(recording?'Stop voice recording':'Start voice input'));
  voiceBtn.setAttribute('aria-keyshortcuts','Alt+M');}
function renderVoiceState(){const bar=$('#voiceBar'),label=$('#voiceLabel');if(!bar||!label)return;
  if(!voiceJob){bar.hidden=true;bar.className='';syncVoiceComposerState();syncVoiceButton();return;}
  bar.hidden=false;bar.className=voiceJob.mode==='transcribing'?'transcribing':'';
  if(voiceJob.mode==='starting')label.textContent='microphone…';
  else if(voiceJob.mode==='transcribing')label.textContent='finalizing transcript…';
  else if(voiceJob.liveUnavailable)label.textContent='recording · final transcript only · '+fmtVoiceTime(Date.now()-voiceJob.startedAt);
  else if(voiceJob.hypothesis)label.textContent='listening · revising recent phrase · '+fmtVoiceTime(Date.now()-voiceJob.startedAt);
  else label.textContent='recording · '+fmtVoiceTime(Date.now()-voiceJob.startedAt)+' / '+fmtVoiceTime(voiceCaps.maxSeconds*1000);
  syncVoiceComposerState();syncVoiceButton();}
/* releasing the tracks is what turns the browser's recording indicator off */
function stopVoiceTracks(job){if(job&&job.stream){job.stream.getTracks().forEach(t=>t.stop());job.stream=null;}}
function clearVoiceTimers(job){if(!job)return;clearInterval(job.tick);clearTimeout(job.limit);clearTimeout(job.previewTimer);
  job.tick=job.limit=job.previewTimer=0;}
function cleanupVoiceJob(job){clearVoiceTimers(job);stopVoiceTracks(job);
  if(job.previewController)job.previewController.abort();if(voiceJob===job){voiceJob=null;renderVoiceState();}}
function voiceMime(){const choices=['audio/webm;codecs=opus','audio/mp4','audio/ogg;codecs=opus','audio/webm'];
  for(const type of choices){try{if(!MediaRecorder.isTypeSupported||MediaRecorder.isTypeSupported(type))return type;}catch(e){}}
  return '';}
/* a space only where two word characters would otherwise collide — CJK needs none */
function voiceSpacing(left,right){return /[A-Za-z0-9_]$/.test(left||'')&&/^[A-Za-z0-9_]/.test(right||'')?' ':'';}
function insertVoiceValue(value,start,end,text){start=Math.max(0,Math.min(Number(start)||0,value.length));end=Math.max(start,Math.min(Number(end)||start,value.length));
  const before=value.slice(0,start),after=value.slice(end),clean=(text||'').trim();
  const inserted=voiceSpacing(before,clean)+clean+voiceSpacing(clean,after);
  return {value:before+inserted+after,start:start,end:start+inserted.length};}
function voiceCommonPrefix(a,b){a=a||'';b=b||'';let i=0,n=Math.min(a.length,b.length);while(i<n&&a[i]===b[i])i++;
  if(i&&i<a.length&&/^[\uDC00-\uDFFF]$/.test(a[i]))i--;return i;}
function voiceTargetValue(job){if(job.targetSid===sid)return ta.value;const d=drafts[job.targetSid];return d&&d.text||'';}
function applyVoiceHypothesis(job,text,stableText,final){if(!job||!job.targetSid||!(text||'').trim())return false;
  const value=voiceTargetValue(job),start=job.previewApplied?job.previewStart:job.insertStart,
    end=job.previewApplied?job.previewEnd:job.insertEnd;
  if(job.previewApplied&&value.slice(start,end)!==job.previewText){job.previewConflict=true;return false;}
  const out=insertVoiceValue(value,start,end,text),preview=out.value.slice(out.start,out.end);
  if(job.targetSid===sid){const common=job.previewApplied?voiceCommonPrefix(job.previewText,preview):0;
    ta.setRangeText(preview.slice(common),start+common,end,'end');if(ta.value!==out.value)ta.value=out.value;
    ta.setSelectionRange(out.end,out.end);ta.dispatchEvent(new Event('input',{bubbles:true}));}
  else{const d=drafts[job.targetSid]||{text:'',images:[],files:[]};d.images=d.images||[];d.files=d.files||[];
    d.text=out.value;drafts[job.targetSid]=d;persistDrafts();}
  job.previewApplied=true;job.previewStart=out.start;job.previewEnd=out.end;
  job.previewText=preview;job.hypothesis=(text||'').trim();job.stableText=final?job.hypothesis:(stableText||'');
  return true;}
function restoreVoicePreview(job){if(!job||!job.previewApplied)return;const value=voiceTargetValue(job);
  if(value.slice(job.previewStart,job.previewEnd)!==job.previewText)return;
  const restored=value.slice(0,job.previewStart)+job.originalText+value.slice(job.previewEnd);
  if(job.targetSid===sid){ta.setRangeText(job.originalText,job.previewStart,job.previewEnd,'end');
    if(ta.value!==restored)ta.value=restored;ta.dispatchEvent(new Event('input',{bubbles:true}));}
  else{const d=drafts[job.targetSid]||{text:'',images:[],files:[]};d.images=d.images||[];d.files=d.files||[];
    d.text=restored;drafts[job.targetSid]=d;persistDrafts();}job.previewApplied=false;}
function voiceErrorMessage(err){const name=err&&err.name;if(name==='NotAllowedError')return 'Microphone permission was denied';
  if(name==='NotFoundError')return 'No microphone was found';if(name==='NotReadableError')return 'The microphone is already in use';
  return (err&&err.message)||String(err||'Voice input failed');}
async function loadVoiceCapabilities(){try{const r=await fetch('api/transcribe',{cache:'no-store'}),j=await r.json();
    voiceCaps=Object.assign(voiceCaps,j||{});if(!r.ok)voiceCaps.available=false;
  }catch(e){voiceCaps.available=false;voiceCaps.reason='Voice transcription is unreachable';}syncVoiceButton();}
async function warmVoiceModel(){try{await fetch('api/transcribe/warmup',{method:'POST',cache:'no-store'});}catch(e){}}
function voiceStreamId(){if(globalThis.crypto&&crypto.randomUUID)return crypto.randomUUID();
  return 'voice-'+Date.now().toString(36)+'-'+Math.random().toString(36).slice(2);}
function queueVoicePreview(job){if(!job||job.cancelled||job.mode!=='recording'||job.liveUnavailable||!voiceCaps.livePreview)return;
  if(Date.now()-job.startedAt<1500||job.chunks.length<2)return;job.previewQueued=true;
  if(job.previewPromise||job.previewTimer)return;const interval=Math.max(1000,Number(voiceCaps.previewIntervalMs)||1600);
  const wait=Math.max(0,interval-(Date.now()-(job.lastPreviewAt||0)));
  job.previewTimer=setTimeout(()=>{job.previewTimer=0;requestVoicePreview(job);},wait);}
async function requestVoicePreview(job){if(!job||job.cancelled||job.mode!=='recording'||job.previewPromise)return;
  job.previewQueued=false;job.lastPreviewAt=Date.now();const type=(job.recorder&&job.recorder.mimeType)||job.mime||'application/octet-stream';
  const blob=new Blob(job.chunks.slice(),{type:type});if(!blob.size||blob.size>voiceCaps.maxBytes)return;
  const controller=new AbortController();job.previewController=controller;const chunkCount=job.chunks.length;
  const work=(async()=>{try{const r=await fetch('api/transcribe?partial=1',{method:'POST',
      headers:{'Content-Type':blob.type||'application/octet-stream','X-Audio-Duration-Ms':String(Math.max(0,Date.now()-job.startedAt)),
        'X-Transcription-Stream':job.streamId},body:blob,signal:controller.signal});
    let j={};try{j=await r.json();}catch(e){}
    if(r.status===422||r.status===429)return;
    if(!r.ok||!j.ok)throw new Error(j.error||('live transcription failed ('+r.status+')'));
    job.previewFailures=0;if(!job.cancelled&&voiceJob===job)applyVoiceHypothesis(job,j.text||'',j.stableText||'',false);
  }catch(e){if(e.name!=='AbortError'){job.previewFailures=(job.previewFailures||0)+1;
      if(job.previewFailures>=3)job.liveUnavailable=true;}}
  finally{if(job.previewController===controller)job.previewController=null;job.previewPromise=null;
    if(job.mode==='recording'&&(job.previewQueued||job.chunks.length>chunkCount))queueVoicePreview(job);renderVoiceState();}})();
  job.previewPromise=work;await work;}
async function settleVoicePreview(job){clearTimeout(job.previewTimer);job.previewTimer=0;job.previewQueued=false;
  if(job.previewPromise){try{await job.previewPromise;}catch(e){}}}
async function uploadVoice(job,blob){if(job.cancelled)return;
  if(!blob.size){addNotice('⚠ no audio was recorded');cleanupVoiceJob(job);return;}
  if(blob.size>voiceCaps.maxBytes){addNotice('⚠ voice recording exceeds the upload limit');cleanupVoiceJob(job);return;}
  job.mode='transcribing';renderVoiceState();await settleVoicePreview(job);if(job.cancelled)return;
  job.controller=new AbortController();
  try{let r=null,j={};for(let attempt=0;attempt<6;attempt++){
      r=await fetch('api/transcribe',{method:'POST',headers:{'Content-Type':blob.type||'application/octet-stream',
        'X-Audio-Duration-Ms':String(job.durationMs||0),'X-Transcription-Stream':job.streamId},body:blob,signal:job.controller.signal});
      try{j=await r.json();}catch(e){j={};}
      if(r.status!==429||attempt===5)break;
      await new Promise(resolve=>setTimeout(resolve,300*(attempt+1)));if(job.cancelled)return;}
    if(!r.ok||!j.ok)throw new Error(j.error||('transcription failed ('+r.status+')'));
    if(!job.cancelled)applyVoiceHypothesis(job,j.text||'','',true);
  }catch(e){if(!job.cancelled&&e.name!=='AbortError')addNotice('⚠ '+voiceErrorMessage(e));}
  finally{cleanupVoiceJob(job);if(!job.cancelled&&job.targetSid===sid){
      requestAnimationFrame(()=>{if(!voiceJob&&job.targetSid===sid)ta.focus();});}}}
function recorderStopped(job){stopVoiceTracks(job);if(job.cancelled){cleanupVoiceJob(job);return;}
  const type=(job.recorder&&job.recorder.mimeType)||job.mime||'application/octet-stream';
  uploadVoice(job,new Blob(job.chunks,{type:type}));}
function stopVoiceInput(){const job=voiceJob;if(!job||job.mode!=='recording')return;
  job.durationMs=Math.max(0,Date.now()-job.startedAt);clearVoiceTimers(job);job.mode='transcribing';renderVoiceState();
  try{if(job.recorder&&job.recorder.state!=='inactive')job.recorder.stop();else recorderStopped(job);}catch(e){addNotice('⚠ '+voiceErrorMessage(e));cleanupVoiceJob(job);}}
function cancelVoiceInput(){const job=voiceJob;if(!job)return;job.cancelled=true;clearVoiceTimers(job);
  if(job.controller)job.controller.abort();if(job.previewController)job.previewController.abort();restoreVoicePreview(job);
  try{if(job.recorder&&job.recorder.state!=='inactive')job.recorder.stop();}catch(e){}
  cleanupVoiceJob(job);}
async function startVoiceInput(){const reason=voiceUnavailableReason();if(reason||!ready||!sid){if(reason)addNotice('⚠ '+reason);return;}
  const job=voiceJob={id:++voiceSeq,mode:'starting',targetSid:sid,insertStart:ta.selectionStart,insertEnd:ta.selectionEnd,
    originalText:ta.value.slice(ta.selectionStart,ta.selectionEnd),previewApplied:false,previewStart:0,previewEnd:0,previewText:'',
    hypothesis:'',stableText:'',streamId:voiceStreamId(),chunks:[],stream:null,recorder:null,startedAt:0,durationMs:0,
    previewTimer:0,previewPromise:null,previewController:null,previewQueued:false,lastPreviewAt:0,previewFailures:0,
    liveUnavailable:false,cancelled:false};renderVoiceState();job.warmPromise=warmVoiceModel();
  try{const stream=await navigator.mediaDevices.getUserMedia({audio:{channelCount:1,echoCancellation:true,noiseSuppression:true,autoGainControl:true},video:false});
    /* the permission prompt can outlive the job that asked for it */
    if(voiceJob!==job||job.cancelled){stream.getTracks().forEach(t=>t.stop());return;}job.stream=stream;job.mime=voiceMime();
    job.recorder=job.mime?new MediaRecorder(stream,{mimeType:job.mime,audioBitsPerSecond:64000}):new MediaRecorder(stream);
    job.recorder.ondataavailable=e=>{if(e.data&&e.data.size){job.chunks.push(e.data);queueVoicePreview(job);}};
    job.recorder.onerror=e=>{if(!job.cancelled)addNotice('⚠ '+voiceErrorMessage(e.error||e));cancelVoiceInput();};
    job.recorder.onstop=()=>recorderStopped(job);job.startedAt=Date.now();job.mode='recording';job.recorder.start(1000);
    job.tick=setInterval(renderVoiceState,250);job.limit=setTimeout(stopVoiceInput,voiceCaps.maxSeconds*1000);renderVoiceState();
  }catch(e){if(!job.cancelled)addNotice('⚠ '+voiceErrorMessage(e));cleanupVoiceJob(job);}}
function toggleVoiceInput(){if(voiceJob&&voiceJob.mode==='recording')stopVoiceInput();else if(!voiceJob)startVoiceInput();}
function sendMsg(){const t=ta.value.trim();
  if(voiceLocksComposer()){        /* silently doing nothing reads as a broken Enter key */
    if(!voiceJob.sendBlocked){voiceJob.sendBlocked=true;
      addNotice('⏸ still dictating — stop the recording (Alt+M), then send');}
    return;}
  if((!t&&!pendingImages.length&&!pendingFiles.length)||!ready||!sid||!ws||ws.readyState!==1)return;
  if(filesLoading||imgsLoading){   /* sending now would silently drop the attachment */
    addNotice('⏳ still reading attachments — press send again in a moment');return;}
  wsSend({type:'user',text:t,images:pendingImages.map(im=>({media_type:im.media_type,data:im.data})),
          files:pendingFiles.map(f=>({name:f.name,data:f.data}))});
  ta.value='';ta.style.height='auto';pendingImages=[];pendingFiles=[];renderAttach();dropDraft(sid);}
  /* busy state (and the thinking word/timer) is driven by the server's
     turn_start, so it stays correct across reattach — no optimistic flip here */

/* queued messages: chips above the composer while the agent is busy. Click a
   chip (or press ↑ on an empty box) to withdraw it back into the editor. */
let queued={};
function renderQueue(){const q=$('#queue');if(!q)return;const ids=Object.keys(queued);
  q.classList.toggle('on',ids.length>0);
  q.innerHTML=ids.map(id=>'<div class="qmsg" data-q="'+id+'" title="sends when the current turn ends · click to edit · ✕ to discard">'+
    '<span class="qicon">⏳</span><span class="qtext">'+esc(queued[id].text||'')+
    (queued[id].images?(' 🖼×'+queued[id].images):'')+
    (queued[id].files?(' 📎×'+queued[id].files.length):'')+'</span>'+
    '<span class="qsteer" title="steer into the RUNNING turn now — Claude sees it immediately but may reply only after finishing current work">⚡</span>'+
    '<span class="qx" title="discard">✕</span></div>').join('');
  q.querySelectorAll('.qmsg').forEach(el=>{const id=el.dataset.q;
    el.querySelector('.qx').onclick=ev=>{ev.stopPropagation();discardQueued(id);};
    el.querySelector('.qsteer').onclick=ev=>{ev.stopPropagation();
      if(ws&&ws.readyState===1)wsSend({type:'steer',qid:id});};
    el.onclick=()=>editQueued(id);});}
function addQueued(ev){queued[ev.qid]={text:ev.text||'',images:ev.images||0,files:ev.files||null};renderQueue();}
function removeQueued(qid){if(queued[qid]){delete queued[qid];renderQueue();}}
function discardQueued(id){if(ws&&ws.readyState===1)wsSend({type:'unqueue',qid:id});removeQueued(id);}
function editQueued(id){const it=queued[id];if(!it)return;
  /* ta.readOnly stops typing, not assignment — this would move the text out from
     under the live preview's recorded range. */
  if(voiceLocksComposer()){addNotice('⏸ stop the recording before editing a queued message');return;}
  if(it.files&&it.files.length)
    addNotice('⚠ '+it.files.length+' attachment'+(it.files.length>1?'s were':' was')+
              ' dropped with the queued message — attach again before sending');
  const draft=ta.value;
  ta.value=draft.trim()?(it.text+'\n'+draft):it.text;   /* keep any in-progress draft */
  ta.dispatchEvent(new Event('input'));ta.focus();
  if(it.images)addNotice('⚠ image(s) on the withdrawn message were dropped — re-paste if needed');
  discardQueued(id);}

/* sidebar open/close (mobile drawer; desktop collapse) */
function openSidebar(){$('#sidebar').classList.add('open');$('#sb-backdrop').classList.add('show');}
function closeSidebar(){$('#sidebar').classList.remove('open');$('#sb-backdrop').classList.remove('show');}
function toggleSidebar(){const sb=$('#sidebar');
  if(window.innerWidth<=860){sb.classList.contains('open')?closeSidebar():openSidebar();}
  else sb.classList.toggle('collapsed');}

/* changes drawer (Edits | Git) */
function drawerOpen(){return $('#drawer').classList.contains('open');}
function gitTab(){return $('#tabGit').classList.contains('on');}
function showTab(w){const e=w==='edits';$('#tabEdits').classList.toggle('on',e);$('#tabGit').classList.toggle('on',!e);
  $('#edits').style.display=e?'':'none';$('#gitc').style.display=e?'none':'';if(e){buildPendingEdits();showAllEdits();}else refreshGit();}
/* clicking "see Changes" focuses the drawer on just that one file's edit;
   the Edits tab header (or "show all") brings the full list back */
function showAllEdits(){const ed=$('#edits');const fb=ed.querySelector('.efocus');if(fb)fb.remove();
  ed.classList.remove('focusone');
  ed.querySelectorAll('.ecard').forEach(c=>c.style.display='');}
function focusEdit(toolId){const ed=$('#edits'),target=toolId&&tools[toolId];
  if(!target){showAllEdits();return;}
  ed.querySelectorAll('.ecard').forEach(c=>{c.style.display=(c===target)?'':'none';});
  ed.classList.add('focusone');
  let fb=ed.querySelector('.efocus');
  if(!fb){fb=document.createElement('div');fb.className='efocus';ed.insertBefore(fb,ed.firstChild);}
  fb.innerHTML='showing one file · <span class="showall">show all changes</span>';
  fb.querySelector('.showall').onclick=showAllEdits;
  target.classList.add('flash');target.scrollIntoView({block:'start'});setTimeout(()=>target.classList.remove('flash'),1200);}
function openDrawer(w){$('#drawer').classList.add('open');showTab(w||'edits');}
async function refreshGit(){if(!cwd){$('#gitc').innerHTML='<div class="empty">no session</div>';return;}
  try{const r=await fetch('api/diff?cwd='+encodeURIComponent(cwd));const j=await r.json();
    if(!j.ok){$('#gitc').innerHTML='<div class="empty">'+esc(j.error||'n/a')+'</div>';return;}
    let h='';if(j.files&&j.files.length)h+=j.files.map(f=>'<div class="gfile"><span class="st">'+esc(f.status)+'</span>'+esc(f.path)+'</div>').join('')+'<hr style="border-color:#333;margin:6px 0">';
    h+=j.diff&&j.diff.trim()?diffHtml(j.diff):'<div class="empty">clean ✓</div>';$('#gitc').innerHTML=h;
  }catch(e){}}

/* bindings */
ta.addEventListener('input',()=>{ta.style.height='auto';ta.style.height=Math.min(ta.scrollHeight,160)+'px';
  if(sid){drafts[sid]={text:ta.value,images:pendingImages.slice(),files:pendingFiles.slice()};schedulePersist();}atQuery();});
/* ── @model completion ────────────────────────────────────────────────────────
   Type `@` at the start of the composer and a list opens showing what each
   token resolves to, by the server's own rule (an exact id as typed, a fragment
   to the newest match, long-window variant when there is one). ↑/↓ pick, Tab or
   Enter insert the token, Esc closes; Enter is a send again once the list is
   gone. The token goes in rather than the full id, so `@fable` keeps meaning
   "the newest fable" the day a newer one appears. */
const AT_ALIASES=['opus','sonnet','haiku','fable'];
let atItems=[],atSel=0;
function resolveModelToken(tok){const t=tok.toLowerCase(),base=t.endsWith('[1m]')?t.slice(0,-4):t,ids=MODELS.map(m=>m.id);
  for(const id of ids)if(id.toLowerCase()===t)return id;
  const long=id=>ids.includes(id+'[1m]')?id+'[1m]':id;
  for(const id of ids)if(!id.endsWith('[1m]')&&id.toLowerCase().includes(base))return long(id);
  if(base==='opus'||base==='sonnet'||base==='haiku')return base==='haiku'?base:base+'[1m]';
  return null;}
function atFrag(){const v=ta.value,m=v.match(/^\s*@([\w.\-\[\]]*)$/);return (m&&ta.selectionStart===v.length)?m[1]:null;}
function atOpen(){return $('#atac').classList.contains('on');}
function atClose(){const b=$('#atac');b.classList.remove('on');b.innerHTML='';atItems=[];atSel=0;}
function atQuery(){const f=atFrag();if(f===null){atClose();return;}
  const q=f.toLowerCase(),seen=new Set(),out=[];
  const add=(tok,name)=>{const to=resolveModelToken(tok);if(!to||seen.has(tok))return;seen.add(tok);out.push({tok:tok,to:to,name:name||''});};
  AT_ALIASES.forEach(a=>{if(a.includes(q)){const r=resolveModelToken(a),m=MODELS.find(x=>x.id===r);add(a,m&&m.name);}});
  MODELS.forEach(m=>{if(m.id.toLowerCase().includes(q)||(m.name||'').toLowerCase().includes(q))add(m.id,m.name);});
  atItems=out.slice(0,14);if(!atItems.length){atClose();return;}
  atSel=Math.min(atSel,atItems.length-1);const b=$('#atac');
  b.innerHTML=atItems.map((it,i)=>'<div class="acitem'+(i===atSel?' sel':'')+'" data-i="'+i+'"><span class="acname">@'+esc(it.tok)+'</span>'+
    '<span class="acto">'+esc(it.to!==it.tok?'→ '+it.to:it.name)+'</span></div>').join('');
  b.classList.add('on');
  b.querySelectorAll('.acitem').forEach(el=>el.onmousedown=ev=>{ev.preventDefault();atPick(+el.dataset.i);});}
function atMove(d){if(!atItems.length)return;atSel=(atSel+d+atItems.length)%atItems.length;
  const els=$('#atac').querySelectorAll('.acitem');els.forEach((el,i)=>el.classList.toggle('sel',i===atSel));els[atSel].scrollIntoView({block:'nearest'});}
function atPick(i){const it=atItems[i];if(!it)return;ta.value='@'+it.tok+' ';atClose();ta.focus();ta.dispatchEvent(new Event('input'));}
ta.addEventListener('blur',()=>setTimeout(atClose,160));
ta.addEventListener('keydown',e=>{
  if(atOpen()){
    if(e.key==='ArrowDown'){e.preventDefault();atMove(1);return;}
    if(e.key==='ArrowUp'){e.preventDefault();atMove(-1);return;}
    if(e.key==='Tab'||(e.key==='Enter'&&!e.shiftKey)){e.preventDefault();atPick(atSel);return;}
    if(e.key==='Escape'){e.preventDefault();atClose();return;}}
  if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendMsg();}
  else if(e.key==='ArrowUp'&&!ta.value&&Object.keys(queued).length){
    e.preventDefault();const ids=Object.keys(queued);editQueued(ids[ids.length-1]);}});
window.addEventListener('paste',handlePaste);
$('#attachBtn').onclick=()=>$('#filePick').click();
voiceBtn.onclick=toggleVoiceInput;
$('#voiceCancel').onclick=cancelVoiceInput;
/* a live recording must not survive the page */
window.addEventListener('beforeunload',cancelVoiceInput);
/* capture phase: the textarea has the focus while dictating */
document.addEventListener('keydown',e=>{if(e.repeat||!e.altKey||e.ctrlKey||e.metaKey||e.shiftKey||e.key.toLowerCase()!=='m')return;
  e.preventDefault();e.stopPropagation();toggleVoiceInput();},true);
/* clearing the value lets the same file be picked twice in a row */
$('#filePick').onchange=e=>{takeFiles(e.target.files);e.target.value='';};
(function(){const comp=$('#composer');let depth=0;   /* depth: dragleave also fires for children */
  comp.addEventListener('dragenter',e=>{e.preventDefault();if(ready&&++depth===1)comp.classList.add('drop');});
  comp.addEventListener('dragover',e=>{e.preventDefault();});
  comp.addEventListener('dragleave',()=>{if(--depth<=0){depth=0;comp.classList.remove('drop');}});
  comp.addEventListener('drop',e=>{e.preventDefault();depth=0;comp.classList.remove('drop');
    if(ready&&e.dataTransfer)takeFiles(e.dataTransfer.files);});})();
sendBtn.onclick=sendMsg;
$('#stop').onclick=doInterrupt;
document.addEventListener('keydown',e=>{
  if(e.key==='Escape'&&running&&!srchOpen()&&!$('#cwdac').classList.contains('on')){e.preventDefault();doInterrupt();}});
$('#newbtn').onclick=newSession;
/* color theme: apply + persist (the <head> script already set it pre-paint) */
function applyTheme(t){if(t&&t!=='dark')document.documentElement.setAttribute('data-theme',t);
  else document.documentElement.removeAttribute('data-theme');}
(function(){const t=localStorage.getItem('al_theme')||'dark';const sel=$('#theme');
  if(sel){sel.value=t;sel.onchange=()=>{const v=sel.value;localStorage.setItem('al_theme',v);applyTheme(v);};}
  applyTheme(t);})();
$('#navtoggle').onclick=toggleSidebar;
$('#sb-backdrop').onclick=closeSidebar;
/* effort pill: click to pick thinking depth (low/medium/high/xhigh/max) */
$('#effort').onclick=ev=>{ev.stopPropagation();toggleCardMenu(ev.currentTarget,
  EFFORTS.map(e=>({label:(e===curEffort?'● ':'○ ')+e,fn:()=>setEffort(e)})));};
setEffortPill(curEffort);
$('#modelpill').onclick=ev=>{ev.stopPropagation();
  const opts=[['default','default (CLI pick)'],...MODEL_ALIASES.map(([v])=>[v,v]),...MODELS.map(m=>[m.id,m.name])];
  toggleCardMenu(ev.currentTarget,opts.map(([v,t])=>({label:(v===curModel?'● ':'○ ')+t,fn:()=>pickModel(v)})));};
setModelPill();
/* desktop: drag the sidebar's right edge to resize (clamped + persisted) */
const SBW_KEY='al_sbw',SBW_MIN=200,SBW_MAX=560;
function setSidebarW(w,save){w=Math.max(SBW_MIN,Math.min(SBW_MAX,Math.round(w)));
  document.documentElement.style.setProperty('--sbw',w+'px');
  if(save)localStorage.setItem(SBW_KEY,w);}
(function(){const saved=parseInt(localStorage.getItem(SBW_KEY)||'',10);if(saved)setSidebarW(saved,false);
  const h=$('#sbresize');if(!h)return;let on=false;
  h.addEventListener('mousedown',e=>{on=true;h.classList.add('drag');document.body.style.userSelect='none';e.preventDefault();});
  window.addEventListener('mousemove',e=>{if(!on)return;setSidebarW(e.clientX-$('#sidebar').getBoundingClientRect().left,false);});
  window.addEventListener('mouseup',()=>{if(!on)return;on=false;h.classList.remove('drag');document.body.style.userSelect='';
    setSidebarW($('#sidebar').getBoundingClientRect().width,true);});
  h.addEventListener('dblclick',()=>setSidebarW(270,true));   /* double-click: reset to default */
})();
/* desktop: drag the Changes drawer's left edge to resize (clamped + persisted) */
const DRW_KEY='al_drw',DRW_MIN=360,DRW_MAX=1100;
function setDrawerW(w,save){const cap=Math.round(window.innerWidth*0.96);
  w=Math.max(DRW_MIN,Math.min(Math.min(DRW_MAX,cap),Math.round(w)));
  document.documentElement.style.setProperty('--drw',w+'px');
  if(save)localStorage.setItem(DRW_KEY,w);}
(function(){const saved=parseInt(localStorage.getItem(DRW_KEY)||'',10);if(saved)setDrawerW(saved,false);
  const h=$('#drresize');if(!h)return;let on=false;
  h.addEventListener('mousedown',e=>{on=true;h.classList.add('drag');document.body.style.userSelect='none';e.preventDefault();});
  window.addEventListener('mousemove',e=>{if(!on)return;setDrawerW(window.innerWidth-e.clientX,false);});
  window.addEventListener('mouseup',()=>{if(!on)return;on=false;h.classList.remove('drag');document.body.style.userSelect='';
    setDrawerW($('#drawer').getBoundingClientRect().width,true);});
  h.addEventListener('dblclick',()=>setDrawerW(560,true));   /* double-click: reset to default */
})();
$('#resumeRef').onclick=e=>{e.stopPropagation();wsSend({type:'proj_refresh'});loadTree();};
/* collapsible sections: clicking the header toggles; restore saved state */
['secFav','secRecent'].forEach(id=>{
  const h=$('#'+id+' .sb-h');if(h)h.onclick=()=>toggleSec(id);});
applySecCollapse();
/* dismiss the ⋯ card menu on outside-click, Escape, scroll or resize */
document.addEventListener('click',e=>{if(!e.target.closest('#cardMenu')&&!e.target.closest('.skebab'))closeCardMenu();});
document.addEventListener('keydown',e=>{if(e.key==='Escape'){closeCardMenu();
  if(fimpOpened())fimpClose();if(pmanOpened())pmanClose();}});
window.addEventListener('resize',closeCardMenu);
window.addEventListener('scroll',closeCardMenu,true);
/* model/mode pickers set the NEW-session default only (persisted). A running
   session is reconfigured per-session via the ⚙ Configure menu, so editing the
   new-session default no longer disturbs the current session. */
$('#model').onchange=()=>localStorage.setItem('al_model',$('#model').value);
$('#mode').onchange=()=>localStorage.setItem('al_mode',$('#mode').value);
{let sm=localStorage.getItem('al_model');if(sm==='default'){localStorage.removeItem('al_model');sm=null;}   /* legacy 'default' → use the new opus default */
 if(sm){const o=$('#model');for(const x of o.options)if(x.value===sm){o.value=sm;break;}}
 const smd=localStorage.getItem('al_mode');if(smd){const o=$('#mode');for(const x of o.options)if(x.value===smd){o.value=smd;break;}}}
$('#tabEdits').onclick=()=>showTab('edits');
$('#tabGit').onclick=()=>showTab('git');
$('#dclose').onclick=()=>$('#drawer').classList.remove('open');
/* dismiss the Changes drawer on outside-click (chat, sidebar, composer…) or Escape,
   so you don't have to aim for the ✕. The .emark "see Changes" markers are the
   openers → excluded, else the opening click would immediately re-close it. */
document.addEventListener('click',e=>{
  if(drawerOpen()&&!e.target.closest('#drawer')&&!e.target.closest('.emark'))$('#drawer').classList.remove('open');});
document.addEventListener('keydown',e=>{if(e.key==='Escape'&&drawerOpen())$('#drawer').classList.remove('open');});
$('#grefresh').onclick=refreshGit;
$('#project').onchange=()=>{const c=$('#project').value==='__custom__';
  $('#cwdwrap').classList.toggle('show',c);
  if(c){if(!$('#cwd').value)$('#cwd').value=HOMEDIR?(HOMEDIR+'/'):'';$('#cwd').focus();acQuery();}else acClose();
  loadTree();};
$('#cwd').addEventListener('input',acQuery);
$('#cwd').addEventListener('focus',acQuery);
$('#cwd').addEventListener('change',loadTree);
$('#cwd').addEventListener('blur',()=>setTimeout(acClose,160));
$('#cwd').addEventListener('keydown',e=>{const b=$('#cwdac');if(!b.classList.contains('on'))return;
  if(e.key==='ArrowDown'){e.preventDefault();acMove(1);}
  else if(e.key==='ArrowUp'){e.preventDefault();acMove(-1);}
  else if(e.key==='Enter'&&acSel>=0){e.preventDefault();acPick(acSel);}
  else if(e.key==='Escape')acClose();});

/* ── Import folder ────────────────────────────────────────────────────────
   Register a directory as a sidebar project. Folders normally reach the sidebar
   by owning a session, so a fresh directory would otherwise be invisible until
   its first session existed. Same /api/dircomplete backend as the path box. */
let fimpItems=[],fimpSel=-1,fimpT=0;
function fimpMsg(t,bad){const e=$('#fimpmsg');if(!e)return;e.textContent=t||'';e.classList.toggle('bad',!!bad);}
function fimpOpen(){const w=$('#fimp');w.removeAttribute('hidden');fimpMsg('');
  const i=$('#fimpq');i.value=HOMEDIR?HOMEDIR+'/':'';i.focus();fimpQuery();}
function fimpClose(){$('#fimp').setAttribute('hidden','');fimpItems=[];fimpSel=-1;}
function fimpOpened(){return !$('#fimp').hasAttribute('hidden');}
function fimpMark(){const b=$('#fimpac');
  b.querySelectorAll('.acitem').forEach((el,i)=>el.classList.toggle('sel',i===fimpSel));
  const cur=b.querySelector('.acitem.sel');if(cur)cur.scrollIntoView({block:'nearest'});}
function fimpRender(j){const b=$('#fimpac');fimpItems=(j&&j.dirs)||[];fimpSel=-1;
  if(!fimpItems.length){b.innerHTML='<div class="acmore">no matching folder</div>';return;}
  b.innerHTML=fimpItems.map((p,i)=>'<div class="acitem" data-i="'+i+'"><div class="acname">'+
    esc(p.split('/').filter(Boolean).slice(-1)[0]||p)+'</div><div class="acpath">'+esc(p)+'</div></div>').join('')
    +((j&&j.more)?'<div class="acmore">… keep typing to narrow</div>':'');
  b.querySelectorAll('.acitem').forEach(el=>el.onclick=()=>{
    $('#fimpq').value=fimpItems[+el.dataset.i]+'/';$('#fimpq').focus();fimpQuery();});}
function fimpQuery(){clearTimeout(fimpT);
  fimpT=setTimeout(()=>fetch('api/dircomplete?q='+encodeURIComponent($('#fimpq').value))
    .then(r=>r.json()).then(fimpRender).catch(()=>{}),130);}
function fimpAdd(){
  let p=$('#fimpq').value.trim();
  if(p.length>1)p=p.replace(/\/+$/,'');
  if(!p){fimpMsg('type a folder path',true);return;}
  fimpMsg('adding…');
  fetch('api/pinfolder',{method:'POST',headers:{'Content-Type':'application/json'},
                         body:JSON.stringify({path:p})})
    .then(r=>r.json()).then(j=>{
      if(j&&j.ok){fimpClose();addNotice('📁 project « '+(j.name||p)+' » added to the sidebar');loadTree();}
      else fimpMsg((j&&j.error)||'could not add that folder',true);})
    .catch(e=>fimpMsg(String(e),true));}
$('#fimpbtn').onclick=fimpOpen;
$('#fimpx').onclick=fimpClose;
$('#fimpcancel').onclick=fimpClose;
$('#fimpok').onclick=fimpAdd;
$('#fimp').addEventListener('mousedown',e=>{if(e.target.id==='fimp')fimpClose();});
$('#fimpq').addEventListener('input',fimpQuery);
$('#fimpq').addEventListener('keydown',e=>{
  if(e.key==='ArrowDown'){e.preventDefault();if(fimpItems.length){fimpSel=Math.min(fimpSel+1,fimpItems.length-1);fimpMark();}}
  else if(e.key==='ArrowUp'){e.preventDefault();if(fimpItems.length){fimpSel=Math.max(fimpSel-1,0);fimpMark();}}
  else if(e.key==='Enter'){e.preventDefault();
    if(fimpSel>=0){$('#fimpq').value=fimpItems[fimpSel]+'/';fimpSel=-1;fimpQuery();}
    else fimpAdd();}
  else if(e.key==='Escape'){e.preventDefault();fimpClose();}});

/* project picker: recent project dirs (+ Custom path…) */
(async function(){try{const r=await fetch('api/projects');const j=await r.json();HOMEDIR=j.home||'';const sel=$('#project');
  /* Custom path… first (easy to reach); recent dirs as a labelled group */
  const cust=document.createElement('option');cust.value='__custom__';cust.textContent='✎  Custom path…';sel.appendChild(cust);
  const mk=(label,items)=>{if(!items.length)return;const g=document.createElement('optgroup');g.label=label;
    items.forEach(p=>{const o=document.createElement('option');o.value=p.path;o.textContent=p.path.split('/').slice(-2).join('/');o.title=p.path;g.appendChild(o);});sel.appendChild(g);};
  const recent=(j.projects||[]);
  mk('Recent',recent);
  const first=recent[0];
  sel.value=first?first.path:'__custom__';
  $('#cwdwrap').classList.toggle('show',sel.value==='__custom__');
  loadTree();
  refreshImportHint();   /* HOMEDIR and the default project are only known now */
}catch(e){}})();

setInterval(()=>reqList(),8000);
setInterval(loadTree,30000);
loadTree();
loadUsage();
setInterval(loadUsage,60000);
loadModels();
wireImport();
wireSearch();
loadVoiceCapabilities();
/* manual model-list refresh: the ↻ button next to the picker re-queries the API,
   bypassing the server's 1h cache — so a just-shipped model appears on click,
   no page reload. Spins while fetching; no automatic polling by design. */
$('#mrefresh').onclick=()=>{const b=$('#mrefresh');if(b.classList.contains('busy'))return;
  b.classList.add('busy');loadModels(true).finally(()=>b.classList.remove('busy'));};
openWs();
</script>
</body>
</html>"""
# A restart with new page code used to leave every open tab running the old
# script against the new server: the socket reconnects, the page does not
# reload, and a feature shipped an hour ago is simply not there. The stamp is
# the page's own hash; the socket announces it on connect and a page that
# was served under another one reloads itself (the draft is saved first).
PAGE_STAMP = hashlib.sha1(CONSOLE_HTML.encode("utf-8")).hexdigest()[:12]


RECAP_KEEP_H = 24   # prune isolated recap-query transcripts older than this (hours)

def _cleanup_recap_artifacts():
    """Prune the isolated recap-query transcripts (the away-summary feature writes
    one per call into ~/.cache/claude-console-recap; the recap text already lives in
    the real session's log, so the file is pure junk). Hard-removes ONLY files in
    our own cache dir — never touches real project transcripts."""
    try:
        cutoff = time.time() - RECAP_KEEP_H * 3600
        for d in glob.glob(os.path.join(CLAUDE_ROOT, "*claude-console-recap*")):
            for p in glob.glob(os.path.join(d, "*.jsonl")):
                try:
                    if os.path.getmtime(p) < cutoff:
                        os.remove(p)
                except OSError:
                    pass
    except Exception:
        pass


def _recap_tick():
    """Periodic sweep: any session idle past the threshold gets a one-line recap,
    so it's already waiting (in the log) when you return — and fires for a session
    you're sitting on idle too. At most one recap per idle window (recap_for dedup)."""
    if not RECAP_ENABLED:
        return
    now = time.time()
    for s in list(CHAT_SESSIONS.values()):
        if (not s.busy and not s.ended and not s.compacting and not s.recap_busy
                and s.recap_for != s.last_activity
                and (now - s.last_activity) >= RECAP_IDLE_SEC):
            tornado.ioloop.IOLoop.current().spawn_callback(s._make_recap)


def main():
    moved = migrate_session_favs_to_projects()
    app = tornado.web.Application([
        (r"/", ConsoleHandler),
        (r"/console", ConsoleHandler),
        (r"/api/projects", ProjectsHandler),
        (r"/api/resumable", ResumableHandler),
        (r"/api/tree", TreeHandler),
        (r"/api/pinfolder", PinFolderHandler),
        (r"/api/dircomplete", DirCompleteHandler),
        (r"/api/diff", DiffHandler),
        (r"/api/usage", UsageHandler),
        (r"/api/models", ModelsHandler),
        (r"/api/export", ExportHandler),
        (r"/api/import", ImportHandler),
        (r"/api/search", SearchHandler),
        (r"/api/thread", ThreadHandler),
        (r"/api/transcribe/warmup", TranscribeWarmupHandler),
        (r"/api/transcribe", TranscribeHandler),
        (r"/ws/chat", ChatSocket),
        (r"/static/(.*)", tornado.web.StaticFileHandler,
         {"path": os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")}),
    ])
    loopback = BIND in ("127.0.0.1", "localhost", "::1")
    # transcripts reach hundreds of MB, and tornado's 100MB default body cap would
    # reject those uploads outright on /api/import.
    app.listen(PORT, address=BIND, max_buffer_size=IMPORT_MAX, max_body_size=IMPORT_MAX)
    # warm the search index in the background; the first build is ~6 s over a 2.4 GB
    # corpus, every pass after that only reads what was appended
    tornado.ioloop.IOLoop.current().run_in_executor(None, reindex)
    if RECAP_ENABLED:   # idle-session recap sweep (every 30s)
        tornado.ioloop.PeriodicCallback(_recap_tick, 30000).start()
        _cleanup_recap_artifacts()   # prune stale recap transcripts now…
        tornado.ioloop.PeriodicCallback(_cleanup_recap_artifacts, 6 * 3600 * 1000).start()   # …and every 6h
    print("Claude Console on http://%s:%d" % (BIND, PORT))
    print("  console: http://%s:%d/" % (BIND, PORT))
    print("  claude bin: %s" % CLAUDE_BIN)
    print("  claude transcripts: %s" % CLAUDE_ROOT)
    print("  codex transcripts:  %s" % CODEX_ROOT)
    print("  auth: %s" % ("enabled" if AUTH else "disabled"))
    _voice = _transcribe_status()
    print("  voice input: %s" % ("enabled" if _voice["available"]
                                 else "off — " + _voice["reason"]))
    if moved:
        print("  migrated %d starred session(s) onto their project folders" % moved)
    if not loopback and not AUTH:
        print("  ⚠️  EXPOSED on %s WITHOUT auth — this serves your agent"
              " transcripts and code diffs. Set CLAUDE_CONSOLE_AUTH=user:pass." % BIND)

    def _shutdown(signum, frame):
        for s in list(CHAT_SESSIONS.values()):
            try:
                s.terminate()
            except Exception:
                pass
        _TRANSCRIBER.shutdown()
        os._exit(0)
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
