#!/usr/bin/env python3
"""Agent Lens — a read-only GUI that separates an AI coding agent's *discussion*
from its *code/file changes*.

It observes Claude Code / Codex session transcripts (the JSONL each writes) plus
the live `git diff` of the session's working directory, and renders them in three
panes: Discussion · Activity · Git diff. It never drives the agent and never
writes to your repos — purely an observer. Run it alongside your normal terminal
workflow.

Env:
  AGENTLENS_PORT   listen port (default 7703)
  AGENTLENS_BIND   bind address (default 127.0.0.1; set 0.0.0.0 to reach from LAN)
  AGENTLENS_AUTH   optional HTTP Basic Auth "user:pass" (default disabled)
"""

import asyncio
import base64
import glob
import json
import os
import secrets
import shutil
import signal
import subprocess
import time
import urllib.parse

import tornado.ioloop
import tornado.iostream
import tornado.process
import tornado.web
import tornado.websocket

try:
    from claude_agent_sdk import (
        ClaudeSDKClient, ClaudeAgentOptions, AssistantMessage, UserMessage,
        SystemMessage, ResultMessage, TextBlock, ThinkingBlock, ToolUseBlock,
        ToolResultBlock, PermissionResultAllow, PermissionResultDeny)
    HAVE_SDK = True
except Exception:
    HAVE_SDK = False

PORT = int(os.environ.get("AGENTLENS_PORT", "7703"))
AUTH = os.environ.get("AGENTLENS_AUTH", "")
# Default to loopback: this serves ALL your agent transcripts + home-wide git
# diffs, so it must not land on the network by accident. Set AGENTLENS_BIND=0.0.0.0
# (ideally together with AGENTLENS_AUTH) to reach it from another device.
BIND = os.environ.get("AGENTLENS_BIND", "127.0.0.1")
HOME = os.path.expanduser("~")
CLAUDE_ROOT = os.path.join(HOME, ".claude", "projects")
CODEX_ROOT = os.path.join(HOME, ".codex", "sessions")
# Interactive console drives the real `claude` CLI in headless stream-json mode.
CLAUDE_BIN = (os.environ.get("AGENTLENS_CLAUDE") or shutil.which("claude")
              or os.path.expanduser("~/.local/bin/claude"))

CAP = 12000          # cap per long string field sent to the browser
RESULT_CAP = 6000    # cap per tool_result body
POLL_MS = 800        # transcript tail interval


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
        content = msg.get("content")
        if isinstance(content, str):
            if content.strip():
                evs.append({**base, "kind": "user_text", "role": "user", "text": _cap(content)})
        elif isinstance(content, list):
            for b in content:
                if not isinstance(b, dict):
                    continue
                bt = b.get("type")
                if bt == "text" and b.get("text", "").strip():
                    evs.append({**base, "kind": "user_text", "role": "user", "text": _cap(b["text"])})
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
                if not title and rec.get("type") == "user":
                    msg = (rec.get("message") or {}).get("content")
                    s = msg if isinstance(msg, str) else _txt(msg)
                    s = (s or "").strip()
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
        items.append({
            "id": path, "source": source, "cwd": cwd, "branch": branch,
            "title": title or os.path.basename(path),
            "mtime": st.st_mtime, "size": st.st_size,
        })
    return items


def list_projects():
    """Real project dirs for the console picker: recent session cwds (filtered)
    plus git repos under ~/Git. Excludes /tmp and runtime/cache dirs."""
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
    for d in sorted(glob.glob(os.path.join(HOME, "Git", "*"))):
        if d not in seen and os.path.isdir(os.path.join(d, ".git")):
            seen.add(d)
            out.append({"path": d, "recent": False, "git": True})
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


def load_transcript_events(cc, cap=2000):
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
                    "title": s.get("title", ""), "mtime": s.get("mtime", 0)})
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


def dir_complete(q, limit=30):
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


def _source_of(path):
    rp = os.path.realpath(path)
    if rp.startswith(os.path.realpath(CLAUDE_ROOT) + os.sep):
        return "claude"
    if rp.startswith(os.path.realpath(CODEX_ROOT) + os.sep):
        return "codex"
    return None


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
        self.set_header("WWW-Authenticate", 'Basic realm="agent-lens"')
        self.finish()
        return False


# ───────────────────────── handlers ─────────────────────────
class IndexHandler(AuthMixin, tornado.web.RequestHandler):
    def get(self):
        if not self._ok_auth():
            return
        self.set_header("Cache-Control", "no-store")
        self.write(HTML)


class SessionsHandler(AuthMixin, tornado.web.RequestHandler):
    def get(self):
        if not self._ok_auth():
            return
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps({"sessions": list_sessions()}))


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


class EventsSocket(AuthMixin, tornado.websocket.WebSocketHandler):
    def check_origin(self, origin):
        return True

    def open(self):
        if AUTH and not self._ok_auth():
            self.close(4401, "Unauthorized")
            return
        raw = self.get_argument("file", "")
        path = os.path.realpath(urllib.parse.unquote(raw))
        self.source = _source_of(path)
        if not self.source or not os.path.isfile(path):
            self.close(4404, "Unknown or invalid session file")
            return
        self.path = path
        self._offset = 0
        self._buf = b""
        self._idx = 0
        self._cb = None
        events, cwd, branch = self._read_new(initial=True)
        self.write_message(json.dumps({
            "type": "init", "source": self.source, "cwd": cwd,
            "branch": branch, "events": events,
        }))
        self._cb = tornado.ioloop.PeriodicCallback(self._poll, POLL_MS)
        self._cb.start()

    def _read_new(self, initial=False):
        cwd, branch = "", ""
        events = []
        try:
            size = os.path.getsize(self.path)
            if size < self._offset:  # rotated/truncated
                self._offset, self._buf = 0, b""
            with open(self.path, "rb") as f:
                f.seek(self._offset)
                data = f.read()
                self._offset += len(data)
            self._buf += data
            *lines, self._buf = self._buf.split(b"\n")
            for raw in lines:
                if not raw.strip():
                    self._idx += 1
                    continue
                line = raw.decode("utf-8", errors="replace")
                if initial and (not cwd):
                    try:
                        rec = json.loads(line)
                        cwd = rec.get("cwd", "") or (rec.get("payload") or {}).get("cwd", "") or cwd
                        branch = rec.get("gitBranch", "") or branch
                    except Exception:
                        pass
                events.extend(parse_line(line, self._idx, self.source))
                self._idx += 1
        except Exception:
            pass
        if initial and not cwd:
            cwd, branch, _ = (_peek_claude if self.source == "claude" else _peek_codex)(self.path)
        return (events, cwd, branch) if initial else events

    def _poll(self):
        try:
            events = self._read_new()
            if events:
                self.write_message(json.dumps({"type": "append", "events": events}))
        except tornado.websocket.WebSocketClosedError:
            self.on_close()
        except Exception:
            pass

    def on_message(self, message):
        pass

    def on_close(self):
        if getattr(self, "_cb", None):
            try:
                self._cb.stop()
            except Exception:
                pass
            self._cb = None


HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>Agent Lens</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#1e1e1e;--bg2:#252526;--bg3:#2d2d2d;--line:#3c3c3c;--fg:#d4d4d4;--mut:#858585;
  --acc:#4fc1ff;--usr:#3794ff;--asn:#cdcdcd;--think:#7a7a7a;--add:#2ea043;--del:#f85149;--tool:#e0c080}
html,body{height:100%;background:var(--bg);color:var(--fg);overflow:hidden;
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif;font-size:14px}
body{display:flex;flex-direction:column}
header{display:flex;gap:8px;align-items:center;padding:6px 10px;background:var(--bg2);
  border-bottom:1px solid var(--line);flex-shrink:0;flex-wrap:wrap}
header .brand{font-weight:600;color:var(--acc);white-space:nowrap}
header select{flex:1;min-width:140px;background:var(--bg3);color:var(--fg);border:1px solid var(--line);
  border-radius:5px;padding:5px 8px;font-size:13px;max-width:560px}
.badge{font-size:11px;padding:2px 7px;border-radius:10px;background:var(--bg3);color:var(--mut);
  border:1px solid var(--line);white-space:nowrap}
.badge.claude{color:#d98c5f;border-color:#5a3c2a}
.badge.codex{color:#8fd0a0;border-color:#2a5a3c}
.dot{width:8px;height:8px;border-radius:50%;background:#555;flex-shrink:0}
.dot.live{background:var(--add);box-shadow:0 0 6px var(--add)}
.toggle{font-size:12px;color:var(--mut);cursor:pointer;user-select:none;white-space:nowrap}
.toggle input{vertical-align:middle;margin-right:3px}

main{flex:1;display:flex;min-height:0}
.pane{flex:1;min-width:0;display:flex;flex-direction:column;border-right:1px solid var(--line)}
.pane:last-child{border-right:none}
.pane>.ph{padding:5px 10px;font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--mut);
  background:var(--bg2);border-bottom:1px solid var(--line);display:flex;gap:8px;align-items:center;flex-shrink:0}
.ph .tab{cursor:pointer;padding:2px 6px;border-radius:4px}
.ph .tab.on{background:var(--bg3);color:var(--fg)}
.ph .grow{flex:1}
.scroll{flex:1;overflow-y:auto;overflow-x:hidden;padding:10px;-webkit-overflow-scrolling:touch}

/* discussion */
.msg{margin-bottom:12px;line-height:1.5;word-wrap:break-word;overflow-wrap:anywhere}
.msg .who{font-size:11px;color:var(--mut);margin-bottom:3px}
.msg.user .bubble{background:#10243e;border:1px solid #1f4a7a;border-radius:8px;padding:8px 10px}
.msg.user .who{color:var(--usr)}
.msg.assistant .bubble{color:var(--asn)}
.msg .bubble{white-space:normal}
.think{color:var(--think);font-style:italic;font-size:13px;border-left:2px solid #3a3a3a;
  padding:2px 0 2px 9px;margin-bottom:10px;white-space:pre-wrap}
.think.hide{display:none}
.toolchip{font-size:12px;color:var(--tool);background:#2a2519;border:1px solid #4a3f28;border-radius:5px;
  padding:2px 7px;margin-bottom:10px;display:inline-block;cursor:pointer;font-family:ui-monospace,monospace}
.toolchip.hide{display:none}
.toolchip:hover{filter:brightness(1.25)}

/* code / markdown */
pre{background:#161616;border:1px solid var(--line);border-radius:6px;padding:8px;overflow-x:auto;
  margin:6px 0;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12.5px;line-height:1.45}
code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12.5px;
  background:#161616;border:1px solid var(--line);border-radius:3px;padding:0 4px}
pre code{background:none;border:none;padding:0}
.bubble h1,.bubble h2,.bubble h3{font-size:14px;margin:8px 0 4px}
.bubble ul,.bubble ol{margin:4px 0 4px 20px}
.bubble a{color:var(--acc)}

/* activity */
.act{border:1px solid var(--line);border-radius:7px;margin-bottom:8px;background:var(--bg2);overflow:hidden}
.act .head{padding:7px 9px;cursor:pointer;display:flex;gap:8px;align-items:center;user-select:none}
.act .head:hover{background:var(--bg3)}
.act .ico{flex-shrink:0}
.act .tn{color:var(--tool);font-weight:600;font-family:ui-monospace,monospace;font-size:12.5px}
.act .arg{color:var(--mut);font-family:ui-monospace,monospace;font-size:12px;white-space:nowrap;
  overflow:hidden;text-overflow:ellipsis;flex:1}
.act .body{display:none;border-top:1px solid var(--line);padding:8px 9px}
.act.open .body{display:block}
.act.err .tn{color:var(--del)}
.act.flash{outline:2px solid var(--acc);outline-offset:-2px}
.diffline{font-family:ui-monospace,monospace;font-size:12px;white-space:pre-wrap;line-height:1.4}
.dl-add{color:#7ee787}.dl-del{color:#ffa198}.dl-hdr{color:var(--acc)}.dl-ctx{color:var(--mut)}
.reslabel{font-size:11px;color:var(--mut);margin:6px 0 2px}

/* git */
.gfiles{padding:4px 0;border-bottom:1px solid var(--line);margin-bottom:6px}
.gfile{font-family:ui-monospace,monospace;font-size:12px;padding:1px 0;color:var(--fg)}
.gfile .st{display:inline-block;width:22px;color:var(--tool);font-weight:700}
.empty{color:var(--mut);padding:20px;text-align:center;font-size:13px}

#mtabs{display:none;gap:4px;padding:6px;background:var(--bg2);border-bottom:1px solid var(--line)}
#mtabs .mt{flex:1;text-align:center;padding:7px;border-radius:6px;background:var(--bg3);color:var(--mut);
  cursor:pointer;font-size:13px}
#mtabs .mt.on{background:var(--acc);color:#04121f;font-weight:600}

@media(max-width:760px){
  main{flex-direction:column}
  #mtabs{display:flex}
  .pane{border-right:none;border-bottom:1px solid var(--line);display:none}
  .pane.show{display:flex;flex:1}
}
</style>
</head>
<body>
<header>
  <span class="brand">⬡ Agent Lens</span>
  <select id="sessions"></select>
  <span class="badge" id="srcBadge">—</span>
  <span class="badge" id="branchBadge" style="display:none"></span>
  <label class="toggle"><input type="checkbox" id="tgThink">thinking</label>
  <label class="toggle"><input type="checkbox" id="tgChips" checked>markers</label>
  <a class="badge" href="/console" style="text-decoration:none" title="interactive console">⌨ Console</a>
  <span class="dot" id="live" title="live tail"></span>
</header>

<div id="mtabs">
  <div class="mt on" data-p="0">💬 Discussion</div>
  <div class="mt" data-p="1">🔧 Activity</div>
  <div class="mt" data-p="2">±  Git</div>
</div>

<main>
  <section class="pane show" data-pane="0">
    <div class="ph">💬 Discussion <span class="grow"></span><span id="dCount" class="badge">0</span></div>
    <div class="scroll" id="discussion"></div>
  </section>
  <section class="pane" data-pane="1">
    <div class="ph">🔧 Activity <span class="grow"></span><span id="aCount" class="badge">0</span></div>
    <div class="scroll" id="activity"></div>
  </section>
  <section class="pane" data-pane="2">
    <div class="ph">± Git diff
      <span class="grow"></span>
      <label class="toggle"><input type="checkbox" id="tgAuto" checked>auto</label>
      <span class="tab" id="gRefresh">↻</span>
      <span id="gBranch" class="badge"></span>
    </div>
    <div class="scroll" id="git"></div>
  </section>
</main>

<script>
const $ = s => document.querySelector(s);
const elDisc = $('#discussion'), elAct = $('#activity'), elGit = $('#git');
let ws = null, curCwd = '', autoTimer = 0;
let nDisc = 0, nAct = 0;
const toolMap = {};   // toolId -> activity DOM node

/* ---------- tiny markdown ---------- */
function esc(s){return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function md(src){
  src = src || '';
  const blocks = [];
  src = src.replace(/```(\w*)\n?([\s\S]*?)```/g,(m,l,c)=>{
    blocks.push('<pre><code>'+esc(c.replace(/\n$/,''))+'</code></pre>');
    return '%%CB'+(blocks.length-1)+'%%';
  });
  let h = esc(src);
  h = h.replace(/`([^`\n]+)`/g,(m,c)=>'<code>'+c+'</code>');
  h = h.replace(/\*\*([^*\n]+)\*\*/g,'<b>$1</b>');
  h = h.replace(/^### (.*)$/gm,'<h3>$1</h3>').replace(/^## (.*)$/gm,'<h2>$1</h2>').replace(/^# (.*)$/gm,'<h2>$1</h2>');
  h = h.replace(/\[([^\]]+)\]\((https?:[^)\s]+)\)/g,'<a href="$2" target="_blank">$1</a>');
  h = h.replace(/(^|[^"=])(https?:\/\/[^\s<)]+)/g,'$1<a href="$2" target="_blank">$2</a>');
  h = h.replace(/^[\-\*] (.*)$/gm,'<li>$1</li>');
  h = h.replace(/(<li>[\s\S]*?<\/li>)/g,'<ul>$1</ul>');
  h = h.replace(/\n/g,'<br>');
  h = h.replace(/<br>(<(?:pre|h2|h3|ul)>)/g,'$1').replace(/(<\/(?:pre|h2|h3|ul)>)<br>/g,'$1');
  h = h.replace(/%%CB(\d+)%%/g,(m,i)=>blocks[+i]);
  return h;
}

/* ---------- discussion ---------- */
function nearBottom(el){return el.scrollHeight-el.scrollTop-el.clientHeight < 120;}
function addMsg(ev){
  const stick = nearBottom(elDisc);
  const d = document.createElement('div');
  d.className = 'msg '+(ev.role==='user'?'user':'assistant');
  d.innerHTML = '<div class="who">'+(ev.role==='user'?'You':'Assistant')+'</div><div class="bubble">'+md(ev.text)+'</div>';
  elDisc.appendChild(d);
  nDisc++; $('#dCount').textContent = nDisc;
  if(stick) elDisc.scrollTop = elDisc.scrollHeight;
}
function addThink(ev){
  const stick = nearBottom(elDisc);
  const d = document.createElement('div');
  d.className = 'think'+($('#tgThink').checked?'':' hide');
  d.dataset.role='think';
  d.textContent = ev.text;
  elDisc.appendChild(d);
  if(stick) elDisc.scrollTop = elDisc.scrollHeight;
}
function addChip(ev){
  const stick = nearBottom(elDisc);
  const c = document.createElement('div');
  c.className = 'toolchip'+($('#tgChips').checked?'':' hide');
  c.dataset.role='chip';
  c.textContent = '🔧 '+ev.tool+(primaryArg(ev)?' · '+primaryArg(ev):'');
  c.onclick = ()=>{ const n=toolMap[ev.toolId]; if(n){ switchPane(1); n.classList.add('open','flash'); n.scrollIntoView({block:'center'}); setTimeout(()=>n.classList.remove('flash'),1200);} };
  elDisc.appendChild(c);
  if(stick) elDisc.scrollTop = elDisc.scrollHeight;
}

/* ---------- activity ---------- */
const ICON = {Edit:'✏️',MultiEdit:'✏️',Write:'📝',Bash:'▶',Read:'📖',Glob:'🔍',Grep:'🔍',
  Task:'🤖',WebFetch:'🌐',WebSearch:'🌐',TodoWrite:'☑️',NotebookEdit:'📓',
  shell:'▶',apply_patch:'✏️',exec:'▶',update_plan:'☑️'};
function primaryArg(ev){
  const i = ev.input||{};
  if(typeof i==='string') return i.slice(0,80);
  if(i.file_path) return i.file_path.split('/').slice(-2).join('/');
  if(i.path) return (''+i.path).split('/').slice(-2).join('/');
  if(i.command) return (Array.isArray(i.command)?i.command.join(' '):i.command).slice(0,80);
  if(i.pattern) return i.pattern;
  if(i.description) return i.description.slice(0,80);
  return '';
}
function diffHtml(text){
  return text.split('\n').map(l=>{
    let c='dl-ctx';
    if(l.startsWith('@@')||l.startsWith('diff ')||l.startsWith('+++')||l.startsWith('---')) c='dl-hdr';
    else if(l.startsWith('+')) c='dl-add';
    else if(l.startsWith('-')) c='dl-del';
    return '<div class="diffline '+c+'">'+esc(l||' ')+'</div>';
  }).join('');
}
function toolBody(ev){
  const i = ev.input||{};
  const t = ev.tool;
  if((t==='Edit') && i.old_string!==undefined){
    return diffHtml(i.old_string.split('\n').map(x=>'-'+x).join('\n')+'\n'+
                    i.new_string.split('\n').map(x=>'+'+x).join('\n'));
  }
  if(t==='Write' && i.content!==undefined){
    return '<div class="reslabel">new content</div>'+diffHtml(i.content.split('\n').map(x=>'+'+x).join('\n'));
  }
  if(t==='Bash' && i.command){ return '<pre><code>'+esc(i.command)+'</code></pre>'; }
  if((t==='shell'||t==='exec') && i.command){
    return '<pre><code>'+esc(Array.isArray(i.command)?i.command.join(' '):i.command)+'</code></pre>';
  }
  if(typeof i==='string') return '<pre><code>'+esc(i)+'</code></pre>';
  return '<pre><code>'+esc(JSON.stringify(i,null,2))+'</code></pre>';
}
function addTool(ev){
  const stick = nearBottom(elAct);
  const a = document.createElement('div');
  a.className = 'act';
  a.innerHTML = '<div class="head"><span class="ico">'+(ICON[ev.tool]||'🔧')+'</span>'+
    '<span class="tn">'+esc(ev.tool)+'</span><span class="arg">'+esc(primaryArg(ev))+'</span></div>'+
    '<div class="body">'+toolBody(ev)+'<div class="res"></div></div>';
  a.querySelector('.head').onclick = ()=>a.classList.toggle('open');
  elAct.appendChild(a);
  if(ev.toolId) toolMap[ev.toolId] = a;
  nAct++; $('#aCount').textContent = nAct;
  if(stick) elAct.scrollTop = elAct.scrollHeight;
}
function addResult(ev){
  const a = toolMap[ev.toolId];
  if(!a){ return; }
  if(ev.isError) a.classList.add('err');
  const res = a.querySelector('.res');
  const body = (ev.content||'').trim();
  res.innerHTML = '<div class="reslabel">'+(ev.isError?'error ⤵':'output ⤵')+'</div><pre><code>'+
    esc(body.length>2500?body.slice(0,2500)+'\n…':body)+'</code></pre>';
}

function route(ev){
  if(ev.kind==='user_text'||ev.kind==='assistant_text') addMsg(ev);
  else if(ev.kind==='thinking') addThink(ev);
  else if(ev.kind==='tool_use'){ addTool(ev); addChip(ev); }
  else if(ev.kind==='tool_result') addResult(ev);
}

/* ---------- session wiring ---------- */
function reltime(ts){
  const s=(Date.now()/1000)-ts; if(s<60)return Math.round(s)+'s';
  if(s<3600)return Math.round(s/60)+'m'; if(s<86400)return Math.round(s/3600)+'h';
  return Math.round(s/86400)+'d';
}
async function loadSessions(keep){
  const r = await fetch('api/sessions'); const j = await r.json();
  const sel = $('#sessions'); const prev = sel.value;
  sel.innerHTML='';
  j.sessions.forEach(s=>{
    const o=document.createElement('option');
    o.value=s.id; o.dataset.source=s.source; o.dataset.cwd=s.cwd; o.dataset.branch=s.branch||'';
    const proj = (s.cwd||'').split('/').slice(-2).join('/') || '?';
    o.textContent = '['+s.source[0].toUpperCase()+'] '+proj+'  ·  '+reltime(s.mtime)+'  ·  '+(s.title||'');
    sel.appendChild(o);
  });
  if(keep && prev) sel.value=prev;
  if(!sel.value && sel.options.length) sel.selectedIndex=0;
  if(!keep) connect();
}
function connect(){
  const opt = $('#sessions').selectedOptions[0]; if(!opt) return;
  if(ws){ try{ws.close();}catch(e){} ws=null; }
  elDisc.innerHTML=''; elAct.innerHTML=''; for(const k in toolMap) delete toolMap[k];
  nDisc=nAct=0; $('#dCount').textContent=0; $('#aCount').textContent=0;
  curCwd = opt.dataset.cwd||'';
  const src = opt.dataset.source;
  $('#srcBadge').textContent = src; $('#srcBadge').className='badge '+src;
  const proto = location.protocol==='https:'?'wss:':'ws:';
  ws = new WebSocket(proto+'//'+location.host+'/ws/events?file='+encodeURIComponent(opt.value));
  ws.onopen = ()=>$('#live').classList.add('live');
  ws.onclose = ()=>$('#live').classList.remove('live');
  ws.onmessage = e=>{
    const m = JSON.parse(e.data);
    if(m.type==='init'){
      if(m.cwd){ curCwd=m.cwd; }
      const b=m.branch||opt.dataset.branch;
      $('#branchBadge').style.display = b?'':'none'; $('#branchBadge').textContent = b?('⎇ '+b):'';
      m.events.forEach(route);
      elDisc.scrollTop=elDisc.scrollHeight; elAct.scrollTop=elAct.scrollHeight;
      refreshGit();
    } else if(m.type==='append'){
      m.events.forEach(route);
      if(m.events.some(x=>x.kind==='tool_use'||x.kind==='tool_result')) refreshGit();
    }
  };
}

/* ---------- git ---------- */
async function refreshGit(){
  if(!curCwd){ elGit.innerHTML='<div class="empty">no working dir for this session</div>'; return; }
  try{
    const r = await fetch('api/diff?cwd='+encodeURIComponent(curCwd));
    const j = await r.json();
    if(!j.ok){ elGit.innerHTML='<div class="empty">'+esc(j.error||'no diff')+'<br><small>'+esc(curCwd)+'</small></div>'; return; }
    $('#gBranch').textContent = j.branch?('⎇ '+j.branch):'';
    const stick = nearBottom(elGit);
    let html='';
    if(j.files && j.files.length){
      html+='<div class="gfiles">'+j.files.map(f=>'<div class="gfile"><span class="st">'+esc(f.status)+'</span>'+esc(f.path)+'</div>').join('')+'</div>';
    }
    if(j.diff && j.diff.trim()){ html+=diffHtml(j.diff); }
    else if(!(j.files&&j.files.length)){ html='<div class="empty">working tree clean ✓<br><small>'+esc(curCwd)+'</small></div>'; }
    elGit.innerHTML=html;
    if(stick) elGit.scrollTop = elGit.scrollHeight;
  }catch(e){ /* keep last */ }
}
function startAuto(){ clearInterval(autoTimer); if($('#tgAuto').checked) autoTimer=setInterval(()=>{ if(document.querySelector('[data-pane="2"]').classList.contains('show')||window.innerWidth>760) refreshGit(); }, 2600); }

/* ---------- ui ---------- */
function switchPane(i){
  document.querySelectorAll('#mtabs .mt').forEach(t=>t.classList.toggle('on',t.dataset.p==i));
  document.querySelectorAll('.pane').forEach(p=>p.classList.toggle('show',p.dataset.pane==i));
  if(i==2) refreshGit();
}
document.querySelectorAll('#mtabs .mt').forEach(t=>t.onclick=()=>switchPane(t.dataset.p));
$('#tgThink').onchange=()=>document.querySelectorAll('[data-role=think]').forEach(e=>e.classList.toggle('hide',!$('#tgThink').checked));
$('#tgChips').onchange=()=>document.querySelectorAll('[data-role=chip]').forEach(e=>e.classList.toggle('hide',!$('#tgChips').checked));
$('#sessions').onchange=connect;
$('#gRefresh').onclick=refreshGit;
$('#tgAuto').onchange=startAuto;

loadSessions(false);
setInterval(()=>loadSessions(true), 15000);
startAuto();
</script>
</body>
</html>"""


CHAT_SESSIONS = {}  # id -> ChatSession (live, independent of any browser connection)


def safe_cwd(cwd):
    rp = os.path.realpath(cwd or "")
    if not (rp == os.path.realpath(HOME) or rp.startswith(os.path.realpath(HOME) + os.sep)):
        return None
    return rp if os.path.isdir(rp) else None


def _sanitize_mode(m):
    return m if m in ("acceptEdits", "plan", "default", "bypassPermissions") else "acceptEdits"


class ChatSession:
    """A persistent Claude Agent SDK client, independent of any browser connection.
    Survives navigation/reload (viewers attach/detach); ends only on explicit end().
    Provides per-action approval (can_use_tool) and interrupt()."""

    def __init__(self, sid, cwd, model, mode, resume_cc=None):
        self.id = sid
        self.cwd = cwd
        self.model = model
        self.mode = mode
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

    def preload(self):
        """Populate history from the on-disk transcript before resuming."""
        if self.resume_cc:
            self.log = load_transcript_events(self.resume_cc)

    def title(self):
        """First user message, as a short label to disambiguate sessions."""
        for e in self.log:
            if e.get("kind") == "user_text" and (e.get("text") or "").strip():
                return e["text"].strip().replace("\n", " ")[:60]
        return ""

    async def start(self):
        if not HAVE_SDK:
            raise RuntimeError("claude-agent-sdk not installed")
        opts = ClaudeAgentOptions(
            cwd=self.cwd, permission_mode=self.mode, can_use_tool=self._can_use_tool,
            add_dirs=[self.cwd], cli_path=CLAUDE_BIN)
        if self.model and self.model != "default":
            opts.model = self.model
        if self.resume_cc:
            opts.resume = self.resume_cc
        self.client = ClaudeSDKClient(options=opts)
        await self.client.connect()
        tornado.ioloop.IOLoop.current().spawn_callback(self._consume)

    async def _can_use_tool(self, tool_name, tool_input, context):
        """SDK permission hook: surface an Approve/Deny prompt and await the user."""
        self._aid += 1
        aid = "ap%d" % self._aid
        fut = asyncio.get_running_loop().create_future()
        self._pending[aid] = fut
        self._push([{"kind": "approval", "aid": aid, "tool": tool_name,
                     "input": _cap_input(tool_input),
                     "toolId": getattr(context, "tool_use_id", None)}])
        try:
            allow = await fut
        except Exception:
            allow = False
        self._push([{"kind": "approval_resolved", "aid": aid, "allow": bool(allow)}])
        return PermissionResultAllow() if allow else PermissionResultDeny(message="Denied by user")

    def resolve_approval(self, aid, allow):
        fut = self._pending.pop(aid, None)
        if fut and not fut.done():
            fut.set_result(bool(allow))

    async def _consume(self):
        try:
            async for msg in self.client.receive_messages():
                evs = self._normalize(msg)
                for e in evs:
                    if e["kind"] == "turn_done":
                        self.busy = False
                    elif e["kind"] == "ready":
                        self.cc_id = e.get("session_id") or self.cc_id
                if evs:
                    self._push(evs)
                # refresh context-window usage after a turn settles or on init
                if any(e["kind"] in ("turn_done", "ready") for e in evs):
                    await self._refresh_context()
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
                    "percentage": u.get("percentage"), "model": u.get("model")}
        self._emit({"type": "context", "ctx": self.ctx})

    def _normalize(self, msg):
        evs = []
        if isinstance(msg, SystemMessage):
            if msg.subtype == "init":
                d = msg.data or {}
                evs.append({"kind": "ready", "session_id": d.get("session_id"),
                            "model": d.get("model"), "cwd": d.get("cwd")})
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
        return evs

    def send_user(self, text):
        if not text.strip() or not self.client or self.ended:
            return
        self.busy = True
        # Echo the prompt into the shared log so every viewer (and a later
        # reattach) sees it — the SDK stream never replays the user's own text.
        self._push([{"kind": "user_text", "text": _cap(text)}])
        client = self.client
        async def _q():
            try:
                await client.query(text)
            except Exception as ex:
                self.busy = False
                self._push([{"kind": "notice", "text": "send failed: %r" % ex}])
        tornado.ioloop.IOLoop.current().spawn_callback(_q)

    def interrupt(self):
        if self.client and not self.ended:
            client = self.client
            async def _i():
                try:
                    await client.interrupt()
                except Exception:
                    pass
            tornado.ioloop.IOLoop.current().spawn_callback(_i)

    def _notice(self, text):
        """Transient note to current viewers (not persisted in the log)."""
        self._emit({"type": "events", "events": [{"kind": "notice", "text": text}]})

    def set_model(self, model):
        """Switch the model on the live session (SDK set_model). 'default'→None."""
        if not self.client or self.ended:
            return
        m = None if (not model or model == "default") else model
        self.model = model or "default"   # optimistic; reflected in list/attach
        client = self.client
        async def _s():
            try:
                await client.set_model(m)
                self._notice("⚙ model → %s (this session)" % self.model)
            except Exception as ex:
                self._notice("model change failed: %r" % ex)
        tornado.ioloop.IOLoop.current().spawn_callback(_s)

    def set_mode(self, mode):
        """Switch the permission mode on the live session (SDK set_permission_mode)."""
        if not self.client or self.ended:
            return
        mode = _sanitize_mode(mode)
        self.mode = mode
        client = self.client
        async def _s():
            try:
                await client.set_permission_mode(mode)
                self._notice("⚙ permission mode → %s (this session)" % mode)
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

    def _push(self, evs):
        self.log.extend(evs)
        if len(self.log) > 5000:
            self.log = self.log[-5000:]
        self._emit({"type": "events", "events": evs})

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

    def open(self):
        if AUTH and not self._ok_auth():
            self.close(4401, "Unauthorized")
            return
        self.session = None

    def _say(self, obj):
        try:
            self.write_message(json.dumps(obj))
        except Exception:
            pass

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
            sess = ChatSession(sid, cwd, msg.get("model") or "", _sanitize_mode(msg.get("mode")))
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
                       "model": sess.model or "default", "mode": sess.mode})
        elif mt == "attach":
            sess = CHAT_SESSIONS.get(msg.get("id"))
            if not sess:
                self._say({"type": "no_session", "id": msg.get("id")})
                return
            if self.session and self.session is not sess:
                self.session.detach(self)
            self.session = sess
            sess.attach(self)
            self._say({"type": "attached", "id": sess.id, "cwd": sess.cwd,
                       "name": os.path.basename(sess.cwd) or sess.cwd, "cc": sess.cc_id,
                       "title": sess.title(), "ctx": sess.ctx,
                       "model": sess.model or "default", "mode": sess.mode,
                       "busy": sess.busy, "ended": sess.ended, "events": sess.log})
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
                sess = ChatSession(secrets.token_hex(6), cwd, msg.get("model") or "",
                                   _sanitize_mode(msg.get("mode")), resume_cc=cc)
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
                       "resumed": True})
        elif mt == "approve" and self.session:
            self.session.resolve_approval(msg.get("aid"), bool(msg.get("allow")))
        elif mt == "interrupt" and self.session:
            self.session.interrupt()
        elif mt == "set_model" and self.session:
            self.session.set_model(msg.get("model") or "")
        elif mt == "set_mode" and self.session:
            self.session.set_mode(msg.get("mode") or "")
        elif mt == "user" and self.session:
            self.session.send_user(msg.get("text", ""))
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
                {"id": s.id, "cwd": s.cwd, "name": os.path.basename(s.cwd) or s.cwd,
                 "cc": s.cc_id, "model": s.model or "default", "title": s.title(),
                 "busy": s.busy, "ended": s.ended, "n": len(s.log)}
                for s in CHAT_SESSIONS.values()]})

    def on_close(self):
        if self.session:
            self.session.detach(self)   # keep the claude process alive


class ConsoleHandler(AuthMixin, tornado.web.RequestHandler):
    def get(self):
        if not self._ok_auth():
            return
        self.set_header("Cache-Control", "no-store")
        self.write(CONSOLE_HTML)


CONSOLE_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,interactive-widget=resizes-content">
<title>Agent Lens · Console</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#1e1e1e;--bg2:#252526;--bg3:#2d2d2d;--line:#3c3c3c;--fg:#d4d4d4;--mut:#858585;
  --acc:#4fc1ff;--usr:#3794ff;--add:#2ea043;--del:#f85149;--tool:#e0c080;--think:#7a7a7a}
html,body{height:100%;background:var(--bg);color:var(--fg);overflow:hidden;
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif;font-size:14px}
body{display:flex;flex-direction:column}
header{display:flex;gap:6px;align-items:center;padding:6px 10px;background:var(--bg2);
  border-bottom:1px solid var(--line);flex-shrink:0;flex-wrap:wrap}
header .brand{font-weight:600;color:var(--acc);white-space:nowrap}
header select,header input{background:var(--bg3);color:var(--fg);border:1px solid var(--line);
  border-radius:5px;padding:4px 7px;font-size:12.5px}
header select#project{flex:1;min-width:120px;max-width:380px}
header input#cwd{flex:1;min-width:120px;display:none}
.navlink{color:var(--mut);text-decoration:none;font-size:12.5px;padding:3px 7px;border:1px solid var(--line);border-radius:5px}
.navlink:hover{color:var(--fg)}
.status{font-size:11.5px;color:var(--mut);white-space:nowrap;margin-left:auto;display:flex;gap:6px;align-items:center}
.dot{width:8px;height:8px;border-radius:50%;background:#555}
.dot.on{background:var(--add);box-shadow:0 0 6px var(--add)}
.dot.busy{background:var(--tool);box-shadow:0 0 6px var(--tool);animation:pulse 1s infinite}
@keyframes pulse{50%{opacity:.35}}
.btn{background:var(--bg3);color:var(--fg);border:1px solid var(--line);border-radius:5px;
  padding:4px 9px;font-size:12.5px;cursor:pointer;white-space:nowrap}
.btn:hover{background:#383838}

#chat{flex:1;overflow-y:auto;overflow-x:hidden;padding:14px;-webkit-overflow-scrolling:touch}
.wrap{max-width:820px;margin:0 auto}
.msg{margin-bottom:14px;line-height:1.5;word-wrap:break-word;overflow-wrap:anywhere}
.msg.user{display:flex;justify-content:flex-end}
.msg.user .b{background:#10243e;border:1px solid #1f4a7a;border-radius:10px;padding:8px 12px;max-width:85%;white-space:pre-wrap}
.msg.asst .b{color:#cdcdcd}
.think{color:var(--think);font-style:italic;font-size:13px;border-left:2px solid #3a3a3a;padding:3px 0 3px 10px;margin-bottom:12px;white-space:pre-wrap}
.think.hide{display:none}
.notice{color:var(--mut);font-size:11.5px;margin:6px 0}
.errline{color:var(--del);font-size:12px;font-family:ui-monospace,monospace;margin:4px 0;white-space:pre-wrap}

/* collapsed change/tool cards */
.tool{border:1px solid var(--line);border-radius:8px;margin:6px 0 12px;background:var(--bg2);overflow:hidden}
.tool .th{padding:7px 10px;cursor:pointer;display:flex;gap:8px;align-items:center;user-select:none}
.tool .th:hover{background:var(--bg3)}
.tool .ico{flex-shrink:0}
.tool .tn{color:var(--tool);font-weight:600;font-family:ui-monospace,monospace;font-size:12.5px;flex-shrink:0}
.tool .tp{color:var(--mut);font-family:ui-monospace,monospace;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1}
.tool .cnt{font-size:11.5px;font-family:ui-monospace,monospace;flex-shrink:0}
.tool .cnt .a{color:#7ee787}.tool .cnt .d{color:#ffa198}
.tool .chev{color:var(--mut);flex-shrink:0;transition:transform .15s}
.tool.open .chev{transform:rotate(90deg)}
.tool .tb{display:none;border-top:1px solid var(--line);padding:8px 10px}
.tool.open .tb{display:block}
.tool.err .tn{color:var(--del)}
pre{background:#161616;border:1px solid var(--line);border-radius:6px;padding:8px;overflow-x:auto;margin:5px 0;
  font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12.5px;line-height:1.45}
code{font-family:ui-monospace,Menlo,monospace;font-size:12.5px;background:#161616;border:1px solid var(--line);border-radius:3px;padding:0 4px}
pre code{background:none;border:none;padding:0}
.bubble h1,.bubble h2,.bubble h3{font-size:14px;margin:8px 0 4px}
.msg.asst ul,.msg.asst ol{margin:4px 0 4px 20px}
.msg.asst a{color:var(--acc)}
.diffline{font-family:ui-monospace,monospace;font-size:12px;white-space:pre-wrap;line-height:1.4}
.dl-add{color:#7ee787}.dl-del{color:#ffa198}.dl-hdr{color:var(--acc)}.dl-ctx{color:var(--mut)}
.reslabel{font-size:11px;color:var(--mut);margin:6px 0 2px}

#composer{flex-shrink:0;border-top:1px solid var(--line);background:var(--bg2);padding:8px 10px;
  padding-bottom:calc(8px + env(safe-area-inset-bottom));display:flex;gap:8px;align-items:flex-end}
#composer .wrap2{max-width:820px;margin:0 auto;width:100%;display:flex;gap:8px;align-items:flex-end}
#ta{flex:1;background:var(--bg3);color:var(--fg);border:1px solid var(--line);border-radius:10px;
  padding:9px 12px;font-size:14px;font-family:inherit;resize:none;max-height:160px;line-height:1.4}
#ta:focus{outline:1px solid var(--acc)}
#send{background:var(--acc);color:#04121f;border:none;border-radius:10px;padding:9px 14px;font-size:16px;font-weight:700;cursor:pointer}
#send:disabled{background:#3a3a3a;color:#777;cursor:default}

#drawer{position:fixed;top:0;right:0;width:min(560px,92vw);height:100%;background:var(--bg);border-left:1px solid var(--line);
  transform:translateX(100%);transition:transform .2s;z-index:20;display:flex;flex-direction:column}
#drawer.open{transform:none}
#drawer .dh{padding:8px 12px;background:var(--bg2);border-bottom:1px solid var(--line);display:flex;gap:8px;align-items:center}
#drawer .dh .grow{flex:1}
#drawer .dc{flex:1;overflow:auto;padding:10px}
.gfile{font-family:ui-monospace,monospace;font-size:12px;padding:1px 0}.gfile .st{display:inline-block;width:24px;color:var(--tool);font-weight:700}
.empty{color:var(--mut);padding:18px;text-align:center}
/* edits-out-of-chat */
#chBadge{font-size:10px;padding:0 5px;border-radius:8px;background:#444;color:#bbb;margin-left:3px}
.dh .tab{cursor:pointer;padding:3px 9px;border-radius:5px;color:var(--mut);font-size:12.5px;user-select:none}
.dh .tab.on{background:var(--bg3);color:var(--fg)}
.dh .tab span{font-size:10px;opacity:.8}
.emark{font-size:12px;color:var(--tool);background:#2a2519;border:1px solid #4a3f28;border-radius:6px;
  padding:3px 9px;margin:2px 0 12px;display:inline-flex;gap:7px;cursor:pointer;font-family:ui-monospace,monospace;align-items:center}
.emark:hover{filter:brightness(1.25)}
.emark .a{color:#7ee787}.emark .d{color:#ffa198}.emark .mut{color:var(--mut)}
.ecard{border:1px solid var(--line);border-radius:8px;margin-bottom:10px;background:var(--bg2);overflow:hidden}
.ecard .eh{padding:7px 9px;display:flex;gap:7px;align-items:center;background:#2a2519;border-bottom:1px solid var(--line)}
.ecard .ef{color:var(--tool);font-family:ui-monospace,monospace;font-size:12px;font-weight:600;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ecard .cnt{font-size:11px;font-family:ui-monospace,monospace}.ecard .cnt .a{color:#7ee787}.ecard .cnt .d{color:#ffa198}
.ecard.flash{outline:2px solid var(--acc);outline-offset:-2px}
.ecard .ed{max-height:320px;overflow:auto;padding:6px 9px}
.ecard .res{padding:0 9px}
/* approval prompts */
.approval{border:1px solid #8a7430;border-radius:8px;margin:6px 0 14px;background:#2a2519;overflow:hidden}
.approval .ah{padding:8px 10px;color:var(--tool);font-weight:600;display:flex;gap:7px;align-items:center}
.approval .ah .tp{color:var(--mut);font-family:ui-monospace,monospace;font-weight:400;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.approval .abody{max-height:240px;overflow:auto;padding:4px 10px;border-top:1px solid #4a3f28}
.approval .abtns{display:flex;gap:8px;padding:8px 10px;border-top:1px solid #4a3f28}
.approval .abtns button{flex:1;padding:9px;border-radius:6px;cursor:pointer;font-size:14px;font-weight:700}
.approval .appr{background:#143524;color:#7ee787;border:1px solid #2ea043}
.approval .deny{background:#3a1b1b;color:#ffa198;border:1px solid #f85149}
.approval.done .abtns{opacity:.85}
.approval .ok{color:#7ee787;font-weight:700}.approval .no{color:#ffa198;font-weight:700}
#stop{background:#5a2020;color:#ffb3b3;border:1px solid #f85149;border-radius:10px;padding:9px 14px;font-size:16px;font-weight:700;cursor:pointer}

/* sessions sidebar + shell layout */
.iconbtn{background:none;border:none;color:var(--fg);font-size:17px;cursor:pointer;padding:2px 5px;line-height:1}
.curname{font-size:13px;color:var(--mut);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1;min-width:0;max-width:46vw}
.ctx{display:none;align-items:center;gap:5px;font-size:11px;color:var(--mut);white-space:nowrap;font-family:ui-monospace,monospace}
.ctx .bar{width:52px;height:6px;border-radius:3px;background:var(--bg3);border:1px solid var(--line);overflow:hidden}
.ctx .fill{display:block;height:100%;width:0;background:var(--add);transition:width .3s,background .3s}
.ctx.warn .fill{background:var(--tool)}
.ctx.hot .fill{background:var(--del)}
#shell{flex:1;display:flex;min-height:0;position:relative}
#mainCol{flex:1;display:flex;flex-direction:column;min-width:0}
#sidebar{width:250px;flex-shrink:0;background:var(--bg2);border-right:1px solid var(--line);display:flex;flex-direction:column;overflow-y:auto}
#sidebar.collapsed{display:none}
.sb-new{padding:8px;border-bottom:1px solid var(--line);display:flex;flex-direction:column;gap:6px}
.sb-new select,.sb-new input{width:100%;background:var(--bg3);color:var(--fg);border:1px solid var(--line);border-radius:5px;padding:5px 7px;font-size:12.5px}
.cwdwrap{position:relative;display:none}
.cwdwrap.show{display:block}
#cwdac{position:absolute;left:0;right:0;top:100%;z-index:50;background:var(--bg3);border:1px solid var(--acc);border-top:none;border-radius:0 0 6px 6px;max-height:240px;overflow:auto;display:none;box-shadow:0 8px 18px rgba(0,0,0,.5)}
#cwdac.on{display:block}
.acitem{padding:5px 8px;cursor:pointer;border-bottom:1px solid var(--line)}
.acitem:last-child{border-bottom:none}
.acitem:hover,.acitem.sel{background:var(--acc)}
.acname{font-size:12px;font-family:ui-monospace,monospace;color:var(--fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.acpath{font-size:10px;font-family:ui-monospace,monospace;color:var(--mut);overflow-wrap:anywhere;line-height:1.3}
.acitem:hover .acname,.acitem.sel .acname,.acitem:hover .acpath,.acitem.sel .acpath{color:#04121f}
.acmore{padding:5px 8px;font-size:10px;color:var(--mut);font-style:italic}
.sb-row2{display:flex;gap:6px}.sb-row2 select{flex:1;min-width:0}
.newbtn{background:var(--acc);color:#04121f;font-weight:700;border:none;border-radius:6px;padding:7px;font-size:13px;cursor:pointer}
.newbtn:hover{filter:brightness(1.08)}
.sb-sec{border-bottom:1px solid var(--line);padding:4px 0 6px}
.sb-h{font-size:11px;letter-spacing:.05em;text-transform:uppercase;color:var(--mut);padding:6px 10px 4px;display:flex;align-items:center;gap:6px}
.sb-h .cnt{background:var(--bg3);border-radius:8px;padding:0 6px;font-size:10px;color:var(--mut)}
.sb-h .grow{flex:1}
.sb-ref{cursor:pointer}.sb-ref:hover{color:var(--fg)}
.sb-empty{color:var(--mut);font-size:12px;padding:5px 10px;line-height:1.4}
.srow{padding:7px 9px;cursor:pointer;display:flex;gap:8px;align-items:center;border-left:2px solid transparent}
.srow:hover{background:var(--bg3)}
.srow.active{background:#10243e;border-left-color:var(--acc)}
.srow.ended{opacity:.6}
.srow .sdot{width:7px;height:7px;border-radius:50%;background:#666;flex-shrink:0}
.srow .sdot.on{background:var(--add)}
.srow .sdot.busy{background:var(--tool);box-shadow:0 0 5px var(--tool);animation:pulse 1s infinite}
.srow .smeta{flex:1;min-width:0}
.srow .sname{font-size:12.5px;color:var(--fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.srow .ssub{font-size:11px;color:var(--mut);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.srow .sx{color:var(--mut);flex-shrink:0;font-size:13px;padding:0 3px;opacity:0}
.srow:hover .sx{opacity:.6}.srow .sx:hover{opacity:1;color:var(--del)}
.srow .star{flex-shrink:0;font-size:13px;padding:0 3px;color:var(--mut);cursor:pointer}
.srow .star.on{color:var(--tool)}
.srow .star:hover{color:var(--tool);filter:brightness(1.2)}
.fscope{font-size:10px;color:var(--mut);font-family:ui-monospace,monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:130px}
#sb-backdrop{display:none}
@media(max-width:860px){
  #sidebar{position:fixed;left:0;top:0;bottom:0;z-index:40;transform:translateX(-100%);transition:transform .2s;width:min(310px,86vw);box-shadow:2px 0 14px rgba(0,0,0,.5)}
  #sidebar.open{transform:none}
  #sidebar.collapsed{display:flex}
  #sb-backdrop.show{display:block;position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:35}
}
</style>
</head>
<body>
<header>
  <button class="iconbtn" id="navtoggle" title="sessions">☰</button>
  <span class="brand">⬡ Console</span>
  <span class="curname" id="curname">— no session —</span>
  <span class="ctx" id="ctx" title="context-window usage"></span>
  <button class="btn" id="chgbtn">± Changes<span id="chBadge">0</span></button>
  <a class="navlink" href="/">Observer</a>
  <span class="status"><span class="dot" id="dot"></span><span id="statxt">idle</span></span>
</header>

<div id="shell">
  <aside id="sidebar">
    <div class="sb-new">
      <select id="project" title="working directory for a new session"></select>
      <div class="cwdwrap" id="cwdwrap"><input id="cwd" placeholder="type a path…  ↑↓ to pick" autocomplete="off"><div id="cwdac"></div></div>
      <div class="sb-row2">
        <select id="model" title="model"><option value="default">model: default</option><option>opus</option><option>sonnet</option><option>haiku</option></select>
        <select id="mode" title="permission mode"><option value="acceptEdits">⚡ Auto-accept</option><option value="default">🔐 Approve</option><option value="plan">📋 Plan</option><option value="bypassPermissions">⏩ Full auto</option></select>
      </div>
      <button class="newbtn" id="newbtn">＋ New session</button>
    </div>
    <div class="sb-sec">
      <div class="sb-h">Live <span id="liveN" class="cnt">0</span></div>
      <div id="liveList"><div class="sb-empty">none running</div></div>
    </div>
    <div class="sb-sec">
      <div class="sb-h">★ Favorites <span id="favN" class="cnt">0</span></div>
      <div id="favList"><div class="sb-empty">star a session to pin it here</div></div>
    </div>
    <div class="sb-sec">
      <div class="sb-h">🕘 Recent <span class="grow"></span><span class="sb-ref" id="resumeRef" title="refresh">↻</span></div>
      <div id="recentList"><div class="sb-empty">—</div></div>
    </div>
    <div class="sb-sec">
      <div class="sb-h">📁 In folder <span class="grow"></span><span id="folderScope" class="fscope"></span></div>
      <div id="folderList"><div class="sb-empty">—</div></div>
    </div>
  </aside>
  <div id="sb-backdrop"></div>
  <div id="mainCol">
    <div id="chat"><div class="wrap" id="stream"></div></div>
    <div id="composer"><div class="wrap2">
      <textarea id="ta" rows="1" placeholder="Type a message…  (Enter to send · Shift+Enter newline)" disabled></textarea>
      <button id="stop" title="interrupt / stop" style="display:none">⏹</button>
      <button id="send" disabled>➤</button>
    </div></div>
  </div>
</div>

<div id="drawer">
  <div class="dh"><span class="tab on" id="tabEdits">Edits <span id="editN">0</span></span><span class="tab" id="tabGit">Git diff</span><span class="grow"></span><span class="btn" id="grefresh">↻</span><span class="btn" id="dclose">✕</span></div>
  <div class="dc" id="edits"><div class="empty">no file changes yet</div></div>
  <div class="dc" id="gitc" style="display:none"><div class="empty">—</div></div>
</div>

<script>
const $=s=>document.querySelector(s);
const stream=$('#stream'), ta=$('#ta'), sendBtn=$('#send');
let ws=null, running=false, ready=false, cwd='', tools={};
let sid=null, editCount=0, pendingStart=false, reconnectT=0;
let showThink=false;
let liveCCs=new Set(), recentData=[], folderData=[], HOMEDIR='';
const EDIT_TOOLS=new Set(['Edit','MultiEdit','Write','NotebookEdit']);
const SKEY='al_session';

function esc(s){return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function md(src){
  src=src||''; const bl=[];
  src=src.replace(/```(\w*)\n?([\s\S]*?)```/g,(m,l,c)=>{bl.push('<pre><code>'+esc(c.replace(/\n$/,''))+'</code></pre>');return ' %%CB'+(bl.length-1)+'%% ';});
  let h=esc(src);
  h=h.replace(/`([^`\n]+)`/g,(m,c)=>'<code>'+c+'</code>');
  h=h.replace(/\*\*([^*\n]+)\*\*/g,'<b>$1</b>');
  h=h.replace(/^### (.*)$/gm,'<h3>$1</h3>').replace(/^## (.*)$/gm,'<h2>$1</h2>').replace(/^# (.*)$/gm,'<h2>$1</h2>');
  h=h.replace(/\[([^\]]+)\]\((https?:[^)\s]+)\)/g,'<a href="$2" target="_blank">$1</a>');
  h=h.replace(/^[\-\*] (.*)$/gm,'<li>$1</li>').replace(/(<li>[\s\S]*?<\/li>)/g,'<ul>$1</ul>');
  h=h.replace(/\n/g,'<br>').replace(/<br>(<(?:pre|h2|h3|ul)>)/g,'$1').replace(/(<\/(?:pre|h2|h3|ul)>)<br>/g,'$1');
  h=h.replace(/%%CB(\d+)%%/g,(m,i)=>bl[+i]);
  return h;
}
function diffHtml(t){return t.split('\n').map(l=>{let c='dl-ctx';
  if(l.startsWith('@@')||l.startsWith('diff ')||l.startsWith('+++')||l.startsWith('---'))c='dl-hdr';
  else if(l.startsWith('+'))c='dl-add';else if(l.startsWith('-'))c='dl-del';
  return '<div class="diffline '+c+'">'+esc(l||' ')+'</div>';}).join('');}
function atBottom(){const c=$('#chat');return c.scrollHeight-c.scrollTop-c.clientHeight<140;}
function scroll(){const c=$('#chat');c.scrollTop=c.scrollHeight;}

const ICON={Edit:'✏️',MultiEdit:'✏️',Write:'📝',Bash:'▶',Read:'📖',Glob:'🔍',Grep:'🔍',Task:'🤖',
  WebFetch:'🌐',WebSearch:'🌐',TodoWrite:'☑️',NotebookEdit:'📓'};
function primaryArg(i){if(!i)return '';if(typeof i==='string')return i.slice(0,80);
  if(i.file_path)return i.file_path.split('/').slice(-2).join('/');
  if(i.command)return (''+i.command).split('\n')[0].slice(0,90);
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

function addUser(text){const s=atBottom();const d=document.createElement('div');d.className='msg user';
  d.innerHTML='<div class="b">'+esc(text)+'</div>';stream.appendChild(d);scroll();}
function addAsst(text){const s=atBottom();const d=document.createElement('div');d.className='msg asst';
  d.innerHTML='<div class="b bubble">'+md(text)+'</div>';stream.appendChild(d);if(s)scroll();}
function addThink(text){const s=atBottom();const d=document.createElement('div');d.className='think'+(showThink?'':' hide');d.dataset.t=1;
  d.textContent=text;stream.appendChild(d);if(s)scroll();}
function addNotice(t){const d=document.createElement('div');d.className='notice';d.textContent=t;stream.appendChild(d);}
function addErr(t){const d=document.createElement('div');d.className='errline';d.textContent=t;stream.appendChild(d);if(atBottom())scroll();}
function addTool(ev){const s=atBottom();const c=document.createElement('div');c.className='tool';
  const cn=counts(ev);const cnt=cn?('<span class="a">+'+cn.a+'</span> <span class="d">−'+cn.d+'</span>'):'';
  c.innerHTML='<div class="th"><span class="ico">'+(ICON[ev.tool]||'🔧')+'</span><span class="tn">'+esc(ev.tool)+'</span>'+
    '<span class="tp">'+esc(primaryArg(ev.input))+'</span><span class="cnt">'+cnt+'</span><span class="chev">▸</span></div>'+
    '<div class="tb">'+toolBody(ev)+'<div class="res"></div></div>';
  c.querySelector('.th').onclick=()=>c.classList.toggle('open');
  stream.appendChild(c);if(ev.toolId)tools[ev.toolId]=c;if(s)scroll();}
function addResult(ev){const c=tools[ev.toolId];if(!c)return;if(ev.isError)c.classList.add('err');
  const b=(ev.content||'').trim();c.querySelector('.res').innerHTML='<div class="reslabel">'+(ev.isError?'error ⤵':'output ⤵')+
    '</div><pre><code>'+esc(b.length>2200?b.slice(0,2200)+'\n…':b)+'</code></pre>';}

function statset(t){$('#statxt').textContent=t;}
function bindProject(p){if(!p)return;const sel=$('#project');let ok=false;
  for(const o of sel.options){if(o.value===p){ok=true;break;}}
  if(!ok){const o=document.createElement('option');o.value=p;o.textContent='● '+p.split('/').slice(-2).join('/');sel.insertBefore(o,sel.firstChild);}
  sel.value=p;}
function setBusy(b){running=b;$('#dot').className='dot '+(b?'busy':(ready?'on':''));
  if(b)statset('working…');else if(ready)statset('ready');
  sendBtn.disabled=b||!ready;ta.disabled=!ready;
  $('#stop').style.display=b?'':'none';sendBtn.style.display=b?'none':'';}
function clearUI(){stream.innerHTML='';$('#edits').innerHTML='<div class="empty">no file changes yet</div>';
  $('#gitc').innerHTML='<div class="empty">—</div>';tools={};editCount=0;updateEditBadge();renderCtx(null);ready=false;}

/* file edits → out of chat, into the Changes drawer */
function updateEditBadge(){$('#editN').textContent=editCount;const b=$('#chBadge');b.textContent=editCount;
  b.style.background=editCount?'#d4a017':'#444';b.style.color=editCount?'#000':'#bbb';}
function addEditCard(ev){if(editCount===0)$('#edits').innerHTML='';
  const c=document.createElement('div');c.className='ecard';const cn=counts(ev);
  const cnt=cn?('<span class="cnt"><span class="a">+'+cn.a+'</span> <span class="d">−'+cn.d+'</span></span>'):'';
  c.innerHTML='<div class="eh"><span>'+(ICON[ev.tool]||'✏️')+'</span><span class="ef">'+esc(primaryArg(ev.input)||ev.tool)+'</span>'+cnt+'</div>'+
    '<div class="ed">'+toolBody(ev)+'</div><div class="res"></div>';
  $('#edits').appendChild(c);if(ev.toolId)tools[ev.toolId]=c;editCount++;updateEditBadge();
  const ed=$('#edits');ed.scrollTop=ed.scrollHeight;}
function addMarker(ev){const s=atBottom();const cn=counts(ev);const m=document.createElement('div');m.className='emark';
  m.innerHTML=(ICON[ev.tool]||'✏️')+'<span>'+esc(primaryArg(ev.input)||ev.tool)+'</span>'+
    (cn?'<span><span class="a">+'+cn.a+'</span> <span class="d">−'+cn.d+'</span></span>':'')+'<span class="mut">— see Changes</span>';
  m.onclick=()=>{openDrawer('edits');const c=ev.toolId&&tools[ev.toolId];if(c){c.classList.add('flash');c.scrollIntoView({block:'center'});setTimeout(()=>c.classList.remove('flash'),1200);}};
  stream.appendChild(m);if(s)scroll();}

function addApproval(ev){const c=document.createElement('div');c.className='approval';c.dataset.aid=ev.aid;
  c.innerHTML='<div class="ah">🔐 Approve <b>'+esc(ev.tool)+'</b> <span class="tp">'+esc(primaryArg(ev.input)||'')+'</span></div>'+
    '<div class="abody">'+toolBody(ev)+'</div>'+
    '<div class="abtns"><button class="appr">✓ Approve</button><button class="deny">✕ Deny</button></div>';
  c.querySelector('.appr').onclick=()=>decide(ev.aid,true);
  c.querySelector('.deny').onclick=()=>decide(ev.aid,false);
  stream.appendChild(c);scroll();}
function decide(aid,allow){wsSend({type:'approve',aid:aid,allow:allow});resolveApprovalCard(aid,allow);}
function resolveApprovalCard(aid,allow){const c=stream.querySelector('.approval[data-aid="'+aid+'"]');
  if(c&&!c.classList.contains('done')){c.classList.add('done');
    const bt=c.querySelector('.abtns');if(bt)bt.innerHTML='<span class="'+(allow?'ok':'no')+'">'+(allow?'✓ Approved':'✕ Denied')+'</span>';}}
function route(ev){
  if(ev.kind==='user_text')addUser(ev.text);
  else if(ev.kind==='ready'){ready=true;cwd=ev.cwd||cwd;addNotice('● session ready · '+(ev.model||'')+' · '+(ev.cwd||''));}
  else if(ev.kind==='assistant_text')addAsst(ev.text);
  else if(ev.kind==='thinking')addThink(ev.text);
  else if(ev.kind==='tool_use'){if(EDIT_TOOLS.has(ev.tool)){addEditCard(ev);addMarker(ev);}else addTool(ev);}
  else if(ev.kind==='tool_result')addResult(ev);
  else if(ev.kind==='approval')addApproval(ev);
  else if(ev.kind==='approval_resolved')resolveApprovalCard(ev.aid,ev.allow);
  else if(ev.kind==='turn_done'){setBusy(false);if(drawerOpen()&&gitTab())refreshGit();}
  else if(ev.kind==='notice')addNotice(ev.text);
}

/* persistent server-side session: attach / reattach / switch */
function markEnded(msg){ready=false;ta.disabled=true;sendBtn.disabled=true;$('#dot').className='dot';statset('ended');
  localStorage.removeItem(SKEY);if(msg)addNotice(msg);}
function onMsg(e){const m=JSON.parse(e.data);
  if(m.type==='started'){pendingStart=false;sid=m.id;cwd=m.cwd;bindProject(m.cwd);localStorage.setItem(SKEY,sid);
    ready=true;setBusy(false);ta.focus();setCurname(m.name||'session');syncPickers(m.model,m.mode);renderCtx(null);statset('ready');
    addNotice('new session « '+(m.name||'')+' » in '+m.cwd+' — type your first message to begin');reqList();loadPast();}
  else if(m.type==='attached'){clearUI();pendingStart=false;sid=m.id;localStorage.setItem(SKEY,sid);cwd=m.cwd;bindProject(m.cwd);
    ready=!m.ended;setCurname((m.title||m.name||'session')+(m.ended?' · ended':''));syncPickers(m.model,m.mode);renderCtx(m.ctx);statset(m.ended?'ended':'ready');
    m.events.forEach(route);setBusy(!!m.busy);
    if(m.ended){markEnded('— this session has ended (history shown · you can resume it from disk) —');}
    else{ta.disabled=false;sendBtn.disabled=!!m.busy;addNotice('— '+(m.resumed?'resumed':'reattached to')+' « '+(m.name||'')+' » ('+m.events.length+' events) —');}
    reqList();loadPast();}
  else if(m.type==='no_session'){localStorage.removeItem(SKEY);sid=null;ready=false;setBusy(false);setCurname('');renderCtx(null);statset('idle');
    addNotice('that session is no longer running — pick it under “Resume from disk”, or ＋ New.');reqList();loadPast();}
  else if(m.type==='events')m.events.forEach(route);
  else if(m.type==='stderr')addErr(m.text);
  else if(m.type==='error'){pendingStart=false;addErr('⚠ '+m.error);}
  else if(m.type==='exit'){if(!pendingStart){markEnded('session process exited (code '+m.code+')');setCurname('');}reqList();loadPast();}
  else if(m.type==='ended'){if(m.id&&m.id===sid){sid=null;setCurname('');markEnded('session ended');}reqList();loadPast();}
  else if(m.type==='sessions')renderLive(m.sessions);
  else if(m.type==='context')renderCtx(m.ctx);
}
function openWs(cb){const proto=location.protocol==='https:'?'wss:':'ws:';
  ws=new WebSocket(proto+'//'+location.host+'/ws/chat');
  ws.onopen=()=>{clearTimeout(reconnectT);$('#dot').className='dot '+(ready?'on':'');if(cb)cb();reqList();
    const saved=localStorage.getItem(SKEY);if(saved&&!sid&&!pendingStart){statset('reattaching…');ws.send(JSON.stringify({type:'attach',id:saved}));}};
  ws.onclose=()=>{$('#dot').className='dot';statset('disconnected');ta.disabled=true;sendBtn.disabled=true;
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
function renderCtx(c){const el=$('#ctx');
  if(!c||c.percentage==null){el.style.display='none';return;}
  const pct=Math.round(c.percentage);
  el.className='ctx'+(pct>=85?' hot':(pct>=65?' warn':''));
  el.style.display='inline-flex';
  el.innerHTML='<span class="bar"><span class="fill" style="width:'+Math.min(100,pct)+'%"></span></span>'+
    '<span>'+pct+'% · '+fmtTok(c.totalTokens)+'/'+fmtTok(c.maxTokens)+'</span>';
  el.title='context '+(c.totalTokens||'?')+' / '+(c.maxTokens||'?')+' tokens ('+pct+'%)'+(c.model?' · '+c.model:'');}
/* reflect the active session's real model/mode in the pickers (programmatic
   .value set does NOT fire onchange, so this won't echo back to the server) */
function syncPickers(model,mode){
  if(model){const o=$('#model');for(const x of o.options){if(x.value===model){o.value=model;break;}}}
  if(mode){const o=$('#mode');for(const x of o.options){if(x.value===mode){o.value=mode;break;}}}
}

/* sidebar: live sessions (in-RAM) + resume-from-disk (past transcripts) */
function renderLive(list){const box=$('#liveList');
  liveCCs=new Set(list.map(s=>s.cc).filter(Boolean));
  $('#liveN').textContent=list.length;
  if(!list.length){box.innerHTML='<div class="sb-empty">none running — pick a project, ＋ New</div>';}
  else{box.innerHTML='';list.forEach(s=>{
    const r=document.createElement('div');
    r.className='srow'+(s.id===sid?' active':'')+(s.ended?' ended':'');
    const proj=(s.cwd||'').split('/').slice(-2).join('/');
    const dot=s.busy?'busy':(s.ended?'':'on');
    r.innerHTML='<span class="sdot '+dot+'"></span><div class="smeta">'+
      '<div class="sname">'+esc(s.title||s.name||'new session')+(s.ended?' · ended':'')+'</div>'+
      '<div class="ssub">'+esc(proj)+' · '+s.n+(s.busy?' · working…':'')+'</div></div>'+
      '<span class="sx" title="end session">✕</span>';
    r.querySelector('.smeta').onclick=()=>switchSession(s.id);
    r.querySelector('.sdot').onclick=()=>switchSession(s.id);
    r.querySelector('.sx').onclick=ev=>{ev.stopPropagation();endSessionById(s.id,s.name);};
    box.appendChild(r);});}
  renderPast();
}

/* favorites: starred sessions, persisted in localStorage by claude session id */
const FKEY='al_favs';
function getFavs(){try{return JSON.parse(localStorage.getItem(FKEY)||'[]');}catch(e){return [];}}
function isFav(cc){return getFavs().some(f=>f.cc===cc);}
function toggleFav(s){let a=getFavs();
  if(a.some(f=>f.cc===s.cc))a=a.filter(f=>f.cc!==s.cc);
  else a.unshift({cc:s.cc,cwd:s.cwd,name:s.name||'',title:s.title||''});
  localStorage.setItem(FKEY,JSON.stringify(a));renderPast();}

/* a past-session row: click to resume; star toggles favorite */
function pastRow(s,fav){const r=document.createElement('div');r.className='srow';
  const proj=(s.cwd||'').split('/').slice(-2).join('/');
  const sub=(fav?'':'↺ ')+esc(proj)+(s.mtime?(' · '+reltime(s.mtime)):'');
  r.innerHTML='<span class="sdot"></span><div class="smeta">'+
    '<div class="sname">'+esc(s.title||proj||'session')+'</div><div class="ssub">'+sub+'</div></div>'+
    '<span class="star'+(fav?' on':'')+'" title="'+(fav?'unfavorite':'favorite')+'">'+(fav?'★':'☆')+'</span>';
  r.querySelector('.smeta').onclick=()=>resumeSession(s);
  r.querySelector('.sdot').onclick=()=>resumeSession(s);
  r.querySelector('.star').onclick=ev=>{ev.stopPropagation();toggleFav(s);};
  return r;}

function renderPast(){
  const favs=getFavs(),favCC=new Set(favs.map(f=>f.cc));
  const fb=$('#favList');$('#favN').textContent=favs.length;
  if(!favs.length)fb.innerHTML='<div class="sb-empty">star a session to pin it here</div>';
  else{fb.innerHTML='';favs.forEach(f=>fb.appendChild(pastRow(f,true)));}
  const rec=(recentData||[]).filter(s=>!liveCCs.has(s.cc)&&!favCC.has(s.cc)).slice(0,8);
  const rb=$('#recentList');
  if(!rec.length)rb.innerHTML='<div class="sb-empty">no recent sessions</div>';
  else{rb.innerHTML='';rec.forEach(s=>rb.appendChild(pastRow(s,false)));}
  const fol=(folderData||[]).filter(s=>!liveCCs.has(s.cc)&&!favCC.has(s.cc)).slice(0,15);
  const ob=$('#folderList');
  if(!fol.length)ob.innerHTML='<div class="sb-empty">no past sessions in this folder</div>';
  else{ob.innerHTML='';fol.forEach(s=>ob.appendChild(pastRow(s,false)));}
}
function currentFolder(){const p=$('#project').value;
  return p==='__custom__'?$('#cwd').value.trim():(p||'');}
function loadPast(){const folder=currentFolder();
  $('#folderScope').textContent=folder?(folder.split('/').filter(Boolean).slice(-1)[0]||folder):'';
  fetch('api/resumable').then(r=>r.json()).then(j=>{recentData=j.resumable||[];renderPast();}).catch(()=>{});
  if(folder)fetch('api/resumable?cwd='+encodeURIComponent(folder)).then(r=>r.json()).then(j=>{folderData=j.resumable||[];renderPast();}).catch(()=>{});
  else{folderData=[];renderPast();}}

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
  $('#cwd').focus();loadPast();acQuery();}
function acMove(d){const els=$('#cwdac').querySelectorAll('.acitem');if(!els.length)return;
  acSel=(acSel+d+els.length)%els.length;els.forEach((el,i)=>el.classList.toggle('sel',i===acSel));els[acSel].scrollIntoView({block:'nearest'});}
function acQuery(){clearTimeout(acTimer);const q=$('#cwd').value;
  acTimer=setTimeout(()=>fetch('api/dircomplete?q='+encodeURIComponent(q)).then(r=>r.json()).then(acRender).catch(acClose),130);}

function switchSession(id){if(!id||id===sid)return;clearUI();statset('switching…');wsSend({type:'attach',id:id});
  if(window.innerWidth<=860)closeSidebar();}
function resumeSession(s){if(!s||!s.cc)return;clearUI();pendingStart=true;sid=null;statset('resuming…');
  const go=()=>wsSend({type:'resume',cc:s.cc,cwd:s.cwd,model:$('#model').value,mode:$('#mode').value});
  if(ws&&ws.readyState===1)go();else openWs(go);
  if(window.innerWidth<=860)closeSidebar();}
function newSession(){const proj=$('#project').value;const dir=proj==='__custom__'?$('#cwd').value.trim():proj;
  if(!dir){addErr('pick a project directory first');return;}
  /* keep any current session alive in the background — just spin up another */
  clearUI();pendingStart=true;sid=null;
  const start=()=>wsSend({type:'start',cwd:dir,model:$('#model').value,mode:$('#mode').value});
  if(ws&&ws.readyState===1)start();else openWs(start);statset('starting…');
  if(window.innerWidth<=860)closeSidebar();}
function endSessionById(id,name){if(!id)return;
  if(!confirm('End session '+(name?'« '+name+' »':'')+'?\nIts claude process stops; you can still resume it from disk later.'))return;
  wsSend({type:'end',id:id});
  if(id===sid){sid=null;setCurname('');markEnded('session ended');}
  reqList();loadPast();}
function sendMsg(){const t=ta.value.trim();if(!t||!ready||running||!sid||!ws||ws.readyState!==1)return;
  wsSend({type:'user',text:t});ta.value='';ta.style.height='auto';setBusy(true);}

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
  $('#edits').style.display=e?'':'none';$('#gitc').style.display=e?'none':'';if(!e)refreshGit();}
function openDrawer(w){$('#drawer').classList.add('open');showTab(w||'edits');}
async function refreshGit(){if(!cwd){$('#gitc').innerHTML='<div class="empty">no session</div>';return;}
  try{const r=await fetch('api/diff?cwd='+encodeURIComponent(cwd));const j=await r.json();
    if(!j.ok){$('#gitc').innerHTML='<div class="empty">'+esc(j.error||'n/a')+'</div>';return;}
    let h='';if(j.files&&j.files.length)h+=j.files.map(f=>'<div class="gfile"><span class="st">'+esc(f.status)+'</span>'+esc(f.path)+'</div>').join('')+'<hr style="border-color:#333;margin:6px 0">';
    h+=j.diff&&j.diff.trim()?diffHtml(j.diff):'<div class="empty">clean ✓</div>';$('#gitc').innerHTML=h;
  }catch(e){}}

/* bindings */
ta.addEventListener('input',()=>{ta.style.height='auto';ta.style.height=Math.min(ta.scrollHeight,160)+'px';});
ta.addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendMsg();}});
sendBtn.onclick=sendMsg;
$('#stop').onclick=()=>{wsSend({type:'interrupt'});addNotice('⏹ interrupt sent');};
$('#newbtn').onclick=newSession;
$('#navtoggle').onclick=toggleSidebar;
$('#sb-backdrop').onclick=closeSidebar;
$('#resumeRef').onclick=loadPast;
/* model/mode: apply live to the active session; otherwise just seed the next New */
$('#model').onchange=()=>{if(sid&&ws&&ws.readyState===1)wsSend({type:'set_model',model:$('#model').value});};
$('#mode').onchange=()=>{if(sid&&ws&&ws.readyState===1)wsSend({type:'set_mode',mode:$('#mode').value});};
$('#tabEdits').onclick=()=>showTab('edits');
$('#tabGit').onclick=()=>showTab('git');
$('#chgbtn').onclick=()=>{if(drawerOpen())$('#drawer').classList.remove('open');else openDrawer('edits');};
$('#dclose').onclick=()=>$('#drawer').classList.remove('open');
$('#grefresh').onclick=refreshGit;
$('#project').onchange=()=>{const c=$('#project').value==='__custom__';
  $('#cwdwrap').classList.toggle('show',c);
  if(c){if(!$('#cwd').value)$('#cwd').value=HOMEDIR?(HOMEDIR+'/'):'';$('#cwd').focus();acQuery();}else acClose();
  loadPast();};
$('#cwd').addEventListener('input',acQuery);
$('#cwd').addEventListener('focus',acQuery);
$('#cwd').addEventListener('change',loadPast);
$('#cwd').addEventListener('blur',()=>setTimeout(acClose,160));
$('#cwd').addEventListener('keydown',e=>{const b=$('#cwdac');if(!b.classList.contains('on'))return;
  if(e.key==='ArrowDown'){e.preventDefault();acMove(1);}
  else if(e.key==='ArrowUp'){e.preventDefault();acMove(-1);}
  else if(e.key==='Enter'&&acSel>=0){e.preventDefault();acPick(acSel);}
  else if(e.key==='Escape')acClose();});

/* project picker: real projects (recent dirs + git repos under ~/Git) */
(async function(){try{const r=await fetch('api/projects');const j=await r.json();HOMEDIR=j.home||'';const sel=$('#project');
  /* Custom path… first (easy to reach); recent vs git-repos as labelled groups
     instead of a ★ marker (★ now means "favorite" in the session lists) */
  const cust=document.createElement('option');cust.value='__custom__';cust.textContent='✎  Custom path…';sel.appendChild(cust);
  const mk=(label,items)=>{if(!items.length)return;const g=document.createElement('optgroup');g.label=label;
    items.forEach(p=>{const o=document.createElement('option');o.value=p.path;o.textContent=p.path.split('/').slice(-2).join('/');o.title=p.path;g.appendChild(o);});sel.appendChild(g);};
  const recent=(j.projects||[]).filter(p=>p.recent), repos=(j.projects||[]).filter(p=>!p.recent);
  mk('Recent',recent); mk('Git repos (~/Git)',repos);
  const first=recent[0]||repos[0];
  sel.value=first?first.path:'__custom__';
  $('#cwdwrap').classList.toggle('show',sel.value==='__custom__');
  loadPast();
}catch(e){}})();

setInterval(()=>reqList(),8000);
setInterval(loadPast,30000);
loadPast();
openWs();
</script>
</body>
</html>"""


def main():
    app = tornado.web.Application([
        (r"/", IndexHandler),
        (r"/console", ConsoleHandler),
        (r"/api/sessions", SessionsHandler),
        (r"/api/projects", ProjectsHandler),
        (r"/api/resumable", ResumableHandler),
        (r"/api/dircomplete", DirCompleteHandler),
        (r"/api/diff", DiffHandler),
        (r"/ws/events", EventsSocket),
        (r"/ws/chat", ChatSocket),
    ])
    loopback = BIND in ("127.0.0.1", "localhost", "::1")
    app.listen(PORT, address=BIND)
    print("Agent Lens on http://%s:%d" % (BIND, PORT))
    print("  observer: http://%s:%d/   console: http://%s:%d/console" % (BIND, PORT, BIND, PORT))
    print("  claude bin: %s" % CLAUDE_BIN)
    print("  claude transcripts: %s" % CLAUDE_ROOT)
    print("  codex transcripts:  %s" % CODEX_ROOT)
    print("  auth: %s" % ("enabled" if AUTH else "disabled"))
    if not loopback and not AUTH:
        print("  ⚠️  EXPOSED on %s WITHOUT auth — this serves your agent"
              " transcripts and code diffs. Set AGENTLENS_AUTH=user:pass." % BIND)

    def _shutdown(signum, frame):
        for s in list(CHAT_SESSIONS.values()):
            try:
                s.terminate()
            except Exception:
                pass
        os._exit(0)
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
