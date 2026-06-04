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
import urllib.request

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

def _env(name, default=""):
    """Read CLAUDE_CONSOLE_<name>, falling back to the legacy AGENTLENS_<name>."""
    return (os.environ.get("CLAUDE_CONSOLE_" + name)
            or os.environ.get("AGENTLENS_" + name) or default)

PORT = int(_env("PORT", "7703"))
AUTH = _env("AUTH", "")
# Default to loopback: this serves ALL your agent transcripts + home-wide git
# diffs, so it must not land on the network by accident. Set CLAUDE_CONSOLE_BIND=
# 0.0.0.0 (ideally with CLAUDE_CONSOLE_AUTH) to reach it from another device.
BIND = _env("BIND", "127.0.0.1")
HOME = os.path.expanduser("~")
CLAUDE_ROOT = os.path.join(HOME, ".claude", "projects")
CODEX_ROOT = os.path.join(HOME, ".codex", "sessions")
# Interactive console drives the real `claude` CLI in headless stream-json mode.
CLAUDE_BIN = (_env("CLAUDE") or shutil.which("claude")
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


_PLUMBING_TAGS = ("<command-name>", "<command-message>", "<command-args>",
                  "<local-command-stdout>", "<local-command-caveat>", "<system-reminder>")
def _is_plumbing(s):
    """CLI-injected user content (slash-command markup, local-command stdout/caveats,
    system reminders) — recorded in transcripts but never real chat to display."""
    return isinstance(s, str) and s.lstrip().startswith(_PLUMBING_TAGS)


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
            if content.strip() and not _is_plumbing(content):
                evs.append({**base, "kind": "user_text", "role": "user", "text": _cap(content)})
        elif isinstance(content, list):
            for b in content:
                if not isinstance(b, dict):
                    continue
                bt = b.get("type")
                if bt == "text" and b.get("text", "").strip() and not _is_plumbing(b["text"]):
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
                if not title and rec.get("type") == "user" and not rec.get("isCompactSummary"):
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
    """All starred sessions across every device, newest-starred first."""
    out = []
    for cc, p in load_prefs().items():
        f = p.get("fav") if isinstance(p, dict) else None
        if isinstance(f, dict):
            out.append({"cc": cc, "cwd": f.get("cwd", ""), "name": f.get("name", ""),
                        "title": f.get("title", ""), "ts": f.get("ts", 0)})
    out.sort(key=lambda x: x.get("ts", 0), reverse=True)
    for x in out:
        x.pop("ts", None)
    return out


_usage_cache = {"t": 0.0, "v": None}

def fetch_usage():
    """The rolling 5h / 7d usage limits, from Claude's OAuth usage endpoint — the
    same numbers the CLI shows. Reads the local OAuth token; cached ~5 min (the
    endpoint is itself rate-limited). Blocking (run it off the IO loop). Returns a
    dict {window: {utilization, resets_at}}, the last good value on a transient
    error, or None when never available (no token)."""
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
        _usage_cache["t"], _usage_cache["v"] = now, out
        return out
    except Exception:
        # transient failure (rate-limited 429 / network): keep serving the last
        # good value so the indicator doesn't blink out. Don't touch the timer,
        # so a real refresh is retried on the next poll.
        return _usage_cache["v"]


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


CHAT_SESSIONS = {}  # id -> ChatSession (live, independent of any browser connection)


def safe_cwd(cwd):
    rp = os.path.realpath(cwd or "")
    if not (rp == os.path.realpath(HOME) or rp.startswith(os.path.realpath(HOME) + os.sep)):
        return None
    return rp if os.path.isdir(rp) else None


def _sanitize_mode(m):
    return m if m in ("acceptEdits", "plan", "default", "bypassPermissions") else "acceptEdits"

EFFORTS = ("low", "medium", "high", "xhigh", "max")
def _sanitize_effort(e):
    return e if e in EFFORTS else "max"   # default: deepest thinking


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
        self.turn_started = None  # wall time the current turn began (for the timer)
        self.turn_word = 0        # seed for the "thinking" word, stable across reattach
        self.compacting = False   # True while a manual /compact is running

    def preload(self):
        """Populate history from the on-disk transcript before resuming."""
        if self.resume_cc:
            self.log = load_transcript_events(self.resume_cc)

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
            add_dirs=[self.cwd], cli_path=CLAUDE_BIN,
            extra_args={"dangerously-skip-permissions": None})
        if self.model and self.model != "default":
            opts.model = self.model
        if self.effort:
            opts.effort = self.effort       # SDK passes through as --effort
        if self.resume_cc:
            opts.resume = self.resume_cc
        self.client = ClaudeSDKClient(options=opts)
        await self.client.connect()
        tornado.ioloop.IOLoop.current().spawn_callback(self._consume)

    async def _can_use_tool(self, tool_name, tool_input, context):
        """SDK permission hook. AskUserQuestion is answered in-band: we surface the
        choices, collect the user's picks, and feed them back via
        updated_input['answers'] so the tool resolves with the real answer. Every
        other tool gets a plain Approve/Deny prompt. Both await the browser."""
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
                evs = self._normalize(msg)
                for e in evs:
                    if e["kind"] == "turn_done":
                        self.busy = False
                        self.turn_started = None
                        self.compacting = False
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
                # a turn just settled → send the next queued message, if any
                if any(e["kind"] == "turn_done" for e in evs):
                    self._drain_queue()
                # steering: at a tool boundary, inject queued messages into the
                # running turn so Claude sees them at its next step (like the CLI)
                elif self.busy and self.queue and any(e["kind"] == "tool_use" for e in evs):
                    self._flush_queue_midturn()
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
                            "model": d.get("model"), "cwd": d.get("cwd"),
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

    def turn_age(self):
        """Seconds the current turn has been running (0 if idle) — lets a
        re-attaching viewer resume the elapsed-time display instead of resetting."""
        return (time.time() - self.turn_started) if (self.busy and self.turn_started) else 0

    def send_user(self, text, images=None):
        images = [im for im in (images or []) if im.get("data")]
        if (not text.strip() and not images) or not self.client or self.ended:
            return
        if self.busy:
            # a turn is running — queue it. It's injected into the live turn at
            # the next tool boundary (steering), or dispatched when the turn ends.
            self._qid += 1
            qid = "q%d" % self._qid
            self.queue.append({"qid": qid, "text": text, "images": images})
            ev = {"kind": "queued", "qid": qid, "text": _cap(text)}
            if images:
                ev["images"] = len(images)
            self._push([ev])
            return
        self._dispatch(text, images)

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

    def _echo_user(self, text, images, qid=None, start=False):
        """Push the console events for one outgoing user message."""
        evs = []
        if qid:                       # came off the queue — drop its chip
            evs.append({"kind": "dequeued", "qid": qid})
        if start:                     # begins a fresh turn (vs mid-turn injection)
            evs.append({"kind": "turn_start", "word": self.turn_word})
        ue = {"kind": "user_text", "text": _cap(text)}
        if images:
            ue["images"] = len(images)
        evs.append(ue)
        self._push(evs)

    def _dispatch(self, text, images, qid=None, start=True):
        """Send one message to the live client. start=True begins a new turn;
        start=False injects into the running turn without flipping busy."""
        if start:
            self.busy = True
            self.turn_started = time.time()
            self.turn_word = secrets.randbelow(100000)
            cmd0 = text.strip().split(None, 1)[0] if text.strip() else ""
            self.compacting = (cmd0 == "/compact")
        self._echo_user(text, images, qid=qid, start=start)
        if start and self.compacting:   # surface the otherwise-silent long compaction
            self._push([{"kind": "compacting", "word": self.turn_word}])
        client = self.client
        payload = self._make_payload(text, images)
        async def _q():
            try:
                await client.query(payload)
            except Exception as ex:
                if start:
                    self.busy = False
                self._push([{"kind": "notice", "text": "send failed: %r" % ex}])
                self._drain_queue()    # don't strand the rest of the queue
        tornado.ioloop.IOLoop.current().spawn_callback(_q)

    def _flush_queue_midturn(self):
        """Steering: inject every pending queued message into the running turn
        now (write to the CLI), so Claude sees them at its next step. Echoes are
        emitted immediately; the writes are serialized in one coroutine."""
        if not self.queue or not self.client or self.ended:
            return
        items, self.queue = self.queue, []
        payloads = []
        for it in items:
            self._echo_user(it["text"], it["images"], qid=it["qid"], start=False)
            payloads.append(self._make_payload(it["text"], it["images"]))
        client = self.client
        async def _q():
            try:
                for p in payloads:
                    await client.query(p)
            except Exception as ex:
                self._push([{"kind": "notice", "text": "steer failed: %r" % ex}])
        tornado.ioloop.IOLoop.current().spawn_callback(_q)

    def _drain_queue(self):
        """Dispatch the next queued message as a fresh turn (used once a turn has
        fully settled and anything still queued needs its own turn)."""
        if self.busy or self.ended or not self.client or not self.queue:
            return
        item = self.queue.pop(0)
        self._dispatch(item["text"], item["images"], qid=item["qid"], start=True)

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

    def _notice(self, text):
        """Transient note to current viewers (not persisted in the log)."""
        self._emit({"type": "events", "events": [{"kind": "notice", "text": text}]})

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

    clients = set()          # every open socket — for cross-device favorite sync

    def open(self):
        if AUTH and not self._ok_auth():
            self.close(4401, "Unauthorized")
            return
        self.session = None
        ChatSocket.clients.add(self)
        self._say({"type": "favorites", "favorites": list_favorites()})

    def _say(self, obj):
        try:
            self.write_message(json.dumps(obj))
        except Exception:
            pass

    def _broadcast_favorites(self):
        favs = list_favorites()
        for ws in list(ChatSocket.clients):
            ws._say({"type": "favorites", "favorites": favs})

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
            self._say({"type": "attached", "id": sess.id, "cwd": sess.cwd,
                       "name": os.path.basename(sess.cwd) or sess.cwd, "cc": sess.cc_id,
                       "title": sess.title(), "ctx": sess.ctx,
                       "model": sess.model or "default", "mode": sess.mode,
                       "busy": sess.busy, "ended": sess.ended, "events": sess.log,
                       "turn_age": sess.turn_age(), "word": sess.turn_word, "effort": sess.effort,
                       "compacting": sess.compacting})
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
                       "compacting": sess.compacting, "resumed": True})
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
        elif mt == "set_effort" and self.session:
            # --effort is launch-time only (no live SDK control), so changing it
            # relaunches the session: resume the same cc with the new --effort.
            eff = _sanitize_effort(msg.get("effort"))
            sess = self.session
            if not sess.ended and eff != sess.effort:
                if sess.busy:
                    sess._notice("⚙ finish or interrupt the current turn before changing effort")
                elif not _valid_cc(sess.cc_id):
                    sess.effort = eff      # not ready yet; will apply on its own start
                else:
                    cc, cwd, model, mode = sess.cc_id, sess.cwd, sess.model, sess.mode
                    save_pref(cc, effort=eff)
                    sess.terminate(); sess.detach(self); CHAT_SESSIONS.pop(sess.id, None)
                    self.session = None
                    new = ChatSession(secrets.token_hex(6), cwd, model, mode, resume_cc=cc, effort=eff)
                    new.preload()
                    try:
                        await new.start()
                    except Exception as e:
                        self._say({"type": "error", "error": "effort relaunch failed: %s" % e})
                        return
                    CHAT_SESSIONS[new.id] = new
                    self.session = new
                    new.attach(self)
                    self._say({"type": "attached", "id": new.id, "cwd": new.cwd,
                               "name": os.path.basename(new.cwd) or new.cwd, "cc": new.cc_id,
                               "title": new.title(), "ctx": new.ctx,
                               "model": new.model or "default", "mode": new.mode,
                               "busy": new.busy, "ended": new.ended, "events": new.log,
                               "turn_age": new.turn_age(), "word": new.turn_word,
                               "effort": new.effort, "compacting": new.compacting, "resumed": True})
        elif mt == "user" and self.session:
            self.session.send_user(msg.get("text", ""), msg.get("images"))
        elif mt == "unqueue" and self.session:
            self.session.unqueue(msg.get("qid"))
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
        elif mt == "rename":
            # Sidebar ✎: set/clear a custom label for a session (by claude id).
            cc = msg.get("cc")
            ok = set_name(cc, msg.get("name") or "")
            self._say({"type": "renamed", "cc": cc,
                       "name": (msg.get("name") or "").strip()[:120], "ok": bool(ok)})
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
                 "busy": s.busy, "ended": s.ended}
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
        self.write(CONSOLE_HTML)


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
  --acc:#4fc1ff;--usr:#3794ff;--add:#2ea043;--del:#f85149;--tool:#e0c080;--think:#7a7a7a;--onacc:#04121f}
:root[data-theme="light"]{
  --bg:#ffffff;--bg2:#f6f8fa;--bg3:#eaeef2;--line:#d0d7de;--fg:#1f2328;--mut:#656d76;
  --acc:#0969da;--usr:#0969da;--add:#1a7f37;--del:#cf222e;--tool:#9a6700;--think:#8a8a8a;--onacc:#ffffff}
:root[data-theme="dracula"]{
  --bg:#282a36;--bg2:#21222c;--bg3:#343746;--line:#44475a;--fg:#f8f8f2;--mut:#6272a4;
  --acc:#bd93f9;--usr:#8be9fd;--add:#50fa7b;--del:#ff5555;--tool:#f1fa8c;--think:#6272a4;--onacc:#282a36}
:root[data-theme="nord"]{
  --bg:#2e3440;--bg2:#2b303b;--bg3:#3b4252;--line:#434c5e;--fg:#d8dee9;--mut:#7b88a1;
  --acc:#88c0d0;--usr:#81a1c1;--add:#a3be8c;--del:#bf616a;--tool:#ebcb8b;--think:#69758c;--onacc:#2e3440}
:root[data-theme="solarized-light"]{
  --bg:#fdf6e3;--bg2:#eee8d5;--bg3:#e4ddc8;--line:#d6cfb8;--fg:#586e75;--mut:#93a1a1;
  --acc:#268bd2;--usr:#268bd2;--add:#859900;--del:#dc322f;--tool:#b58900;--think:#93a1a1;--onacc:#fdf6e3}
:root[data-theme="tokyo-night"]{
  --bg:#1a1b26;--bg2:#1f2335;--bg3:#292e42;--line:#3b4261;--fg:#c0caf5;--mut:#565f89;
  --acc:#7aa2f7;--usr:#7dcfff;--add:#9ece6a;--del:#f7768e;--tool:#e0af68;--think:#565f89;--onacc:#1a1b26}
:root[data-theme="catppuccin"]{
  --bg:#1e1e2e;--bg2:#181825;--bg3:#313244;--line:#45475a;--fg:#cdd6f4;--mut:#7f849c;
  --acc:#89b4fa;--usr:#89dceb;--add:#a6e3a1;--del:#f38ba8;--tool:#f9e2af;--think:#6c7086;--onacc:#1e1e2e}
:root[data-theme="gruvbox"]{
  --bg:#282828;--bg2:#1d2021;--bg3:#3c3836;--line:#504945;--fg:#ebdbb2;--mut:#a89984;
  --acc:#83a598;--usr:#8ec07c;--add:#b8bb26;--del:#fb4934;--tool:#fabd2f;--think:#928374;--onacc:#282828}
:root[data-theme="catppuccin-latte"]{
  --bg:#eff1f5;--bg2:#e6e9ef;--bg3:#dce0e8;--line:#ccd0da;--fg:#4c4f69;--mut:#8c8fa1;
  --acc:#1e66f5;--usr:#04a5e5;--add:#40a02b;--del:#d20f39;--tool:#df8e1d;--think:#8c8fa1;--onacc:#ffffff}
:root[data-theme="gruvbox-light"]{
  --bg:#fbf1c7;--bg2:#f2e5bc;--bg3:#ebdbb2;--line:#d5c4a1;--fg:#3c3836;--mut:#7c6f64;
  --acc:#458588;--usr:#689d6a;--add:#98971a;--del:#cc241d;--tool:#b57614;--think:#928374;--onacc:#fbf1c7}
:root[data-theme="rose-pine-dawn"]{
  --bg:#faf4ed;--bg2:#fffaf3;--bg3:#f2e9e1;--line:#dfdad9;--fg:#575279;--mut:#9893a5;
  --acc:#907aa9;--usr:#286983;--add:#5b8a3a;--del:#b4637a;--tool:#ea9d34;--think:#9893a5;--onacc:#faf4ed}
:root[data-theme="one-light"]{
  --bg:#fafafa;--bg2:#f0f0f0;--bg3:#e5e5e6;--line:#d4d4d6;--fg:#383a42;--mut:#a0a1a7;
  --acc:#4078f2;--usr:#0184bc;--add:#50a14f;--del:#e45649;--tool:#c18401;--think:#a0a1a7;--onacc:#ffffff}
:root[data-theme="ayu-light"]{
  --bg:#fcfcfc;--bg2:#f3f4f5;--bg3:#e7e8e9;--line:#dcdde0;--fg:#5c6166;--mut:#9ca0a6;
  --acc:#399ee6;--usr:#55b4d4;--add:#86b300;--del:#e65050;--tool:#f2ae49;--think:#9ca0a6;--onacc:#ffffff}
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
  --delfg:color-mix(in srgb, var(--del) 58%, var(--fg))}
/* THEME-END */
html,body{height:100%;background:var(--bg);color:var(--fg);overflow:hidden;
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif;font-size:14px}
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
#effort{flex:none;max-width:100%;padding:3px 11px;background:var(--bg3);border:1px solid var(--line);
  border-radius:9px;box-shadow:0 2px 10px rgba(0,0,0,.28);user-select:none;cursor:pointer;
  font-size:13px;line-height:1.1;color:var(--fg);white-space:nowrap}
#effort:hover{border-color:var(--acc);color:var(--acc)}
#thinking{flex:none;max-width:100%;padding:3px 12px;
  background:var(--bg3);border:1px solid var(--line);border-radius:9px;
  box-shadow:0 2px 10px rgba(0,0,0,.28);user-select:none}
#thinking .twrap{display:flex;align-items:center;gap:8px;font-size:13px;line-height:1.1}
#thinking .dot{flex:none}
#thinking.busy .dot{display:none}
#thinking .glyph{display:none;font-size:13px;color:var(--tool);width:1.1em;text-align:center;
  text-shadow:0 0 8px var(--tool);animation:thinkpulse 1.4s ease-in-out infinite}
#thinking.busy .glyph{display:inline-block}
#thinking.compacting .glyph{width:2em;text-align:left;letter-spacing:1px;font-weight:700}
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
.msg.user{display:flex;justify-content:flex-end}
.msg.user .b{background:var(--sel);border:1px solid var(--selln);border-radius:10px;padding:8px 12px;max-width:85%;white-space:pre-wrap}
.msg.asst .b{color:var(--fg)}
.think{color:var(--think);font-style:italic;font-size:13px;border-left:2px solid var(--line);padding:3px 0 3px 10px;margin-bottom:12px;white-space:pre-wrap}
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
.tool .cnt .a{color:var(--addfg)}.tool .cnt .d{color:var(--delfg)}
.tool .chev{color:var(--mut);flex-shrink:0;transition:transform .15s}
.tool.open .chev{transform:rotate(90deg)}
.tool .tb{display:none;border-top:1px solid var(--line);padding:8px 10px}
.tool.open .tb{display:block}
.tool.err .tn{color:var(--del)}
pre{background:var(--codebg);border:1px solid var(--line);border-radius:6px;padding:8px;overflow-x:auto;margin:5px 0;
  font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12.5px;line-height:1.45}
code{font-family:ui-monospace,Menlo,monospace;font-size:12.5px;background:var(--codebg);border:1px solid var(--line);border-radius:3px;padding:0 4px}
pre code{background:none;border:none;padding:0}
.bubble h1,.bubble h2,.bubble h3{font-size:14px;margin:8px 0 4px}
.msg.asst ul,.msg.asst ol{margin:4px 0 4px 20px}
.msg.asst a{color:var(--acc)}
.diffline{font-family:ui-monospace,monospace;font-size:12px;white-space:pre-wrap;line-height:1.4}
.dl-add{color:var(--addfg)}.dl-del{color:var(--delfg)}.dl-hdr{color:var(--acc)}.dl-ctx{color:var(--mut)}
.reslabel{font-size:11px;color:var(--mut);margin:6px 0 2px}

#composer{flex-shrink:0;border-top:1px solid var(--line);background:var(--bg2);padding:8px 10px;
  padding-bottom:calc(8px + env(safe-area-inset-bottom));display:flex;flex-direction:column;gap:0;align-items:stretch}
#composer .wrap2{width:100%;display:flex;gap:8px;align-items:flex-end}
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
#ta{flex:1;background:var(--bg3);color:var(--fg);border:1px solid var(--line);border-radius:10px;
  padding:9px 12px;font-size:14px;font-family:inherit;resize:none;max-height:160px;line-height:1.4}
#ta:focus{outline:1px solid var(--acc)}
#send{background:var(--acc);color:var(--onacc);border:none;border-radius:10px;width:38px;height:38px;flex:none;display:inline-flex;align-items:center;justify-content:center;font-size:15px;font-weight:700;cursor:pointer}
#send:disabled{background:var(--line);color:var(--mut);cursor:default}

#drawer{position:fixed;top:0;right:0;width:min(560px,92vw);height:100%;background:var(--bg);border-left:1px solid var(--line);
  transform:translateX(100%);transition:transform .2s;z-index:20;display:flex;flex-direction:column}
#drawer.open{transform:none}
#drawer .dh{padding:8px 12px;background:var(--bg2);border-bottom:1px solid var(--line);display:flex;gap:8px;align-items:center}
#drawer .dh .grow{flex:1}
#drawer .dc{flex:1;overflow:auto;padding:10px}
.gfile{font-family:ui-monospace,monospace;font-size:12px;padding:1px 0}.gfile .st{display:inline-block;width:24px;color:var(--tool);font-weight:700}
.empty{color:var(--mut);padding:18px;text-align:center}
/* edits-out-of-chat */
.dh .tab{cursor:pointer;padding:3px 9px;border-radius:5px;color:var(--mut);font-size:12.5px;user-select:none}
.dh .tab.on{background:var(--bg3);color:var(--fg)}
.dh .tab span{font-size:10px;opacity:.8}
.emark{font-size:12px;color:var(--tool);background:var(--toolbg);border:1px solid var(--toolln);border-radius:6px;
  padding:3px 9px;margin:2px 0 12px;display:inline-flex;gap:7px;cursor:pointer;font-family:ui-monospace,monospace;align-items:center}
.emark:hover{filter:brightness(1.25)}
.emark .a{color:var(--addfg)}.emark .d{color:var(--delfg)}.emark .mut{color:var(--mut)}
.ecard{border:1px solid var(--line);border-radius:8px;margin-bottom:10px;background:var(--bg2);overflow:hidden}
.ecard .eh{padding:7px 9px;display:flex;gap:7px;align-items:center;background:var(--toolbg);border-bottom:1px solid var(--line)}
.ecard .ef{color:var(--tool);font-family:ui-monospace,monospace;font-size:12px;font-weight:600;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ecard .cnt{font-size:11px;font-family:ui-monospace,monospace}.ecard .cnt .a{color:var(--addfg)}.ecard .cnt .d{color:var(--delfg)}
.ecard.flash{outline:2px solid var(--acc);outline-offset:-2px}
.efocus{font-size:11.5px;color:var(--mut);margin:0 0 9px;padding:5px 8px;background:var(--bg2);border:1px solid var(--line);border-radius:6px}
.efocus .showall{color:var(--acc);cursor:pointer;text-decoration:underline}
.ecard .ed{max-height:320px;overflow:auto;padding:6px 9px}
.ecard .res{padding:0 9px}
/* approval prompts */
.approval{border:1px solid var(--toolln);border-radius:8px;margin:6px 0 14px;background:var(--toolbg);overflow:hidden}
.approval .ah{padding:8px 10px;color:var(--tool);font-weight:600;display:flex;gap:7px;align-items:center}
.approval .ah .tp{color:var(--mut);font-family:ui-monospace,monospace;font-weight:400;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
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
.ctx{display:none;align-items:center;gap:6px;font-size:12.5px;font-weight:600;color:var(--fg);white-space:nowrap;font-family:ui-monospace,monospace}
.ctx .bar{width:54px;height:7px;border-radius:3px;background:var(--bg3);border:1px solid var(--line);overflow:hidden}
.ctx .fill{display:block;height:100%;width:0;background:var(--add);transition:width .3s,background .3s}
.ctx.warn .fill{background:var(--tool)}
.ctx.hot .fill{background:var(--del)}
.usage{display:none;align-items:center;gap:6px;font-size:12.5px;font-weight:600;color:var(--fg);white-space:nowrap;font-family:ui-monospace,monospace;
  border-left:1px solid var(--line);padding-left:11px;margin-left:4px}
.ctx .ulabel,.usage .ulabel{opacity:.7}
.usage .bar{width:54px;height:7px;border-radius:3px;background:var(--bg3);border:1px solid var(--line);overflow:hidden}
.usage .fill{display:block;height:100%;width:0;background:var(--add);transition:width .3s,background .3s}
.usage.warn .fill{background:var(--tool)}
.usage.hot .fill{background:var(--del)}
@media(max-width:680px){.usage{display:none!important}}
#shell{flex:1;display:flex;min-height:0;position:relative}
#mainCol{flex:1;display:flex;flex-direction:column;min-width:0}
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
.acname{font-size:12px;font-family:ui-monospace,monospace;color:var(--fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.acpath{font-size:10px;font-family:ui-monospace,monospace;color:var(--mut);overflow-wrap:anywhere;line-height:1.3}
.acitem:hover .acname,.acitem.sel .acname,.acitem:hover .acpath,.acitem.sel .acpath{color:var(--onacc)}
.acmore{padding:5px 8px;font-size:10px;color:var(--mut);font-style:italic}
.sb-row2{display:flex;gap:6px}.sb-row2 select{flex:1;min-width:0}
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
.srow .sdot{width:7px;height:7px;border-radius:50%;background:var(--mut);flex-shrink:0}
.srow .sdot.on{background:var(--add)}
.srow .sdot.busy{background:var(--tool);box-shadow:0 0 5px var(--tool);animation:pulse 1s infinite}
.srow .smeta{flex:1;min-width:0}
.srow .sname{font-size:12.5px;color:var(--fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.srow .ssub{font-size:11px;color:var(--mut);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.srow .skebab{flex-shrink:0;font-size:18px;line-height:1;padding:1px 6px;color:var(--fg);cursor:pointer;opacity:.9;border-radius:5px;user-select:none}
.srow:hover .skebab{opacity:1}.srow .skebab:hover{color:var(--acc);background:var(--bg3)}
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
.seclist{max-height:266px;overflow-y:auto}
/* shared per-card action menu (⋯) */
#cardMenu{position:fixed;z-index:60;min-width:152px;background:var(--bg2);border:1px solid var(--line);border-radius:8px;box-shadow:0 8px 26px rgba(0,0,0,.55);padding:4px;display:none}
#cardMenu.on{display:block}
#cardMenu .mi{padding:7px 10px;font-size:12.5px;color:var(--fg);cursor:pointer;border-radius:5px;white-space:nowrap}
#cardMenu .mi:hover{background:var(--bg3)}
#cardMenu .mi.danger{color:var(--delfg)}#cardMenu .mi.danger:hover{background:var(--nobg)}
.fscope{font-size:10px;color:var(--mut);font-family:ui-monospace,monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:130px}
#sb-backdrop{display:none}
@media(max-width:860px){
  #sidebar{position:fixed;left:0;top:0;bottom:0;z-index:40;transform:translateX(-100%);transition:transform .2s;width:min(310px,86vw);box-shadow:2px 0 14px rgba(0,0,0,.5)}
  #sidebar.open{transform:none}
  #sidebar.collapsed{display:flex}
  #sb-backdrop.show{display:block;position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:35}
  #sbresize{display:none}
}
.bubble .math.display{display:block;margin:6px 0;overflow-x:auto;overflow-y:hidden;max-width:100%}
.katex-display{margin:.35em 0!important}
</style>
<link rel="stylesheet" href="/static/katex/katex.min.css">
<script src="/static/katex/katex.min.js"></script>
</head>
<body>
<header>
  <button class="iconbtn" id="navtoggle" title="sessions">☰</button>
  <span class="curname" id="curname">— no session —</span>
  <span class="ctx" id="ctx" title="context-window usage"></span>
  <span class="usage" id="usage" title="5-hour usage limit"></span>
</header>

<div id="shell">
  <aside id="sidebar">
    <div class="sb-brand">⬡ Claude Console</div>
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
    <div class="sb-sec" id="secFav">
      <div class="sb-h sb-toggle"><span class="caret"></span>★ Favorites <span id="favN" class="cnt">0</span></div>
      <div id="favList" class="seclist"><div class="sb-empty">star a session to pin it here</div></div>
    </div>
    <div class="sb-sec" id="secRecent">
      <div class="sb-h sb-toggle"><span class="caret"></span>🕘 Recent <span class="grow"></span><span class="sb-ref" id="resumeRef" title="refresh">↻</span></div>
      <div id="recentList" class="seclist"><div class="sb-empty">—</div></div>
    </div>
    <div class="sb-sec" id="secFolder">
      <div class="sb-h sb-toggle"><span class="caret"></span>📁 In folder <span class="grow"></span><span id="folderScope" class="fscope"></span></div>
      <div id="folderList" class="seclist"><div class="sb-empty">—</div></div>
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
  <div id="mainCol">
    <div id="chat"><div class="wrap" id="stream"></div></div>
    <div id="composer">
      <div class="pillrow">
        <div id="thinking"><div class="twrap"><span class="dot" id="dot"></span><span class="glyph">✶</span><span class="word">idle</span><span class="meta"></span></div></div>
        <span id="effort" title="thinking effort — click to change">🧠 max</span>
      </div>
      <div id="queue"></div>
      <div id="attach"></div>
      <div class="wrap2">
      <textarea id="ta" rows="1" placeholder="Type a message…  (Enter to send · Shift+Enter newline · paste an image)" disabled></textarea>
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
<div id="cardMenu"></div>

<script>
const $=s=>document.querySelector(s);
const stream=$('#stream'), ta=$('#ta'), sendBtn=$('#send');
let ws=null, running=false, ready=false, compacting=false, cwd='', tools={};
let sid=null, curCC=null, editCount=0, pendingStart=false, reconnectT=0;
const EFFORTS=['low','medium','high','xhigh','max'];
let curEffort=localStorage.getItem('al_effort')||'max';
let showThink=false;
let liveCCs=new Set(), recentData=[], folderData=[], favData=[], HOMEDIR='';
const EDIT_TOOLS=new Set(['Edit','MultiEdit','Write','NotebookEdit']);
const SKEY='al_session';

function esc(s){return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
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
  h=h.replace(/`([^`\n]+)`/g,(m,c)=>'<code>'+c+'</code>');
  h=h.replace(/\*\*([^*\n]+)\*\*/g,'<b>$1</b>');
  h=h.replace(/^### (.*)$/gm,'<h3>$1</h3>').replace(/^## (.*)$/gm,'<h2>$1</h2>').replace(/^# (.*)$/gm,'<h2>$1</h2>');
  h=h.replace(/\[([^\]]+)\]\((https?:[^)\s]+)\)/g,'<a href="$2" target="_blank">$1</a>');
  h=h.replace(/^[\-\*] (.*)$/gm,'<li>$1</li>').replace(/(<li>[\s\S]*?<\/li>)/g,'<ul>$1</ul>');
  h=h.replace(/\n/g,'<br>').replace(/<br>(<(?:pre|h2|h3|ul)>)/g,'$1').replace(/(<\/(?:pre|h2|h3|ul)>)<br>/g,'$1');
  h=h.replace(/%%CB(\d+)%%/g,(m,i)=>bl[+i]);
  h=h.replace(/%%MJ(\d+)%%/g,(m,i)=>'<span class="math'+(ml[+i].d?' display':'')+'" data-d="'+ml[+i].d+'">'+esc(ml[+i].t)+'</span>');
  return h;
}
/* render protected LaTeX spans with KaTeX (textContent decodes the escaped TeX) */
function typesetMath(root){if(!window.katex)return;
  root.querySelectorAll('.math').forEach(el=>{if(el.dataset.done)return;el.dataset.done='1';
    try{katex.render(el.textContent,el,{displayMode:el.dataset.d==='1',throwOnError:false,errorColor:'#f85149'});}catch(e){}});}
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

function addUser(text,nImg){const s=atBottom();const d=document.createElement('div');d.className='msg user';
  d.innerHTML='<div class="b">'+esc(text)+'</div>'+(nImg?'<div class="imgs">🖼 '+nImg+' image'+(nImg>1?'s':'')+' attached</div>':'');
  stream.appendChild(d);scroll();}
function addAsst(text){const s=atBottom();const d=document.createElement('div');d.className='msg asst';
  d.innerHTML='<div class="b bubble">'+md(text)+'</div>';typesetMath(d);stream.appendChild(d);if(s)scroll();}
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

function statset(t){const el=$('#thinking');if(!el)return;
  const w=el.querySelector('.word');if(w)w.textContent=t;
  if(!running){const m=el.querySelector('.meta');if(m)m.textContent='';}}
function bindProject(p){if(!p)return;const sel=$('#project');let ok=false;
  for(const o of sel.options){if(o.value===p){ok=true;break;}}
  if(!ok){const o=document.createElement('option');o.value=p;o.textContent='● '+p.split('/').slice(-2).join('/');sel.insertBefore(o,sel.firstChild);}
  sel.value=p;}
function setBusy(b,wordSeed,elapsedMs){running=b;$('#dot').className='dot '+(b?'busy':(ready?'on':''));
  $('#thinking').classList.toggle('busy',b);
  if(!b&&ready)statset('ready');      /* busy: startThinking owns the word + timer */
  ta.disabled=!ready;
  sendBtn.disabled=!ready;            /* send stays available while busy → queues */
  $('#stop').style.display=b?'':'none';   /* interrupt button only while busy */
  sendBtn.style.display='';               /* send always visible */
  if(b)startThinking(wordSeed,elapsedMs);else stopThinking();}

/* in-chat "thinking" indicator — animated glyph + cycling word + elapsed timer */
const THINK_WORDS=['Thinking','Pondering','Cogitating','Brewing','Conjuring','Percolating',
  'Ruminating','Noodling','Mulling','Synthesizing','Computing','Untangling','Divining','Tinkering'];
const THINK_GLYPHS=['✶','✷','✸','✹','✺','✹','✸','✷'];
const DREAM_GLYPHS=['z','z','zz','zz','zzz','zzz'];   /* compacting: a slow Zzz drifting up */
let thinkTimer=0,thinkStart=0,thinkGi=0,thinkWi=0;
function startThinking(wordSeed,elapsedMs){const el=$('#thinking');if(!el)return;
  const fresh=!thinkTimer;         /* new turn, or reattaching to a running one */
  if(fresh||elapsedMs!=null)thinkStart=Date.now()-(elapsedMs||0);
  el.classList.toggle('compacting',compacting);
  if(compacting){el.querySelector('.word').textContent='Compacting';el.querySelector('.glyph').textContent='z';}
  else if(fresh||wordSeed!=null){  /* server-provided seed keeps the word stable across reattach */
    thinkWi=(wordSeed!=null)?(wordSeed%THINK_WORDS.length):Math.floor(Math.random()*THINK_WORDS.length);
    el.querySelector('.word').textContent=THINK_WORDS[thinkWi];
  }
  if(fresh&&atBottom())scroll();
  clearInterval(thinkTimer);
  thinkTimer=setInterval(()=>{     /* during the turn only the glyph + timer move */
    const arr=compacting?DREAM_GLYPHS:THINK_GLYPHS;
    thinkGi=(thinkGi+1)%arr.length;
    el.querySelector('.glyph').textContent=arr[thinkGi];
    const s=Math.floor((Date.now()-thinkStart)/1000);
    el.querySelector('.meta').textContent=compacting?(s+'s · compacting · esc to interrupt'):(s+'s · esc to interrupt');
  },130);}
function stopThinking(){clearInterval(thinkTimer);thinkTimer=0;const el=$('#thinking');if(el)el.classList.remove('compacting');}
function doInterrupt(){if(!running)return;wsSend({type:'interrupt'});addNotice('⏹ interrupt sent');}
function clearUI(){stream.innerHTML='';$('#edits').innerHTML='<div class="empty">no file changes yet</div>';
  $('#gitc').innerHTML='<div class="empty">—</div>';tools={};editCount=0;updateEditBadge();renderCtx(null);ready=false;
  queued={};renderQueue();stopThinking();}

/* file edits → out of chat, into the Changes drawer */
function updateEditBadge(){$('#editN').textContent=editCount;}
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
  m.onclick=()=>{openDrawer('edits');focusEdit(ev.toolId);};
  stream.appendChild(m);if(s)scroll();}

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
  /* if activity resumes while we think we're idle (e.g. the CLI ran an injected
     queued message as its own turn), step back into the busy state */
  if(!running&&(ev.kind==='assistant_text'||ev.kind==='thinking'||ev.kind==='tool_use'))setBusy(true);
  if(ev.kind==='user_text')addUser(ev.text,ev.images);
  else if(ev.kind==='ready'){ready=true;cwd=ev.cwd||cwd;curCC=ev.session_id||curCC;if(ev.model)setResolvedModel(ev.model);addNotice('● session ready · '+(ev.model||'')+(ev.effort?' · '+ev.effort+' effort':'')+' · '+(ev.cwd||''));}
  else if(ev.kind==='assistant_text')addAsst(ev.text);
  else if(ev.kind==='thinking')addThink(ev.text);
  else if(ev.kind==='tool_use'){if(EDIT_TOOLS.has(ev.tool)){addEditCard(ev);addMarker(ev);}else addTool(ev);}
  else if(ev.kind==='tool_result')addResult(ev);
  else if(ev.kind==='approval')addApproval(ev);
  else if(ev.kind==='approval_resolved')resolveApprovalCard(ev.aid,ev.allow,ev.always);
  else if(ev.kind==='question')addQuestion(ev);
  else if(ev.kind==='question_resolved')resolveQuestionCard(ev.aid,ev.answers);
  else if(ev.kind==='turn_start')setBusy(true,ev.word,0);
  else if(ev.kind==='compacting'){compacting=true;setBusy(true,ev.word,0);}
  else if(ev.kind==='compacted'){compacting=false;addNotice(fmtCompacted(ev));if(ev.trigger!=='auto')setBusy(false);}
  else if(ev.kind==='turn_done'){compacting=false;setBusy(false);if(drawerOpen()&&gitTab())refreshGit();}
  else if(ev.kind==='queued')addQueued(ev);
  else if(ev.kind==='dequeued'||ev.kind==='unqueued')removeQueued(ev.qid);
  else if(ev.kind==='notice')addNotice(ev.text);
}

/* persistent server-side session: attach / reattach / switch */
function markEnded(msg){ready=false;ta.disabled=true;sendBtn.disabled=true;$('#dot').className='dot';statset('ended');
  localStorage.removeItem(SKEY);if(msg)addNotice(msg);}
function onMsg(e){const m=JSON.parse(e.data);
  if(m.type==='started'){pendingStart=false;sid=m.id;cwd=m.cwd;bindProject(m.cwd);localStorage.setItem(SKEY,sid);
    ready=true;setBusy(false);ta.focus();setCurname(m.name||'session');syncPickers(m.model,m.mode);setEffortPill(m.effort);renderCtx(null);statset('ready');
    addNotice('new session « '+(m.name||'')+' »'+(m.effort?' · '+m.effort+' effort':'')+' in '+m.cwd+' — type your first message to begin');reqList();loadPast();}
  else if(m.type==='attached'){clearUI();pendingStart=false;sid=m.id;curCC=m.cc||null;localStorage.setItem(SKEY,sid);cwd=m.cwd;bindProject(m.cwd);
    ready=!m.ended;setCurname((m.title||m.name||'session')+(m.ended?' · ended':''));syncPickers(m.model,m.mode);setEffortPill(m.effort);renderCtx(m.ctx);statset(m.ended?'ended':'ready');
    m.events.forEach(route);compacting=!!m.compacting;setBusy(!!m.busy,m.word,(m.turn_age||0)*1000);
    if(m.ended){markEnded('— this session has ended (history shown · you can resume it from disk) —');}
    else{ta.disabled=false;sendBtn.disabled=false;addNotice('— '+(m.resumed?'resumed':'reattached to')+' « '+(m.name||'')+' » ('+m.events.length+' events)'+(m.effort?' with '+m.effort+' effort':'')+' —');}
    reqList();loadPast();}
  else if(m.type==='no_session'){localStorage.removeItem(SKEY);sid=null;ready=false;setBusy(false);setCurname('');renderCtx(null);statset('idle');
    addNotice('that session is no longer running — pick it under “Resume from disk”, or ＋ New.');reqList();loadPast();}
  else if(m.type==='events')m.events.forEach(route);
  else if(m.type==='stderr')addErr(m.text);
  else if(m.type==='error'){pendingStart=false;addErr('⚠ '+m.error);}
  else if(m.type==='exit'){if(!pendingStart){markEnded('session process exited (code '+m.code+')');setCurname('');}reqList();loadPast();}
  else if(m.type==='ended'){if(m.id&&m.id===sid){sid=null;setCurname('');markEnded('session ended');}reqList();loadPast();}
  else if(m.type==='resumable_deleted'){addNotice(m.ok?'🗑 session moved to trash':('delete failed: '+(m.error||'?')));loadPast();}
  else if(m.type==='renamed'){if(m.ok){if(m.cc&&m.cc===curCC&&m.name)setCurname(m.name);addNotice('✎ renamed');reqList();loadPast();}else addNotice('rename failed');}
  else if(m.type==='sessions')renderLive(m.sessions);
  else if(m.type==='context')renderCtx(m.ctx);
  else if(m.type==='favorites'){favData=m.favorites||[];renderPast();}
}
function openWs(cb){const proto=location.protocol==='https:'?'wss:':'ws:';
  ws=new WebSocket(proto+'//'+location.host+'/ws/chat');
  ws.onopen=()=>{clearTimeout(reconnectT);$('#dot').className='dot '+(ready?'on':'');if(cb)cb();reqList();maybeMigrateFavs();
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
  el.innerHTML='<span class="ulabel">Context</span><span class="bar"><span class="fill" style="width:'+Math.min(100,pct)+'%"></span></span>'+
    '<span>'+pct+'%</span>';
  el.title='context '+(c.totalTokens||'?')+' / '+(c.maxTokens||'?')+' tokens ('+pct+'%)'+(c.model?' · '+c.model:'');
  setResolvedModel(c.model, c.maxTokens);}
/* rolling 5-hour usage limit (Claude-Code-CLI style: Usage ░░░ 2% (4h 38m / 5h)) */
function fmtDur(ms){if(ms==null||ms<=0)return '0m';const m=Math.floor(ms/60000),h=Math.floor(m/60);
  return h>0?(h+'h '+(m%60)+'m'):(m+'m');}
function renderUsage(u){const el=$('#usage');const f=u&&u.five_hour;
  if(!f||f.utilization==null){el.style.display='none';return;}
  const pct=Math.round(f.utilization);
  const rem=f.resets_at?fmtDur(new Date(f.resets_at)-Date.now()):'';
  el.className='usage'+(pct>=85?' hot':(pct>=60?' warn':''));
  el.style.display='inline-flex';
  el.innerHTML='<span class="ulabel">Usage</span><span class="bar"><span class="fill" style="width:'+Math.min(100,pct)+'%"></span></span>'+
    '<span>'+pct+'%'+(rem?(' ('+rem+' / 5h)'):'')+'</span>';
  let t='5-hour limit: '+pct+'% used'+(rem?(' · resets in '+rem):'');
  const w7=[['seven_day','7-day'],['seven_day_sonnet','7-day Sonnet'],['seven_day_opus','7-day Opus']];
  w7.forEach(([k,lbl])=>{if(u[k]&&u[k].utilization!=null)t+='\n'+lbl+': '+Math.round(u[k].utilization)+'%';});
  el.title=t;}
function loadUsage(){fetch('api/usage').then(r=>r.json()).then(j=>renderUsage(j.usage)).catch(()=>{});}
/* reflect the active session's real model/mode in the pickers (programmatic
   .value set does NOT fire onchange, so this won't echo back to the server) */
function syncPickers(model,mode){
  if(model){const o=$('#model');for(const x of o.options){if(x.value===model){o.value=model;break;}}}
  if(mode){const o=$('#mode');for(const x of o.options){if(x.value===mode){o.value=mode;break;}}}
}
/* thinking effort: a clickable pill on the right of the status row. --effort is
   launch-time only, so changing it on a live session relaunches it (server-side
   resume with the new --effort); the choice is remembered for new sessions. */
function setEffortPill(e){if(e)curEffort=e;const el=$('#effort');if(el)el.textContent='🧠 '+curEffort;}
function setEffort(e){if(!EFFORTS.includes(e)||e===curEffort)return;
  if(running){addNotice('⚙ finish or interrupt the current turn before changing effort');return;}
  curEffort=e;localStorage.setItem('al_effort',e);setEffortPill(e);
  if(sid&&ready&&ws&&ws.readyState===1)wsSend({type:'set_effort',effort:e});}   /* live session → relaunch */
/* show what the live session's model actually resolves to in the picker's default
   option, e.g. "model: opus 4.8 [1M]" — family+version from the model id, the
   [ctx] window from maxTokens. Fed by the ready event and the context usage. */
let _rmodel='', _rmax=0;
function modelLabel(real,maxTok){
  if(!real)return '';
  const s=(''+real).toLowerCase();
  const fam=s.includes('opus')?'opus':s.includes('sonnet')?'sonnet':s.includes('haiku')?'haiku':'';
  if(!fam)return ''+real;
  const m=s.match(new RegExp(fam+'-(\\d+)-(\\d+)'))||s.match(new RegExp('(\\d+)-(\\d+)-'+fam));
  let lbl=fam+(m?(' '+m[1]+'.'+m[2]):'');
  if(maxTok)lbl+='['+fmtTok(maxTok)+']';
  return lbl;}
function setResolvedModel(real,maxTok){
  if(real)_rmodel=real;
  if(maxTok)_rmax=maxTok;
  const lbl=modelLabel(_rmodel,_rmax);
  const o=$('#model');if(!o)return;
  for(const x of o.options){if(x.value==='default'){
    x.textContent=lbl||'model: default';break;}}}

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

/* collapsible sidebar sections (Favorites / Recent / In folder) */
const SECKEY='al_seccol';
function toggleSec(id){const el=$('#'+id);if(!el)return;el.classList.toggle('collapsed');
  let c={};try{c=JSON.parse(localStorage.getItem(SECKEY)||'{}');}catch(e){}
  c[id]=el.classList.contains('collapsed');localStorage.setItem(SECKEY,JSON.stringify(c));}
function applySecCollapse(){let c={};try{c=JSON.parse(localStorage.getItem(SECKEY)||'{}');}catch(e){}
  ['secFav','secRecent','secFolder'].forEach(id=>{const el=$('#'+id);if(el)el.classList.toggle('collapsed',!!c[id]);});}

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
      '<div class="ssub">'+esc(proj)+(s.busy?' · working…':'')+'</div></div>'+
      '<span class="skebab" title="more">⋮</span>';
    r.querySelector('.smeta').onclick=()=>switchSession(s.id);
    r.querySelector('.sdot').onclick=()=>switchSession(s.id);
    r.querySelector('.skebab').onclick=ev=>{ev.stopPropagation();const items=[
      {label:'✎ Rename',fn:()=>renameSession(s.cc,s.title||s.name)}];
      if(s.cc)items.push({label:isFav(s.cc)?'★ Unfavorite':'☆ Favorite',fn:()=>toggleFav(s)});
      items.push({label:'✕ End session',danger:true,fn:()=>endSessionById(s.id,s.name)});
      toggleCardMenu(ev.currentTarget,items);};
    box.appendChild(r);});}
  renderPast();
}

/* favorites: starred sessions, persisted SERVER-SIDE by claude session id so every
   device shares one list. favData mirrors the server; the server pushes it on connect
   and re-broadcasts on every change. */
const FKEY='al_favs';   /* legacy per-device store — read once to migrate, then ignored */
function getFavs(){return favData;}
function isFav(cc){return favData.some(f=>f.cc===cc);}
function toggleFav(s){
  if(isFav(s.cc)){favData=favData.filter(f=>f.cc!==s.cc);
    wsSend({type:'set_favorite',cc:s.cc,fav:false});}
  else{favData=[{cc:s.cc,cwd:s.cwd||'',name:s.name||'',title:s.title||''},...favData];
    wsSend({type:'set_favorite',cc:s.cc,fav:true,cwd:s.cwd||'',name:s.name||'',title:s.title||''});}
  renderPast();}
function maybeMigrateFavs(){   /* one-time: lift this device's old localStorage stars to the server */
  if(localStorage.getItem('al_favs_migrated'))return;
  if(!ws||ws.readyState!==1)return;        /* need the socket open; onopen retries */
  let old=[];try{old=JSON.parse(localStorage.getItem(FKEY)||'[]');}catch(e){}
  old.forEach(f=>{if(f&&f.cc)wsSend({type:'set_favorite',cc:f.cc,fav:true,
    cwd:f.cwd||'',name:f.name||'',title:f.title||''});});
  localStorage.setItem('al_favs_migrated','1');}

/* a past-session row: click to resume; star toggles favorite */
function pastRow(s,fav){const r=document.createElement('div');r.className='srow';
  const proj=(s.cwd||'').split('/').slice(-2).join('/');
  const sub=(fav?'':'↺ ')+esc(proj)+(s.mtime?(' · '+reltime(s.mtime)):'');
  r.innerHTML='<span class="sdot"></span><div class="smeta">'+
    '<div class="sname">'+esc(s.title||proj||'session')+'</div><div class="ssub">'+sub+'</div></div>'+
    '<span class="skebab" title="more">⋮</span>';
  r.querySelector('.smeta').onclick=()=>resumeSession(s);
  r.querySelector('.sdot').onclick=()=>resumeSession(s);
  r.querySelector('.skebab').onclick=ev=>{ev.stopPropagation();toggleCardMenu(ev.currentTarget,[
    {label:'✎ Rename',fn:()=>renameSession(s.cc,s.title)},
    {label:fav?'★ Unfavorite':'☆ Favorite',fn:()=>toggleFav(s)},
    {label:'🗑 Delete (to trash)',danger:true,fn:()=>delResumable(s)}]);};
  return r;}
function renameSession(cc,cur){
  if(!cc){alert('This session is still starting — try again in a moment.');return;}
  const nm=prompt('Rename session (leave empty to reset to the auto name):',cur||'');
  if(nm===null)return;
  wsSend({type:'rename',cc:cc,name:nm});}
function delResumable(s){
  if(!confirm('Delete this session from disk?\n\n'+(s.title||s.name||s.cc||'session')+
    '\n\nIt is moved to the trash (recoverable), not permanently deleted.'))return;
  wsSend({type:'del_resumable',cc:s.cc});
  /* optimistic: drop it from favorites + cached lists so it vanishes at once */
  favData=favData.filter(f=>f.cc!==s.cc);
  recentData=(recentData||[]).filter(x=>x.cc!==s.cc);
  folderData=(folderData||[]).filter(x=>x.cc!==s.cc);
  renderPast();}

function renderPast(){
  const favs=getFavs(),favCC=new Set(favs.map(f=>f.cc));
  const fb=$('#favList');$('#favN').textContent=favs.length;
  if(!favs.length)fb.innerHTML='<div class="sb-empty">star a session to pin it here</div>';
  else{fb.innerHTML='';favs.forEach(f=>fb.appendChild(pastRow(f,true)));}
  const rec=(recentData||[]).filter(s=>!liveCCs.has(s.cc)&&!favCC.has(s.cc)).slice(0,30);
  const rb=$('#recentList');
  if(!rec.length)rb.innerHTML='<div class="sb-empty">no recent sessions</div>';
  else{rb.innerHTML='';rec.forEach(s=>rb.appendChild(pastRow(s,false)));}
  const fol=(folderData||[]).filter(s=>!liveCCs.has(s.cc)&&!favCC.has(s.cc)).slice(0,30);
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
  const start=()=>wsSend({type:'start',cwd:dir,model:$('#model').value,mode:$('#mode').value,effort:curEffort});
  if(ws&&ws.readyState===1)start();else openWs(start);statset('starting…');
  if(window.innerWidth<=860)closeSidebar();}
function endSessionById(id,name){if(!id)return;
  if(!confirm('End session '+(name?'« '+name+' »':'')+'?\nIts claude process stops; you can still resume it from disk later.'))return;
  wsSend({type:'end',id:id});
  if(id===sid){sid=null;setCurname('');markEnded('session ended');}
  reqList();loadPast();}
/* image attachments: paste (Ctrl/Cmd+V) an image into the composer */
let pendingImages=[];
const MAX_IMG=8, MAX_IMG_BYTES=5*1024*1024, OK_IMG=['image/png','image/jpeg','image/gif','image/webp'];
function renderAttach(){const a=$('#attach');a.classList.toggle('on',pendingImages.length>0);
  a.innerHTML=pendingImages.map((im,i)=>'<div class="att"><img src="'+im.url+'"><button class="rm" data-i="'+i+'" title="remove">✕</button></div>').join('');
  a.querySelectorAll('.rm').forEach(b=>b.onclick=()=>{pendingImages.splice(+b.dataset.i,1);renderAttach();});}
function addImageFile(file){
  if(pendingImages.length>=MAX_IMG){addNotice('⚠ up to '+MAX_IMG+' images at once');return;}
  if(OK_IMG.indexOf(file.type)<0){addNotice('⚠ unsupported image type: '+(file.type||'?'));return;}
  if(file.size>MAX_IMG_BYTES){addNotice('⚠ image too large ('+Math.round(file.size/1048576)+'MB, max 5MB)');return;}
  const r=new FileReader();
  r.onload=()=>{const url=''+r.result;pendingImages.push({media_type:file.type,data:url.split(',')[1]||'',url:url});renderAttach();};
  r.readAsDataURL(file);}
function handlePaste(e){const items=(e.clipboardData||{}).items||[];let got=false;
  for(const it of items){if(it.kind==='file'&&it.type.indexOf('image/')===0){const f=it.getAsFile();if(f){addImageFile(f);got=true;}}}
  if(got)e.preventDefault();}
function sendMsg(){const t=ta.value.trim();
  if((!t&&!pendingImages.length)||!ready||!sid||!ws||ws.readyState!==1)return;
  wsSend({type:'user',text:t,images:pendingImages.map(im=>({media_type:im.media_type,data:im.data}))});
  ta.value='';ta.style.height='auto';pendingImages=[];renderAttach();}
  /* busy state (and the thinking word/timer) is driven by the server's
     turn_start, so it stays correct across reattach — no optimistic flip here */

/* queued messages: chips above the composer while the agent is busy. Click a
   chip (or press ↑ on an empty box) to withdraw it back into the editor. */
let queued={};
function renderQueue(){const q=$('#queue');if(!q)return;const ids=Object.keys(queued);
  q.classList.toggle('on',ids.length>0);
  q.innerHTML=ids.map(id=>'<div class="qmsg" data-q="'+id+'" title="click to edit · ✕ to discard">'+
    '<span class="qicon">⏳</span><span class="qtext">'+esc(queued[id].text||'')+
    (queued[id].images?(' 🖼×'+queued[id].images):'')+'</span><span class="qx" title="discard">✕</span></div>').join('');
  q.querySelectorAll('.qmsg').forEach(el=>{const id=el.dataset.q;
    el.querySelector('.qx').onclick=ev=>{ev.stopPropagation();discardQueued(id);};
    el.onclick=()=>editQueued(id);});}
function addQueued(ev){queued[ev.qid]={text:ev.text||'',images:ev.images||0};renderQueue();}
function removeQueued(qid){if(queued[qid]){delete queued[qid];renderQueue();}}
function discardQueued(id){if(ws&&ws.readyState===1)wsSend({type:'unqueue',qid:id});removeQueued(id);}
function editQueued(id){const it=queued[id];if(!it)return;
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
  $('#edits').style.display=e?'':'none';$('#gitc').style.display=e?'none':'';if(e)showAllEdits();else refreshGit();}
/* clicking "see Changes" focuses the drawer on just that one file's edit;
   the Edits tab header (or "show all") brings the full list back */
function showAllEdits(){const ed=$('#edits');const fb=ed.querySelector('.efocus');if(fb)fb.remove();
  ed.querySelectorAll('.ecard').forEach(c=>c.style.display='');}
function focusEdit(toolId){const ed=$('#edits'),target=toolId&&tools[toolId];
  if(!target){showAllEdits();return;}
  ed.querySelectorAll('.ecard').forEach(c=>{c.style.display=(c===target)?'':'none';});
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
ta.addEventListener('input',()=>{ta.style.height='auto';ta.style.height=Math.min(ta.scrollHeight,160)+'px';});
ta.addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendMsg();}
  else if(e.key==='ArrowUp'&&!ta.value&&Object.keys(queued).length){
    e.preventDefault();const ids=Object.keys(queued);editQueued(ids[ids.length-1]);}});
window.addEventListener('paste',handlePaste);
sendBtn.onclick=sendMsg;
$('#stop').onclick=doInterrupt;
document.addEventListener('keydown',e=>{
  if(e.key==='Escape'&&running&&!$('#cwdac').classList.contains('on')){e.preventDefault();doInterrupt();}});
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
$('#resumeRef').onclick=e=>{e.stopPropagation();loadPast();};
/* collapsible sections: clicking the header toggles; restore saved state */
['secFav','secRecent','secFolder'].forEach(id=>{
  const h=$('#'+id+' .sb-h');if(h)h.onclick=()=>toggleSec(id);});
applySecCollapse();
/* dismiss the ⋯ card menu on outside-click, Escape, scroll or resize */
document.addEventListener('click',e=>{if(!e.target.closest('#cardMenu')&&!e.target.closest('.skebab'))closeCardMenu();});
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeCardMenu();});
window.addEventListener('resize',closeCardMenu);
window.addEventListener('scroll',closeCardMenu,true);
/* model/mode: apply live to the active session; otherwise just seed the next New */
$('#model').onchange=()=>{if(sid&&ws&&ws.readyState===1)wsSend({type:'set_model',model:$('#model').value});};
$('#mode').onchange=()=>{if(sid&&ws&&ws.readyState===1)wsSend({type:'set_mode',mode:$('#mode').value});};
$('#tabEdits').onclick=()=>showTab('edits');
$('#tabGit').onclick=()=>showTab('git');
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
loadUsage();
setInterval(loadUsage,60000);
openWs();
</script>
</body>
</html>"""


def main():
    app = tornado.web.Application([
        (r"/", ConsoleHandler),
        (r"/console", ConsoleHandler),
        (r"/api/projects", ProjectsHandler),
        (r"/api/resumable", ResumableHandler),
        (r"/api/dircomplete", DirCompleteHandler),
        (r"/api/diff", DiffHandler),
        (r"/api/usage", UsageHandler),
        (r"/ws/chat", ChatSocket),
        (r"/static/(.*)", tornado.web.StaticFileHandler,
         {"path": os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")}),
    ])
    loopback = BIND in ("127.0.0.1", "localhost", "::1")
    app.listen(PORT, address=BIND)
    print("Claude Console on http://%s:%d" % (BIND, PORT))
    print("  console: http://%s:%d/" % (BIND, PORT))
    print("  claude bin: %s" % CLAUDE_BIN)
    print("  claude transcripts: %s" % CLAUDE_ROOT)
    print("  codex transcripts:  %s" % CODEX_ROOT)
    print("  auth: %s" % ("enabled" if AUTH else "disabled"))
    if not loopback and not AUTH:
        print("  ⚠️  EXPOSED on %s WITHOUT auth — this serves your agent"
              " transcripts and code diffs. Set CLAUDE_CONSOLE_AUTH=user:pass." % BIND)

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
