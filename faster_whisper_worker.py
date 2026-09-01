#!/usr/bin/env python3
"""Long-lived local faster-whisper worker using JSON lines over stdio."""

import argparse
import difflib
import json
import re
import select
import sys
import time


_PUNCTUATION = ",.!?;:，。！？；：、…"
_TERMINAL_PUNCTUATION = ".!?。！？"
_NONTERMINAL_PUNCTUATION = ",;:，；：、"
_TRAILING_CLOSERS = "'\"”’）)]}】」』"
_QUESTION_PREFIX = re.compile(
    r"^(?:请问)?(?:是否|能否|可否|有没有|有沒有|是不是|能不能|可不可以|"
    r"为什么|為什麼|为何|為何|怎么|怎麼|怎样|怎樣|如何|哪里|哪裡|哪儿|"
    r"谁|誰|什么时候|什麼時候|何时|何時|几点|幾點|多少|哪个|哪個|哪一个|哪一個)")
_QUESTION_SUFFIX = re.compile(
    r"(?:吗|嗎|对不对|對不對|是不是|有没有|有沒有|能不能|"
    r"可不可以|行不行|好不好|要不要|该不该|該不該)$")
_ENGLISH_QUESTION_PREFIX = re.compile(
    r"^(?:please\s+)?(?:what|why|when|where|who|whom|whose|which|how|"
    r"is|are|am|was|were|do|does|did|can|could|would|will|should|"
    r"have|has|had|may|might|must)\b", re.IGNORECASE)
_ENGLISH_IF_QUESTION = re.compile(
    r"^if\b.+\b(?:will|would|can|could|should|is|are|was|were|do|does|did|"
    r"have|has|had)\s+(?:i|we|you|it|they|he|she|this|that|there)\b",
    re.IGNORECASE)


def _reply(payload):
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _has_terminal_punctuation(text):
    return str(text or "").rstrip().rstrip(_TRAILING_CLOSERS).endswith(
        tuple(_TERMINAL_PUNCTUATION))


def _looks_like_question(text, is_chinese):
    core = str(text or "").strip().rstrip(_TRAILING_CLOSERS)
    core = core.rstrip(_NONTERMINAL_PUNCTUATION).strip()
    if is_chinese:
        return bool(_QUESTION_PREFIX.search(core) or _QUESTION_SUFFIX.search(core))
    return bool(_ENGLISH_QUESTION_PREFIX.search(core)
                or _ENGLISH_IF_QUESTION.search(core))


def _ensure_terminal_punctuation(text, is_chinese):
    text = str(text or "").strip()
    if not text or _has_terminal_punctuation(text):
        return text
    split = len(text.rstrip(_TRAILING_CLOSERS))
    core, closers = text[:split], text[split:]
    if core.endswith(tuple(_NONTERMINAL_PUNCTUATION)):
        core = core[:-1].rstrip()
    question = _looks_like_question(core, is_chinese)
    mark = ("？" if is_chinese else "?") if question else (
        "。" if is_chinese else ".")
    return core + mark + closers


def _duration_seconds(path):
    try:
        import av

        with av.open(path) as container:
            if container.duration is not None:
                return float(container.duration / av.time_base)
            durations = [float(stream.duration * stream.time_base)
                         for stream in container.streams
                         if stream.duration is not None and stream.time_base is not None]
            return max(durations) if durations else None
    except Exception:
        return None


# How long a silence has to be before it reads as punctuation. Tuned by ear, so
# they are knobs rather than constants: COMMA_GAP is a breath, PERIOD_GAP is a
# full stop. Below COMMA_GAP the pause is just how someone talks.
COMMA_GAP = 0.5
PERIOD_GAP = 1.2
LIVE_REVISABLE_CJK_CHARS = 10
LIVE_REVISABLE_LATIN_CHARS = 32
LIVE_STATE_TTL_SECONDS = 300.0
LIVE_STATE_LIMIT = 16


def _segment_pieces(segments):
    pieces = []
    for segment in segments:
        words = list(getattr(segment, "words", None) or [])
        if words:
            for word in words:
                pieces.append({
                    "text": str(getattr(word, "word", "") or ""),
                    "start": getattr(word, "start", None),
                    "end": getattr(word, "end", None),
                })
        else:
            pieces.append({
                "text": str(getattr(segment, "text", "") or ""),
                "start": getattr(segment, "start", None),
                "end": getattr(segment, "end", None),
            })
    return pieces


def _join_pieces(pieces, language, pause_punctuation=False,
                 comma_gap=COMMA_GAP, period_gap=PERIOD_GAP, final=True):
    pieces = list(pieces)
    if not pause_punctuation:
        return "".join(piece["text"] for piece in pieces).strip()

    out = []
    previous_end = None
    is_chinese = str(language or "").lower().startswith("zh")

    def append_piece(piece, start=None, end=None):
        nonlocal previous_end
        piece = str(piece or "")
        current = "".join(out).rstrip()
        gap = ((float(start) - previous_end)
               if start is not None and previous_end is not None else 0.0)
        if (gap >= comma_gap and current and not _has_terminal_punctuation(current)
                and current[-1] not in _PUNCTUATION
                and piece.lstrip()[:1] not in _PUNCTUATION):
            out.append(("。" if is_chinese else ".")
                       if gap >= period_gap else ("，" if is_chinese else ","))
        out.append(piece)
        if end is not None:
            previous_end = float(end)

    for piece in pieces:
        append_piece(piece["text"], piece.get("start"), piece.get("end"))
    text = "".join(out).strip()
    return _ensure_terminal_punctuation(text, is_chinese) if final else text


def _join_segments(segments, language, pause_punctuation=False,
                   comma_gap=COMMA_GAP, period_gap=PERIOD_GAP, final=True):
    return _join_pieces(
        _segment_pieces(list(segments)), language, pause_punctuation,
        comma_gap, period_gap, final)


def _common_prefix(previous, current):
    limit = min(len(previous), len(current))
    index = 0
    while index < limit and previous[index] == current[index]:
        index += 1
    return current[:index]


def _stable_live_prefix(previous, current, confirmed, language):
    confirmed = confirmed or ""
    common = _common_prefix(previous or "", current or "")
    reserve = (LIVE_REVISABLE_CJK_CHARS
               if str(language or "").lower().startswith("zh")
               else LIVE_REVISABLE_LATIN_CHARS)
    candidate = min(len(common), max(0, len(current) - reserve))
    while (candidate > len(confirmed) and candidate < len(current)
           and current[candidate - 1].isalnum() and current[candidate].isalnum()
           and current[candidate - 1].isascii() and current[candidate].isascii()):
        candidate -= 1
    return current[:max(len(confirmed), candidate)]


def _map_text_boundary(previous, current, boundary):
    boundary = max(0, min(int(boundary), len(previous)))
    for tag, old_start, old_end, new_start, new_end in difflib.SequenceMatcher(
            None, previous, current, autojunk=False).get_opcodes():
        if boundary < old_start:
            return new_start
        if old_start <= boundary <= old_end:
            if tag == "equal":
                return new_start + min(boundary - old_start,
                                       new_end - new_start)
            if tag == "insert":
                return new_start
            return new_end
    return len(current)


def _preserve_confirmed_prefix(previous, current, confirmed):
    previous = previous or ""
    current = current or ""
    confirmed = confirmed or ""
    if not confirmed or current.startswith(confirmed):
        return current
    tail_start = _map_text_boundary(previous, current, len(confirmed))
    tail = current[tail_start:]
    if confirmed[-1:].isspace() and tail[:1].isspace():
        tail = tail[1:]
    merged = confirmed + tail
    # Honouring the confirmed prefix must never cost the speaker words. Repeated
    # phrasing, or a final pass that re-segments the audio, can map the boundary
    # to the end of the fresh text; the tail then comes back empty and the merge
    # silently drops however much was said after the confirmed part. `current`
    # is a transcription of the whole recording, so when the merge is shorter
    # than that, the merge is what is wrong. A late correction to a confirmed
    # word is a smaller harm than a lost sentence.
    if len(merged) < len(current):
        return current
    return merged


def _purge_live_states(states, now):
    stale = [key for key, value in states.items()
             if now - value.get("seen", 0) > LIVE_STATE_TTL_SECONDS]
    for key in stale:
        states.pop(key, None)
    if len(states) > LIVE_STATE_LIMIT:
        oldest = sorted(states, key=lambda key: states[key].get("seen", 0))
        for key in oldest[:len(states) - LIVE_STATE_LIMIT]:
            states.pop(key, None)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--device", default="auto")
    parser.add_argument("--device-index", type=int, default=0)
    parser.add_argument("--compute-type", default="default")
    parser.add_argument("--language", default="")
    parser.add_argument("--chinese-conversion", default="none",
                        choices=("none", "t2s", "tw2sp"))
    parser.add_argument("--pause-punctuation", action="store_true")
    parser.add_argument("--comma-gap", type=float, default=COMMA_GAP)
    parser.add_argument("--period-gap", type=float, default=PERIOD_GAP)
    parser.add_argument("--idle-seconds", type=int, default=600)
    parser.add_argument("--max-seconds", type=int, default=120)
    args = parser.parse_args()

    model = None
    chinese_converter = None
    live_states = {}
    while True:
        readable, _, _ = select.select([sys.stdin], [], [], max(30, args.idle_seconds))
        if not readable:
            return
        line = sys.stdin.readline()
        if not line:
            return
        try:
            request = json.loads(line)
        except Exception:
            _reply({"ok": False, "error": "invalid worker request"})
            continue
        request_type = request.get("type")
        if request_type == "shutdown":
            return

        request_id = str(request.get("id") or "")
        stream_id = str(request.get("stream_id") or "")[:120]
        final = request.get("final", True) is not False
        started = time.monotonic()
        try:
            if model is None:
                from faster_whisper import WhisperModel

                model = WhisperModel(
                    args.model,
                    device=args.device,
                    device_index=args.device_index,
                    compute_type=args.compute_type,
                )
            if request_type == "warmup":
                _reply({
                    "id": request_id,
                    "ok": True,
                    "warmed": True,
                    "elapsed": round(time.monotonic() - started, 3),
                })
                continue
            if request_type != "transcribe":
                raise ValueError("unsupported worker request")
            measured_duration = _duration_seconds(request["path"])
            if measured_duration is not None and measured_duration > args.max_seconds + 2:
                _reply({
                    "id": request_id,
                    "ok": False,
                    "code": "too_long",
                    "error": "recording is too long",
                })
                continue
            duration_hint = request.get("duration_hint")
            try:
                duration_hint = float(duration_hint) if duration_hint is not None else None
            except (TypeError, ValueError):
                duration_hint = None
            duration = measured_duration if measured_duration is not None else duration_hint
            state = live_states.get(stream_id) if stream_id else None
            segments, info = model.transcribe(
                request["path"],
                language=args.language or (state or {}).get("language") or None,
                task="transcribe",
                beam_size=5,
                vad_filter=True,
                condition_on_previous_text=False,
                word_timestamps=args.pause_punctuation or not final,
            )
            language = getattr(info, "language", "") or (state or {}).get("language", "")
            text = _join_pieces(
                _segment_pieces(list(segments)), language,
                args.pause_punctuation, args.comma_gap, args.period_gap,
                final=final)
            if args.chinese_conversion != "none":
                if chinese_converter is None:
                    from opencc import OpenCC

                    chinese_converter = OpenCC(args.chinese_conversion + ".json")
                text = chinese_converter.convert(text)
            if state and state.get("stable_text"):
                text = _preserve_confirmed_prefix(
                    state.get("text", ""), text, state["stable_text"])
            stable_text = ""
            if final:
                if stream_id:
                    live_states.pop(stream_id, None)
            else:
                stable_text = _stable_live_prefix(
                    (state or {}).get("text", ""), text,
                    (state or {}).get("stable_text", ""), language)
                if stream_id:
                    live_states[stream_id] = {
                        "text": text,
                        "stable_text": stable_text,
                        "language": language,
                        "seen": time.monotonic(),
                    }
                    _purge_live_states(live_states, time.monotonic())
            _reply({
                "id": request_id,
                "ok": True,
                "text": text,
                "stable_text": stable_text,
                "partial": not final,
                "language": language,
                "language_probability": getattr(info, "language_probability", None),
                "duration": duration or getattr(info, "duration", None),
                "elapsed": round(time.monotonic() - started, 3),
            })
        except Exception as exc:
            _reply({
                "id": request_id,
                "ok": False,
                "error": (str(exc) or type(exc).__name__)[:1000],
            })


if __name__ == "__main__":
    main()
