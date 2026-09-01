#!/usr/bin/env python3
"""Long-lived local faster-whisper worker using JSON lines over stdio."""

import argparse
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


def _join_segments(segments, language, pause_punctuation=False,
                   comma_gap=COMMA_GAP, period_gap=PERIOD_GAP):
    segments = list(segments)
    if not pause_punctuation:
        return "".join(segment.text for segment in segments).strip()

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

    for segment in segments:
        words = list(getattr(segment, "words", None) or [])
        if words:
            for word in words:
                append_piece(word.word, word.start, word.end)
        else:
            append_piece(segment.text, getattr(segment, "start", None),
                         getattr(segment, "end", None))
    text = "".join(out).strip()
    return _ensure_terminal_punctuation(text, is_chinese)


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
        if request.get("type") == "shutdown":
            return

        request_id = str(request.get("id") or "")
        started = time.monotonic()
        try:
            measured_duration = _duration_seconds(request["path"])
            if measured_duration is not None and measured_duration > args.max_seconds + 2:
                _reply({
                    "id": request_id,
                    "ok": False,
                    "code": "too_long",
                    "error": "recording is too long",
                })
                continue
            if model is None:
                from faster_whisper import WhisperModel

                model = WhisperModel(
                    args.model,
                    device=args.device,
                    device_index=args.device_index,
                    compute_type=args.compute_type,
                )
            segments, info = model.transcribe(
                request["path"],
                language=args.language or None,
                task="transcribe",
                beam_size=5,
                vad_filter=True,
                condition_on_previous_text=False,
                word_timestamps=args.pause_punctuation,
            )
            text = _join_segments(segments, getattr(info, "language", ""),
                                  args.pause_punctuation,
                                  args.comma_gap, args.period_gap)
            if args.chinese_conversion != "none":
                if chinese_converter is None:
                    from opencc import OpenCC

                    chinese_converter = OpenCC(args.chinese_conversion + ".json")
                text = chinese_converter.convert(text)
            _reply({
                "id": request_id,
                "ok": True,
                "text": text,
                "language": getattr(info, "language", "") or "",
                "language_probability": getattr(info, "language_probability", None),
                "duration": measured_duration or getattr(info, "duration", None),
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
