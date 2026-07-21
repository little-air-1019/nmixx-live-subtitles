"""Streaming ASR: PCM chunks in -> committed Korean text segments out.

Thin wrapper over WhisperLiveKit run as a library. Uses the default
simulstreaming policy, which auto-selects the mlx-whisper backend on Apple
Silicon. Language forced to ko, model from config.WHISPER_MODEL.
"""
import asyncio
import re
from dataclasses import dataclass
from typing import AsyncGenerator, AsyncIterable, NamedTuple

from whisperlivekit import AudioProcessor, TranscriptionEngine
from whisperlivekit.config import WhisperLiveKitConfig

from nmixx_subtitles import config

# WhisperLiveKit only closes out a line on a >5s silence (its hardcoded
# MIN_DURATION_REAL_SILENCE), so a committed speech line can keep growing for
# a long time. Split the committed stream ourselves when sentence punctuation
# appears inside that growing line.
_SENTENCE_END = re.compile(r"[.!?。！？…]+")


@dataclass
class Segment:
    text: str
    start: float  # seconds from stream start; 0.0 if unavailable
    end: float


class _TextSpan(NamedTuple):
    start_char: int
    end_char: int
    start_ts: float
    end_ts: float


def build_engine(model_size: str | None = None) -> TranscriptionEngine:
    """Load the model once (weights download on first use). Reuse across streams."""
    cfg = WhisperLiveKitConfig(
        model_size=model_size or config.WHISPER_MODEL,
        lan="ko",
        pcm_input=True,
        # mlx auto-selected via default backend_policy="simulstreaming", backend="auto"
    )
    return TranscriptionEngine(config=cfg)


def _parse_ts(t: str) -> float:
    """WhisperLiveKit formats time as 'H:MM:SS' (see timed_objects.format_time)."""
    try:
        parts = [float(p) for p in t.split(":")]
    except (ValueError, AttributeError):
        return 0.0
    while len(parts) < 3:
        parts.insert(0, 0.0)
    return parts[0] * 3600 + parts[1] * 60 + parts[2]


def _committed_speech(lines: list[dict]) -> tuple[str, list[_TextSpan]]:
    """Return normalized committed speech text and char spans per source line."""
    parts: list[str] = []
    spans: list[_TextSpan] = []
    pos = 0

    for ln in lines:
        if ln.get("speaker") == -2:
            continue
        text = (ln.get("text") or "").strip()
        if not text:
            continue
        if parts:
            parts.append(" ")
            pos += 1
        start_char = pos
        parts.append(text)
        pos += len(text)
        spans.append(_TextSpan(
            start_char=start_char,
            end_char=pos,
            start_ts=_parse_ts(ln.get("start", "")),
            end_ts=_parse_ts(ln.get("end", "")),
        ))

    return "".join(parts), spans


def _time_at(span: _TextSpan, char_pos: int) -> float:
    if span.end_char <= span.start_char or span.end_ts <= span.start_ts:
        return span.start_ts
    ratio = (char_pos - span.start_char) / (span.end_char - span.start_char)
    ratio = min(1.0, max(0.0, ratio))
    return span.start_ts + ratio * (span.end_ts - span.start_ts)


def _bounds_for_chars(spans: list[_TextSpan], start_char: int, end_char: int) -> tuple[float, float]:
    if not spans:
        return 0.0, 0.0

    first = spans[0]
    last = spans[-1]
    for span in spans:
        if span.end_char > start_char:
            first = span
            break
    for span in reversed(spans):
        if span.start_char < end_char:
            last = span
            break

    return _time_at(first, start_char), _time_at(last, end_char)


def _next_emit_end(text: str, cursor: int, finalized_end: int) -> int | None:
    """Next committed text boundary to emit, or None if no boundary is ready."""
    punctuation = _SENTENCE_END.search(text, cursor)
    punct_end = punctuation.end() if punctuation else None
    line_end = finalized_end if finalized_end > cursor else None

    if line_end is not None and (punct_end is None or line_end < punct_end):
        return line_end
    return punct_end


async def transcribe(
    pcm_chunks: AsyncIterable[bytes],
    engine: TranscriptionEngine | None = None,
) -> AsyncGenerator[Segment, None]:
    """Feed PCM chunks through the ASR and yield committed segments as they finalize.

    WhisperLiveKit snapshots carry committed text in `lines`, while
    `buffer_transcription` is still unstable. With diarization off, committed
    speech often arrives as one line whose text grows for many seconds; later
    snapshots may also add silence lines around it. Treat the speech lines as
    one normalized committed text stream, keep a cursor into that stream, and
    emit every punctuation-delimited or finalized-line chunk once.
    """
    engine = engine or build_engine()
    processor = AudioProcessor(transcription_engine=engine, language="ko")
    processor.is_pcm_input = True

    results = await processor.create_tasks()

    async def feed() -> None:
        try:
            async for chunk in pcm_chunks:
                await processor.process_audio(chunk)
        finally:
            await processor.process_audio(b"")  # signal EOF -> flush + stop

    feeder = asyncio.create_task(feed())
    emitted_prefix = ""
    last_text = ""
    last_spans: list[_TextSpan] = []
    try:
        async for front in results:
            lines = front.to_dict().get("lines", [])
            text, spans = _committed_speech(lines)
            if not text:
                continue
            last_text = text
            last_spans = spans

            # Full-mode sessions should keep a stable prefix. If the backend
            # ever rewrites or prunes earlier text, avoid indexing past the new
            # snapshot and continue from the longest still-matching prefix.
            while emitted_prefix and not text.startswith(emitted_prefix):
                emitted_prefix = emitted_prefix[:-1]
            emitted_chars = len(emitted_prefix)

            finalized_end = spans[-2].end_char if len(spans) > 1 else emitted_chars
            while True:
                emit_end = _next_emit_end(text, emitted_chars, finalized_end)
                if emit_end is None or emit_end <= emitted_chars:
                    break
                chunk = text[emitted_chars:emit_end].strip()
                if chunk:
                    start, end = _bounds_for_chars(spans, emitted_chars, emit_end)
                    yield Segment(text=chunk, start=start, end=end)
                emitted_chars = emit_end
                emitted_prefix = text[:emitted_chars]
    finally:
        feeder.cancel()
        await processor.cleanup()

    # A capture/feed failure (e.g. ffmpeg died) must surface here, not vanish as a
    # silent 0-segment "clean" finish -- re-raise anything but our own cancellation.
    try:
        await feeder
    except asyncio.CancelledError:
        pass

    emitted_chars = len(emitted_prefix)
    remainder = last_text[emitted_chars:].strip()
    if remainder:
        start, end = _bounds_for_chars(last_spans, emitted_chars, len(last_text))
        yield Segment(text=remainder, start=start, end=end)
