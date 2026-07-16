"""Streaming ASR: PCM chunks in -> committed Korean text segments out.

Thin wrapper over WhisperLiveKit run as a library. Uses the default
simulstreaming policy, which auto-selects the mlx-whisper backend on Apple
Silicon. Language forced to ko, model from config.WHISPER_MODEL.
"""
import asyncio
from dataclasses import dataclass
from typing import AsyncGenerator, AsyncIterable

from whisperlivekit import AudioProcessor, TranscriptionEngine
from whisperlivekit.config import WhisperLiveKitConfig

from nmixx_transcribe import config


@dataclass
class Segment:
    text: str
    start: float  # seconds from stream start; 0.0 if unavailable
    end: float


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


async def transcribe(
    pcm_chunks: AsyncIterable[bytes],
    engine: TranscriptionEngine | None = None,
) -> AsyncGenerator[Segment, None]:
    """Feed PCM chunks through the ASR and yield committed segments as they finalize.

    WhisperLiveKit does NOT deliver committed text as append-only lines. Each
    snapshot carries all lines so far; a speaker's current sentence-run is a
    single line whose `text` grows in place (word by word) until a silence or
    punctuation boundary starts a *new* line with a later start time. Silence
    lines have speaker == -2 and empty text.

    So we key committed speech lines by their start time, keep the latest text
    per key, and emit a segment once it is finalized -- i.e. when a line with a
    later start appears (the earlier one is done) or at end of stream. That
    yields whole sentence-grouped segments, which is what the downstream
    translator wants (not word fragments). `buffer_transcription` (the
    uncommitted tail) is dropped.
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
    latest: dict[float, dict] = {}  # start_ts -> newest line dict for that run
    emitted_starts: set[float] = set()
    try:
        async for front in results:
            speech = [
                ln for ln in front.to_dict().get("lines", [])
                if ln.get("speaker") != -2 and ln.get("text", "").strip()
            ]
            for ln in speech:
                latest[_parse_ts(ln.get("start", ""))] = ln

            if not latest:
                continue
            # Any run whose start is strictly before the newest run's start is
            # finalized -- no more tokens will be appended to it.
            newest_start = max(latest)
            for start in sorted(s for s in latest if s < newest_start):
                if start in emitted_starts:
                    continue
                ln = latest[start]
                yield Segment(text=ln["text"].strip(),
                              start=start, end=_parse_ts(ln.get("end", "")))
                emitted_starts.add(start)
    finally:
        feeder.cancel()
        await processor.cleanup()

    # Flush the final in-progress run(s) after the stream ends.
    for start in sorted(latest):
        if start in emitted_starts:
            continue
        ln = latest[start]
        yield Segment(text=ln["text"].strip(),
                      start=start, end=_parse_ts(ln.get("end", "")))
