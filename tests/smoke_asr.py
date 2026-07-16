"""Smoke test: NMIXX VOD audio -> capture (file mode) -> ASR -> Korean segments.

Downloads ~2-3 min of audio from the NMIXX channel with yt-dlp (once, cached
under tests/), runs it through the pipeline, prints segments as emitted, and
reports the realtime factor. Model weights download on first run.

Run:  uv run python tests/smoke_asr.py
"""
import asyncio
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nmixx_transcribe import capture, asr  # noqa: E402

HERE = Path(__file__).resolve().parent
AUDIO = HERE / "nmixx_sample.m4a"
CHANNEL_STREAMS = "https://www.youtube.com/channel/UCnUAyD4t2LkvW68YrDh7fDg/streams"
# Known past NMIXX live VOD, offset into a chatty stretch (they talk almost
# continuously here -- the 0-150s head was ~84% silence and validated nothing).
KNOWN_VOD = "Kr0_24yx0SU"
CLIP_START = 600
CLIP_END = 780  # 180s window

# YouTube forces SABR on the default web client (yt-dlp #12482) -> "page needs
# to be reloaded". The android player client still serves plain formats.
PLAYER_CLIENT = "youtube:player_client=android"


def _candidate_videos() -> list[str]:
    """Known-good VOD first, then a scan of the channel's streams tab as fallback."""
    out = subprocess.run(
        ["yt-dlp", "--flat-playlist", "--playlist-end", "8",
         "--print", "id", CHANNEL_STREAMS],
        capture_output=True, text=True,
    )
    ids = [line.strip() for line in out.stdout.splitlines() if line.strip()]
    return [KNOWN_VOD] + [v for v in ids if v != KNOWN_VOD]


def _ensure_audio() -> None:
    if AUDIO.exists():
        print(f"[smoke] using cached audio {AUDIO}")
        return
    for vid in _candidate_videos():
        print(f"[smoke] trying {CLIP_START}-{CLIP_END}s from video {vid}")
        r = subprocess.run(
            ["yt-dlp", "-f", "bestaudio/best",
             "--extractor-args", PLAYER_CLIENT,
             "--download-sections", f"*{CLIP_START}-{CLIP_END}",
             "--force-keyframes-at-cuts",
             "-o", str(AUDIO), f"https://www.youtube.com/watch?v={vid}"],
        )
        if r.returncode == 0 and AUDIO.exists():
            return
        print(f"[smoke] {vid} failed, trying next")
    raise RuntimeError("could not download audio from any candidate video")


def _audio_duration() -> float:
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nw=1:nk=1", str(AUDIO)],
        capture_output=True, text=True, check=True,
    )
    return float(out.stdout.strip())


async def _run() -> None:
    print("[smoke] loading model (first run downloads weights)...")
    load_t0 = time.monotonic()
    engine = asr.build_engine()
    load_wall = time.monotonic() - load_t0
    print(f"[smoke] model load: {load_wall:.1f}s")

    duration = _audio_duration()
    print(f"[smoke] audio duration: {duration:.1f}s")

    hangul = 0
    n = 0
    emitted_at: list[float] = []
    t0 = time.monotonic()
    async for seg in asr.transcribe(capture.pcm_stream(str(AUDIO)), engine=engine):
        elapsed = time.monotonic() - t0
        emitted_at.append(elapsed)
        n += 1
        hangul += sum(1 for ch in seg.text if "가" <= ch <= "힣")
        print(f"[+{elapsed:6.1f}s] [{seg.start:6.1f}-{seg.end:6.1f}] {seg.text}")
    wall = time.monotonic() - t0

    rtf = wall / duration if duration else float("inf")
    spread = emitted_at[-1] - emitted_at[0] if len(emitted_at) >= 2 else 0.0
    print("\n[smoke] === summary ===")
    print(f"segments emitted : {n}")
    print(f"hangul chars     : {hangul}")
    print(f"model load       : {load_wall:.1f}s")
    print(f"audio duration   : {duration:.1f}s")
    print(f"wall clock       : {wall:.1f}s")
    print(f"emission spread  : {spread:.1f}s")
    print(f"realtime factor  : {rtf:.2f}x  ({'FASTER' if rtf < 1 else 'SLOWER'} than real time)")

    # A talk-heavy 180s clip must yield a real transcript, not one stray token,
    # must emit during transcription, and must beat real time (or live
    # transcription is impossible).
    assert n >= 5, f"too few segments ({n}); expected >= 5 on a chatty clip"
    assert hangul >= 100, f"too little Korean ({hangul} hangul); expected >= 100"
    assert emitted_at and emitted_at[0] < max(0.0, wall - 10.0), "first segment emitted only at stream end"
    assert spread >= 10.0, f"segments clustered at end (spread={spread:.1f}s); expected >= 10s"
    assert rtf < 1.0, f"slower than real time (rtf={rtf:.2f})"
    print("[smoke] PASS")


if __name__ == "__main__":
    _ensure_audio()
    asyncio.run(_run())
