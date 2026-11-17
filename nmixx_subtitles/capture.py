"""Audio capture: YouTube live/VOD or a local file -> 16 kHz mono s16le PCM.

Spawns yt-dlp to pull the stream, pipes it through ffmpeg to raw PCM, and
yields fixed-size chunks off an asyncio stream.
"""
import asyncio
import logging
import os
import shutil
from pathlib import Path
from typing import AsyncGenerator
from urllib.parse import urlsplit

SAMPLE_RATE = 16000
CHUNK_BYTES = SAMPLE_RATE * 2 // 10  # 100 ms of mono s16le

FFMPEG = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"

_log = logging.getLogger(__name__)


def _is_local_file(source: str) -> bool:
    return Path(source).exists()


def _youtube_url(source: str) -> str:
    """Accept a full URL or a bare 11-char video ID."""
    if source.startswith("http://") or source.startswith("https://"):
        return source
    return f"https://www.youtube.com/watch?v={source}"


async def _drain_stderr(proc: asyncio.subprocess.Process, name: str) -> None:
    """Keep a child's stderr pipe from filling and blocking it. Discard output."""
    if proc.stderr is None:
        return
    while True:
        line = await proc.stderr.readline()
        if not line:
            break
        _log.debug("[%s] %s", name, line.decode(errors="replace").rstrip())


async def _spawn_local_ffmpeg(path: str) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        FFMPEG, "-nostdin", "-i", path,
        "-f", "s16le", "-acodec", "pcm_s16le", "-ar", str(SAMPLE_RATE), "-ac", "1",
        "-loglevel", "error", "pipe:1",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )


async def _spawn_stream_pipeline(source: str) -> asyncio.subprocess.Process:
    """Resolve the live HLS manifest with yt-dlp, then read it with ffmpeg -> PCM.

    yt-dlp's own downloader (-o -) hands ffmpeg the whole DVR playlist from its
    first segment, so a live stream that's been running a while starts minutes
    behind and can never catch up at ~1x. Instead resolve just the manifest URL
    (-g) and let our ffmpeg open it directly: ffmpeg starts a live HLS playlist
    near the live edge, so subtitles track the current moment.
    """
    url = _youtube_url(source)

    if not shutil.which("yt-dlp"):
        raise RuntimeError("yt-dlp not found on PATH")

    # streamlink's YouTube plugin misclassifies these live URLs as VOD and exits 1,
    # so we use yt-dlp for extraction. android player_client avoids yt-dlp's
    # web-client SABR format-resolution error (github.com/yt-dlp/yt-dlp/issues/12482)
    # that otherwise 403s live segment fetches; drop it once fixed upstream.
    resolver = await asyncio.create_subprocess_exec(
        "yt-dlp", "-f", "bestaudio/best", "--extractor-args", "youtube:player_client=android",
        "-g", url,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, err = await resolver.communicate()
    if resolver.returncode != 0 or not out.strip():
        raise RuntimeError(f"yt-dlp failed to resolve manifest for {source!r}: {err.decode(errors='replace')[:300]}")
    manifest = out.decode().strip().splitlines()[0]

    # HLS defaults to starting three segments from the end. YouTube currently
    # uses five-second segments, so start at the latest available segment to
    # avoid adding another ten seconds before ASR even sees the audio.
    path = urlsplit(manifest).path
    input_options = ["-live_start_index", "-1"] if ".m3u8" in path or "/manifest/hls_" in path else []

    return await asyncio.create_subprocess_exec(
        FFMPEG, "-nostdin", *input_options, "-i", manifest,
        "-f", "s16le", "-acodec", "pcm_s16le", "-ar", str(SAMPLE_RATE), "-ac", "1",
        "-loglevel", "error", "pipe:1",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )


async def pcm_stream(
    source: str,
    chunk_bytes: int = CHUNK_BYTES,
) -> AsyncGenerator[bytes, None]:
    """Yield raw 16 kHz mono s16le PCM chunks from a YouTube URL/ID or local file.

    Terminates when the source ends (VOD/file). For a live stream it runs until
    the upstream stops or the consumer stops pulling.
    """
    if _is_local_file(source):
        ffmpeg = await _spawn_local_ffmpeg(source)
    else:
        ffmpeg = await _spawn_stream_pipeline(source)

    assert ffmpeg.stdout is not None
    stderr_task = asyncio.create_task(_drain_stderr(ffmpeg, "ffmpeg"))

    saw_any = False
    reached_eof = False
    try:
        while True:
            chunk = await ffmpeg.stdout.readexactly(chunk_bytes)
            saw_any = True
            yield chunk
    except asyncio.IncompleteReadError as e:
        reached_eof = True
        if e.partial:
            saw_any = True
            yield e.partial  # final short chunk at EOF
    finally:
        stderr_task.cancel()
        await asyncio.gather(stderr_task, return_exceptions=True)
        if ffmpeg.returncode is None:
            try:
                ffmpeg.terminate()
            except ProcessLookupError:
                pass
        # Drain both pipes even when the consumer stops early (e.g. API rejects the
        # request). wait() alone can deadlock on a full stdout pipe and leave transports
        # alive after the event loop closes. The stderr reader has relinquished its pipe.
        try:
            await asyncio.wait_for(ffmpeg.communicate(), timeout=5)
        except asyncio.TimeoutError:
            try:
                ffmpeg.kill()
            except ProcessLookupError:
                pass
            await ffmpeg.communicate()
        # ffmpeg exiting non-zero without ever producing audio means the pull failed
        # (expired manifest, geo-block, 403). Surface it instead of letting the caller
        # read "0 chunks" as a normal empty stream. A non-zero code after we already
        # got audio is just our own terminate() above -- not a failure.
        if reached_eof and not saw_any and ffmpeg.returncode not in (None, 0):
            raise RuntimeError(f"capture ffmpeg exited {ffmpeg.returncode} for source={source!r}")
