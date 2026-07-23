"""Audio capture: YouTube live/VOD or a local file -> 16 kHz mono s16le PCM.

Spawns yt-dlp to pull the stream, pipes it through ffmpeg to raw PCM, and
yields fixed-size chunks off an asyncio stream.
"""
import asyncio
import logging
import os
import shutil
from pathlib import Path
from typing import AsyncGenerator, Optional

SAMPLE_RATE = 16000
CHUNK_BYTES = SAMPLE_RATE * 2 * 1  # 1 s of mono s16le

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


async def _spawn_stream_pipeline(source: str) -> tuple[asyncio.subprocess.Process, asyncio.subprocess.Process]:
    """puller (streamlink|yt-dlp) stdout -> ffmpeg stdin -> PCM stdout.

    Prefer streamlink for live HLS; fall back to yt-dlp. Both write the
    container to stdout, ffmpeg transcodes to PCM.
    """
    url = _youtube_url(source)

    def _yt_dlp_cmd() -> list[str]:
        # ponytail: android player_client avoids yt-dlp's web-client SABR format-resolution
        # error (github.com/yt-dlp/yt-dlp/issues/12482) that otherwise 403s live segment
        # fetches; upgrade yt-dlp and drop this once that's fixed upstream.
        return ["yt-dlp", "-f", "bestaudio/best", "-o", "-", "--extractor-args", "youtube:player_client=android", url]

    # streamlink's YouTube plugin misclassifies these live URLs as VOD and exits 1
    # immediately, poisoning ffmpeg's pipe (a race that surfaced as flaky
    # "capture puller exited 1"). yt-dlp handles YouTube live reliably, so use it
    # directly rather than trying streamlink first and racing on the fallback.
    if not shutil.which("yt-dlp"):
        raise RuntimeError("yt-dlp not found on PATH")

    # asyncio.subprocess can't wire one child's StreamReader directly into another
    # child's stdin (it needs a real fd) -> use an actual OS pipe between them.
    r_fd, w_fd = os.pipe()
    try:
        puller = await asyncio.create_subprocess_exec(
            *_yt_dlp_cmd(), stdout=w_fd, stderr=asyncio.subprocess.PIPE,
        )

        ffmpeg = await asyncio.create_subprocess_exec(
            FFMPEG, "-nostdin", "-i", "pipe:0",
            "-f", "s16le", "-acodec", "pcm_s16le", "-ar", str(SAMPLE_RATE), "-ac", "1",
            "-loglevel", "error", "pipe:1",
            stdin=r_fd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
    finally:
        # Parent must close both ends after handing them to the children, or EOF
        # never propagates (ffmpeg would block forever waiting for more input).
        os.close(w_fd)
        os.close(r_fd)
    return puller, ffmpeg


async def pcm_stream(
    source: str,
    chunk_bytes: int = CHUNK_BYTES,
) -> AsyncGenerator[bytes, None]:
    """Yield raw 16 kHz mono s16le PCM chunks from a YouTube URL/ID or local file.

    Terminates when the source ends (VOD/file). For a live stream it runs until
    the upstream stops or the consumer stops pulling.
    """
    puller: Optional[asyncio.subprocess.Process] = None
    if _is_local_file(source):
        ffmpeg = await _spawn_local_ffmpeg(source)
    else:
        puller, ffmpeg = await _spawn_stream_pipeline(source)

    assert ffmpeg.stdout is not None
    stderr_tasks = [asyncio.create_task(_drain_stderr(ffmpeg, "ffmpeg"))]
    if puller is not None:
        stderr_tasks.append(asyncio.create_task(_drain_stderr(puller, "puller")))

    failure: Exception | None = None
    try:
        while True:
            chunk = await ffmpeg.stdout.readexactly(chunk_bytes)
            yield chunk
    except asyncio.IncompleteReadError as e:
        if e.partial:
            yield e.partial  # final short chunk at EOF
    finally:
        for proc in (ffmpeg, puller):
            if proc is not None and proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    proc.kill()
        for t in stderr_tasks:
            t.cancel()
        # A puller that exited non-zero (bad video id, geo-block, 403, etc.) produces
        # an empty pipe that looks just like a clean, silent EOF to ffmpeg -- surface
        # it instead of letting the caller read "0 chunks" as a normal empty stream.
        if puller is not None and puller.returncode not in (None, 0):
            failure = RuntimeError(f"capture puller exited {puller.returncode} for source={source!r}")
    if failure is not None:
        raise failure
