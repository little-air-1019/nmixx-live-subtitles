"""Manually subtitle iPhone Mirroring audio; bounded to ten minutes by default."""
import argparse
import asyncio
import audioop
import fcntl
import logging
import os
from pathlib import Path
import signal
import subprocess
from contextlib import aclosing

from nmixx_subtitles import config
from nmixx_subtitles.recording import quality_session
from nmixx_subtitles.capture import FFMPEG, CHUNK_BYTES
from nmixx_subtitles.discord import DiscordPoster
from nmixx_subtitles.subtitles import stream_transcripts, subtitle_transform, validate_provider

log = logging.getLogger(__name__)
CACHE = Path.home() / "Library/Caches/nmixx-live-subtitles"


def project_discord_webhook() -> str:
    """Pin this manual session's destination to the project, never the host shell."""
    env_file = config.ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            key, separator, value = line.strip().partition("=")
            if separator and key.strip() == "DISCORD_WEBHOOK_URL":
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                    value = value[1:-1]
                if value.startswith(("https://", "http://")):
                    return value
                break
    raise ValueError("A valid DISCORD_WEBHOOK_URL is required in the project .env")


def build_capture() -> Path:
    source = Path(__file__).parent / "native/mirror_audio.swift"
    binary = CACHE / "mirror-audio"
    if not binary.exists() or source.stat().st_mtime > binary.stat().st_mtime:
        subprocess.run(["swiftc", "-parse-as-library", str(source), "-o", str(binary)], check=True)
    return binary


async def stop_process(proc):
    if proc.returncode is None:
        try:
            proc.terminate()
        except ProcessLookupError:
            pass
    try:
        await asyncio.wait_for(proc.communicate(), timeout=5)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()


async def mirror_pcm(binary: Path, seconds: int):
    """Yield 100 ms PCM frames; stop on disconnect or 90 seconds of silence."""
    read_fd, write_fd = os.pipe()
    procs = []
    try:
        ffmpeg = await asyncio.create_subprocess_exec(
            FFMPEG, "-nostdin", "-hide_banner", "-loglevel", "error",
            "-f", "f32le", "-ar", "48000", "-ac", "1", "-i", "pipe:0",
            "-f", "s16le", "-ar", "16000", "-ac", "1", "pipe:1",
            stdin=read_fd, stdout=asyncio.subprocess.PIPE,
        )
        procs.append(ffmpeg)
        capture = await asyncio.create_subprocess_exec(str(binary), stdout=write_fd)
        procs.append(capture)
        os.close(read_fd)
        os.close(write_fd)
        read_fd = write_fd = -1
        heard_audio = False
        quiet_frames = 0
        for _ in range(seconds * 10):
            try:
                chunk = await asyncio.wait_for(ffmpeg.stdout.readexactly(CHUNK_BYTES), timeout=15)
            except asyncio.IncompleteReadError as exc:
                raise RuntimeError("Mirroring audio capture ended unexpectedly") from exc
            if audioop.rms(chunk, 2) >= 16:
                quiet_frames = 0
                if not heard_audio:
                    heard_audio = True
                    log.info("Live audio signal confirmed (16 kHz mono PCM)")
            else:
                quiet_frames += 1
            if quiet_frames >= (900 if heard_audio else 150):
                raise RuntimeError("Mirroring audio is silent; stopping this session")
            yield chunk
    finally:
        for fd in (read_fd, write_fd):
            if fd != -1:
                os.close(fd)
        # Stop capture before draining ffmpeg, including on provider failures/cancellation.
        for proc in reversed(procs):
            await stop_process(proc)


class SessionPoster(DiscordPoster):
    def __init__(self, *, discord: bool, provider: str):
        super().__init__(
            project_discord_webhook() if discord else "http://unused.invalid",
            transform=subtitle_transform(provider),
        )
        self.discord = discord
        self._record_destination = "discord" if discord else "console"
        self.delivered = 0

    async def _post(self, content: str) -> None:
        if self.discord:
            await super()._post(content)
        self.delivered += 1
        log.info("%s #%d: %s", "Discord delivered" if self.discord else "Subtitle", self.delivered, content)


async def run(binary: Path, *, seconds: int, provider: str, discord: bool):
    async with quality_session("fans-mirroring", provider, discord=discord):
        await _run(binary, seconds=seconds, provider=provider, discord=discord)


async def _run(binary: Path, *, seconds: int, provider: str, discord: bool):
    poster = SessionPoster(discord=discord, provider=provider)
    try:
        log.info("FANS session starting: provider=%s max_seconds=%d discord=%s", provider, seconds, discord)
        async with asyncio.timeout(seconds + 45):
            async with aclosing(mirror_pcm(binary, seconds)) as audio:
                # Confirm capture can produce a frame before opening the paid API.
                first = await anext(audio)

                async def frames():
                    yield first
                    async for chunk in audio:
                        yield chunk

                async with aclosing(frames()) as pcm:
                    async with aclosing(stream_transcripts(pcm, provider=provider)) as events:
                        async for kind, text in events:
                            if kind == "source":
                                log.info("Source: %s", text)
                            else:
                                await poster.send(text)
    finally:
        try:
            await asyncio.wait_for(poster.close(), timeout=15)
        finally:
            log.info("FANS session stopped; %d subtitle messages delivered", poster.delivered)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=600)
    parser.add_argument("--provider", choices=("gemini", "soniox"), default=config.SUBTITLE_PROVIDER)
    parser.add_argument("--discord", action="store_true", help="Send subtitles to the configured Discord webhook")
    args = parser.parse_args()
    if not 1 <= args.seconds <= 7200:
        parser.error("--seconds must be between 1 and 7200")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    validate_provider(args.provider)
    if args.discord:
        try:
            project_discord_webhook()
        except ValueError as exc:
            parser.error(str(exc))
        log.info("Discord destination loaded from project file: %s", config.ROOT / ".env")
    CACHE.mkdir(parents=True, exist_ok=True)
    with (CACHE / "mirror.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.exit(1, "A mirroring subtitle session is already running\n")
        lock.seek(0)
        lock.truncate()
        lock.write(str(os.getpid()))
        lock.flush()
        binary = build_capture()

        async def interruptible():
            task = asyncio.current_task()
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGTERM, task.cancel)
            await run(binary, seconds=args.seconds, provider=args.provider, discord=args.discord)

        try:
            asyncio.run(interruptible())
        except (KeyboardInterrupt, asyncio.CancelledError):
            log.info("Stopped by user")
        except Exception as exc:
            # Avoid echoing webhook URLs or arbitrary provider exception bodies.
            log.error("FANS session failed (%s)", type(exc).__name__)
            raise SystemExit(1)


if __name__ == "__main__":
    main()
