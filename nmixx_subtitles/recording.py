"""Local evidence capture. Disk I/O never waits in the live audio/token path."""
import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import queue
import shutil
import threading
import time
import uuid
import wave

from nmixx_subtitles import config

log = logging.getLogger(__name__)
SAMPLE_RATE = 16_000
_current = ContextVar("quality_session", default=None)
_connection = ContextVar("quality_connection", default=None)
_STOP = object()


class AudioFrame(bytes):
    """PCM remains bytes-compatible while retaining its local sample position."""
    def __new__(cls, data, start_sample):
        obj = super().__new__(cls, data)
        obj.start_sample = start_sample
        return obj


def emit(kind, **fields):
    recorder = _current.get()
    if recorder is not None:
        recorder.emit(kind, connection_id=_connection.get(), **fields)


def connection_started(provider, *, resumed=False):
    identifier = uuid.uuid4().hex
    _connection.set(identifier)
    emit("connection_started", provider=provider, resumed=resumed)
    return identifier


def audio_sent(chunk, connection_start_sample):
    emit("audio_sent", input_start_sample=getattr(chunk, "start_sample", None),
         samples=len(chunk) // 2, connection_start_sample=connection_start_sample)


def audio_send_started(chunk, connection_start_sample):
    emit("audio_send_attempt", input_start_sample=getattr(chunk, "start_sample", None),
         samples=len(chunk) // 2, connection_start_sample=connection_start_sample)


def audio_dropped(chunk, reason="provider_queue_full"):
    emit("audio_dropped", input_start_sample=getattr(chunk, "start_sample", None),
         samples=len(chunk) // 2, reason=reason)


async def observe_audio(pcm):
    recorder = _current.get()
    async for chunk in pcm:
        if recorder is not None and chunk:
            frame = AudioFrame(chunk, recorder.input_samples)
            recorder.input_samples += len(chunk) // 2
            recorder.enqueue("audio_received", pcm=bytes(chunk),
                             input_start_sample=frame.start_sample, samples=len(chunk) // 2)
            yield frame
        else:
            yield chunk


def soniox_response(response):
    # Explicit allowlist: never persist arbitrary API error text or configuration/key.
    keys = ("text", "start_ms", "end_ms", "confidence", "is_final", "speaker",
            "translation_status", "language", "source_language")
    for token in response.get("tokens", []):
        emit("soniox_token", **{k: token[k] for k in keys if k in token})
    emit("soniox_progress", **{k: response[k] for k in (
        "final_audio_proc_ms", "total_audio_proc_ms", "finished", "error_code",
    ) if k in response})


def soniox_config(settings):
    emit("provider_config", provider="soniox", settings={k: settings[k] for k in (
        "model", "audio_format", "sample_rate", "num_channels", "language_hints",
        "enable_language_identification", "enable_endpoint_detection", "max_endpoint_delay_ms",
        "translation", "context",
    ) if k in settings})


class SessionRecorder:
    def __init__(self, root, metadata, *, max_queue=2048, max_bytes=0):
        self.root = Path(root)
        self.directory = self.root / (datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:8])
        self.metadata = metadata
        self.queue = queue.Queue(maxsize=max_queue)
        self.max_bytes = max_bytes
        self.input_samples = 0
        self.dropped_records = 0
        self.failure = None
        self.closed = False
        self.sequence = 0
        self.thread = None
        self.pipeline_failed = False

    def start(self):
        try:
            self.directory.mkdir(parents=True, mode=0o700)
            self.directory.chmod(0o700)
            self.manifest = {
                "schema_version": 1, "session_id": self.directory.name,
                "started_at_utc": datetime.now(timezone.utc).isoformat(),
                "status": "recording", "sample_rate": SAMPLE_RATE,
                "channels": 1, "sample_width": 2, "metadata": self.metadata,
                "timing_note": "audio_received is local PCM receipt, not original broadcast speech time",
            }
            self._manifest()
            self.thread = threading.Thread(target=self._write, daemon=True, name="subtitle-evidence")
            self.thread.start()
            log.info("Quality recording: %s", self.directory)
        except OSError as exc:
            self.failure = type(exc).__name__
            log.warning("Quality recording unavailable (%s); subtitles continue", self.failure)

    def _manifest(self):
        target = self.directory / "manifest.json"
        temp = target.with_suffix(".tmp")
        temp.write_text(json.dumps(self.manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(target)

    def emit(self, kind, **fields):
        self.enqueue(kind, **fields)

    def enqueue(self, kind, **fields):
        if kind == "pipeline_error":
            self.pipeline_failed = True
        if self.closed or self.failure:
            return
        self.sequence += 1
        event = dict(seq=self.sequence, kind=kind, monotonic_ns=time.monotonic_ns(),
                     wall_time_utc=datetime.now(timezone.utc).isoformat(), **fields)
        try:
            self.queue.put_nowait(event)
        except queue.Full:
            self.dropped_records += 1
            if self.dropped_records == 1:
                log.warning("Quality writer queue full; evidence may have gaps, subtitles continue")

    def _write(self):
        written = 0
        try:
            with (self.directory / "events.jsonl").open("w", encoding="utf-8") as events, \
                    wave.open(str(self.directory / "audio.wav"), "wb") as audio:
                audio.setparams((1, 2, SAMPLE_RATE, 0, "NONE", "not compressed"))
                while True:
                    try:
                        item = self.queue.get(timeout=0.2)
                    except queue.Empty:
                        if self.closed:
                            break
                        continue
                    if item is _STOP:
                        break
                    pcm = item.pop("pcm", None)
                    if pcm is not None:
                        item["wav_start_sample"] = audio.getnframes()
                        audio.writeframes(pcm)  # Updates WAV header after every chunk (crash recovery).
                        written += len(pcm)
                    line = json.dumps(item, ensure_ascii=False) + "\n"
                    events.write(line)
                    events.flush()
                    written += len(line.encode("utf-8"))
                    if self.max_bytes and written >= self.max_bytes:
                        self.failure = "session_size_limit"
                        log.warning("Quality recording reached storage budget; subtitles continue")
                        break
        except Exception as exc:
            self.failure = type(exc).__name__
            log.warning("Quality writer stopped (%s); subtitles continue", self.failure)

    def finish(self, status):
        self.closed = True
        if self.thread is None:
            return
        if self.thread.is_alive():
            try:
                self.queue.put_nowait(_STOP)
            except queue.Full:
                pass  # Writer also exits when closed and its queue has drained.
            self.thread.join(timeout=5)
        if self.thread.is_alive():
            self.failure = "writer_shutdown_timeout"
            log.warning("Quality writer still stopping; rebuild report later")
            return
        try:
            self.manifest.update(status="error" if self.pipeline_failed else status, stopped_at_utc=datetime.now(timezone.utc).isoformat(),
                                 input_samples=self.input_samples, dropped_records=self.dropped_records,
                                 recording_error=self.failure, evidence_complete=not self.failure and not self.dropped_records)
            self._manifest()
            from nmixx_subtitles.quality_review import analyze_session
            analyze_session(self.directory)
            from nmixx_subtitles.quality_review import summarize
            summarize(self.root)
            log.info("Quality review: %s", self.directory / "review.html")
        except Exception as exc:
            log.warning("Quality report incomplete (%s); rebuild with quality_review CLI", type(exc).__name__)


def prune_sessions(root, days, max_bytes):
    """Only delete our completed, schema-marked session directories, never active ones."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    eligible = []
    total = 0
    for directory in root.iterdir():
        if not directory.is_dir() or directory.is_symlink():
            continue
        try:
            manifest = json.loads((directory / "manifest.json").read_text())
            if manifest.get("schema_version") != 1 or manifest.get("session_id") != directory.name:
                continue
            size = sum(p.stat().st_size for p in directory.rglob("*") if p.is_file() and not p.is_symlink())
            total += size
            if manifest.get("status") not in ("completed", "cancelled", "error"):
                continue
            stamp = datetime.fromisoformat(manifest["started_at_utc"]).timestamp()
            eligible.append((stamp, directory, size))
        except (OSError, ValueError, KeyError):
            continue
    for stamp, directory, size in sorted(eligible):
        if (days > 0 and time.time() - stamp > days * 86400) or (max_bytes > 0 and total >= max_bytes * 0.8):
            shutil.rmtree(directory)
            total -= size
    return max(0, max_bytes - total) if max_bytes > 0 else 0


def _start_session(source, provider, metadata):
    try:
        budget = prune_sessions(config.QUALITY_CAPTURE_DIR, config.QUALITY_RETENTION_DAYS,
                                config.QUALITY_MAX_BYTES)
        if config.QUALITY_MAX_BYTES > 0 and budget < 1024 * 1024:
            log.warning("Quality storage full; subtitles continue without recording")
            return None
        glossary = (config.ROOT / "glossary.md").read_text(encoding="utf-8")
        implementation = hashlib.sha256()
        for source_file in sorted(Path(__file__).parent.glob("*.py")):
            implementation.update(source_file.name.encode() + b"\0" + source_file.read_bytes())
        from nmixx_subtitles.discord import FLUSH_AGE_S, FLUSH_CHARS, DISCORD_MAX_CHARS
        recorder = SessionRecorder(config.QUALITY_CAPTURE_DIR, {
            "source": source, "provider": provider, **metadata,
            "model": config.SONIOX_MODEL if provider == "soniox" else config.GEMINI_LIVE_MODEL,
            "soniox_translate_english": config.SONIOX_TRANSLATE_ENGLISH,
            "text_translation_model": config.TEXT_TRANSLATION_MODEL,
            "glossary": glossary, "glossary_sha256": hashlib.sha256(glossary.encode()).hexdigest(),
            "opencc": "s2twp" if provider == "soniox" else None,
            "implementation_sha256": implementation.hexdigest(),
            "discord_flush_age_s": FLUSH_AGE_S, "discord_flush_chars": FLUSH_CHARS,
            "discord_max_chars": DISCORD_MAX_CHARS,
        }, max_bytes=budget)
        recorder.start()
        return recorder
    except Exception as exc:
        log.warning("Quality recorder setup failed (%s); subtitles continue", type(exc).__name__)
        return None


@asynccontextmanager
async def quality_session(source, provider, **metadata):
    if not config.QUALITY_CAPTURE_ENABLED:
        yield None
        return
    # Shield startup so cancellation cannot orphan a newly created writer thread.
    startup = asyncio.create_task(asyncio.to_thread(_start_session, source, provider, metadata))
    try:
        recorder = await asyncio.shield(startup)
    except asyncio.CancelledError:
        recorder = await startup
        if recorder is not None:
            await asyncio.to_thread(recorder.finish, "cancelled")
        raise
    token = _current.set(recorder)
    status = "completed"
    try:
        yield recorder
    except BaseException as exc:
        status = "cancelled" if isinstance(exc, asyncio.CancelledError) else "error"
        emit("session_exception", exception_type=type(exc).__name__)
        raise
    finally:
        _current.reset(token)
        if recorder is not None:
            await asyncio.to_thread(recorder.finish, status)
