"""16 kHz PCM -> Soniox final source and subtitle events (no synthesized audio)."""
import asyncio
import json
import logging
import re
import uuid
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass

from websockets.asyncio.client import connect

from nmixx_subtitles import config
from nmixx_subtitles import recording
from nmixx_subtitles.text_translate import TranscriptRouter

log = logging.getLogger(__name__)
# WebSocket DEBUG logging includes the first frame, which contains the API key.
wire_log = logging.getLogger(__name__ + ".wire")
wire_log.setLevel(logging.WARNING)
PCM_BYTES_PER_SECOND = 32_000
QUEUE_CHUNKS = 100  # capture produces 100 ms frames: retain at most 10 seconds
MAX_RECONNECT_ATTEMPTS = 5
DRAIN_TIMEOUT_S = 10.0
KEEPALIVE_S = 10.0
ROTATE_AFTER_S = 295 * 60  # below Soniox's 300-minute session limit
_END = object()


def build_context() -> dict:
    glossary = (config.ROOT / "glossary.md").read_text()
    mappings = []
    for line in glossary.splitlines():
        if " → " not in line or line.startswith(("#", "「")):
            continue
        source, target = line.split(" → ", 1)
        target = re.split(r"\s*[（(]", target, maxsplit=1)[0].strip()
        for variant in source.split(" / "):
            mappings.append({"source": variant.strip(), "target": target})
    return {
        "general": [{"key": "topic", "value": "NMIXX livestream with Korean and English speech"}],
        "text": "NMIXX members: Lily, Haewon, Sullyoon, Bae, Jiwoo, Kyujin. "
                "Fans are NSWER. Preserve song titles and honorifics. "
                "Chinese subtitles use Traditional Chinese as used in Taiwan.",
        "terms": list(dict.fromkeys(item["source"] for item in mappings)),
        "translation_terms": mappings,
    }


def build_live_config(*, translate_english: bool, reference_id: str) -> dict:
    result = {
        "api_key": config.SONIOX_API_KEY,
        "model": config.SONIOX_MODEL,
        "audio_format": "pcm_s16le",
        "sample_rate": 16_000,
        "num_channels": 1,
        "language_hints": ["ko", "en", "zh"],
        "enable_language_identification": True,
        "enable_endpoint_detection": True,
        "max_endpoint_delay_ms": 1000,
        "client_reference_id": reference_id,
        "context": build_context(),
    }
    if translate_english:
        result["translation"] = {"type": "one_way", "target_language": "zh"}
    else:
        # Native Korean translation avoids a second request on the common path.
        # English passes through; only languages outside ko/en/zh use text fallback.
        result["translation"] = {"type": "two_way", "language_a": "ko", "language_b": "zh"}
    return result


def final_events(response: dict, *, translate_english: bool) -> list[tuple[str, str]]:
    """Non-final tokens are replaceable hypotheses, never append them to Discord."""
    events = []
    for token in response.get("tokens", []):
        text = token.get("text", "")
        if not token.get("is_final") or not text or text in ("<end>", "<fin>"):
            continue
        language = token.get("language", "").split("-")[0]
        status = token.get("translation_status", "none")
        if status == "translation":
            if language == "zh":
                events.append(("subtitle", text))
            elif not (language == "ko" and not translate_english):
                log.warning("Soniox returned unexpected translation language=%s", language)
        else:
            events.append(("source", text))
            if language == "zh" or (
                not translate_english and status == "none" and language not in ("", "ko")
            ):
                events.append(("subtitle", text))
    return events


class SonioxAPIError(RuntimeError):
    def __init__(self, response: dict):
        self.code = response["error_code"]
        self.error_type = response.get("error_type", "unknown")
        # Do not echo the request or arbitrary server error_message (may include secrets).
        message = f"Soniox error {self.code} ({self.error_type})"
        if self.code == 402:
            message += "; check API balance/budget in console.soniox.com (App credits do not fund the API)"
        super().__init__(message)

    @property
    def retryable(self) -> bool:
        return self.code in (408, 429, 500, 502, 503, 504) or (
            self.code == 413 and self.error_type == "max_duration_reached"
        )


async def _pump_audio(pcm: AsyncIterable[bytes], audio: asyncio.Queue) -> None:
    loop = asyncio.get_running_loop()
    started = loop.time()
    seconds = dropped = 0.0
    async for chunk in pcm:
        if not chunk:  # an empty frame is a protocol end-of-stream, not audio
            continue
        if audio.full():
            discarded = audio.get_nowait()
            recording.audio_dropped(discarded)
            dropped += len(discarded) / PCM_BYTES_PER_SECOND
            log.warning("Soniox audio queue full; dropped %.1fs total to stay near live", dropped)
        audio.put_nowait(chunk)
        seconds += len(chunk) / PCM_BYTES_PER_SECOND
        await asyncio.sleep(max(0, started + seconds - loop.time()))
    await audio.put(_END)


@dataclass
class _InputState:
    pending: bytes | None = None
    ended: bool = False


async def _send_audio(ws, audio: asyncio.Queue, state: _InputState, ending: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    started = last_sent = loop.time()
    sent_samples = 0
    while True:
        if loop.time() - started >= ROTATE_AFTER_S:
            ending.set()
            await ws.send("")
            return
        if state.pending is None:
            try:
                chunk = await asyncio.wait_for(audio.get(), timeout=0.1)
            except asyncio.TimeoutError:
                if loop.time() - last_sent >= KEEPALIVE_S:
                    await ws.send(json.dumps({"type": "keepalive"}))
                    last_sent = loop.time()
                continue
            if chunk is _END:
                # Mark before send: EOF cannot be consumed twice after a socket failure.
                state.ended = True
                ending.set()
                await ws.send("")
                return
            state.pending = chunk
        recording.audio_send_started(state.pending, sent_samples)
        await ws.send(state.pending)
        recording.audio_sent(state.pending, sent_samples)
        sent_samples += len(state.pending) // 2
        state.pending = None
        last_sent = loop.time()


async def _receive(ws, events: asyncio.Queue, translate_english: bool) -> None:
    router = None if translate_english else TranscriptRouter()
    try:
        async for raw in ws:
            response = json.loads(raw)
            recording.soniox_response(response)
            if response.get("error_code") is not None:
                raise SonioxAPIError(response)
            routed = (router.feed(response) if router is not None else
                      final_events(response, translate_english=True))
            for event in routed:
                await events.put(event)
            if response.get("finished"):
                return
    finally:
        if router is not None and not asyncio.current_task().cancelling():
            for event in router.flush():
                await events.put(event)
    raise ConnectionError("Soniox connection closed before finished response")


async def _session(ws, audio, events, state, translate_english) -> None:
    ending = asyncio.Event()
    sender = asyncio.create_task(_send_audio(ws, audio, state, ending))
    receiver = asyncio.create_task(_receive(ws, events, translate_english))
    try:
        done, _ = await asyncio.wait({sender, receiver}, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        if not ending.is_set():
            raise ConnectionError("Soniox finished before audio input ended")
        # Sending EOF isn't completion: final subtitles and API errors can follow it.
        await asyncio.wait_for(asyncio.gather(sender, receiver), timeout=DRAIN_TIMEOUT_S)
    finally:
        for task in (sender, receiver):
            if not task.done():
                task.cancel()
        await asyncio.gather(sender, receiver, return_exceptions=True)


async def _run_connections(audio, events, connector, translate_english, reconnect_delay_s) -> None:
    state = _InputState()
    failures = 0
    while True:
        started = asyncio.get_running_loop().time()
        try:
            reference = "nmixx-" + uuid.uuid4().hex
            async with connector(
                config.SONIOX_WEBSOCKET_URL, open_timeout=10, close_timeout=5,
                max_size=4 * 1024 * 1024, logger=wire_log,
            ) as ws:
                recording.connection_started("soniox")
                live_config = build_live_config(translate_english=translate_english, reference_id=reference)
                recording.soniox_config(live_config)
                await ws.send(json.dumps(live_config))
                log.info("Soniox connected: model=%s reference=%s", config.SONIOX_MODEL, reference)
                await _session(ws, audio, events, state, translate_english)
            if state.ended:
                return
            failures = 0  # graceful planned rotation
            log.info("Soniox rotating connection before session duration limit")
        except Exception as exc:
            recording.emit("connection_error", exception_type=type(exc).__name__)
            if state.ended or isinstance(exc, SonioxAPIError) and not exc.retryable:
                raise
            # A handshake alone doesn't reset retries (e.g. persistent 503 responses).
            if asyncio.get_running_loop().time() - started >= 60:
                failures = 0
            failures += 1
            log.warning("Soniox connection failed (%d/%d): %s; unfinalized text may be lost",
                        failures, MAX_RECONNECT_ATTEMPTS, type(exc).__name__)
            if failures >= MAX_RECONNECT_ATTEMPTS:
                raise
        await asyncio.sleep(reconnect_delay_s * max(1, failures))


async def stream_transcripts(
    pcm: AsyncIterable[bytes], *, connector=None,
    translate_english: bool | None = None, reconnect_delay_s: float = 1.0,
) -> AsyncIterator[tuple[str, str]]:
    if not config.SONIOX_API_KEY and connector is None:
        raise ValueError("SONIOX_API_KEY is required")
    translate_english = config.SONIOX_TRANSLATE_ENGLISH if translate_english is None else translate_english
    audio: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_CHUNKS)
    events: asyncio.Queue = asyncio.Queue(maxsize=1000)
    producer = asyncio.create_task(_pump_audio(pcm, audio))
    worker = asyncio.create_task(_run_connections(
        audio, events, connector or connect, translate_english, reconnect_delay_s,
    ))
    last_queue_warning = 0.0
    try:
        while True:
            if producer.done() and not producer.cancelled() and producer.exception() is not None:
                await producer
            if worker.done() and events.empty():
                await worker
                return
            try:
                event = await asyncio.wait_for(events.get(), timeout=0.1)
                now = asyncio.get_running_loop().time()
                if events.qsize() >= 100 and now - last_queue_warning >= 10:
                    log.warning("Soniox subtitle delivery backlog: %d events", events.qsize())
                    last_queue_warning = now
                yield event
            except asyncio.TimeoutError:
                pass
    finally:
        for task in (producer, worker):
            if not task.done():
                task.cancel()
        await asyncio.gather(producer, worker, return_exceptions=True)
