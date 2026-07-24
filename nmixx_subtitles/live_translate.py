"""16 kHz PCM -> Gemini Live source and zh-Hant transcript events."""
import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator

from google import genai
from google.genai import types

from nmixx_subtitles import config

log = logging.getLogger(__name__)

AUDIO_MIME = "audio/pcm;rate=16000"
QUEUE_SECONDS = 10
CHUNKS_PER_SECOND = 10
QUEUE_CHUNKS = QUEUE_SECONDS * CHUNKS_PER_SECOND
MAX_RECONNECT_ATTEMPTS = 5
DRAIN_TIMEOUT_S = 10.0
SEND_POLL_S = 0.1

_END = object()


def build_live_config(handle: str | None = None) -> types.LiveConnectConfig:
    # response_modalities=["AUDIO"] is mandatory for the Live Translate model; it produces
    # translated *speech* and we read the translated text via output_audio_transcription.
    # ["TEXT"] is not a supported response modality for this model. We never read the
    # model_turn audio parts. See docs/superpowers/plans/2026-07-23-gemini-live-translation.md.
    return types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        input_audio_transcription=types.AudioTranscriptionConfig(),
        output_audio_transcription=types.AudioTranscriptionConfig(),
        translation_config=types.TranslationConfig(
            target_language_code="zh-Hant",
            echo_target_language=False,
        ),
        context_window_compression=types.ContextWindowCompressionConfig(
            sliding_window=types.SlidingWindow(),
        ),
        session_resumption=types.SessionResumptionConfig(handle=handle),
    )


class _TranscriptBuffer:
    def __init__(self) -> None:
        self._parts: list[str] = []

    def push(self, transcript: types.Transcription) -> str | None:
        if transcript.text:
            self._parts.append(transcript.text)
        return self.flush() if transcript.finished else None

    def flush(self) -> str | None:
        text = "".join(self._parts).strip()
        self._parts.clear()
        return text or None

    def clear(self) -> None:
        # Drop any un-finished fragment carried over from a dropped connection so a
        # replayed fragment after resumption cannot concatenate onto stale text.
        self._parts.clear()


async def _put_latest(
    queue: asyncio.Queue,
    item,
    on_drop,
) -> None:
    if queue.full():
        queue.get_nowait()
        on_drop(1)
    queue.put_nowait(item)


async def _pump_audio(
    pcm: AsyncIterable[bytes],
    queue: asyncio.Queue,
) -> None:
    dropped_chunks = 0

    def record_drop(count: int) -> None:
        nonlocal dropped_chunks
        dropped_chunks += count
        if dropped_chunks == 1 or dropped_chunks % CHUNKS_PER_SECOND == 0:
            log.warning(
                "Gemini audio queue full; dropped %.1fs total to stay near live",
                dropped_chunks / CHUNKS_PER_SECOND,
            )

    async for chunk in pcm:
        await _put_latest(queue, chunk, record_drop)
    await queue.put(_END)


async def _send_audio(
    session,
    queue: asyncio.Queue,
    reconnect: asyncio.Event,
    input_done: asyncio.Event,
) -> None:
    """Forward queued PCM to the session until EOF (_END) or a reconnect is signalled.

    Reconnect must interrupt a sender parked on an empty queue, or the reconnect path
    deadlocks. Racing ``queue.get()`` against the event via ``asyncio.wait`` is *not*
    safe here: cancelling a ``Queue.get()`` that concurrently received an item silently
    drops that item (a documented asyncio behaviour). So we poll the queue with a short
    timeout and re-check ``reconnect`` between ticks -- no get is cancelled with a value
    in flight, so no chunk or the _END sentinel is lost.
    """
    # ponytail: 100 ms poll instead of an event-woken get; drop-oldest already tolerates
    # <=100 ms extra queue latency, and this is the only race-free option for asyncio.Queue.
    while not reconnect.is_set():
        try:
            chunk = await asyncio.wait_for(queue.get(), timeout=SEND_POLL_S)
        except asyncio.TimeoutError:
            continue  # nothing queued this tick; loop re-checks reconnect
        if chunk is _END:
            # Mark terminal BEFORE the send: pulling _END means input has logically ended,
            # and the shared queue's only _END sentinel is now consumed. If the send raises
            # (socket drops at EOF), _run_connections must still see input_done and finish
            # instead of reconnecting to a sender that would wait forever on an empty queue.
            input_done.set()
            await session.send_realtime_input(audio_stream_end=True)
            return
        await session.send_realtime_input(
            audio=types.Blob(data=chunk, mime_type=AUDIO_MIME)
        )


async def _emit(
    events: asyncio.Queue,
    kind: str,
    text: str | None,
    last: dict[str, str],
) -> None:
    """Emit a finished transcript, suppressing an immediate exact duplicate. Session
    resumption can replay the last finished line; dropping an exact repeat prevents the
    duplicated subtitle the acceptance gate forbids."""
    if text and text != last.get(kind):
        last[kind] = text
        await events.put((kind, text))


async def _receive(
    session,
    events: asyncio.Queue,
    reconnect: asyncio.Event,
    state: dict[str, str | None],
    input_done: asyncio.Event,
    source: _TranscriptBuffer,
    translation: _TranscriptBuffer,
    last: dict[str, str],
) -> None:
    while not reconnect.is_set():
        saw_message = False
        async for response in session.receive():
            saw_message = True
            update = response.session_resumption_update
            if update and update.resumable and update.new_handle:
                state["handle"] = update.new_handle
            if response.go_away is not None:
                log.info(
                    "Gemini connection rotating; server time_left=%s",
                    response.go_away.time_left,
                )
                reconnect.set()
                return
            content = response.server_content
            if not content:
                continue
            if content.input_transcription:
                await _emit(events, "source", source.push(content.input_transcription), last)
            if content.output_transcription:
                out = content.output_transcription
                if out.language_code not in (None, "zh-Hant", "zh-TW"):
                    log.warning("unexpected output language=%s", out.language_code)
                await _emit(events, "translation", translation.push(out), last)
        if input_done.is_set():
            await _emit(events, "source", source.flush(), last)
            await _emit(events, "translation", translation.flush(), last)
            return
        if not saw_message:
            reconnect.set()
            return


async def _run_connections(
    audio: asyncio.Queue,
    events: asyncio.Queue,
    client: genai.Client,
    reconnect_delay_s: float,
) -> None:
    state: dict[str, str | None] = {"handle": None}
    source = _TranscriptBuffer()
    translation = _TranscriptBuffer()
    last: dict[str, str] = {}
    failures = 0
    while True:
        reconnect = asyncio.Event()
        input_done = asyncio.Event()
        sender = None
        receiver = None
        try:
            async with client.aio.live.connect(
                model=config.GEMINI_LIVE_MODEL,
                config=build_live_config(state["handle"]),
            ) as session:
                failures = 0
                source.clear()  # drop partial fragments carried from a dropped connection
                translation.clear()
                sender = asyncio.create_task(
                    _send_audio(session, audio, reconnect, input_done)
                )
                receiver = asyncio.create_task(
                    _receive(
                        session, events, reconnect, state, input_done,
                        source, translation, last,
                    )
                )
                done, _ = await asyncio.wait(
                    {sender, receiver}, return_when=asyncio.FIRST_COMPLETED
                )
                if input_done.is_set():
                    # Terminal: audio EOF was sent, so the whole stream is ending. Drain
                    # the receiver's final transcripts (bounded) and stop for good -- never
                    # reconnect past real EOF, even if the socket errors during the drain.
                    try:
                        await asyncio.wait_for(receiver, timeout=DRAIN_TIMEOUT_S)
                    except Exception:  # timeout or a socket error during the final drain
                        log.warning("Gemini final transcript drain cut short; flushing")
                        await _emit(events, "source", source.flush(), last)
                        await _emit(events, "translation", translation.flush(), last)
                    return
                # Not terminal. Surface a finished task's error to trigger a reconnect;
                # otherwise this is a clean rotation -> signal both tasks and loop.
                for task in done:
                    task.result()  # re-raise; caught below to reconnect with saved handle
                reconnect.set()
        except Exception:
            if input_done.is_set():
                return  # EOF already delivered; a late socket error is not a reconnect reason
            failures += 1
            log.exception(
                "Gemini live connection failed (%d/%d)", failures, MAX_RECONNECT_ATTEMPTS
            )
            if failures >= MAX_RECONNECT_ATTEMPTS:
                raise
        finally:
            for task in (sender, receiver):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(t for t in (sender, receiver) if t is not None),
                return_exceptions=True,
            )
        await asyncio.sleep(reconnect_delay_s)


async def stream_transcripts(
    pcm: AsyncIterable[bytes],
    *,
    client: genai.Client | None = None,
    reconnect_delay_s: float = 1.0,
) -> AsyncIterator[tuple[str, str]]:
    if not config.GEMINI_API_KEY and client is None:
        raise ValueError("GEMINI_API_KEY is required")
    owns_client = client is None
    live_client = client or genai.Client(api_key=config.GEMINI_API_KEY)
    audio: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_CHUNKS)
    events: asyncio.Queue = asyncio.Queue()
    producer = asyncio.create_task(_pump_audio(pcm, audio))
    worker = asyncio.create_task(
        _run_connections(audio, events, live_client, reconnect_delay_s)
    )
    try:
        while True:
            if (
                producer.done()
                and not producer.cancelled()
                and producer.exception() is not None
            ):
                await producer
            if worker.done() and events.empty():
                await worker
                break
            try:
                yield await asyncio.wait_for(events.get(), timeout=0.1)
            except asyncio.TimeoutError:
                continue
    finally:
        for task in (producer, worker):
            if not task.done():
                task.cancel()
        await asyncio.gather(producer, worker, return_exceptions=True)
        if owns_client:
            await live_client.aio.aclose()
