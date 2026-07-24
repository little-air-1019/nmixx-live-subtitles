# Gemini Live Translation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the lagging local Whisper plus batched text-translation pipeline with one resilient Gemini Live Translate stream that produces bounded-latency Traditional Chinese subtitles for a two-hour YouTube live.

**Architecture:** Keep the existing YouTube trigger, ffmpeg capture, Discord poster, and job state machine. Feed 100 ms 16 kHz mono PCM chunks directly to `gemini-3.5-live-translate-preview`, consume its finished source and `zh-Hant` output transcriptions, and reconnect with session resumption while context-window compression keeps the logical session alive. A bounded ten-second audio queue favors the current live edge over accumulating unbounded lag during an API outage.

**Tech Stack:** Python 3.11, `asyncio`, `google-genai==2.12.0` or newer, FastAPI, httpx, ffmpeg, assert-based test scripts

## Global Constraints

- Use `gemini-3.5-live-translate-preview`; estimated model cost is `120 × $0.0368 = $4.416`, approximately **$4.42 USD per two-hour live** for one target language.
- Input is raw little-endian signed 16-bit mono PCM at 16 kHz, sent in 100 ms / 3,200-byte chunks.
- Output language is Traditional Chinese via BCP-47 code `zh-Hant`; `echo_target_language` stays `False`.
- Keep at most ten seconds of unsent audio. If the API is unavailable longer, drop the oldest audio and log the exact dropped duration so subtitles never drift minutes behind live.
- Enable both context-window compression and session resumption. Google documents a 15-minute uncompressed audio-session limit and an approximately ten-minute WebSocket connection limit.
- Never silently pass Korean text through as translated output. API, socket, language, and queue-drop failures must be visible in logs.
- Keep the existing one-live-at-a-time state machine and Discord rate-limit handling.
- Do not add a dependency or framework; the installed Google SDK and assert-based test style are sufficient.
- Do not deploy until a 30-minute real NMIXX sample passes the acceptance gate in Task 6.
- Before deployment, rotate the YouTube API key and Discord webhook because existing local logs contain secret-bearing request URLs. Never copy either value into commits, tests, the plan, or the handoff.

---

## File Map

- Create `nmixx_subtitles/live_translate.py`: Gemini configuration, transcript assembly, bounded audio queue, connection resumption, and the async transcript stream.
- Create `tests/test_live_translate.py`: offline fake-session checks for config, fragment assembly, audio transport, reconnect, and bounded-queue behavior.
- Create `tests/test_main_pipeline.py`: offline check that only translated output reaches Discord.
- Modify `nmixx_subtitles/capture.py`: change capture chunks from one second to 100 ms.
- Modify `nmixx_subtitles/config.py` and `.env.example`: expose the Live Translate model name and remove local Whisper/text-model settings.
- Modify `nmixx_subtitles/main.py`: wire capture directly to Gemini Live Translate and improve safe logging.
- Modify `nmixx_subtitles/discord.py`: reduce the avoidable four-second posting delay to one second.
- Modify `pyproject.toml` and `uv.lock`: remove the unused local Whisper packages after the new path is green.
- Delete `nmixx_subtitles/asr.py`, `nmixx_subtitles/batcher.py`, `nmixx_subtitles/translate.py`, `tests/test_batcher.py`, and `tests/smoke_asr.py` after the replacement tests pass.
- Modify `README.md` and `ARCHITECTURE.md`: document the deployed pipeline, cost, Preview status, session behavior, and quality-check procedure.
- Keep `glossary.md` as the human review checklist; Gemini Live Translate does not accept instructions or a custom glossary.

---

### Task 1: Lock the audio and Gemini configuration

**Files:**
- Modify: `nmixx_subtitles/capture.py:12-13`
- Modify: `nmixx_subtitles/config.py:26-27`
- Modify: `.env.example:9-11`
- Create: `nmixx_subtitles/live_translate.py`
- Create: `tests/test_live_translate.py`

**Interfaces:**
- Consumes: `config.GEMINI_API_KEY: str`
- Produces: `config.GEMINI_LIVE_MODEL: str`
- Produces: `build_live_config(handle: str | None = None) -> types.LiveConnectConfig`

> **Why `response_modalities=["AUDIO"]` even though we only read text transcripts:**
> Google's Live Translate docs require `["AUDIO"]`; the translate model produces
> translated *speech* and delivers the text we consume via `output_audio_transcription`.
> `["TEXT"]` is not a supported response modality for this model, so we keep AUDIO and
> simply never read `model_turn` audio parts. Verified against
> https://ai.google.dev/gemini-api/docs/live-api/live-translate (2026-07).

- [ ] **Step 1: Write the failing configuration test**

Create `tests/test_live_translate.py` with:

```python
"""Offline checks for Gemini Live Translate. Run with:
uv run python tests/test_live_translate.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nmixx_subtitles import capture, config
from nmixx_subtitles.live_translate import build_live_config


def test_config() -> None:
    assert capture.CHUNK_BYTES == 3_200
    assert config.GEMINI_LIVE_MODEL == "gemini-3.5-live-translate-preview"

    live = build_live_config()
    assert live.response_modalities == ["AUDIO"]
    assert live.input_audio_transcription is not None
    assert live.output_audio_transcription is not None
    assert live.translation_config.target_language_code == "zh-Hant"
    assert live.translation_config.echo_target_language is False
    assert live.context_window_compression.sliding_window is not None
    assert live.session_resumption.handle is None

    resumed = build_live_config("resume-token")
    assert resumed.session_resumption.handle == "resume-token"
```

- [ ] **Step 2: Run it and verify the missing module fails**

Run: `uv run python tests/test_live_translate.py`

Expected: `ModuleNotFoundError: No module named 'nmixx_subtitles.live_translate'`

- [ ] **Step 3: Implement the configuration**

Change `nmixx_subtitles/capture.py` to:

```python
SAMPLE_RATE = 16000
CHUNK_BYTES = SAMPLE_RATE * 2 // 10  # 100 ms of mono s16le
```

Replace the final model settings in `nmixx_subtitles/config.py` with:

```python
GEMINI_LIVE_MODEL = os.environ.get(
    "GEMINI_LIVE_MODEL",
    "gemini-3.5-live-translate-preview",
)
STATE_FILE = ROOT / "state.json"
```

Replace `WHISPER_MODEL` and `GEMINI_MODEL` in `.env.example` with:

```dotenv
GEMINI_LIVE_MODEL=gemini-3.5-live-translate-preview
```

Create `nmixx_subtitles/live_translate.py` with:

```python
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


def build_live_config(handle: str | None = None) -> types.LiveConnectConfig:
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
```

- [ ] **Step 4: Run the configuration check**

Run: `uv run python -c "from tests.test_live_translate import test_config; test_config(); print('PASS config')"`

Expected: `PASS config`

- [ ] **Step 5: Commit the configuration**

```bash
git add .env.example nmixx_subtitles/capture.py nmixx_subtitles/config.py nmixx_subtitles/live_translate.py tests/test_live_translate.py
git commit -m "feat: configure Gemini live translation"
```

---

### Task 2: Assemble finished transcripts without leaking partial deltas

**Files:**
- Modify: `nmixx_subtitles/live_translate.py`
- Modify: `tests/test_live_translate.py`

**Interfaces:**
- Consumes: `types.Transcription(text: str | None, finished: bool | None)`
- Produces: `_TranscriptBuffer.push(transcription: types.Transcription) -> str | None`
- Produces: `_TranscriptBuffer.flush() -> str | None`

- [ ] **Step 1: Add failing fragment tests**

Append to `tests/test_live_translate.py`:

```python
from google.genai import types
from nmixx_subtitles.live_translate import _TranscriptBuffer


def test_transcript_buffer() -> None:
    buf = _TranscriptBuffer()
    assert buf.push(types.Transcription(text="海", finished=False)) is None
    assert buf.push(types.Transcription(text="嫄姐姐", finished=False)) is None
    assert buf.push(types.Transcription(text="好", finished=True)) == "海嫄姐姐好"
    assert buf.flush() is None

    assert buf.push(types.Transcription(text="  第二句  ", finished=True)) == "第二句"
    assert buf.push(types.Transcription(text=None, finished=True)) is None

    assert buf.push(types.Transcription(text="連線前半句", finished=False)) is None
    assert buf.flush() == "連線前半句"
    assert buf.flush() is None
```

- [ ] **Step 2: Run and verify the missing class fails**

Run: `uv run python -c "from tests.test_live_translate import test_transcript_buffer; test_transcript_buffer()"`

Expected: `ImportError: cannot import name '_TranscriptBuffer'`

- [ ] **Step 3: Add the minimal fragment assembler**

Add below the constants in `nmixx_subtitles/live_translate.py`:

```python
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
```

- [ ] **Step 4: Run both checks**

Run:

```bash
uv run python -c "from tests.test_live_translate import test_config, test_transcript_buffer; test_config(); test_transcript_buffer(); print('PASS transcript assembly')"
```

Expected: `PASS transcript assembly`

- [ ] **Step 5: Commit transcript assembly**

```bash
git add nmixx_subtitles/live_translate.py tests/test_live_translate.py
git commit -m "feat: assemble finished live transcripts"
```

---

### Task 3: Stream audio and resume Gemini connections

> **Concurrency contract (rewritten after adversarial review).** The naive design
> (blocking `queue.get()` in the sender, `await sender` before cancellation, buffers
> reused across reconnect) can **deadlock on reconnect**, **duplicate/merge transcripts**
> across session resumption, and **never terminate after a reconnect**. The
> implementation below fixes all of these and each fix has a dedicated test:
>
> - **No reconnect deadlock (F1):** the sender polls `queue.get()` with a short timeout
>   and re-checks the `reconnect` event between ticks. It never cancels a `queue.get()`
>   that may hold a value — cancelling such a get silently drops the item in asyncio, so
>   racing get against an Event via `asyncio.wait` is unsafe here.
> - **No merged fragments (F2):** `_TranscriptBuffer.clear()` drops any un-finished
>   fragment at the start of each connection so a replayed fragment cannot concatenate
>   onto stale text.
> - **No duplicate finished lines (F3):** `_emit` suppresses an immediate exact-duplicate
>   per kind, because session resumption can replay the last finished transcript.
> - **Explicit terminal vs. reconnect (F6):** once audio EOF (`_END`) has been sent,
>   `input_done` is set and the stream ends for good — even if the socket errors during
>   the final drain — instead of reconnecting past real end-of-input.

**Files:**
- Modify: `nmixx_subtitles/live_translate.py`
- Modify: `tests/test_live_translate.py`

**Interfaces:**
- Consumes: `pcm: AsyncIterable[bytes]`
- Produces: `stream_transcripts(pcm: AsyncIterable[bytes], *, client: genai.Client | None = None, reconnect_delay_s: float = 1.0) -> AsyncIterator[tuple[str, str]]`
- Emits tuples whose first element is exactly `"source"` or `"translation"` and whose second element is a finished, non-empty transcript.

- [ ] **Step 1: Add fake Live API objects and failing stream checks**

Append to `tests/test_live_translate.py`:

```python
class FakeSession:
    def __init__(self, messages, *, disconnect=False):
        self.messages = list(messages)
        self.sent: list[bytes] = []
        self.stream_ended = False
        self.disconnect = disconnect

    async def send_realtime_input(self, *, audio=None, audio_stream_end=None):
        await asyncio.sleep(0)
        if audio is not None:
            self.sent.append(audio.data)
        if audio_stream_end:
            self.stream_ended = True

    async def receive(self):
        while self.messages:
            yield self.messages.pop(0)
        while not self.stream_ended and not self.disconnect:
            await asyncio.sleep(0)
        if self.disconnect:
            raise ConnectionError("scripted disconnect")


class FakeConnection:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, *_):
        return False


class FakeLive:
    def __init__(self, sessions):
        self.sessions = list(sessions)
        self.handles = []

    def connect(self, *, model, config):
        assert model == "gemini-3.5-live-translate-preview"
        self.handles.append(config.session_resumption.handle)
        return FakeConnection(self.sessions.pop(0))


class FakeClient:
    def __init__(self, sessions):
        self.aio = type("Aio", (), {})()
        self.aio.live = FakeLive(sessions)


def message(*, source=None, translation=None, handle=None, go_away=False):
    content = None
    if source is not None or translation is not None:
        content = types.LiveServerContent(
            input_transcription=source,
            output_transcription=translation,
        )
    return types.LiveServerMessage(
        server_content=content,
        session_resumption_update=(
            types.LiveServerSessionResumptionUpdate(
                resumable=True,
                new_handle=handle,
            )
            if handle
            else None
        ),
        go_away=types.LiveServerGoAway(time_left="1s") if go_away else None,
    )


async def chunks(n=2):
    for i in range(n):
        yield bytes([97 + i]) * 3_200


async def _collect(agen):
    return [event async for event in agen]


async def test_stream_and_resume() -> None:
    first = FakeSession(
        [
            message(handle="resume-1"),
            message(
                source=types.Transcription(text="안녕", finished=True),
                translation=types.Transcription(text="你好", finished=True),
            ),
            message(go_away=True),
        ]
    )
    second = FakeSession(
        [message(translation=types.Transcription(text="第二句", finished=True))]
    )
    client = FakeClient([first, second])

    got = [
        event
        async for event in stream_transcripts(
            chunks(2), client=client, reconnect_delay_s=0
        )
    ]
    assert ("source", "안녕") in got
    assert ("translation", "你好") in got
    assert ("translation", "第二句") in got
    assert client.aio.live.handles == [None, "resume-1"]
    assert first.sent + second.sent == [b"a" * 3_200, b"b" * 3_200]
    assert second.stream_ended


async def test_reconnect_no_deadlock_empty_queue() -> None:
    """F1: a GoAway on connection 1 while the sender is parked on an EMPTY queue must
    reconnect, not hang. The whole call is bounded by wait_for; a deadlock would raise
    TimeoutError."""
    first = FakeSession([message(handle="h1"), message(go_away=True)])
    second = FakeSession(
        [message(translation=types.Transcription(text="恢復", finished=True))]
    )
    client = FakeClient([first, second])

    async def one_late_chunk():
        await asyncio.sleep(0.05)  # nothing queued before the reconnect
        yield b"z" * 3_200

    got = await asyncio.wait_for(
        _collect(
            stream_transcripts(one_late_chunk(), client=client, reconnect_delay_s=0)
        ),
        timeout=5.0,
    )
    assert ("translation", "恢復") in got
    assert client.aio.live.handles == [None, "h1"]


async def test_dedup_finished_across_resume() -> None:
    """F3: resumption replays the same finished line; it must be emitted only once."""
    first = FakeSession(
        [
            message(translation=types.Transcription(text="重複", finished=True), handle="h1"),
            message(go_away=True),
        ]
    )
    second = FakeSession(
        [
            message(translation=types.Transcription(text="重複", finished=True)),  # replayed
            message(translation=types.Transcription(text="新句", finished=True)),
        ]
    )
    client = FakeClient([first, second])
    got = [
        event
        async for event in stream_transcripts(chunks(1), client=client, reconnect_delay_s=0)
    ]
    assert [e for e in got if e == ("translation", "重複")] == [("translation", "重複")]
    assert ("translation", "新句") in got


async def test_partial_cleared_on_reconnect() -> None:
    """F2: a partial fragment from connection 1 must not concatenate onto a replayed
    fragment after resume."""
    first = FakeSession(
        [
            message(translation=types.Transcription(text="前半", finished=False), handle="h1"),
            message(go_away=True),
        ]
    )
    second = FakeSession(
        [message(translation=types.Transcription(text="完整句", finished=True))]
    )
    client = FakeClient([first, second])
    got = [
        event
        async for event in stream_transcripts(chunks(1), client=client, reconnect_delay_s=0)
    ]
    assert got.count(("translation", "完整句")) == 1
    assert all("前半" not in text for _, text in got)


async def test_disconnect_exception_reconnects() -> None:
    """F4/F6: a dropped socket MID-STREAM (before EOF) reconnects with the saved handle.
    The pcm stream stays open across the disconnect so _END is not reached on conn 1."""
    first = FakeSession([message(handle="h1")], disconnect=True)
    second = FakeSession(
        [message(translation=types.Transcription(text="重連", finished=True))]
    )
    client = FakeClient([first, second])
    done_reconnecting = asyncio.Event()

    async def open_pcm():
        yield b"q" * 3_200
        await done_reconnecting.wait()
        yield b"r" * 3_200

    async def run():
        got = []
        async for event in stream_transcripts(
            open_pcm(), client=client, reconnect_delay_s=0
        ):
            got.append(event)
            if event == ("translation", "重連"):
                done_reconnecting.set()
        return got

    got = await asyncio.wait_for(run(), timeout=5.0)
    assert ("translation", "重連") in got
    assert client.aio.live.handles == [None, "h1"]


async def test_queue_stays_bounded() -> None:
    dropped = []
    queue = asyncio.Queue(maxsize=2)
    await _put_latest(queue, b"a", dropped.append)
    await _put_latest(queue, b"b", dropped.append)
    await _put_latest(queue, b"c", dropped.append)
    assert dropped == [1]
    assert [queue.get_nowait(), queue.get_nowait()] == [b"b", b"c"]
```

Add these imports near the top of the test file:

```python
from nmixx_subtitles.live_translate import _put_latest, stream_transcripts
```

- [ ] **Step 2: Run and verify the stream symbols are missing**

Run:

```bash
uv run python -c "import asyncio; from tests.test_live_translate import test_stream_and_resume; asyncio.run(test_stream_and_resume())"
```

Expected: import failure for `stream_transcripts` or `_put_latest`.

- [ ] **Step 3: Implement the bounded producer, race-free sender, and connection worker**

Add to `nmixx_subtitles/live_translate.py`:

```python
MAX_RECONNECT_ATTEMPTS = 5
DRAIN_TIMEOUT_S = 10.0
SEND_POLL_S = 0.1

_END = object()


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

    # ponytail: 100 ms poll instead of an event-woken get; drop-oldest already tolerates
    # <=100 ms extra queue latency, and this is the only race-free option for asyncio.Queue.
    """
    while not reconnect.is_set():
        try:
            chunk = await asyncio.wait_for(queue.get(), timeout=SEND_POLL_S)
        except asyncio.TimeoutError:
            continue  # nothing queued this tick; loop re-checks reconnect
        if chunk is _END:
            await session.send_realtime_input(audio_stream_end=True)
            input_done.set()
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
    source: "_TranscriptBuffer",
    translation: "_TranscriptBuffer",
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
```

Also add a `clear()` method to `_TranscriptBuffer` (from Task 2):

```python
    def clear(self) -> None:
        # Drop any un-finished fragment carried over from a dropped connection so a
        # replayed fragment after resumption cannot concatenate onto stale text.
        self._parts.clear()
```

- [ ] **Step 4: Run all offline Live Translate checks**

Add to the bottom of `tests/test_live_translate.py`:

```python
async def main() -> None:
    test_config()
    test_transcript_buffer()
    await test_stream_and_resume()
    await test_reconnect_no_deadlock_empty_queue()
    await test_dedup_finished_across_resume()
    await test_partial_cleared_on_reconnect()
    await test_disconnect_exception_reconnects()
    await test_queue_stays_bounded()
    print("all live translation tests passed")


if __name__ == "__main__":
    asyncio.run(main())
```

Run: `uv run python tests/test_live_translate.py`

Expected: `all live translation tests passed`. (An intentional `ConnectionError: scripted disconnect` traceback is logged by the F4 test's `log.exception` and is expected — the run still ends with the pass line.)

- [ ] **Step 5: Commit transport and resumption**

```bash
git add nmixx_subtitles/live_translate.py tests/test_live_translate.py
git commit -m "feat: stream and resume Gemini live translation"
```

---

### Task 4: Replace the production pipeline and shorten Discord delay

**Files:**
- Modify: `nmixx_subtitles/main.py:1-94`
- Modify: `nmixx_subtitles/discord.py:9`
- Create: `tests/test_main_pipeline.py`

**Interfaces:**
- Consumes: `stream_transcripts(pcm_stream(video_id))`
- Consumes: transcript tuples `("source" | "translation", text)`
- Produces: Discord messages containing only finished translation text, with the YouTube URL prefixed once.

- [ ] **Step 1: Write a failing offline pipeline check**

Create `tests/test_main_pipeline.py`:

```python
"""Run with: uv run python tests/test_main_pipeline.py"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nmixx_subtitles import main


class FakePoster:
    instances = []

    def __init__(self):
        self.sent = []
        self.closed = False
        self.__class__.instances.append(self)

    async def send(self, text):
        self.sent.append(text)

    async def close(self):
        self.closed = True


async def fake_pcm(_):
    yield b"pcm"


async def fake_transcripts(_):
    yield ("source", "안녕하세요")
    yield ("translation", "大家好")
    yield ("translation", "今天很開心")


async def check() -> None:
    old_poster = main.DiscordPoster
    old_pcm = main.pcm_stream
    old_stream = main.stream_transcripts
    old_mark = main._mark
    try:
        main.DiscordPoster = FakePoster
        main.pcm_stream = fake_pcm
        main.stream_transcripts = fake_transcripts
        main._mark = lambda *args, **kwargs: None
        await main.run_live_job("abcdefghijk")
    finally:
        main.DiscordPoster = old_poster
        main.pcm_stream = old_pcm
        main.stream_transcripts = old_stream
        main._mark = old_mark

    poster = FakePoster.instances[-1]
    assert poster.sent == [
        "https://www.youtube.com/watch?v=abcdefghijk\n大家好",
        "今天很開心",
    ]
    assert poster.closed
    print("main live translation pipeline passed")


if __name__ == "__main__":
    asyncio.run(check())
```

- [ ] **Step 2: Run and verify the old main module has no new stream symbol**

Run: `uv run python tests/test_main_pipeline.py`

Expected: failure importing or patching `main.stream_transcripts`

- [ ] **Step 3: Replace the ASR/batch/text-translation loop**

At the top of `nmixx_subtitles/main.py`, remove the imports of `transcribe` and `batch_by_window`, and add:

```python
from nmixx_subtitles.live_translate import stream_transcripts
```

Change the module docstring to:

```python
"""Glue: WebSub trigger -> capture -> Gemini Live Translate -> Discord."""
```

Replace the body of the `try` block in `run_live_job` with:

```python
        async for kind, text in stream_transcripts(pcm_stream(video_id)):
            if kind == "source":
                log.info("source transcript video_id=%s: %s", video_id, text)
                continue
            log.info("translation video_id=%s: %s", video_id, text)
            if first_line:
                text = f"https://www.youtube.com/watch?v={video_id}\n{text}"
                first_line = False
            await poster.send(text)
            segment_count += 1
```

Delete the local `translate_batch` wrapper from `nmixx_subtitles/main.py`.

Change `nmixx_subtitles/discord.py`:

```python
FLUSH_AGE_S = 1.0
```

Keep `FLUSH_CHARS`, the 2,000-character split, 429 handling, and failed-post requeue unchanged.

- [ ] **Step 4: Run the focused checks**

Run:

```bash
uv run python tests/test_main_pipeline.py
uv run python -m nmixx_subtitles.discord
uv run python tests/test_live_translate.py
```

Expected final lines:

```text
main live translation pipeline passed
all offline self-checks passed
all live translation tests passed
```

- [ ] **Step 5: Commit the production wiring**

```bash
git add nmixx_subtitles/main.py nmixx_subtitles/discord.py tests/test_main_pipeline.py
git commit -m "feat: post Gemini live translations"
```

---

### Task 5: Remove the obsolete pipeline and stop logging secret URLs

**Files:**
- Delete: `nmixx_subtitles/asr.py`
- Delete: `nmixx_subtitles/batcher.py`
- Delete: `nmixx_subtitles/translate.py`
- Delete: `tests/test_batcher.py`
- Delete: `tests/smoke_asr.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `nmixx_subtitles/main.py:17-19`

**Interfaces:**
- Removes: local Whisper and batch text-translation code paths.
- Preserves: `google-genai`, FastAPI, uvicorn, httpx, trigger, capture, Discord, and job-state behavior.

- [ ] **Step 1: Prove the new path no longer imports the old modules**

Run:

```bash
rg -n "nmixx_subtitles\\.(asr|batcher|translate)|WHISPER_MODEL|GEMINI_MODEL|whisperlivekit|mlx.whisper" nmixx_subtitles tests
```

Expected: matches only inside the files scheduled for deletion.

- [ ] **Step 2: Remove old dependencies and files**

Run:

```bash
uv remove whisperlivekit mlx-whisper
rm nmixx_subtitles/asr.py nmixx_subtitles/batcher.py nmixx_subtitles/translate.py
rm tests/test_batcher.py tests/smoke_asr.py
```

Expected: `pyproject.toml` no longer lists either Whisper package and `uv.lock` is regenerated.

- [ ] **Step 3: Suppress dependency request logs that expose query secrets**

Immediately after `logging.basicConfig(...)` in `nmixx_subtitles/main.py`, add:

```python
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
```

Do not lower application logs: reconnects, queue drops, source transcripts, translations, and job exceptions remain visible.

- [ ] **Step 4: Run the complete offline suite and import audit**

Run:

```bash
uv sync
uv run python tests/test_live_translate.py
uv run python tests/test_main_pipeline.py
uv run python -m nmixx_subtitles.discord
uv run python -c "import nmixx_subtitles.trigger, nmixx_subtitles.youtube; print('PASS trigger imports')"
rg -n "nmixx_subtitles\\.(asr|batcher|translate)|WHISPER_MODEL|GEMINI_MODEL|whisperlivekit|mlx.whisper" nmixx_subtitles tests pyproject.toml
```

Expected:

- The three offline checks pass.
- The trigger and YouTube modules import successfully.
- The final `rg` returns no matches.

- [ ] **Step 5: Commit cleanup**

```bash
git add -A nmixx_subtitles tests pyproject.toml uv.lock
git commit -m "refactor: remove lagging subtitle pipeline"
```

---

### Task 6: Document, measure, and gate deployment

**Files:**
- Modify: `README.md`
- Modify: `ARCHITECTURE.md`
- Reference: `docs/model-research.md`
- Reference: `glossary.md`

**Interfaces:**
- Documents the exact deployed model, cost, Preview risk, session behavior, and operator checks.
- Establishes pass/fail gates for latency, completeness, and NMIXX terminology.

- [ ] **Step 1: Update user-facing documentation**

In `README.md`, replace the old Whisper/Gemini Flash pipeline and setup text with:

```markdown
Pipeline: YouTube WebSub -> ffmpeg 16 kHz PCM -> Gemini Live Translate
(`gemini-3.5-live-translate-preview`, target `zh-Hant`) -> Discord.

The model is Preview. At the documented $0.0368 per audio minute, one two-hour
live costs approximately $4.42 USD for one target language. The service sends
100 ms audio chunks, keeps at most ten seconds queued during an outage, and
resumes the Gemini session when its WebSocket rotates.
```

Replace the old ASR test command with:

```bash
uv run python tests/test_live_translate.py
uv run python tests/test_main_pipeline.py
```

State that `glossary.md` is a manual quality checklist because the Live Translate model does not accept instructions or a custom glossary.

Update `ARCHITECTURE.md` so its diagram and Pipeline section show only:

```text
YouTube -> ffmpeg 16 kHz mono PCM/100 ms
        -> Gemini Live Translate/zh-Hant
        -> finished output transcription
        -> Discord 1 s coalescing buffer
```

Document context-window compression, resumption handles, the ten-second queue ceiling, and explicit drop/reconnect logs.

- [ ] **Step 2: Rotate compromised credentials before ANY online call**

Do this **before** the paid smoke test in Step 3 and before any other command that can
contact YouTube, Discord, or Gemini with the old values — not merely before deployment.
In the Google Cloud/YouTube console, revoke the old YouTube API key and create a
restricted replacement for the YouTube Data API. In Discord, delete the old webhook and
create a replacement. Put the new values only in the ignored `.env`. Also review any
existing `service.log`/shell history/CI artifacts that may already contain the old
secret-bearing URLs; rotation invalidates them, but do not commit or copy those logs.

Expected:

- The old YouTube key fails.
- The old Discord webhook returns a non-success response.
- `git status --short` does not list `.env`.

- [ ] **Step 3: Run a five-minute paid smoke test**

Use a local Korean sample or a current live source:

```bash
uv run python - <<'PY'
import asyncio
from nmixx_subtitles.capture import pcm_stream
from nmixx_subtitles.live_translate import stream_transcripts

def _has_cjk(s):
    return any("一" <= ch <= "鿿" for ch in s)

async def main():
    translations = 0
    cjk = 0
    async for kind, text in stream_transcripts(pcm_stream("tests/nmixx_sample.m4a")):
        print(kind, text)
        if kind == "translation":
            translations += 1
            if _has_cjk(text):
                cjk += 1
    assert translations > 0, "no translated subtitles returned"
    # Guard against silent Korean passthrough being counted as a translation: the
    # smoke test must see actual Chinese characters, not just any translation event.
    assert cjk > 0, "translation events contained no CJK characters (possible KR passthrough)"

asyncio.run(main())
PY
```

Expected:

- At least one `source` and one `translation` event.
- Translation events contain Traditional Chinese (CJK) rather than `[KR]` pass-through.
- No queue-drop warning under normal network conditions.

- [ ] **Step 4: Run the 30-minute quality gate**

Choose a 30-minute NMIXX clip containing member names, honorifics, fast speech, overlap, music, and at least one Korean/English code switch. Record source timestamp, subtitle arrival timestamp, source transcript, translated transcript, and glossary result in a temporary CSV outside the repository.

Pass only if all are true:

```text
p95 subtitle arrival latency <= 5.0 seconds
maximum subtitle arrival latency <= 10.0 seconds
latency in the final five minutes is no more than 2.0 seconds worse than the first five minutes
non-empty Korean speech with no translation output < 1% of reviewed utterances
member-name and honorific entries from glossary.md >= 95% exact
no Simplified-only character usage in the reviewed output
one forced connection close resumes output within 5.0 seconds
no duplicate finished translation around the forced reconnect
```

If the terminology threshold fails, do not add speculative replacement rules. Keep the current production pipeline running and open a separate plan for the two-stage fallback described in `docs/model-research.md`: streaming ASR followed by a promptable text translator.

- [ ] **Step 5: Run final verification and commit docs**

Run:

```bash
uv run python tests/test_live_translate.py
uv run python tests/test_main_pipeline.py
uv run python -m nmixx_subtitles.discord
git diff --check
git status --short
```

Expected: all checks pass, `git diff --check` prints nothing, and status contains only the intended source, test, lockfile, and documentation changes.

Commit:

```bash
git add README.md ARCHITECTURE.md docs/model-research.md docs/superpowers/plans/2026-07-23-gemini-live-translation.md
git commit -m "docs: describe Gemini live subtitle pipeline"
```

---

## Deployment Decision

Deploy only after Task 6 passes. Restart the launch agent, watch the first ten minutes for `Gemini live connection failed`, `audio queue full`, and `unexpected output language` warnings, and confirm Discord receives finished translations within the five-second p95 target.

If the 30-minute gate passes, this is the complete fix. Do not retain a hidden Whisper fallback: it reintroduces the measured bottleneck and doubles operational paths. If the gate fails on NMIXX terminology, use the documented two-stage fallback as a separate, measured change rather than layering it into this implementation.
