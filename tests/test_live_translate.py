"""Offline checks for Gemini Live Translate. Run with:
uv run python tests/test_live_translate.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google.genai import types

from nmixx_subtitles import capture, config
from nmixx_subtitles.live_translate import (
    _TranscriptBuffer,
    _put_latest,
    build_live_config,
    stream_transcripts,
)


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


def test_transcript_buffer() -> None:
    buf = _TranscriptBuffer()
    assert buf.push(types.Transcription(text="海", finished=False)) is None
    assert buf.push(types.Transcription(text="嫄姐姐", finished=False)) is None
    assert buf.push(types.Transcription(text="好", finished=True)) == "海嫄姐姐好"
    assert buf.flush() is None

    assert buf.push(types.Transcription(text="  第二句  ", finished=True)) == "第二句"
    assert buf.push(types.Transcription(text=None, finished=True)) is None

    assert buf.push(types.Transcription(text="連線前半句", finished=False)) is None
    buf.clear()  # a dropped connection clears the partial instead of flushing it
    assert buf.flush() is None


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


class EofFailSession:
    """A session whose socket dies exactly when the audio-stream-end marker is sent:
    send_realtime_input(audio_stream_end=True) raises, and receive() then also raises
    (dead socket). This is the realistic shape of the EOF/disconnect race."""

    def __init__(self, messages):
        self.messages = list(messages)
        self.sent: list[bytes] = []
        self.dead = False

    async def send_realtime_input(self, *, audio=None, audio_stream_end=None):
        await asyncio.sleep(0)
        if audio is not None:
            self.sent.append(audio.data)
        if audio_stream_end:
            self.dead = True
            raise ConnectionError("socket dropped exactly at EOF")

    async def receive(self):
        while self.messages:
            yield self.messages.pop(0)
        while not self.dead:
            await asyncio.sleep(0)
        raise ConnectionError("dead socket")


async def test_eof_send_failure_terminates() -> None:
    """Regression: if the socket drops while sending audio_stream_end, the _END sentinel
    has already been consumed. The stream must still terminate (delivering transcripts it
    already had) and must NOT reconnect to a sender that would wait forever on an empty
    queue. A bounded wait_for makes a hang surface as TimeoutError; a spurious reconnect
    would exhaust the single fake session."""
    session = EofFailSession(
        [message(translation=types.Transcription(text="你好", finished=True))]
    )
    client = FakeClient([session])

    async def one_chunk():
        yield b"a" * 3_200

    got = await asyncio.wait_for(
        _collect(stream_transcripts(one_chunk(), client=client, reconnect_delay_s=0)),
        timeout=4.0,
    )
    assert ("translation", "你好") in got
    assert client.aio.live.handles == [None]  # no reconnect past EOF


async def test_queue_stays_bounded() -> None:
    dropped = []
    queue = asyncio.Queue(maxsize=2)
    await _put_latest(queue, b"a", dropped.append)
    await _put_latest(queue, b"b", dropped.append)
    await _put_latest(queue, b"c", dropped.append)
    assert dropped == [1]
    assert [queue.get_nowait(), queue.get_nowait()] == [b"b", b"c"]


async def main() -> None:
    test_config()
    test_transcript_buffer()
    await test_stream_and_resume()
    await test_reconnect_no_deadlock_empty_queue()
    await test_dedup_finished_across_resume()
    await test_partial_cleared_on_reconnect()
    await test_disconnect_exception_reconnects()
    await test_eof_send_failure_terminates()
    await test_queue_stays_bounded()
    print("all live translation tests passed")


if __name__ == "__main__":
    asyncio.run(main())
