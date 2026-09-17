"""Offline protocol/lifecycle tests: uv run python tests/test_soniox_translate.py."""
import asyncio
import json
import sys
import unittest
from contextlib import aclosing
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nmixx_subtitles import soniox_translate as st
from nmixx_subtitles import subtitles
from nmixx_subtitles.discord import DiscordPoster
from nmixx_subtitles.preview import limited_pcm


def token(text, language="zh", status="translation", final=True):
    return {"text": text, "language": language, "translation_status": status, "is_final": final}


class Socket:
    def __init__(self, *, initial=(), final=(), send_failure=False, eof_failure=False, finish=True):
        self.messages = asyncio.Queue()
        for message in initial:
            self.messages.put_nowait(json.dumps(message))
        self.final = final
        self.send_failure = send_failure
        self.eof_failure = eof_failure
        self.finish = finish
        self.sent = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        self.closed = True

    async def send(self, data):
        if isinstance(data, bytes) and self.send_failure:
            raise ConnectionError("send failed")
        self.sent.append(data)
        if data == "":
            if self.eof_failure:
                raise ConnectionError("EOF send failed")
            for message in self.final:
                self.messages.put_nowait(json.dumps(message))
            if self.finish:
                self.messages.put_nowait(json.dumps({"finished": True}))

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.messages.get()


class Connector:
    def __init__(self, *sockets):
        self.sockets = list(sockets)
        self.calls = 0

    def __call__(self, *args, **kwargs):
        socket = self.sockets[self.calls]
        self.calls += 1
        return socket


async def audio():
    yield b"\x00\x00"


async def paused_audio():
    yield b"\x00\x00"
    await asyncio.Event().wait()


async def collect(pcm, connector):
    async with asyncio.timeout(2):
        return [event async for event in st.stream_transcripts(
            pcm, connector=connector, translate_english=False, reconnect_delay_s=0,
        )]


class ProtocolTests(unittest.TestCase):
    def test_mixed_final_tokens_and_real_repetitions(self):
        response = {"tokens": [
            token("wrong hypothesis", final=False),
            token("안녕", "ko", "original"), token("你好"),
            token("I really", "en", "none"), token(" really", "en", "none"),
            token(" like it.", "en", "none"),
            token("谢谢", "zh", "original"), token("감사", "ko"),
            token("<end>"), token("<fin>"),
        ]}
        events = st.final_events(response, translate_english=False)
        self.assertEqual("".join(t for k, t in events if k == "subtitle"),
                         "你好I really really like it.谢谢")
        self.assertIn(("source", "안녕"), events)

    def test_translate_all_does_not_duplicate_english_source(self):
        events = st.final_events({"tokens": [token("Hello", "en", "original"), token("你好")]},
                                 translate_english=True)
        self.assertEqual(events, [("source", "Hello"), ("subtitle", "你好")])

    def test_untranslated_korean_never_leaks_as_subtitle(self):
        self.assertEqual(st.final_events({"tokens": [token("안녕", "ko", "none")]},
                                         translate_english=False), [("source", "안녕")])

    def test_config_and_glossary(self):
        cfg = st.build_live_config(translate_english=False, reference_id="test")
        self.assertEqual(cfg["translation"], {"type": "two_way", "language_a": "ko", "language_b": "zh"})
        self.assertEqual((cfg["audio_format"], cfg["sample_rate"], cfg["num_channels"]),
                         ("pcm_s16le", 16000, 1))
        self.assertTrue(cfg["enable_language_identification"])
        terms = cfg["context"]["translation_terms"]
        self.assertIn({"source": "해원", "target": "海嫄"}, terms)
        self.assertIn({"source": "믹스", "target": "神祕盒子 MIXX"}, terms)
        self.assertIn({"source": "언니", "target": "姐姐"}, terms)
        self.assertFalse(any("(" in item["target"] for item in terms))
        self.assertEqual(st.build_live_config(translate_english=True, reference_id="test")["translation"],
                         {"type": "one_way", "target_language": "zh"})

    def test_provider_validation(self):
        with patch.object(subtitles.config, "SONIOX_API_KEY", ""):
            with self.assertRaisesRegex(ValueError, "SONIOX_API_KEY"):
                subtitles.validate_provider("soniox")
        with self.assertRaisesRegex(ValueError, "SUBTITLE_PROVIDER"):
            subtitles.validate_provider("typo")
        self.assertIsNone(subtitles.subtitle_transform("gemini"))


class StreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_final_subtitles_drained_after_audio_eof(self):
        ws = Socket(final=[{"tokens": [token("最后一句。")]}])
        connector = Connector(ws)
        self.assertEqual(await collect(audio(), connector), [("subtitle", "最后一句。")])
        self.assertEqual(ws.sent[1:], [b"\x00\x00", ""])
        self.assertTrue(ws.closed)
        self.assertEqual(connector.calls, 1)

    async def test_auth_and_balance_errors_are_not_retried(self):
        for code in (400, 401, 402, 403):
            connector = Connector(Socket(initial=[{"error_code": code, "error_type": "test_error"}]))
            async with aclosing(paused_audio()) as pcm:
                with self.assertRaises(st.SonioxAPIError):
                    await collect(pcm, connector)
            self.assertEqual(connector.calls, 1)

    async def test_error_after_eof_is_not_silently_successful(self):
        connector = Connector(Socket(final=[{"error_code": 503}]))
        with self.assertRaises(st.SonioxAPIError):
            await collect(audio(), connector)
        self.assertEqual(connector.calls, 1)

    async def test_eof_send_failure_does_not_reconnect_and_hang(self):
        connector = Connector(Socket(eof_failure=True))
        with self.assertRaises(ConnectionError):
            await collect(audio(), connector)
        self.assertEqual(connector.calls, 1)

    async def test_missing_finished_response_times_out(self):
        connector = Connector(Socket(finish=False))
        with patch.object(st, "DRAIN_TIMEOUT_S", 0.01):
            with self.assertRaises(TimeoutError):
                await collect(audio(), connector)

    async def test_failed_send_retains_pending_audio_on_reconnect(self):
        first, second = Socket(send_failure=True), Socket(final=[{"tokens": [token("恢复。")]}])
        connector = Connector(first, second)
        self.assertEqual(await collect(audio(), connector), [("subtitle", "恢复。")])
        self.assertEqual(second.sent[1:], [b"\x00\x00", ""])
        self.assertTrue(first.closed and second.closed)

    async def test_persistent_503_reaches_retry_limit(self):
        connector = Connector(*(Socket(initial=[{"error_code": 503}]) for _ in range(st.MAX_RECONNECT_ATTEMPTS)))
        async with aclosing(paused_audio()) as pcm:
            with self.assertRaises(st.SonioxAPIError):
                await collect(pcm, connector)
        self.assertEqual(connector.calls, st.MAX_RECONNECT_ATTEMPTS)

    async def test_capture_failure_propagates(self):
        async def broken_audio():
            yield b"\x00\x00"
            raise RuntimeError("capture failed")
        with self.assertRaisesRegex(RuntimeError, "capture failed"):
            await collect(broken_audio(), Connector(Socket()))

    async def test_rotation_interrupts_empty_queue_then_drains(self):
        ws = Socket(final=[{"tokens": [token("轮替。")]}])
        events = asyncio.Queue()
        state = st._InputState()
        with patch.object(st, "ROTATE_AFTER_S", 0.01):
            await asyncio.wait_for(st._session(ws, asyncio.Queue(), events, state, False), 1)
        self.assertFalse(state.ended)
        self.assertEqual(events.get_nowait(), ("subtitle", "轮替。"))
        self.assertEqual(ws.sent, [""])

    async def test_keepalive_on_idle_input(self):
        ws = Socket()
        with patch.object(st, "KEEPALIVE_S", 0.01):
            task = asyncio.create_task(st._send_audio(ws, asyncio.Queue(), st._InputState(), asyncio.Event()))
            try:
                await asyncio.sleep(0.15)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertIn(json.dumps({"type": "keepalive"}), ws.sent)

    async def test_conversion_after_joining_and_retry(self):
        class Poster(DiscordPoster):
            posts = []
            fail_once = True

            async def _post(self, text):
                if self.fail_once:
                    self.fail_once = False
                    raise RuntimeError("temporary webhook failure")
                self.posts.append(text)

        poster = Poster("http://unused.invalid", transform=subtitles.subtitle_transform("soniox"))
        try:
            await poster.send("软")
            with self.assertRaises(RuntimeError):
                await poster.send("件很好。Lily is here!")
        finally:
            await poster.close()
        self.assertEqual(poster.posts, ["軟體很好。Lily is here!"])

    async def test_router_selects_soniox_and_closes_generator(self):
        closed = []
        async def fake_stream(_):
            try:
                yield ("subtitle", "hello")
                yield ("subtitle", "world")
            finally:
                closed.append(True)
        with patch.object(subtitles.config, "SONIOX_API_KEY", "test"), patch.object(st, "stream_transcripts", fake_stream):
            async with aclosing(subtitles.stream_transcripts(audio(), provider="soniox")) as events:
                self.assertEqual(await anext(events), ("subtitle", "hello"))
        self.assertEqual(closed, [True])

    async def test_preview_limits_audio_and_closes_capture(self):
        closed = []
        async def fake_capture(_):
            try:
                while True:
                    yield b"\x00" * 3200
            finally:
                closed.append(True)
        with patch("nmixx_subtitles.preview.pcm_stream", fake_capture):
            chunks = [chunk async for chunk in limited_pcm("unused", 1)]
        self.assertEqual(sum(map(len, chunks)), 32000)
        self.assertEqual(closed, [True])

    async def test_main_routes_soniox_to_discord_with_traditional_chinese(self):
        from nmixx_subtitles import main

        class Poster(DiscordPoster):
            posts = []

            def __init__(self, **kwargs):
                super().__init__("http://unused.invalid", **kwargs)

            async def _post(self, text):
                self.posts.append(text)

        async def fake_capture(_):
            yield b"\x00\x00"

        ws = Socket(final=[{"tokens": [
            token("소프트웨어 좋아요", "ko", "original"),
            token("软件真好。"),
            token("Hello everyone.", "en", "none"),
        ]}])
        marks = []
        with (
            patch.object(subtitles.config, "SUBTITLE_PROVIDER", "soniox"),
            patch.object(subtitles.config, "SONIOX_API_KEY", "test"),
            patch.object(subtitles.config, "SONIOX_TRANSLATE_ENGLISH", False),
            patch.object(st, "connect", Connector(ws)),
            patch.object(main, "pcm_stream", fake_capture),
            patch.object(main, "DiscordPoster", Poster),
            patch.object(main, "_mark", lambda *args, **kwargs: marks.append(args)),
        ):
            await main.run_live_job("abcdefghijk")
        self.assertEqual(Poster.posts, [
            "https://www.youtube.com/watch?v=abcdefghijk\n軟體真好。", "Hello everyone.",
        ])
        self.assertEqual(marks, [("abcdefghijk", "completed")])


if __name__ == "__main__":
    unittest.main()
