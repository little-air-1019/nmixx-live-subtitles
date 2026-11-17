"""Selective translation routing, ordering, errors, and lifecycle (no paid calls)."""
import asyncio
import sys
import unittest
from contextlib import aclosing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from nmixx_subtitles import text_translate as tt
from nmixx_subtitles import soniox_translate as st
from nmixx_subtitles import subtitles


def token(text, language="ja", **extra):
    return {"text": text, "language": language, "is_final": True, **extra}


class RoutingTests(unittest.TestCase):
    def test_multilingual_speech_across_frames_and_eof(self):
        router = tt.TranscriptRouter()
        events = router.feed({"tokens": [token("wrong", is_final=False), token("こんに")]})
        events += router.feed({"tokens": [token("ちは。"), token(" Hello", "en-US"),
                                         token(" everyone.", "en"), token("こんにちは", "ja")]})
        events += router.feed({"tokens": [token("谢谢", "zh-CN"), token("bonjour", "fr")]})
        events += router.feed({"finished": True})
        self.assertEqual([x for x in events if x[0] != "source"], [
            ("translate", "こんにちは。"), ("subtitle", " Hello"), ("subtitle", " everyone."),
            ("translate", "こんにちは"), ("subtitle", "谢谢"), ("translate", "bonjour"),
        ])
        self.assertEqual(router.flush(), [])

    def test_endpoints_flush_once_and_preserve_repeated_words(self):
        router = tt.TranscriptRouter()
        events = router.feed({"tokens": [token("本当に本当に"), token("<end>"), token("<fin>")]})
        self.assertEqual([x for x in events if x[0] == "translate"], [("translate", "本当に本当に")])
        self.assertEqual(router.feed({"finished": True}), [])

    def test_long_monologue_and_unknown_language_are_translated(self):
        router = tt.TranscriptRouter()
        events = router.feed({"tokens": [token("長い文", start_ms=0, end_ms=2000),
                                         token("の続き", start_ms=2000, end_ms=6500)]})
        self.assertIn(("translate", "長い文の続き"), events)
        router = tt.TranscriptRouter()
        self.assertIn(("translate", "bonjour"), router.feed({
            "tokens": [token("bonjour", None)], "finished": True}))

    def test_selective_mode_uses_native_korean_translation(self):
        self.assertEqual(st.build_live_config(translate_english=False, reference_id="test")["translation"],
                         {"type": "two_way", "language_a": "ko", "language_b": "zh"})
        with patch.object(subtitles.config, "SONIOX_API_KEY", "test"), \
             patch.object(subtitles.config, "GEMINI_API_KEY", ""), \
             patch.object(subtitles.config, "SONIOX_TRANSLATE_ENGLISH", False):
            with self.assertRaisesRegex(ValueError, "GEMINI_API_KEY"):
                subtitles.validate_provider("soniox")


async def events(*items):
    for item in items:
        yield item


class TranslationTests(unittest.IsolatedAsyncioTestCase):
    async def test_english_and_chinese_never_create_translator(self):
        with patch.object(tt, "TextTranslator") as factory:
            result = [item async for item in tt.translate_events(events(
                ("subtitle", "I really really like it."), ("subtitle", "谢谢。"))) ]
        factory.assert_not_called()
        self.assertEqual(result, [("subtitle", "I really really like it."), ("subtitle", "谢谢。")])

    async def test_translation_order_and_cleanup(self):
        translator = SimpleNamespace(translate=AsyncMock(side_effect=["你好。", "謝謝。"]), close=AsyncMock())
        with patch.object(tt, "TextTranslator", return_value=translator):
            result = [item async for item in tt.translate_events(events(
                ("translate", "안녕하세요."), ("subtitle", " Hello!"), ("translate", "ありがとう。"))) ]
        self.assertEqual(result, [("subtitle", "你好。"), ("subtitle", " Hello!"), ("subtitle", "謝謝。")])
        self.assertEqual([call.args[0] for call in translator.translate.call_args_list],
                         ["안녕하세요.", "ありがとう。"])
        translator.close.assert_awaited_once()

    async def test_translation_failure_closes_client_and_surfaces(self):
        translator = SimpleNamespace(translate=AsyncMock(side_effect=RuntimeError("failed")), close=AsyncMock())
        with patch.object(tt, "TextTranslator", return_value=translator):
            with self.assertRaisesRegex(RuntimeError, "failed"):
                _ = [item async for item in tt.translate_events(events(("translate", "안녕")))]
        translator.close.assert_awaited_once()

    async def test_consumer_closes_before_next_translation(self):
        translator = SimpleNamespace(translate=AsyncMock(return_value="你好。"), close=AsyncMock())
        with patch.object(tt, "TextTranslator", return_value=translator):
            async with aclosing(tt.translate_events(events(("translate", "안녕"), ("translate", "감사")))) as stream:
                self.assertEqual(await anext(stream), ("subtitle", "你好。"))
        translator.translate.assert_awaited_once()
        translator.close.assert_awaited_once()

    async def test_disconnect_flushes_only_final_text(self):
        class Socket:
            async def __aiter__(self):
                yield '{"tokens":[{"text":"こんにちは","language":"ja","is_final":true}]}'
                raise ConnectionError("drop")
        queue = asyncio.Queue()
        with self.assertRaises(ConnectionError):
            await st._receive(Socket(), queue, False)
        self.assertEqual([queue.get_nowait(), queue.get_nowait()],
                         [("source", "こんにちは"), ("translate", "こんにちは")])

    async def test_sdk_request_is_text_only_and_error_is_sanitized(self):
        response = SimpleNamespace(text="你好。", candidates=[SimpleNamespace(finish_reason=tt.types.FinishReason.STOP)],
                                   usage_metadata=None)
        generate = AsyncMock(return_value=response)
        client = SimpleNamespace(aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate), aclose=AsyncMock()))
        with patch.object(tt.genai, "Client", return_value=client), patch.object(tt.config, "GEMINI_API_KEY", "test"):
            translator = tt.TextTranslator()
            self.assertEqual(await translator.translate("안녕"), "你好。")
            options = generate.call_args.kwargs
            self.assertEqual(options["contents"], "안녕")
            self.assertEqual(options["config"].response_modalities, ["TEXT"])
            response.text = ""
            with self.assertRaisesRegex(RuntimeError, "Text translation failed"):
                await translator.translate("안녕")
            generate.side_effect = ValueError("secret-request-url")
            with self.assertRaises(RuntimeError) as raised:
                await translator.translate("안녕")
            self.assertNotIn("secret", str(raised.exception))
            await translator.close()
        client.aio.aclose.assert_awaited_once()

    async def test_cancellation_with_full_event_queue_does_not_hang(self):
        class Socket:
            async def __aiter__(self):
                yield '{"tokens":[{"text":"こんにちは","language":"ja","is_final":true}]}'
                await asyncio.Event().wait()
        queue = asyncio.Queue(maxsize=1)
        task = asyncio.create_task(st._receive(Socket(), queue, False))
        await asyncio.sleep(0)
        self.assertTrue(queue.full())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 0.2)


if __name__ == "__main__":
    unittest.main()
