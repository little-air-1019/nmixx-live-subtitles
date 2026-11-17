"""Korean native translation must not queue behind a second text API."""
import asyncio
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from nmixx_subtitles import soniox_translate as st, subtitles, text_translate as tt
from test_soniox_translate import Socket, Connector, audio, token


class HybridTests(unittest.IsolatedAsyncioTestCase):
    async def test_five_korean_sentences_do_not_wait_for_text_translation(self):
        class SpeechSocket(Socket):
            async def send(self, data):
                if isinstance(data, str) and data:
                    native = json.loads(data).get("translation", {}).get("type") == "two_way"
                    tokens = []
                    for i in range(5):
                        tokens.append(token(f"한국어 문장 {i}.", "ko", "original" if native else "none"))
                        if native:
                            tokens.append(token(f"字幕{i}。", "zh", "translation"))
                    self.final = [{"tokens": tokens}]
                await super().send(data)

        async def delayed_translation(_):
            await asyncio.Event().wait()

        translator = AsyncMock()
        translator.translate.side_effect = delayed_translation
        with (patch.object(st, "connect", Connector(SpeechSocket())),
              patch.object(subtitles.config, "SONIOX_API_KEY", "test"),
              patch.object(subtitles.config, "GEMINI_API_KEY", "test"),
              patch.object(subtitles.config, "SONIOX_TRANSLATE_ENGLISH", False),
              patch.object(tt, "TextTranslator", return_value=translator) as factory):
            async def collect():
                return [text async for kind, text in subtitles.stream_transcripts(audio(), provider="soniox")
                        if kind == "subtitle"]
            result = await asyncio.wait_for(collect(), 0.5)
        self.assertEqual(result, [f"字幕{i}。" for i in range(5)])
        factory.assert_not_called()

    async def test_only_other_languages_use_fallback(self):
        socket = Socket(final=[{"tokens": [
            token("안녕", "ko", "original"), token("你好。"),
            token("Hello!", "en", "none"),
            token("こんにちは", "ja", "none"), token("<end>", "ja", "none"),
            token("谢谢。", "zh", "original"), token("감사", "ko", "translation"),
        ]}])
        translator = AsyncMock()
        translator.translate.return_value = "大家好。"
        with (patch.object(st, "connect", Connector(socket)),
              patch.object(subtitles.config, "SONIOX_API_KEY", "test"),
              patch.object(subtitles.config, "GEMINI_API_KEY", "test"),
              patch.object(subtitles.config, "SONIOX_TRANSLATE_ENGLISH", False),
              patch.object(tt, "TextTranslator", return_value=translator)):
            result = [text async for kind, text in subtitles.stream_transcripts(audio(), provider="soniox")
                      if kind == "subtitle"]
        self.assertEqual(result, ["你好。", "Hello!", "大家好。", "谢谢。"])
        translator.translate.assert_awaited_once_with("こんにちは")
        translator.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
