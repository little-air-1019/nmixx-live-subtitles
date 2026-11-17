"""Recording and evidence review regressions. All audio/API/Discord data is synthetic."""
import asyncio
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import struct
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import wave

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from nmixx_subtitles import config, recording, quality_review as qr, subtitles, soniox_translate
from nmixx_subtitles.discord import DiscordPoster
from test_soniox_translate import Socket, Connector, token


class RecordingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for key, value in dict(QUALITY_CAPTURE_ENABLED=True, QUALITY_CAPTURE_DIR=self.root,
                               QUALITY_RETENTION_DAYS=7, QUALITY_MAX_BYTES=0,
                               SONIOX_API_KEY="secret-that-must-not-be-recorded",
                               SONIOX_TRANSLATE_ENGLISH=True).items():
            self.stack.enter_context(patch.object(config, key, value))

    async def test_live_path_records_exact_audio_raw_tokens_transform_and_delivery(self):
        pcm = struct.pack("<h", 1234) * 3200
        source = token("순대.", "ko", "original") | dict(start_ms=0, end_ms=200, confidence=0.9)
        socket = Socket(final=[{"tokens": [token("暫定猜測", final=False), source, token("酸菜汤。")],
                                "api_key": "must-not-save-server-unknown-fields"}])
        delivered = []

        class Response:
            status_code = 204
            headers = {}
            def raise_for_status(self):
                pass

        async def post(url, json):
            delivered.append(json["content"])
            return Response()

        async def audio():
            yield pcm[:3200]
            yield pcm[3200:]

        with patch.object(soniox_translate, "connect", Connector(socket)):
            async with recording.quality_session("test", "soniox") as session:
                poster = DiscordPoster("https://unused.invalid/secret-webhook", transform=subtitles.subtitle_transform("soniox"))
                with patch.object(poster._client, "post", side_effect=post):
                    async for kind, text in subtitles.stream_transcripts(audio(), provider="soniox"):
                        if kind == "subtitle":
                            await poster.send(text)
                    await poster.close()
        self.assertEqual(delivered, ["酸菜湯。"])
        with wave.open(str(session.directory / "audio.wav"), "rb") as audio_file:
            self.assertEqual(audio_file.getparams()[:3], (1, 2, 16000))
            self.assertEqual(audio_file.readframes(audio_file.getnframes()), pcm)
        events, bad = qr.read_events(session.directory)
        self.assertEqual(bad, 0)
        self.assertEqual(len([e for e in events if e["kind"] == "audio_sent"]), 2)
        self.assertTrue(any(e["kind"] == "soniox_token" and e.get("is_final") is False for e in events))
        transform = next(e for e in events if e["kind"] == "subtitle_transform")
        self.assertEqual((transform["before"], transform["after"]), ("酸菜汤。", "酸菜湯。"))
        self.assertTrue(any(e["kind"] == "discord_accepted" for e in events))
        report = json.loads((session.directory / "quality.json").read_text())
        candidate = next(c for c in report["candidates"] if c["category"] == "R01")
        self.assertEqual(candidate["source"]["text"], "순대.")
        self.assertEqual(candidate["source"]["audio_start_s"], 0)
        self.assertEqual(candidate["source"]["audio_end_s"], 0.2)
        self.assertTrue(candidate["known_pattern"])
        self.assertIsNone(candidate["review"])
        for path in session.directory.iterdir():
            if path.suffix in (".json", ".jsonl", ".md", ".html"):
                text = path.read_text()
                self.assertNotIn("secret-that-must-not-be-recorded", text)
                self.assertNotIn("secret-webhook", text)
                self.assertNotIn("must-not-save-server-unknown-fields", text)
        with self.assertRaises(ValueError):
            qr.label(session.directory, candidate["id"], verdict="confirmed", cause="asr", note="not listened")
        qr.label(session.directory, candidate["id"], verdict="confirmed", cause="translation", note="synthetic test")
        self.assertIn("R01 / translation：1", qr.summarize(self.root).read_text())
        qr.analyze_session(session.directory)
        self.assertEqual(qr.load_reviews(session.directory)[candidate["id"]]["verdict"], "confirmed")

    async def test_cancellation_keeps_playable_audio_and_marks_session(self):
        async def audio():
            yield b"\x12\x00" * 1600
        with self.assertRaises(asyncio.CancelledError):
            async with recording.quality_session("test", "soniox") as session:
                async for _ in recording.observe_audio(audio()):
                    pass
                raise asyncio.CancelledError()
        manifest = json.loads((session.directory / "manifest.json").read_text())
        self.assertEqual(manifest["status"], "cancelled")
        with wave.open(str(session.directory / "audio.wav"), "rb") as audio_file:
            self.assertEqual(audio_file.getnframes(), 1600)
        self.assertTrue((session.directory / "review.html").exists())

    async def test_storage_failure_does_not_block_audio(self):
        async def audio():
            yield b"\x00\x00" * 16
        with patch.object(recording.wave, "open", side_effect=OSError("private path details")):
            async with recording.quality_session("test", "soniox") as session:
                got = [chunk async for chunk in recording.observe_audio(audio())]
                await asyncio.sleep(0.02)
        self.assertEqual(got, [b"\x00\x00" * 16])
        manifest = json.loads((session.directory / "manifest.json").read_text())
        self.assertEqual(manifest["recording_error"], "OSError")
        self.assertFalse(manifest["evidence_complete"])

    async def test_both_live_entrypoints_enable_recording(self):
        from nmixx_subtitles import main, mirror
        async def audio(*args):
            yield b"\x12\x00" * 1600
        with (patch.object(soniox_translate, "connect", Connector(Socket(final=[]))),
              patch.object(mirror, "mirror_pcm", side_effect=audio)):
            await mirror.run(Path("unused"), seconds=1, provider="soniox", discord=False)
        class Poster(DiscordPoster):
            async def _post(self, content):
                pass
        with (patch.object(config, "SUBTITLE_PROVIDER", "soniox"),
              patch.object(config, "DISCORD_WEBHOOK_URL", "https://unused.invalid"),
              patch.object(main, "DiscordPoster", side_effect=lambda **kw: Poster("https://unused.invalid", **kw)),
              patch.object(main, "pcm_stream", side_effect=audio),
              patch.object(main, "_mark"),
              patch.object(soniox_translate, "connect", Connector(Socket(final=[])))):
            await main.run_live_job("offline-test")
        manifests = [json.loads(p.read_text()) for p in self.root.glob("*/manifest.json")]
        self.assertEqual({m["metadata"]["source"] for m in manifests}, {"youtube", "fans-mirroring"})
        self.assertTrue(all(m["input_samples"] == 1600 for m in manifests))

    async def test_cancel_during_startup_does_not_orphan_writer(self):
        entered, release = threading.Event(), threading.Event()
        original = recording._start_session
        def delayed(*args):
            entered.set()
            release.wait(2)
            return original(*args)
        async def run():
            async with recording.quality_session("test", "soniox"):
                self.fail("Cancelled startup must not enter live body")
        with patch.object(recording, "_start_session", side_effect=delayed):
            task = asyncio.create_task(run())
            await asyncio.to_thread(entered.wait, 1)
            task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        manifest = json.loads(next(self.root.glob("*/manifest.json")).read_text())
        self.assertEqual(manifest["status"], "cancelled")


class EvidenceTests(unittest.TestCase):
    def test_reconnect_and_queue_gaps_map_to_correct_recorded_audio(self):
        events = [
            dict(kind="audio_received", input_start_sample=0, wav_start_sample=0, samples=1600, monotonic_ns=1),
            # Input sample 1600–3200 was not recorded (disk writer queue dropped it).
            dict(kind="audio_received", input_start_sample=3200, wav_start_sample=1600, samples=1600, monotonic_ns=3),
            dict(kind="audio_sent", connection_id="a", connection_start_sample=0, input_start_sample=0, samples=1600),
            dict(kind="audio_sent", connection_id="a", connection_start_sample=1600, input_start_sample=1600, samples=1600),
            # New socket starts at zero; its source really belongs at WAV offset 0.1 s.
            dict(kind="audio_sent", connection_id="b", connection_start_sample=0, input_start_sample=3200, samples=1600),
        ]
        index = qr.AudioIndex(events)
        self.assertIsNone(index.locate("a", 150))
        self.assertAlmostEqual(index.locate("b", 50)[0], 0.15)
        self.assertAlmostEqual(index.locate("b", 100, end=True)[0], 0.2)
        self.assertIsNone(index.locate("unknown", 0))

    def test_bounded_writer_marks_gaps_without_waiting_for_disk(self):
        with tempfile.TemporaryDirectory() as folder:
            recorder = recording.SessionRecorder(folder, {}, max_queue=1)
            original = recorder._write
            ready = threading.Event()
            def delayed():
                ready.wait(2)
                original()
            with patch.object(recorder, "_write", side_effect=delayed):
                recorder.start()
                started = time.monotonic()
                for i in range(100):
                    recorder.enqueue("audio_received", pcm=b"\x00\x00", input_start_sample=i, samples=1)
                elapsed = time.monotonic() - started
                ready.set()
                recorder.finish("completed")
            self.assertLess(elapsed, 0.2)
            self.assertGreater(recorder.dropped_records, 0)
            manifest = json.loads((recorder.directory / "manifest.json").read_text())
            self.assertFalse(manifest["evidence_complete"])

    def test_retention_preserves_active_and_unrelated_directories(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for name, status in [("old", "completed"), ("active", "recording")]:
                directory = root / name
                directory.mkdir()
                (directory / "manifest.json").write_text(json.dumps(dict(
                    schema_version=1, session_id=name, status=status,
                    started_at_utc=(datetime.now(timezone.utc) - timedelta(days=9)).isoformat())))
            (root / "unrelated").mkdir()
            recording.prune_sessions(root, 7, 0)
            self.assertFalse((root / "old").exists())
            self.assertTrue((root / "active").exists())
            self.assertTrue((root / "unrelated").exists())

    def test_all_categories_exist_and_no_hit_is_never_a_pass(self):
        with tempfile.TemporaryDirectory() as folder:
            recorder = recording.SessionRecorder(folder, {})
            recorder.start()
            recorder.finish("completed")
            result = qr.analyze_session(recorder.directory)
            self.assertEqual([r["id"] for r in result["categories"]], [f"R{i:02}" for i in range(1, 17)])
            self.assertTrue(all(r["status"] == "未命中規則；不代表通過" for r in result["categories"]))
            self.assertEqual(result["reviewed_causes"], {})
            with (recorder.directory / "events.jsonl").open("a") as file:
                file.write('{"partial":')
            self.assertTrue(qr.analyze_session(recorder.directory)["warnings"])


if __name__ == "__main__":
    unittest.main()
