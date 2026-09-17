"""Local ffmpeg regression check; no network/API calls. uv run python tests/test_capture.py"""
import shutil
import subprocess
import sys
import tempfile
import unittest
import wave
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


@unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg is required")
class CaptureTests(unittest.TestCase):
    def test_early_stop_drains_pipes_and_reaps_ffmpeg_before_loop_closes(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "sample.wav"
            with wave.open(str(path), "wb") as wav:
                wav.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
                wav.writeframes(b"\x00" * 32000 * 30)
            # A separate interpreter makes asyncio's transport cleanup errors observable.
            script = """
import asyncio, sys
from contextlib import aclosing
from nmixx_subtitles.capture import pcm_stream
async def check():
    async with aclosing(pcm_stream(sys.argv[1])) as audio:
        assert len(await anext(audio)) == 3200
asyncio.run(check())
"""
            result = subprocess.run([sys.executable, "-c", script, str(path)], cwd=ROOT,
                                    capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")


if __name__ == "__main__":
    unittest.main()
