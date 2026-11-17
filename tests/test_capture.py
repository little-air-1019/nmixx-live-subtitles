"""Local ffmpeg regression check; no network/API calls. uv run python tests/test_capture.py"""
import shutil
import subprocess
import sys
import tempfile
import unittest
import wave
import asyncio
import array
from unittest.mock import patch, AsyncMock
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from nmixx_subtitles import capture


@unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg is required")
class CaptureTests(unittest.TestCase):
    def test_non_hls_video_does_not_receive_hls_only_option(self):
        async def check():
            resolver = AsyncMock()
            resolver.returncode = 0
            resolver.communicate.return_value = (b"https://example.invalid/audio.m4a", b"")
            with patch.object(capture.asyncio, "create_subprocess_exec", side_effect=[resolver, object()]) as spawn, \
                 patch.object(capture.shutil, "which", return_value="yt-dlp"):
                await capture._spawn_stream_pipeline("test-video")
            self.assertNotIn("-live_start_index", spawn.call_args.args)
        asyncio.run(check())

    def test_live_hls_starts_at_latest_segment_instead_of_two_segments_behind(self):
        with tempfile.TemporaryDirectory() as folder:
            playlist = Path(folder) / "live.m3u8"
            # Three two-second segments have distinct tones, making the starting
            # segment observable in decoded audio rather than testing argument text.
            subprocess.run([
                capture.FFMPEG, "-v", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=2",
                "-f", "lavfi", "-i", "sine=frequency=880:duration=2",
                "-f", "lavfi", "-i", "sine=frequency=1320:duration=2",
                "-filter_complex", "[0:a][1:a][2:a]concat=n=3:v=0:a=1[a]", "-map", "[a]",
                "-c:a", "aac", "-f", "hls", "-hls_time", "2", "-hls_list_size", "0",
                "-hls_flags", "omit_endlist", str(playlist),
            ], check=True, capture_output=True, timeout=10)
            # Trim any final encoder-tail segment; keep the three full tones.
            lines = playlist.read_text().splitlines()
            segments = 0
            kept = []
            for line in lines:
                if line.startswith('#EXTINF:'):
                    segments += 1
                    if segments > 3:break
                kept.append(line)
            playlist.write_text('\n'.join(kept)+'\n')

            async def check():
                real_spawn = asyncio.create_subprocess_exec
                resolver = AsyncMock()
                resolver.returncode = 0
                resolver.communicate.return_value = (str(playlist).encode(), b"")
                async def spawn(*args, **kwargs):
                    if args[0] == "yt-dlp":return resolver
                    return await real_spawn(*args, **kwargs)
                with patch.object(capture.asyncio, "create_subprocess_exec", side_effect=spawn), \
                     patch.object(capture.shutil, "which", return_value="yt-dlp"):
                    proc = await capture._spawn_stream_pipeline("test-video")
                try:
                    data = await asyncio.wait_for(proc.stdout.readexactly(16000), 5)
                finally:
                    if proc.returncode is None:proc.terminate()
                    await asyncio.wait_for(proc.communicate(), 5)
                samples = array.array('h', data)[1600:]
                crossings = sum(a <= 0 < b for a,b in zip(samples,samples[1:]))
                return crossings / (len(samples)/16000)
            frequency = asyncio.run(check())
        self.assertGreater(frequency, 1100, f"Started stale HLS segment: frequency={frequency:.0f}Hz")

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
