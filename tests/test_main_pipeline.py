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
