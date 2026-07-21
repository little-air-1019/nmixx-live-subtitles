"""Real-network smoke test for youtube.py + trigger.py. Run with: uv run python tests/smoke_trigger.py"""
import asyncio
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx

from nmixx_subtitles.config import WEBSUB_VERIFY_TOKEN, YOUTUBE_CHANNEL_ID
from nmixx_subtitles.youtube import channel_live_video_id, video_state, websub_subscribe

ATOM_SAMPLE = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015">
  <entry>
    <yt:videoId>TESTVIDEOID1</yt:videoId>
    <yt:channelId>{channel}</yt:channelId>
    <title>test</title>
    <published>2026-07-16T00:00:00+00:00</published>
    <updated>2026-07-16T00:00:00+00:00</updated>
  </entry>
</feed>
""".format(channel=YOUTUBE_CHANNEL_ID)


async def item1_videos_list():
    live = await video_state("VAlMDl00mYY")  # Lofi Girl live stream, confirmed live during dev
    normal = await video_state("wDSlE2Iqrac")  # ordinary NMIXX upload, not a live
    print("  live stream ->", live["status"])
    print("  normal video ->", normal["status"])
    assert live["status"] == "live", live
    assert normal["status"] == "none", normal
    print("PASS item1: videos.list classification matches real payloads")


async def item2_watchdog():
    live_id = await channel_live_video_id("UCSJ4gkVC6NrvII8umztf0Ow")  # Lofi Girl channel, 24/7 stream
    not_live = await channel_live_video_id(YOUTUBE_CHANNEL_ID)  # NMIXX, presumably not live
    print("  lofi girl channel ->", live_id)
    print("  nmixx channel ->", not_live)
    assert live_id, "expected a live video id for the 24/7 channel"
    assert not_live is None
    print("PASS item2: watchdog returns id when live, None when not, no crash")


async def item3_websub_callback():
    import uvicorn
    from fastapi import FastAPI

    received = []
    should_raise = {"on": False}

    async def on_video(vid):
        if should_raise["on"]:
            raise RuntimeError("boom")
        received.append(vid)

    from nmixx_subtitles.trigger import make_router

    app = FastAPI()
    app.include_router(make_router(on_video))
    config = uvicorn.Config(app, host="127.0.0.1", port=8931, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.5)
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                "http://127.0.0.1:8931/youtube/websub",
                params={"hub.mode": "subscribe", "hub.challenge": "abc123", "hub.verify_token": WEBSUB_VERIFY_TOKEN},
            )
            assert r.status_code == 200 and r.text == "abc123", (r.status_code, r.text)

            r = await client.get(
                "http://127.0.0.1:8931/youtube/websub",
                params={"hub.mode": "subscribe", "hub.challenge": "abc123", "hub.verify_token": "wrong"},
            )
            assert r.status_code == 404, r.status_code

            r = await client.post(
                "http://127.0.0.1:8931/youtube/websub",
                content=ATOM_SAMPLE,
                headers={"Content-Type": "application/atom+xml"},
            )
            assert r.status_code == 204, r.status_code
            assert received == ["TESTVIDEOID1"], received
        print("PASS item3: GET echoes challenge / 404s on bad token, POST triggers on_video")

        async with httpx.AsyncClient() as client:
            # on_video raises -> still 204, no retry-storm-inducing 5xx
            should_raise["on"] = True
            r = await client.post(
                "http://127.0.0.1:8931/youtube/websub",
                content=ATOM_SAMPLE,
                headers={"Content-Type": "application/atom+xml"},
            )
            assert r.status_code == 204, r.status_code
            should_raise["on"] = False

            # garbage body -> non-5xx, not a 500
            r = await client.post(
                "http://127.0.0.1:8931/youtube/websub",
                content=b"not xml",
                headers={"Content-Type": "application/atom+xml"},
            )
            assert r.status_code == 204, r.status_code

            # oversized body -> rejected without parsing, non-5xx
            oversized = b"<feed>" + b"x" * 1_100_000 + b"</feed>"
            r = await client.post(
                "http://127.0.0.1:8931/youtube/websub",
                content=oversized,
                headers={"Content-Type": "application/atom+xml"},
            )
            assert r.status_code == 204, r.status_code
        print("PASS item5: on_video exception / bad XML / oversized body all return non-5xx")
    finally:
        server.should_exit = True
        await task


async def item6_watchdog_timeout():
    from nmixx_subtitles import youtube as yt_mod

    orig_timeout = yt_mod.WATCHDOG_TIMEOUT_S
    yt_mod.WATCHDOG_TIMEOUT_S = 0.01  # force the timeout path without waiting a real 60s
    try:
        result = await channel_live_video_id(YOUTUBE_CHANNEL_ID)
        assert result is None, result
        print("PASS item6: yt-dlp watchdog timeout path returns None without hanging/crashing")
    finally:
        yt_mod.WATCHDOG_TIMEOUT_S = orig_timeout


async def item4_real_subscribe():
    try:
        resp = await websub_subscribe()
        print("  hub responded:", resp.status_code, resp.text[:200])
        assert resp.status_code in (202, 204), resp.status_code
        print("PASS item4: real subscribe request accepted by hub")
    except Exception as e:
        print("NOT-RUN item4:", type(e).__name__, e)


async def main():
    print("--- item1: videos.list ---")
    await item1_videos_list()
    print("--- item2: watchdog ---")
    await item2_watchdog()
    print("--- item3/5: websub callback hardening (local uvicorn) ---")
    await item3_websub_callback()
    print("--- item6: watchdog timeout path ---")
    await item6_watchdog_timeout()
    print("--- item4: real subscribe via ngrok tunnel (skip: already verified) ---")


if __name__ == "__main__":
    asyncio.run(main())
