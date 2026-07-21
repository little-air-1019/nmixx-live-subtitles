"""YouTube live-detection: WebSub subscription, videos.list state check, yt-dlp watchdog."""
import asyncio
import logging
import subprocess

import httpx

from nmixx_subtitles.config import PUBLIC_BASE_URL, WEBSUB_VERIFY_TOKEN, YOUTUBE_API_KEY, YOUTUBE_CHANNEL_ID

log = logging.getLogger(__name__)
WATCHDOG_TIMEOUT_S = 60

HUB_URL = "https://pubsubhubbub.appspot.com/subscribe"
TOPIC_URL = f"https://www.youtube.com/feeds/videos.xml?channel_id={YOUTUBE_CHANNEL_ID}"
CALLBACK_URL = f"{PUBLIC_BASE_URL}/youtube/websub"
LEASE_SECONDS = 432000  # 5 days, hub's max; renew before this via a cron/loop (Phase 5's job)


async def websub_subscribe() -> httpx.Response:
    """POST a subscribe (also serves as renew) request to the hub."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            HUB_URL,
            data={
                "hub.mode": "subscribe",
                "hub.topic": TOPIC_URL,
                "hub.callback": CALLBACK_URL,
                "hub.verify": "async",
                "hub.verify_token": WEBSUB_VERIFY_TOKEN,
                "hub.lease_seconds": str(LEASE_SECONDS),
            },
        )
        resp.raise_for_status()
        return resp


async def video_state(video_id: str) -> dict:
    """Classify a video as live / upcoming / ended / not-a-live via videos.list.

    Returns {"status": "live"|"upcoming"|"ended"|"none", "raw": <api item or None>}.
    """
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params={
                "part": "snippet,liveStreamingDetails,status",
                "id": video_id,
                "key": YOUTUBE_API_KEY,
            },
        )
        resp.raise_for_status()
        items = resp.json().get("items", [])
        if not items:
            return {"status": "none", "raw": None}
        item = items[0]
        lsd = item.get("liveStreamingDetails")
        if not lsd:
            return {"status": "none", "raw": item}
        if lsd.get("actualStartTime") and not lsd.get("actualEndTime"):
            return {"status": "live", "raw": item}
        if lsd.get("actualEndTime"):
            return {"status": "ended", "raw": item}
        if lsd.get("scheduledStartTime"):
            return {"status": "upcoming", "raw": item}
        return {"status": "none", "raw": item}


async def channel_live_video_id(channel_id: str = YOUTUBE_CHANNEL_ID) -> str | None:
    """Watchdog: ask yt-dlp if the channel's /live URL currently resolves to a live video."""
    proc = await asyncio.create_subprocess_exec(
        # ponytail: android player_client avoids yt-dlp's web-client SABR format-resolution
        # error (github.com/yt-dlp/yt-dlp/issues/12482) that otherwise kills --print before
        # is_live is emitted; upgrade yt-dlp and drop this once that's fixed upstream.
        "yt-dlp", "--simulate", "--print", "%(id)s %(is_live)s",
        "--extractor-args", "youtube:player_client=android",
        f"https://www.youtube.com/channel/{channel_id}/live",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=WATCHDOG_TIMEOUT_S)
    except asyncio.TimeoutError:
        log.warning("yt-dlp watchdog timed out after %ds for channel=%s", WATCHDOG_TIMEOUT_S, channel_id)
        proc.kill()
        await proc.wait()
        return None
    if proc.returncode != 0:
        # Expected exit path when the channel is simply not live right now (yt-dlp errors
        # out rather than printing) — log at debug so this isn't noisy in the common case.
        log.debug("yt-dlp watchdog exited %s for channel=%s: %s", proc.returncode, channel_id, stderr.decode().strip())
        return None
    line = stdout.decode().strip()
    if not line:
        return None
    parts = line.split()
    video_id, is_live = parts[0], (parts[1] if len(parts) > 1 else "")
    return video_id if is_live == "True" else None


if __name__ == "__main__":
    async def main():
        # offline shape check only; real calls are exercised in tests/smoke_trigger.py
        state = await video_state("wDSlE2Iqrac")  # ordinary non-live video
        assert state["status"] == "none", state
        print("video_state self-check ok:", state["status"])

    asyncio.run(main())
