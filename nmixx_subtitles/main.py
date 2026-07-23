"""Glue: WebSub trigger -> capture -> Gemini Live Translate -> Discord."""
import asyncio
import json
import logging
import time

from fastapi import FastAPI

from nmixx_subtitles import config
from nmixx_subtitles.capture import pcm_stream
from nmixx_subtitles.discord import DiscordPoster
from nmixx_subtitles.live_translate import stream_transcripts
from nmixx_subtitles.trigger import make_router
from nmixx_subtitles.youtube import channel_live_video_id, video_state, websub_subscribe

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
# httpx/httpcore INFO logs echo full request URLs, which for the YouTube API and Discord
# webhook carry secrets in the query string / path. Keep them at WARNING so those secrets
# never land in service.log. Application logs (reconnects, drops, transcripts) stay at INFO.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

RESUBSCRIBE_INTERVAL_S = 4 * 24 * 3600  # renew comfortably before the hub's 5-day lease expires
WATCHDOG_INTERVAL_S = 10 * 60
UPCOMING_POLL_INTERVAL_S = 60
UPCOMING_POLL_START_BEFORE_S = 2 * 60
UPCOMING_POLL_GIVEUP_AFTER_S = 2 * 3600
MAX_JOB_ATTEMPTS = 5
TERMINAL_STATUSES = ("completed", "ended", "none", "upcoming_timeout", "failed")

# ponytail: plain dict persisted as JSON, single process, no DB. Fine for a single-stream service.
_state: dict[str, dict] = {}
_current_job: asyncio.Task | None = None
_current_video_id: str | None = None


def _load_state() -> None:
    global _state
    if config.STATE_FILE.exists():
        _state = json.loads(config.STATE_FILE.read_text())
    else:
        _state = {}


def _save_state() -> None:
    config.STATE_FILE.write_text(json.dumps(_state, indent=2))


def _mark(video_id: str, status: str, **extra) -> None:
    entry = {"status": status, "ts": time.time(), **extra}
    _state[video_id] = entry
    _save_state()


async def run_live_job(video_id: str) -> None:
    """Capture -> ASR -> translate -> Discord for one live video. Runs until stream ends.

    A transient failure (network blip, Discord 5xx, capture hiccup) must not permanently
    suppress retries: only a clean finish is "completed". A failure records "error" with an
    attempt count so on_video/watchdog will retry it while videos.list still says it's live,
    up to MAX_JOB_ATTEMPTS -- beyond that it's marked "failed" (terminal).
    """
    global _current_job, _current_video_id
    attempt = _state.get(video_id, {}).get("attempt", 0) + 1
    log.info("live job starting for video_id=%s (attempt %d)", video_id, attempt)
    poster = DiscordPoster()
    first_line = True
    segment_count = 0
    try:
        async for kind, text in stream_transcripts(pcm_stream(video_id)):
            if kind == "source":
                log.info("source transcript video_id=%s: %s", video_id, text)
                continue
            log.info("translation video_id=%s: %s", video_id, text)
            if first_line:
                text = f"https://www.youtube.com/watch?v={video_id}\n{text}"
                first_line = False
            await poster.send(text)
            segment_count += 1
        log.info("live job for video_id=%s ended: stream finished (%d segments)", video_id, segment_count)
        _mark(video_id, "completed")
    except Exception:
        log.exception("live job for video_id=%s crashed after %d segments (attempt %d)", video_id, segment_count, attempt)
        if attempt >= MAX_JOB_ATTEMPTS:
            log.error("live job for video_id=%s failed permanently after %d attempts", video_id, attempt)
            _mark(video_id, "failed", attempt=attempt)
        else:
            _mark(video_id, "error", attempt=attempt)
    finally:
        await poster.close()
        _current_job = None
        _current_video_id = None


async def start_live_job(video_id: str) -> None:
    global _current_job, _current_video_id
    if _current_job is not None and not _current_job.done():
        log.info("live job already active (video_id=%s), ignoring new live video_id=%s", _current_video_id, video_id)
        return
    _current_video_id = video_id
    prior_attempts = _state.get(video_id, {}).get("attempt", 0)
    _mark(video_id, "live", attempt=prior_attempts)
    _current_job = asyncio.create_task(run_live_job(video_id))


async def poll_upcoming(video_id: str, scheduled_start_epoch: float) -> None:
    """Poll an upcoming video until it goes live, ends, or the poll window expires."""
    start_polling_at = scheduled_start_epoch - UPCOMING_POLL_START_BEFORE_S
    giveup_at = scheduled_start_epoch + UPCOMING_POLL_GIVEUP_AFTER_S
    delay = max(0.0, start_polling_at - time.time())
    if delay:
        await asyncio.sleep(delay)
    while time.time() < giveup_at:
        if video_id in _state and _state[video_id]["status"] in ("live", "completed"):
            return  # handled via another path (e.g. watchdog) meanwhile
        state = await video_state(video_id)
        if state["status"] == "live":
            await start_live_job(video_id)
            return
        if state["status"] in ("ended", "none"):
            _mark(video_id, state["status"])
            return
        await asyncio.sleep(UPCOMING_POLL_INTERVAL_S)
    log.info("gave up polling upcoming video_id=%s after %ds window", video_id, UPCOMING_POLL_GIVEUP_AFTER_S)
    _mark(video_id, "upcoming_timeout")


async def on_video(video_id: str) -> None:
    existing = _state.get(video_id, {}).get("status")
    if existing in TERMINAL_STATUSES or existing == "live":
        log.info("video_id=%s already handled (status=%s), ignoring", video_id, existing)
        return
    # existing == "error" falls through to retry below.
    state = await video_state(video_id)
    if state["status"] == "live":
        await start_live_job(video_id)
    elif state["status"] == "upcoming":
        scheduled = state["raw"]["liveStreamingDetails"]["scheduledStartTime"]
        # e.g. "2026-07-20T12:00:00Z"
        epoch = time.mktime(time.strptime(scheduled, "%Y-%m-%dT%H:%M:%SZ")) - time.timezone
        _mark(video_id, "upcoming")
        asyncio.create_task(poll_upcoming(video_id, epoch))
    else:
        _mark(video_id, state["status"])


async def resubscribe_loop() -> None:
    """Renews the hub lease periodically. Startup already did the initial subscribe."""
    while True:
        await asyncio.sleep(RESUBSCRIBE_INTERVAL_S)
        try:
            await websub_subscribe()
            log.info("websub subscribe/renew ok")
        except Exception:
            log.exception("websub subscribe/renew failed")


async def watchdog_loop() -> None:
    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL_S)
        try:
            video_id = await channel_live_video_id()
            if video_id and (video_id not in _state or _state[video_id]["status"] == "error"):
                log.info("watchdog found unhandled live video_id=%s", video_id)
                await on_video(video_id)
        except Exception:
            log.exception("watchdog loop iteration failed")


app = FastAPI()
app.include_router(make_router(on_video))


async def _resume_stale_jobs() -> None:
    """A restart mid-stream leaves state="live" (or "error") with no _current_job running.
    Recheck each against videos.list: still live -> restart the job; otherwise finalize it.
    A stale "live"/"error" entry must never be silently treated as handled."""
    stale = [vid for vid, entry in _state.items() if entry["status"] in ("live", "error")]
    for video_id in stale:
        state = await video_state(video_id)
        if state["status"] == "live":
            log.info("resuming stale live job for video_id=%s after restart", video_id)
            await start_live_job(video_id)
        else:
            log.info("stale video_id=%s is now status=%s, finalizing", video_id, state["status"])
            _mark(video_id, state["status"])


@app.on_event("startup")
async def startup() -> None:
    _load_state()
    try:
        await _resume_stale_jobs()
    except Exception:
        log.exception("resuming stale jobs failed, continuing boot")
    try:
        await websub_subscribe()
        log.info("websub subscribe ok")
    except Exception:
        log.exception("initial websub subscribe failed")
    asyncio.create_task(resubscribe_loop())
    asyncio.create_task(watchdog_loop())
    log.info("startup complete, %d video(s) in state", len(_state))


if __name__ == "__main__":
    import os
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
