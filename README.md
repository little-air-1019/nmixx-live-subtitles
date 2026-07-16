# nmixx-live-transcribe

Watches a YouTube channel for live streams, transcribes Korean speech, translates it to
Traditional Chinese (zh-TW), and posts subtitle lines to a Discord webhook in near real time.

Pipeline: YouTube WebSub push -> live/upcoming/ended classification -> audio capture ->
Whisper (Korean) -> Gemini Flash (zh-TW) -> Discord.

## Setup

```
uv sync
cp .env.example .env   # fill in DISCORD_WEBHOOK_URL, GEMINI_API_KEY, YOUTUBE_API_KEY, PUBLIC_BASE_URL
```

`PUBLIC_BASE_URL` must be a stable HTTPS URL that YouTube's PubSubHubbub hub can reach and
that forwards to this service's port (8080 by default, see below) -- e.g. an ngrok tunnel:

```
ngrok http 8080
```

Set `PUBLIC_BASE_URL` to the ngrok URL and `WEBSUB_VERIFY_TOKEN` to a random secret shared
between this service and the hub (the app itself owns the verification handshake).

On a Mac, prevent the machine from sleeping while the service should be watching for streams:

```
caffeinate -i uv run uvicorn nmixx_transcribe.main:app --port 8080
```

## Run

```
uv run uvicorn nmixx_transcribe.main:app --port 8080
```

On startup the service loads `state.json` (handled-video dedupe log), subscribes to the
channel's WebSub feed, and starts a renewal loop (re-subscribes every ~4 days, ahead of the
hub's 5-day lease) plus a watchdog loop (polls the channel's `/live` URL every 10 minutes as
a backstop in case a WebSub push is missed).

When a video goes live, the service captures audio, transcribes it, translates each segment,
and posts it to Discord -- one live stream at a time. Upcoming (scheduled) streams are polled
every 60s starting ~2 minutes before their scheduled start, giving up after a 2-hour window.

## Test

```
uv run python tests/smoke_trigger.py   # WebSub + videos.list + watchdog against real APIs
uv run python tests/smoke_asr.py       # capture -> ASR against a local sample file
```

Manual end-to-end check without waiting for a real stream: call
`nmixx_transcribe.main.run_live_job(video_id_or_url_or_local_path)` directly against any
currently-live video or a local audio file.
