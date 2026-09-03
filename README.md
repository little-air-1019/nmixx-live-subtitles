# nmixx-live-subtitles

Watches a YouTube channel for live streams, translates the Korean speech to Traditional
Chinese (zh-TW), and posts subtitle lines to a Discord webhook in near real time.

Pipeline: YouTube WebSub push -> live/upcoming/ended classification -> ffmpeg 16 kHz mono
PCM in 100 ms chunks paced at real time -> Gemini Live Translate (zh-Hant) -> Discord.

Translation runs as a single Gemini Live Translate WebSocket stream (`GEMINI_LIVE_MODEL`,
default `gemini-3.5-live-translate-preview`) -- audio goes in, translated Traditional
Chinese text comes out, with no separate ASR or text-translation step. Note this is a
**Preview** model; a two-hour live stream costs roughly **$4.42 USD** (~$0.0368/audio
minute, billed by stream duration, not speaking time).

Full architecture design and diagram: [ARCHITECTURE.md](ARCHITECTURE.md).

## Setup

```
brew install ffmpeg yt-dlp ngrok   # external binaries, not covered by uv sync
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
caffeinate -i uv run uvicorn nmixx_subtitles.main:app --port 8080
```

## Run

```
uv run uvicorn nmixx_subtitles.main:app --port 8080
```

On startup the service loads `state.json` (handled-video dedupe log), subscribes to the
channel's WebSub feed, and starts a renewal loop (re-subscribes every ~4 days, ahead of the
hub's 5-day lease) plus a watchdog loop (polls the channel's `/live` URL every 10 minutes as
a backstop in case a WebSub push is missed).

When a video goes live, the service captures audio, streams it to Gemini Live Translate, and
posts the translated lines to Discord -- one live stream at a time. Upcoming (scheduled) streams are polled
every 60s starting ~2 minutes before their scheduled start, giving up after a 2-hour window.

Gemini returns incremental transcription fragments. The service forwards every non-empty
fragment immediately (the API's optional `finished` flag is not a delivery boundary), preserves
spaces and repeated words, and joins fragments until sentence punctuation. A one-second fallback
keeps unpunctuated speech bounded instead of waiting indefinitely.

## Running as a launchd service (survives reboots)

Two launch agents keep the pipeline alive across crashes, logouts, and reboots:

- `~/Library/LaunchAgents/com.nmixx.subtitles.plist` — the service on port 8080, wrapped in
  `caffeinate -s` (keeps the Mac awake while on AC power). Logs to `service.log` in this repo.
- `~/Library/LaunchAgents/com.nmixx.ngrok.plist` — the ngrok tunnel (static domain → 8080).
  Logs to `~/Library/Logs/nmixx-ngrok.log`.

Both are `RunAtLoad` (start at login) + `KeepAlive` (auto-restart on crash).

One-time install from templates in `deploy/` (run from the repo root on the target machine;
use your ngrok dev domain — the free one permanently assigned to your ngrok account):

```
mkdir -p ~/Library/Logs ~/Library/LaunchAgents
sed "s|__REPO_DIR__|$PWD|g" deploy/com.nmixx.subtitles.plist \
  > ~/Library/LaunchAgents/com.nmixx.subtitles.plist
sed -e "s|__HOME__|$HOME|g" -e "s|__NGROK_DOMAIN__|https://YOUR-DOMAIN.ngrok-free.app|g" \
  deploy/com.nmixx.ngrok.plist > ~/Library/LaunchAgents/com.nmixx.ngrok.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.nmixx.subtitles.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.nmixx.ngrok.plist
```

After this one-time bootstrap, both services start automatically at every login.

```
# status (PID / last exit code)
launchctl list | grep com.nmixx

# restart
launchctl kickstart -k gui/$(id -u)/com.nmixx.subtitles
launchctl kickstart -k gui/$(id -u)/com.nmixx.ngrok

# stop (until next login)
launchctl bootout gui/$(id -u)/com.nmixx.subtitles
launchctl bootout gui/$(id -u)/com.nmixx.ngrok

# start again after a bootout
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.nmixx.subtitles.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.nmixx.ngrok.plist

# follow logs
tail -f service.log
```

Launch agents start at **login**, not at raw boot — enable auto-login if the Mac may reboot
unattended. It is safe to shut the Mac down when no stream is expected: on the next login the
tunnel reconnects to the same domain, the service re-subscribes to WebSub immediately, resumes
any stream that was interrupted mid-capture, and the watchdog catches an already-running live
within ~10 minutes. Streams that happen while the Mac is off are simply missed.

## Test

```
uv run python tests/smoke_trigger.py         # WebSub + videos.list + watchdog against real APIs
uv run python tests/test_live_translate.py   # Gemini Live Translate stream, offline checks
uv run python tests/test_main_pipeline.py    # pipeline wiring, offline checks
uv run python -m nmixx_subtitles.discord     # Discord poster, offline checks
```

Manual end-to-end check without waiting for a real stream: call
`nmixx_subtitles.main.run_live_job(video_id_or_url_or_local_path)` directly against any
currently-live video or a local audio file.

`glossary.md` is now a manual human quality checklist (member names, slang, lore terms) for
reviewers to check subtitles against -- the Live Translate model doesn't accept a custom
prompt or glossary, so nothing from it is injected at translation time.

Google documents reduced accuracy around background music/noise, overlapping speakers, heavy
accents, similar languages, and rapid language switching. If those cases still miss the glossary
quality gate, use the two-stage fallback in `docs/model-research.md`; client-side chunking cannot
correct a model translation after it has been emitted.
