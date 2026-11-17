# nmixx-live-subtitles

Watches a YouTube channel for live streams, translates the Korean speech to Traditional
Chinese (zh-TW), and posts subtitle lines to a Discord webhook in near real time.

Pipeline: YouTube WebSub push -> live/upcoming/ended classification -> ffmpeg 16 kHz mono
PCM in 100 ms chunks paced at real time -> selected subtitle provider -> Discord.

The default provider remains Gemini. Translation runs as a single Gemini Live Translate WebSocket stream (`GEMINI_LIVE_MODEL`,
default `gemini-3.5-live-translate-preview`) -- audio goes in, translated Traditional
Chinese text comes out, with no separate ASR or text-translation step. Note this is a
**Preview** model; a two-hour live stream costs roughly **$4.42 USD** (~$0.0368/audio
minute, billed by stream duration, not speaking time).

Soniox is also available: by default `stt-rt-v5` transcribes and translates Korean into
Chinese in one stream. English goes directly to subtitles, and only other languages use
the Gemini text API (`gemini-3.5-flash-lite`). Spoken Chinese is normalized locally. No translated speech is
generated. OpenCC converts Chinese output to Traditional Chinese with Taiwan phrase mappings.
See the trial instructions below.

Full architecture design and diagram: [ARCHITECTURE.md](ARCHITECTURE.md).

## Setup

```
brew install ffmpeg yt-dlp ngrok   # external binaries, not covered by uv sync
uv sync
cp .env.example .env   # first setup only; do not overwrite an existing .env
```

`PUBLIC_BASE_URL` must be a stable HTTPS URL that YouTube's PubSubHubbub hub can reach and
that forwards to this service's port (8080 by default, see below) -- e.g. an ngrok tunnel:

```
ngrok http 8080
```

Set `PUBLIC_BASE_URL` to the ngrok URL and `WEBSUB_VERIFY_TOKEN` to a random secret shared
between this service and the hub (the app itself owns the verification handshake).
Also fill in `DISCORD_WEBHOOK_URL`, `YOUTUBE_API_KEY`, and the API key for your selected provider.

## Try Soniox with the existing Discord subtitles

Create a key in the [Soniox Console](https://console.soniox.com) and add these settings to
your existing `.env` (unquoted values). Keep the actual key local:

```dotenv
SONIOX_API_KEY=your-key-here
SONIOX_MODEL=stt-rt-v5
SONIOX_TRANSLATE_ENGLISH=false
TEXT_TRANSLATION_MODEL=gemini-3.5-flash-lite
```

The App's weekly free credits do not supply the API balance. Soniox
[ended free API credits for new signups](https://soniox.com/blog/2025-10-27-free-credits-update-for-soniox-api).
If a preview returns `402 organization_balance_exhausted`, stop and check the API Console;
the adapter does not retry that error or add funds. You can evaluate the App using its own
credits before deciding whether to fund the API, but that does not validate this integration.

First preview 30 seconds of a Korean/English audio file or a YouTube video in the terminal:

```sh
uv sync
uv run python -m nmixx_subtitles.preview '/path/to/sample.m4a' --seconds 30
# Or use a YouTube URL as the source. The default preview provider is soniox.
```

This calls the real API and uses your paid balance, but sends no Discord messages and does
not modify `state.json` or start live detection. Subtitles go to stdout; source transcripts
and errors go to stderr. It caps audio at the requested duration, then waits for final text.
Check member names, mixed Korean/English speech, missing words, and perceived delay. Offline
tests cannot validate recognition/translation quality or real network latency.

After checking the preview, set `SUBTITLE_PROVIDER=soniox` in `.env` and restart the existing
service. If installed using the launchd instructions below:

```sh
launchctl kickstart -k gui/$(id -u)/com.nmixx.subtitles
```

For a manually started service, stop and restart the existing `uvicorn` process. Configuration
is read at process startup. Roll back by setting `SUBTITLE_PROVIDER=gemini` and restarting;
retain `GEMINI_API_KEY` for this. The service does not automatically switch paid providers on errors.

With `SONIOX_TRANSLATE_ENGLISH=false`, Soniox uses Korean/Chinese `two_way` translation.
Korean's native Chinese translation goes directly to Discord; English uses its original
transcript. Only finalized Japanese and other languages outside Korean/English/Chinese are
sent to `TEXT_TRANSLATION_MODEL` using `GEMINI_API_KEY`. Spoken Chinese is normalized with
OpenCC; the reverse Korean translation is suppressed locally but may still incur Soniox
output charges. Soniox rejects `exclude_source_languages`, so this hybrid handles every
non-English language without translating English. Gemini fallback usage is billed separately.
No Korean or English segments are submitted to Gemini, and no translated audio is generated.

For fallback languages, final subword tokens are joined until sentence punctuation, a language change, an endpoint,
six seconds of finalized speech, or 600 characters. Requests run in order to preserve subtitle
sequence; only fallback languages incur an extra text-generation delay. A final partial sentence
is drained when the audio ends. Translation failures surface as job errors rather than silently
dropping text. Language routing depends on Soniox's recognition and may misclassify short phrases.

YouTube HLS capture starts at the latest available segment (`live_start_index=-1`), avoiding
FFmpeg's default two extra segments of lag (about ten seconds with five-second segments).
Segment publication, recognition finalization, and Discord delivery still add latency; the
latest segment can also be more sensitive to brief network stalls.

With `SONIOX_TRANSLATE_ENGLISH=true`, Soniox's native `one_way` translation sends all supported
source languages to Chinese, with no separate text API. Language identification happens in
Soniox, with no local recognizer or separate language-detection API call.
See [translation modes](https://soniox.com/docs/translation/stt-translation).

Only final tokens are posted: revisable hypotheses and `<end>`/`<fin>` markers are excluded.
`glossary.md` mappings are sent as context/translation terms; these guide the model and do not
guarantee exact names. Final fragments are joined before OpenCC conversion and Discord delivery.
OpenCC normalizes writing; it cannot fix translation errors.

Connections rotate before the documented 300-minute limit; transient errors retry with backoff,
and authentication/balance errors stop the current attempt. A disconnect can lose already-sent
audio's unfinalized text because this adapter has no session resumption/replay. A bounded audio
queue drops oldest input during prolonged stalls to stay near live. See [WebSocket protocol](https://soniox.com/docs/api-reference/stt/websocket-api).

On a Mac, prevent the machine from sleeping while the service should be watching for streams:

```
caffeinate -i uv run uvicorn nmixx_subtitles.main:app --port 8080
```

## FANS through iPhone Mirroring (Mac)

Open iPhone Mirroring on the Mac, play FANS, and confirm that its audio is audible on
the Mac. The manual runner uses macOS ScreenCaptureKit to capture only iPhone Mirroring
audio and converts it to the existing 16 kHz mono subtitle input. It requires macOS 15+,
the Swift compiler (`swiftc`), ffmpeg, and Screen & System Audio Recording permission for
the terminal/app running the command. No cable or virtual audio driver is needed.

```sh
# Ten-minute trial using the configured provider, with subtitles sent to Discord:
uv run python -m nmixx_subtitles.mirror --seconds 600 --discord

# Terminal subtitles only (still calls the paid translation API):
uv run python -m nmixx_subtitles.mirror --seconds 60
```

For this runner, `--discord` reads `DISCORD_WEBHOOK_URL` directly from **this project's
`.env`**, even if the shell has a different webhook configured. A missing project webhook
is an error; there is no fallback to a machine-wide destination.

Keep FANS playing through iPhone Mirroring. Ctrl+C stops the session. `--seconds` sets a
limit of 1–7200 seconds (default 600); capture also stops on lost audio input or 90 seconds
of silence (15 seconds if no signal has appeared yet). Each invocation must be started
manually; it does not detect FANS broadcasts or modify YouTube job state. Only one mirror
session may run at a time. The Swift helper is compiled into `~/Library/Caches/nmixx-live-subtitles`.

## Local recording and quality review

Live YouTube jobs and manual FANS sessions now record the input audio and structured
evidence locally by default. After the session, an offline check screens for the 16
previously observed issue categories and creates an HTML audio review page. It adds no
AI API requests. Candidates require review; they are not confirmed translation errors.

Files go to `~/Library/Application Support/nmixx-subtitles/sessions/<session>/`.
Default retention is seven days with an approximate 2 GiB storage budget; completed old
sessions are cleaned at the next session start. Set `QUALITY_CAPTURE_ENABLED=false` to
disable. See [recording, review and retention instructions](docs/quality-review.md).

## Run

```
uv run uvicorn nmixx_subtitles.main:app --port 8080
```

On startup the service loads `state.json` (handled-video dedupe log), subscribes to the
channel's WebSub feed, and starts a renewal loop (re-subscribes every ~4 days, ahead of the
hub's 5-day lease) plus a watchdog loop (polls the channel's `/live` URL every 10 minutes as
a backstop in case a WebSub push is missed).

When a video goes live, the service captures audio, streams it to the selected provider, and
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
uv run python tests/test_soniox_translate.py # Soniox protocol, mixed languages, conversion, offline checks
uv run python tests/test_text_translate.py   # English bypass, multilingual routing, text API lifecycle
uv run python tests/test_hybrid_subtitles.py # native Korean avoids text API waits; fallback languages only
uv run python tests/test_capture.py         # early capture shutdown (local ffmpeg, no APIs)
uv run python tests/test_mirror.py          # project-specific Discord destination, no APIs
uv run python tests/test_main_pipeline.py    # pipeline wiring, offline checks
uv run python -m nmixx_subtitles.discord     # Discord poster, offline checks
```

Use `python -m nmixx_subtitles.preview` as above for a bounded real-API check without waiting
for a live stream. Add `--provider gemini` to compare against Gemini.

`glossary.md` is a human quality checklist (member names, slang, lore terms). Soniox also uses
its simple `source → target` mappings as translation context. Gemini Live Translate does not
accept a custom prompt/glossary, so that provider uses it only for manual review.

Google documents reduced accuracy around background music/noise, overlapping speakers, heavy
accents, similar languages, and rapid language switching. If those cases still miss the glossary
quality gate, use the two-stage fallback in `docs/model-research.md`; client-side chunking cannot
correct a model translation after it has been emitted.
