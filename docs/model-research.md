# Live subtitle model research

Research date: 2026-07-23
Scope: Korean YouTube live audio → English and/or Traditional Chinese subtitles

## Recommendation

Use **Google `gemini-3.5-live-translate-preview`** as a single-stage
audio-to-translated-subtitle model.

For one target language, a two-hour live costs:

```text
120 audio minutes × $0.0368/minute = $4.416 ≈ $4.42 USD
```

Google prices input and output audio by duration, so the safe budget is the full live
duration, not speaking time.

Why this is the best first change:

- It is purpose-built for continuous interpretation, accepts live audio, and emits
  translated output transcription while the source audio is still arriving.
- It removes the overloaded local Whisper stage that is currently falling farther
  behind real time.
- It removes the separate text-translation call and its all-or-nothing batch contract,
  the path currently responsible for untranslated `[KR]` output.
- Google explicitly documents both Korean (`ko`) and Traditional Chinese (`zh-Hant`);
  OpenAI documents only generic “Chinese.”
- It accepts the project's existing 16 kHz PCM format and uses the already-installed
  Google SDK.

The tradeoff is that the model is Preview and costs about **$0.34 more per two-hour
live** than OpenAI's comparable model. That is a better trade than making this
zh-TW-specific project depend on an undocumented Chinese variant.

## What is wrong with the current pipeline

Current flow:

```text
YouTube → 16 kHz PCM → local WhisperLiveKit/mlx-whisper medium
        → up to 4 s translation batch
        → Gemini 3.1 Flash-Lite
        → up to 4 s Discord buffer
```

The observed speed problem is primarily local ASR throughput, not Gemini latency.
`service.log` shows Whisper lag increasing beyond **860 seconds** while the last
processed source timestamp is only about **267 seconds**. The log also contains 115
successful HTTP 200 Gemini calls, so this run was not simply waiting on failed Gemini
requests.

There are also deterministic delay and failure paths in the code:

- `asr.py` waits for committed Whisper text; its own comment notes WhisperLiveKit can
  keep a committed line growing until more than five seconds of silence.
- `batcher.py` deliberately waits up to four seconds after the first segment.
- `discord.py` can then wait another four seconds before posting.
- `translate.py` returns `[KR] <source>` for an empty model response, a translated
  line-count mismatch, **or any exception**, and logs none of those causes.

Therefore, changing only the Gemini text model would not fix the dominant lag. It also
would not reveal why a specific batch stayed Korean.

## Direct OpenAI vs. direct Gemini

| Choice | Live translated text | Korean input | Traditional Chinese target | Custom glossary | Status | Two-hour cost |
|---|---:|---:|---:|---:|---|---:|
| OpenAI `gpt-realtime-translate` | Yes, transcript deltas | Yes | Only generic “Chinese” documented | No | Default model; not labeled Preview | **$4.08** |
| **Google `gemini-3.5-live-translate-preview`** | Yes, output transcription | Yes | Yes, `zh-Hant` | No instructions | Preview | **$4.416 ≈ $4.42** |

Google's live-translate contract is clearer for this project's exact output locale:
its official language table explicitly includes Korean (`ko`) and Traditional Chinese
(`zh-Hant`). It also accepts the project's existing 16 kHz PCM, while OpenAI's
server-side WebSocket path requires 24 kHz PCM16. However, Google is Preview, costs
about $0.34 more per two-hour target, and normal audio-only Live API sessions are
limited to 15 minutes unless session-management extensions are implemented.

No official Korean→Traditional-Chinese head-to-head evaluation compares these two
models. It would be unsupported to claim that OpenAI is linguistically more accurate
based on vendor documentation alone. The recommendation is based on explicit zh-Hant
support, direct live translation, compatibility with the existing audio/SDK path, and
the small absolute price difference—not an invented benchmark.

## One model or two stages?

One model **can** directly translate this live Korean audio:
`gemini-3.5-live-translate-preview` uses the Gemini Live API and exposes translated
output transcription continuously. The project does not have to keep separate ASR
and text-translation stages.

The other OpenAI speech routes do require two stages for Chinese:

- `gpt-realtime-whisper` is live speech-to-text, not translation. It costs
  $0.017/minute, or $2.04 for two hours, before adding a text translation model.
- `gpt-4o-transcribe` is the higher-accuracy transcription choice, but OpenAI
  recommends it for request/response workflows where native streaming is not
  required. It costs $0.006/minute, or $0.72 for two hours, before translation.
- The Audio API `translations` endpoint translates audio to **English**, not
  Traditional Chinese, and its translation route uses `whisper-1`.

A two-stage OpenAI design is cheaper and preserves a promptable text-translation
stage, but it also preserves more latency, two failure surfaces, and transcript-error
propagation. It is the fallback architecture if exact glossary/zh-TW control proves
more important than the one-model speed and simplicity.

## Important acceptance gates

Dedicated live-translation models have a material tradeoff for this repository: they
do not expose the prompt/glossary control used by the current text-translation step.
The existing `glossary.md` and honorific rules cannot simply be moved into the live
model, so NMIXX names need direct testing and, if necessary, deterministic
post-processing.

Before replacing the current path, run a 20–30 minute bilingual golden-set test using
real NMIXX clips and require all of these:

1. Korean member names, fandom terms, honorifics, numbers, and fast overlapping speech
   meet an agreed accuracy threshold.
2. `zh-Hant` output uses Taiwan-acceptable wording, not merely Traditional script.
3. Subtitle delay stays bounded for the whole clip instead of growing with runtime.
4. Disconnect/reconnect testing produces an explicit delayed/unavailable state and
   does not silently lose transcript deltas.
5. Source transcript and translated transcript are retained during the pilot so a
   reviewer can distinguish “heard wrong” from “translated wrong.”

Google documents language-detection limitations for heavy accents and rapid language
switching. This can look like “it didn't translate,” so the application should
preserve source captions or visibly mark that case.

## Implementation implications (not implemented here)

The minimal migration shape would be:

```text
YouTube → ffmpeg 16 kHz PCM16
        → one Gemini Live translation WebSocket
        → output transcription
        → Discord
```

That bypasses local Whisper, `batch_by_window`, and the Gemini text call. Discord may
still coalesce deltas enough to avoid webhook spam, but keeping the current four-second
translation batch would defeat the purpose. Because normal audio-only Live sessions
are limited to 15 minutes, the implementation must rotate/resume sessions without
dropping or duplicating transcript output during a two-hour live.

## Primary sources

OpenAI:

- [`gpt-realtime-translate` model card and $0.034/min pricing](https://developers.openai.com/api/docs/models/gpt-realtime-translate)
- [Realtime translation guide](https://developers.openai.com/api/docs/guides/realtime-translation)
- [Official OpenAI Cookbook: supported languages, 24 kHz transport, no custom glossary, and production caveats](https://github.com/openai/openai-cookbook/blob/main/examples/voice_solutions/realtime_translation_guide.mdx)
- [Speech-to-text and translation endpoint behavior](https://developers.openai.com/api/docs/guides/speech-to-text)
- [Realtime transcription model selection](https://developers.openai.com/api/docs/guides/realtime-transcription)
- [OpenAI API pricing](https://developers.openai.com/api/docs/pricing)

Google:

- [Gemini Live Translate guide, language list, transcripts, formats, and limitations](https://ai.google.dev/gemini-api/docs/live-api/live-translate)
- [`gemini-3.5-live-translate-preview` model page](https://ai.google.dev/gemini-api/docs/models/gemini-3.5-live-translate-preview)
- [Gemini API pricing](https://ai.google.dev/gemini-api/docs/pricing)
- [Gemini Live API session-duration limits](https://ai.google.dev/gemini-api/docs/live-api/capabilities)
