"""Select a live subtitle provider and normalize its display text."""
from collections.abc import AsyncIterable, AsyncIterator, Callable
from contextlib import aclosing

from nmixx_subtitles import config
from nmixx_subtitles import recording


def validate_provider(provider: str | None = None) -> str:
    selected = provider or config.SUBTITLE_PROVIDER
    if selected not in ("gemini", "soniox"):
        raise ValueError("SUBTITLE_PROVIDER must be gemini or soniox")
    key_name = "SONIOX_API_KEY" if selected == "soniox" else "GEMINI_API_KEY"
    if not getattr(config, key_name):
        raise ValueError(f"{key_name} is required for {selected} subtitles")
    if selected == "soniox" and not config.SONIOX_TRANSLATE_ENGLISH and not config.GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY is required for non-English text translation")
    return selected


def subtitle_transform(provider: str | None = None) -> Callable[[str], str] | None:
    if (provider or config.SUBTITLE_PROVIDER) != "soniox":
        return None
    from opencc import OpenCC

    # Convert after fragments have been joined, so phrases split across API frames
    # (e.g. "软" + "件") can be converted together. English is preserved.
    return OpenCC("s2twp").convert


async def stream_transcripts(
    pcm: AsyncIterable[bytes], *, provider: str | None = None,
) -> AsyncIterator[tuple[str, str]]:
    selected = validate_provider(provider)
    if selected == "soniox":
        from nmixx_subtitles.soniox_translate import stream_transcripts as stream
    else:
        from nmixx_subtitles.live_translate import stream_transcripts as stream
    async with aclosing(recording.observe_audio(pcm)) as observed, aclosing(stream(observed)) as events:
        if selected == "soniox" and not config.SONIOX_TRANSLATE_ENGLISH:
            from nmixx_subtitles.text_translate import translate_events

            async with aclosing(translate_events(events)) as translated:
                async for event in translated:
                    recording.emit("pipeline_text", text_kind=event[0], text=event[1])
                    yield event
        else:
            async for event in events:
                recording.emit("pipeline_text", text_kind=event[0], text=event[1])
                yield event
