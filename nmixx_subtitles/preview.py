"""Bounded real-API subtitle preview in the terminal, without Discord or job state."""
import argparse
import asyncio
import logging
from contextlib import aclosing

from nmixx_subtitles.capture import pcm_stream
from nmixx_subtitles.discord import DiscordPoster
from nmixx_subtitles.subtitles import stream_transcripts, subtitle_transform, validate_provider


class ConsolePoster(DiscordPoster):
    async def _post(self, content: str) -> None:
        print(content, flush=True)


async def limited_pcm(source: str, seconds: int):
    remaining = seconds * 32_000
    async with aclosing(pcm_stream(source)) as audio:
        async for chunk in audio:
            piece = chunk[:remaining]
            if piece:
                yield piece
                remaining -= len(piece)
            if remaining <= 0:
                return


async def preview(source: str, *, seconds: int, provider: str) -> None:
    validate_provider(provider)
    poster = ConsolePoster("http://unused.invalid", transform=subtitle_transform(provider))
    count = 0
    try:
        async with asyncio.timeout(seconds + 60):
            async with aclosing(limited_pcm(source, seconds)) as audio:
                async with aclosing(stream_transcripts(audio, provider=provider)) as events:
                    async for kind, text in events:
                        if kind == "source":
                            logging.info("source: %s", text)
                        else:
                            await poster.send(text)
                            count += 1
    finally:
        await poster.close()
    logging.info("Preview finished: %d subtitle fragments; no Discord messages sent", count)
    if not count:
        logging.warning("No subtitles received; verify the sample contains speech")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="YouTube URL/video ID or local audio file")
    parser.add_argument("--seconds", type=int, default=30, help="Maximum audio duration (default: 30)")
    parser.add_argument("--provider", choices=("gemini", "soniox"), default="soniox")
    args = parser.parse_args()
    if not 1 <= args.seconds <= 300:
        parser.error("--seconds must be between 1 and 300")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        asyncio.run(preview(args.source, seconds=args.seconds, provider=args.provider))
    except (ValueError, RuntimeError, TimeoutError) as exc:
        parser.exit(1, f"Preview failed: {exc}\n")


if __name__ == "__main__":
    main()
