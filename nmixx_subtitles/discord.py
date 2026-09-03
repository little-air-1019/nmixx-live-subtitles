"""Coalescing Discord webhook poster for streamed subtitle lines."""
import asyncio
import re
import time

import httpx

from nmixx_subtitles.config import DISCORD_WEBHOOK_URL

FLUSH_AGE_S = 1.0
FLUSH_CHARS = 500
DISCORD_MAX_CHARS = 2000
SENTENCE_END = re.compile(
    r"[。！？…]+[」』”’）》】)\]]*|[.!?]+[\"'”’」』）》】)\]]*(?=\s|$)"
)


class DiscordPoster:
    """Buffers subtitle fragments and posts them to a Discord webhook."""

    def __init__(self, webhook_url: str = DISCORD_WEBHOOK_URL):
        if not webhook_url or not webhook_url.startswith(("http://", "https://")):
            raise ValueError(f"DiscordPoster needs a valid http(s) webhook URL, got: {webhook_url!r}")
        self.webhook_url = webhook_url
        self._buf: list[str] = []
        self._buf_chars = 0
        self._buf_started: float | None = None
        self._lock = asyncio.Lock()
        self._client = httpx.AsyncClient(timeout=10.0)
        self._closed = False
        self._stop = asyncio.Event()
        # ponytail: watcher task is created lazily on first send() (line ~34), not here,
        # so constructing a DiscordPoster outside a running event loop doesn't raise.
        self._flush_task: asyncio.Task | None = None

    async def send(self, fragment: str) -> None:
        if self._closed:
            raise RuntimeError("DiscordPoster.send() called after close()")
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._age_watcher())
        async with self._lock:
            start = 0
            for boundary in SENTENCE_END.finditer(fragment):
                part = fragment[start:boundary.end()]
                self._buf.append(part)
                self._buf_chars += len(part)
                if self._buf_started is None:
                    self._buf_started = time.monotonic()
                try:
                    await self._flush_locked()
                except Exception:
                    suffix = fragment[boundary.end():]
                    if suffix:
                        self._buf.append(suffix)
                        self._buf_chars += len(suffix)
                    raise
                start = boundary.end()
            tail = fragment[start:]
            if tail:
                self._buf.append(tail)
                self._buf_chars += len(tail)
                if self._buf_started is None:
                    self._buf_started = time.monotonic()
                if self._buf_chars >= FLUSH_CHARS:
                    await self._flush_locked()

    async def close(self) -> None:
        self._closed = True
        self._stop.set()
        if self._flush_task:
            await self._flush_task  # let it finish its current cycle, never cancel mid-flush
        async with self._lock:
            await self._flush_locked()
        await self._client.aclose()

    async def _age_watcher(self) -> None:
        # ponytail: polling is simpler than per-buffer deadline scheduling and precise enough here.
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
            async with self._lock:
                if self._buf_started is not None and time.monotonic() - self._buf_started >= FLUSH_AGE_S:
                    try:
                        await self._flush_locked()
                    except Exception:
                        # A failed flush must not kill the watcher, or all future age-flushes stop forever.
                        pass

    async def _flush_locked(self) -> None:
        """Caller must hold self._lock. Only removes delivered fragments from the buffer;
        undelivered fragments are put back so a POST failure never loses text."""
        if not self._buf:
            return
        text = "".join(self._buf)
        chunks = _split_chunks(text, DISCORD_MAX_CHARS)
        for i, chunk in enumerate(chunks):
            try:
                await self._post(chunk)
            except Exception:
                # Requeue this chunk and everything after it; already-sent chunks stay gone.
                remainder = "".join(chunks[i:])
                self._buf = [remainder] if remainder else []
                self._buf_chars = len(remainder)
                self._buf_started = time.monotonic() if remainder else None
                raise
        self._buf = []
        self._buf_chars = 0
        self._buf_started = None

    async def _post(self, content: str) -> None:
        while True:
            resp = await self._client.post(self.webhook_url, json={"content": content})
            if resp.status_code == 429:
                retry_after = 1.0
                try:
                    retry_after = float(resp.json().get("retry_after") or 1.0)
                except (ValueError, TypeError):
                    pass
                await asyncio.sleep(retry_after)
                continue
            resp.raise_for_status()
            remaining = resp.headers.get("X-RateLimit-Remaining")
            reset_after = resp.headers.get("X-RateLimit-Reset-After")
            if remaining is not None and reset_after is not None and float(remaining) <= 0:
                await asyncio.sleep(float(reset_after))
            return


def _split_chunks(text: str, limit: int) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks = []
    while text:
        chunks.append(text[:limit])
        text = text[limit:]
    return chunks


if __name__ == "__main__":
    class FakeResponse:
        def __init__(self, status_code, json_data=None, headers=None):
            self.status_code = status_code
            self._json = json_data or {}
            self.headers = headers or {}

        def json(self):
            return self._json

        def raise_for_status(self):
            if self.status_code >= 400 and self.status_code != 429:
                raise httpx.HTTPStatusError("error", request=None, response=self)

    class FakeClient:
        def __init__(self, script=None):
            self.posts = []
            self.script = script or []  # list of status codes (or exceptions) to return in sequence per call

        async def post(self, url, json):
            self.posts.append(json["content"])
            if self.script:
                item = self.script.pop(0)
                if isinstance(item, Exception):
                    raise item
                if item == 429:
                    return FakeResponse(429, {"retry_after": 0.01})
                return FakeResponse(item)
            return FakeResponse(204)

        async def aclose(self):
            pass

    async def main():
        # Test 1: fragments batch together under age/char thresholds
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient()
        await poster.send("line1")
        await poster.send("line2")
        await poster.send("line3")
        await poster.close()
        assert poster._client.posts == ["line1line2line3"], poster._client.posts
        print("test1 (coalesce small lines) ok:", poster._client.posts)

        # Test 1b: streamed translation fragments stay joined and sentence punctuation
        # flushes immediately instead of waiting for an arbitrary wall-clock boundary.
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient()
        await poster.send("海")
        await poster.send("嫄姐姐")
        await poster.send("好！")
        assert poster._client.posts == ["海嫄姐姐好！"], poster._client.posts
        await poster.close()
        print("test1b (join fragments and flush sentence) ok")

        # Test 1c: English code-switch sentence punctuation is also a boundary.
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient()
        await poster.send("Hello world.")
        assert poster._client.posts == ["Hello world."], poster._client.posts
        await poster.close()
        print("test1c (flush English sentence) ok")

        # Test 1d: a complete sentence inside a larger API chunk flushes while the
        # unfinished tail stays buffered for the next chunk.
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient()
        await poster.send("第一句。第二")
        assert poster._client.posts == ["第一句。"], poster._client.posts
        await poster.close()
        assert poster._client.posts == ["第一句。", "第二"], poster._client.posts
        print("test1d (flush punctuation inside fragment) ok")

        # Test 1e: periods inside the first-message YouTube URL are not boundaries.
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient()
        first_message = "https://www.youtube.com/watch?v=abcdefghijk\n大家好。"
        await poster.send(first_message)
        assert poster._client.posts == [first_message], poster._client.posts
        await poster.close()
        print("test1e (preserve URL periods) ok")

        # Test 1f: trailing quotes/brackets stay attached to the sentence punctuation.
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient()
        await poster.send("她說：「好。」下一句")
        assert poster._client.posts == ["她說：「好。」"], poster._client.posts
        await poster.close()
        assert poster._client.posts == ["她說：「好。」", "下一句"], poster._client.posts
        print("test1f (keep closing quote with sentence) ok")

        # Test 2: 500-char flush triggers mid-stream
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient()
        await poster.send("a" * 300)
        await poster.send("b" * 300)  # crosses 500 -> flush
        await poster.send("c" * 10)
        await poster.close()
        assert len(poster._client.posts) == 2, poster._client.posts
        assert poster._client.posts[0] == "a" * 300 + "b" * 300
        assert poster._client.posts[1] == "c" * 10
        print("test2 (500-char flush) ok:", [len(p) for p in poster._client.posts])

        # Test 3: 2000-char split on a single flush
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient()
        big = "x" * 2500
        async with poster._lock:
            poster._buf = [big]
            poster._buf_chars = len(big)
            await poster._flush_locked()
        assert len(poster._client.posts) == 2, [len(p) for p in poster._client.posts]
        assert len(poster._client.posts[0]) == 2000
        assert len(poster._client.posts[1]) == 500
        await poster.close()
        print("test3 (2000-char split) ok:", [len(p) for p in poster._client.posts])

        # Test 4: 429 with retry_after retries then succeeds
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient(script=[429, 204])
        await poster._post("retry me")
        assert poster._client.posts == ["retry me", "retry me"], poster._client.posts
        await poster.close()
        print("test4 (429 retry_after retry) ok")

        # Test 5: failed POST does not lose the batch, and a later send still gets through
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient(script=[httpx.ConnectError("boom")])
        try:
            async with poster._lock:
                poster._buf = ["will fail"]
                poster._buf_chars = len("will fail")
                await poster._flush_locked()
            assert False, "expected exception"
        except httpx.ConnectError:
            pass
        assert poster._buf == ["will fail"], poster._buf  # requeued, not lost
        # retry succeeds now
        poster._client.script = []
        async with poster._lock:
            await poster._flush_locked()
        assert poster._client.posts == ["will fail", "will fail"], poster._client.posts
        assert poster._buf == []
        await poster.close()
        print("test5 (failed POST requeues batch, retry succeeds) ok")

        # Test 5b: watcher survives a flush exception and keeps age-flushing afterward
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient(script=[httpx.ConnectError("boom")])
        await poster.send("line-a")  # starts the watcher
        poster._buf_started = time.monotonic() - FLUSH_AGE_S - 1  # force it "old"
        await asyncio.sleep(0.7)  # let the watcher tick, hit the exception, and survive
        assert not poster._flush_task.done(), "watcher must not die from a flush exception"
        poster._client.script = []
        await asyncio.sleep(0.7)  # watcher retries the still-buffered line and succeeds
        assert poster._client.posts, "watcher should have retried and delivered the line"
        await poster.close()
        print("test5b (watcher survives flush exception) ok")

        # Test 5c: a failed punctuation flush retains the unprocessed suffix from the
        # same API fragment, not just the sentence that was being posted.
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient(script=[httpx.ConnectError("boom")])
        try:
            await poster.send("第一句。第二句")
            assert False, "expected exception"
        except httpx.ConnectError:
            pass
        assert "".join(poster._buf) == "第一句。第二句", poster._buf
        poster._client.script = []
        await poster.close()
        assert poster._client.posts[-1] == "第一句。第二句", poster._client.posts
        print("test5c (failed boundary flush retains suffix) ok")

        # Test 6: 429 with null/non-numeric retry_after falls back to a default delay instead of crashing
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient()
        poster._client.script = [FakeResponse(429, {"retry_after": None}), FakeResponse(204)]

        async def post_with_script(url, json):
            item = poster._client.script.pop(0)
            poster._client.posts.append(json["content"])
            return item

        poster._client.post = post_with_script
        await poster._post("null retry_after")
        assert poster._client.posts == ["null retry_after", "null retry_after"]
        await poster.close()
        print("test6 (429 null retry_after falls back to default) ok")

        # Test 7: send() after close() raises instead of silently dropping
        poster = DiscordPoster("http://fake")
        poster._client = FakeClient()
        await poster.close()
        try:
            await poster.send("too late")
            assert False, "expected RuntimeError"
        except RuntimeError:
            pass
        print("test7 (send-after-close raises) ok")

        # Test 8: empty/invalid webhook URL raises at construction, not later
        try:
            DiscordPoster("")
            assert False, "expected ValueError"
        except ValueError:
            pass
        try:
            DiscordPoster("not-a-url")
            assert False, "expected ValueError"
        except ValueError:
            pass
        print("test8 (bad webhook URL raises at construction) ok")

        print("all offline self-checks passed")

    asyncio.run(main())
