"""Batches items from an async stream into fixed-window lists, to cut downstream API calls."""
import asyncio
from typing import AsyncGenerator, AsyncIterable, TypeVar

T = TypeVar("T")

TRANSLATE_BATCH_S = 4.0


async def batch_by_window(items: AsyncIterable[T], window_s: float = TRANSLATE_BATCH_S) -> AsyncGenerator[list[T], None]:
    """Collect items into a batch for up to window_s after the first item arrives, then yield it.
    The window starts on first item, not on the previous flush, so a quiet stream doesn't spin.
    Flushes whatever is buffered when the source stream ends."""
    it = aiter(items)
    pending: asyncio.Task | None = None
    batch: list[T] = []
    deadline: float | None = None

    async def get_next():
        return await anext(it)

    try:
        while True:
            if pending is None:
                pending = asyncio.create_task(get_next())
            timeout = max(0.0, deadline - asyncio.get_event_loop().time()) if deadline is not None else None
            done, _ = await asyncio.wait({pending}, timeout=timeout)
            if pending in done:
                try:
                    item = pending.result()
                except StopAsyncIteration:
                    if batch:
                        yield batch
                    return
                pending = None
                batch.append(item)
                if deadline is None:
                    deadline = asyncio.get_event_loop().time() + window_s
            else:
                yield batch
                batch = []
                deadline = None
    finally:
        if pending is not None:
            pending.cancel()


if __name__ == "__main__":
    async def gen(items_with_delays):
        for item, delay in items_with_delays:
            if delay:
                await asyncio.sleep(delay)
            yield item

    async def main():
        # Test 1: items within the window merge into one batch
        batches = [b async for b in batch_by_window(gen([("a", 0), ("b", 0.1), ("c", 0.1)]), window_s=1.0)]
        assert batches == [["a", "b", "c"]], batches
        print("test1 (merge within window) ok:", batches)

        # Test 2: window expiry flushes, then a new window starts
        batches = [b async for b in batch_by_window(gen([("a", 0), ("b", 0.15), ("c", 0.3)]), window_s=0.2)]
        assert batches == [["a", "b"], ["c"]], batches
        print("test2 (window expiry flush) ok:", batches)

        # Test 3: stream end flushes remainder without waiting out the window
        batches = [b async for b in batch_by_window(gen([("a", 0)]), window_s=10.0)]
        assert batches == [["a"]], batches
        print("test3 (stream end flush) ok:", batches)

        # Test 4: empty stream yields nothing
        batches = [b async for b in batch_by_window(gen([]), window_s=1.0)]
        assert batches == [], batches
        print("test4 (empty stream) ok:", batches)

        print("all offline self-checks passed")

    asyncio.run(main())
