"""Test: batch_by_window merges within-window items, flushes on expiry and on stream end.
Run with: uv run python tests/test_batcher.py"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nmixx_subtitles.batcher import batch_by_window


async def gen(items_with_delays):
    for item, delay in items_with_delays:
        if delay:
            await asyncio.sleep(delay)
        yield item


async def main():
    batches = [b async for b in batch_by_window(gen([("a", 0), ("b", 0.1), ("c", 0.1)]), window_s=1.0)]
    assert batches == [["a", "b", "c"]], batches
    print("PASS: items within window merge into one batch:", batches)

    batches = [b async for b in batch_by_window(gen([("a", 0), ("b", 0.15), ("c", 0.3)]), window_s=0.2)]
    assert batches == [["a", "b"], ["c"]], batches
    print("PASS: window expiry flushes:", batches)

    batches = [b async for b in batch_by_window(gen([("a", 0)]), window_s=10.0)]
    assert batches == [["a"]], batches
    print("PASS: stream end flushes remainder:", batches)

    print("all tests passed")


if __name__ == "__main__":
    asyncio.run(main())
