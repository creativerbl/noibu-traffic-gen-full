import asyncio
import random


def debug_print(enabled: bool, *args, **kwargs):
    if enabled:
        print("[DEBUG]", *args, **kwargs, flush=True)


async def think(min_ms: int, max_ms: int):
    """Random human-like pause between min_ms and max_ms."""
    await asyncio.sleep(random.randint(min_ms, max_ms) / 1000.0)


def weighted_choice(items, weights):
    """Pick one of `items` using `weights` (falls back to uniform)."""
    if not items:
        return None
    if len(weights) != len(items) or sum(max(w, 0) for w in weights) <= 0:
        return random.choice(items)
    return random.choices(items, weights=[max(w, 0) for w in weights], k=1)[0]
