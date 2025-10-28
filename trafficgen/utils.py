import asyncio
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml
from urllib.parse import urlparse

def debug_print(enabled: bool, *args, **kwargs):
    if enabled:
        print("[DEBUG]", *args, **kwargs)

def deep_update(target: dict, src: dict) -> dict:
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(target.get(k), dict):
            deep_update(target[k], v)
        else:
            target[k] = v
    return target

def load_yaml_files(paths: List[str]) -> List[dict]:
    res = []
    for p in paths or []:
        if not p:
            continue
        path = Path(p)
        if not path.exists():
            # attempt direct open; ignore if missing
            try:
                with open(path, "r", encoding="utf-8") as f:
                    res.append(yaml.safe_load(f) or {})
            except FileNotFoundError:
                continue
        else:
            with open(path, "r", encoding="utf-8") as f:
                res.append(yaml.safe_load(f) or {})
    return res

class TokenBucket:
    """Simple token bucket for soft QPS limiting."""
    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = max(rate_per_sec, 0.1)
        self.capacity = capacity or self.rate * 2
        self.tokens = self.capacity
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens < 1.0:
                need = 1.0 - self.tokens
                await asyncio.sleep(need / self.rate)  # wait to accumulate 1 token
                self.tokens = 0.0
            else:
                self.tokens -= 1.0

async def think(min_ms: int, max_ms: int):
    amt = random.randint(min_ms, max_ms)
    await asyncio.sleep(amt / 1000.0)

def same_origin(url: str, allowlist_roots: List[str]) -> bool:
    try:
        host = urlparse(url).netloc
        scheme = urlparse(url).scheme
        if not host:
            return False
        for root in allowlist_roots:
            pu = urlparse(root)
            if pu.scheme == scheme and pu.netloc == host:
                return True
    except Exception:
        return False
    return False

class ExponentialBackoff:
    def __init__(self, base: float = 0.5, factor: float = 2.0, max_wait: float = 10.0):
        self.base = base
        self.factor = factor
        self.max_wait = max_wait
        self.attempts = 0

    async def wait(self):
        wait = min(self.max_wait, self.base * (self.factor ** self.attempts))
        self.attempts += 1
        await asyncio.sleep(wait + random.uniform(0, 0.3))

    def reset(self):
        self.attempts = 0

def choose_weighted(items: List[dict], key: str = "weight") -> Optional[dict]:
    if not items:
        return None
    weights = [max(float(i.get(key, 1.0)), 0.0) for i in items]
    total = sum(weights)
    if total <= 0:
        weights = [1.0 for _ in items]
        total = float(len(items))
    pick = random.uniform(0, total)
    upto = 0.0
    for item, w in zip(items, weights):
        if upto + w >= pick:
            return item
        upto += w
    return items[-1]
