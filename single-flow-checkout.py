#!/usr/bin/env python3
"""
Single-flow checkout generator for noibudemo.com

Repeatedly runs:
  Homepage → Scroll → Click "Orbit Terrarium - Large" → Add to Cart →
  Proceed to Checkout → Fill email → Fill shipping → Fill payment → Place Order

Each session uses randomized device type, browser, locale, timezone, and referrer.
"""

import os
import asyncio
import signal
import random
import math
from dotenv import load_dotenv
from playwright.async_api import async_playwright

from trafficgen.devices import build_device_pool, pick_device
from trafficgen.checkout_session import CheckoutSession
from trafficgen.utils import TokenBucket, debug_print

load_dotenv(override=False)


# ── Parsing helpers ──

def _parse_csv(s):
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _normalize_to_100(weights):
    vals = []
    for w in weights:
        try:
            v = float(w)
        except Exception:
            v = 0.0
        vals.append(max(0.0, v))
    total = sum(vals)
    if total <= 0:
        vals = [1.0 for _ in vals] if vals else [1.0]
        total = float(len(vals) or 1.0)
    scaled = [(v * 100.0) / total for v in vals]
    floored = [int(math.floor(x)) for x in scaled]
    remainder = 100 - sum(floored)
    fracs = [(i, scaled[i] - floored[i]) for i in range(len(scaled))]
    fracs.sort(key=lambda t: t[1], reverse=True)
    for i in range(max(0, remainder)):
        idx = fracs[i % len(fracs)][0]
        floored[idx] += 1
    return floored


def build_referrers():
    sources = _parse_csv(os.getenv("REFERRER_SOURCES", ""))
    weights = _parse_csv(os.getenv("REFERRER_WEIGHTS", ""))
    if not sources:
        sources = ["direct", "google", "bing"]
    if not weights or len(weights) != len(sources):
        defaults = [60, 25, 15] + [5] * max(0, len(sources) - 3)
        weights = [str(w) for w in defaults[:len(sources)]]
    norm = _normalize_to_100(weights)
    return [{"source": s, "weight": int(w)} for s, w in zip(sources, norm)]


def build_device_mix():
    raw = os.getenv("DEVICE_MIX", "")
    pairs = _parse_csv(raw)
    out = []
    for pair in pairs:
        if ":" in pair:
            name, wt = pair.split(":", 1)
            try:
                w = float(wt.strip())
            except Exception:
                w = 1.0
        else:
            name, w = pair, 1.0
        name = name.strip()
        if name:
            out.append({"name": name, "weight": w})
    if not out:
        out = [
            {"name": "iphone-safari", "weight": 15},
            {"name": "iphone-chrome", "weight": 10},
            {"name": "android-chrome", "weight": 20},
            {"name": "desktop-chrome", "weight": 25},
            {"name": "desktop-edge", "weight": 10},
            {"name": "desktop-safari", "weight": 10},
            {"name": "desktop-firefox", "weight": 10},
        ]
    return out


def _weighted_pick(items, key="weight"):
    if not items:
        return None
    weights = []
    total = 0.0
    for it in items:
        try:
            w = float(it.get(key, 0))
        except Exception:
            w = 0.0
        w = max(0.0, w)
        weights.append(w)
        total += w
    if total <= 0:
        return random.choice(items)
    r = random.uniform(0, total)
    acc = 0.0
    for it, w in zip(items, weights):
        acc += w
        if r <= acc:
            return it
    return items[-1]


# ── Runner ──

class SingleFlowRunner:
    def __init__(self):
        self.origin = os.getenv("ORIGIN", "https://noibudemo.com").rstrip("/")
        self.sessions_per_min = float(os.getenv("SESSIONS_PER_MINUTE", "25"))
        self.max_concurrency = int(os.getenv("MAX_CONCURRENCY", "100"))
        self.global_qps_cap = float(os.getenv("GLOBAL_QPS_CAP", "6"))
        self.debug = os.getenv("DEBUG", "0") == "1"
        self.device_mix = build_device_mix()
        self.locales = _parse_csv(os.getenv("LOCALES", "en-US,en-CA,en-GB,fr-CA"))
        self.timezones = _parse_csv(os.getenv("TIMEZONES",
                                               "America/Toronto,America/New_York,America/Vancouver,Europe/London"))
        self.referrers = build_referrers()

        self.stop_event = asyncio.Event()
        self.sem = asyncio.Semaphore(self.max_concurrency)
        self.global_qps = TokenBucket(rate_per_sec=self.global_qps_cap)
        self.session_counter = 0

    async def run(self):
        loop = asyncio.get_running_loop()
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(self._graceful_stop(s)))

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            device_pool = build_device_pool(self.device_mix)

            try:
                await self._schedule_loop(browser, pw, device_pool)
            finally:
                await browser.close()

    async def _schedule_loop(self, browser, pw, device_pool):
        interval = max(60.0 / max(self.sessions_per_min, 0.1), 0.25)
        debug_print(self.debug, f"Schedule interval ~ {interval:.2f}s for {self.sessions_per_min} sessions/min")

        while not self.stop_event.is_set():
            await asyncio.sleep(interval * random.uniform(0.85, 1.15))
            await self.sem.acquire()
            self.session_counter += 1
            asyncio.create_task(
                self._run_session(self.session_counter, browser, pw, device_pool),
                name=f"checkout-{self.session_counter}",
            )

        # Drain in-flight sessions
        while self.sem._value < self.max_concurrency:
            await asyncio.sleep(0.5)

    async def _run_session(self, sid, browser, pw, device_pool):
        try:
            dev = pick_device(device_pool, pw)
            locale = random.choice(self.locales or ["en-US"])
            tz = random.choice(self.timezones or ["America/Toronto"])

            # Pick referrer
            ref = None
            picked = _weighted_pick(self.referrers, key="weight")
            if picked:
                src = (picked.get("source") or "").strip()
                if src and src.lower() != "direct":
                    ref = src

            session = CheckoutSession(
                session_id=sid,
                browser=browser,
                playwright=pw,
                origin=self.origin,
                device_context_args=dev["context_args"],
                locale=locale,
                timezone_id=tz,
                global_qps=self.global_qps,
                debug=self.debug,
                referrer_url=ref,
            )
            await session.run()
        except Exception as e:
            debug_print(self.debug, f"[S{sid}] runner error: {e}")
        finally:
            self.sem.release()

    async def _graceful_stop(self, sig):
        debug_print(self.debug, f"Signal {sig}: draining…")
        self.stop_event.set()


def main():
    runner = SingleFlowRunner()
    print(">> Single-flow checkout generator")
    print(f"   Origin: {runner.origin}")
    print(f"   Rate: {runner.sessions_per_min}/min, max concurrency: {runner.max_concurrency}")
    print("   Flow: Homepage -> Orbit Terrarium PDP -> Add to Cart -> Checkout -> Place Order")
    print(flush=True)
    try:
        asyncio.run(runner.run())
    except KeyboardInterrupt:
        print("SIGINT: shutting down", flush=True)


if __name__ == "__main__":
    main()
