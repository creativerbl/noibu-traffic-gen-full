
import asyncio
import signal
import random
import contextlib
import os
import math
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from playwright.async_api import async_playwright

from trafficgen.devices import build_device_pool, pick_device
from trafficgen.session import Session
from trafficgen.utils import TokenBucket, debug_print

@dataclass
class RunnerConfig:
    origin: str
    allowlist_roots: List[str]
    sessions_per_minute: float
    avg_session_minutes: float
    max_concurrency: int
    global_qps_cap: float
    checkout_complete_rate: float
    allow_checkout: bool
    device_mix: List[dict]
    locales: List[str]
    timezones: List[str]
    flows: List[dict]
    think_times: Dict[str, int]
    smoke: bool = False
    debug: bool = False
    kill_switch_file: Optional[str] = None
    referrers: Optional[List[Dict[str, Any]]] = None

def _weighted_pick(items: List[Dict[str, Any]], key: str = "weight") -> Optional[Dict[str, Any]]:
    if not items:
        return None
    weights = []
    total = 0.0
    for it in items:
        try:
            w = float(it.get(key, 0) or 0)
        except Exception:
            w = 0.0
        if w < 0:
            w = 0.0
        weights.append(w)
        total += w
    if total <= 0:
        import random as _r
        return _r.choice(items)
    import random as _r
    r, acc = _r.uniform(0, total), 0.0
    for it, w in zip(items, weights):
        acc += w
        if r <= acc:
            return it
    return items[-1]

class Runner:
    def __init__(self, cfg: RunnerConfig):
        self.cfg = cfg
        self.stop_event = asyncio.Event()
        self.sem = asyncio.Semaphore(self.cfg.max_concurrency)
        self.global_qps = TokenBucket(rate_per_sec=self.cfg.global_qps_cap)
        self.session_counter = 0
        self.smoke_limit = 3 if self.cfg.smoke else None

    async def run(self):
        loop = asyncio.get_running_loop()
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(self._graceful_stop(s)))

        headless = True  # Chromium-only; headless by default
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=headless)
            device_pool = build_device_pool(self.cfg.device_mix)

            tasks = []
            try:
                tasks.append(asyncio.create_task(self._schedule_loop(browser, pw, device_pool)))
                await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
            finally:
                with contextlib.suppress(Exception):
                    await browser.close()

    async def _schedule_loop(self, browser, pw, device_pool):
        interval = max(60.0 / max(self.cfg.sessions_per_minute, 0.1), 0.25)
        debug_print(self.cfg.debug, f"Start interval ≈ {interval:.2f}s for {self.cfg.sessions_per_minute} sessions/min")
        started_total = 0
        while not self.stop_event.is_set():
            if self.cfg.kill_switch_file:
                try:
                    if os.path.exists(self.cfg.kill_switch_file):
                        debug_print(self.cfg.debug, "Kill switch present; draining…")
                        break
                except Exception:
                    pass
            if self.smoke_limit is not None and started_total >= self.smoke_limit:
                break
            await asyncio.sleep(interval * random.uniform(0.85, 1.15))
            await self.sem.acquire()
            self.session_counter += 1
            started_total += 1
            asyncio.create_task(self._run_session(self.session_counter, browser, pw, device_pool), name=f"session-{self.session_counter}")
        while self.sem._value < self.cfg.max_concurrency:
            await asyncio.sleep(0.5)

    def _choose_referrer_for_session(self) -> Optional[str]:
        items = self.cfg.referrers or []
        if not items:
            return None
        picked = _weighted_pick(items, key="weight") or {}
        src = (picked.get("source") or "").strip()
        if not src or src.lower() == "direct":
            return None
        return src

    async def _run_session(self, sid: int, browser, pw, device_pool):
        try:
            dev = pick_device(device_pool, pw)
            import random as _random
            locale = _random.choice(self.cfg.locales or ["en-US"])
            tz = _random.choice(self.cfg.timezones or ["America/Toronto"])
            ref = self._choose_referrer_for_session()
            s = Session(
                session_id=sid,
                browser=browser,
                playwright=pw,
                origin=self.cfg.origin,
                allowlist_roots=self.cfg.allowlist_roots,
                device_context_args=dev["context_args"],
                locale=locale,
                timezone_id=tz,
                allow_checkout=self.cfg.allow_checkout,
                checkout_complete_rate=self.cfg.checkout_complete_rate,
                flows=self.cfg.flows,
                think_cfg=self.cfg.think_times,
                global_qps=self.global_qps,
                debug=self.cfg.debug,
                fault_profile={"slow_request_fraction": 0.03},
                referrer_url=ref,
            )
            await s.run()
        except Exception as e:
            debug_print(self.cfg.debug, f"[session {sid}] error: {e}")
        finally:
            self.sem.release()

    async def _graceful_stop(self, sig):
        debug_print(self.cfg.debug, f"Signal {sig} received: draining…")
        self.stop_event.set()
