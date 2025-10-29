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
from trafficgen.proxy import NullProxyProvider, PIAProxyProvider, ProxyProvider
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
    rotate_ip_interval_sec: int
    kill_switch_file: Optional[str]
    browsers_enabled: List[str]
    device_mix: List[dict]
    locales: List[str]
    timezones: List[str]
    flows: List[dict]
    proxy_backend: str
    piactl_path: str
    pia_regions: List[str]
    think_times: Dict[str, int]
    smoke: bool = False
    debug: bool = False

    @staticmethod
    def from_dict(d: dict, flows: List[dict], debug: bool = False) -> "RunnerConfig":
        site = d.get("site", {})
        t = d.get("traffic", {})
        return RunnerConfig(
            origin=site.get("origin", "https://noibu.mybigcommerce.com"),
            allowlist_roots=site.get("allowlist_roots", ["https://noibu.mybigcommerce.com"]),
            sessions_per_minute=float(t.get("sessions_per_minute", 25)),
            avg_session_minutes=float(t.get("avg_session_minutes", 3)),
            max_concurrency=int(t.get("max_concurrency", 100)),
            global_qps_cap=float(t.get("global_qps_cap", 6)),
            checkout_complete_rate=float(t.get("checkout_complete_rate", 0.3)),
            allow_checkout=bool(t.get("allow_checkout", True)),
            rotate_ip_interval_sec=int(t.get("rotate_ip_interval_sec", 900)),
            kill_switch_file=t.get("kill_switch_file"),
            browsers_enabled=d.get("browsers", {}).get("enabled", ["chromium", "webkit", "firefox"]),
            device_mix=d.get("devices", {}).get("mix", []),
            locales=d.get("locales", ["en-US"]),
            timezones=d.get("timezones", ["America/Toronto"]),
            flows=flows,
            proxy_backend=d.get("proxy", {}).get("backend", "null"),
            piactl_path=d.get("proxy", {}).get("piactl_path", "/usr/bin/piactl"),
            pia_regions=d.get("proxy", {}).get("pia_regions", []),
            think_times=d.get("think_times", {"page_min_ms": 800, "page_max_ms": 3000, "scroll_min_ms": 200, "scroll_max_ms": 1000}),
            debug=debug,
        )

def _parse_csv(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x and x.strip()]

def _normalize_to_100(weights: List[str]) -> List[int]:
    vals = []
    for w in weights:
        try:
            v = float(w) if str(w).strip() else 0.0
        except Exception:
            v = 0.0
        vals.append(max(0.0, v))
    total = sum(vals)
    if total <= 0:
        vals = [1.0 for _ in vals] if vals else [1.0]
        total = float(len(vals))
    scaled = [ (v * 100.0) / total for v in vals ]
    floored = [int(math.floor(x)) for x in scaled]
    remainder = 100 - sum(floored)
    fracs = [(i, scaled[i] - floored[i]) for i in range(len(scaled))]
    fracs.sort(key=lambda t: t[1], reverse=True)
    for i in range(max(0, remainder)):
        idx = fracs[i % len(fracs)][0]
        floored[idx] += 1
    return floored

def _build_referrers_from_env() -> List[Dict[str, Any]]:
    sources = _parse_csv(os.getenv("REFERRER_SOURCES", ""))
    weights = _parse_csv(os.getenv("REFERRER_WEIGHTS", ""))
    if not sources:
        return []
    if not weights or len(weights) != len(sources):
        defaults = [1 for _ in sources]
        weights = [str(w) for w in defaults]
    norm = _normalize_to_100(weights)
    return [{"source": s, "weight": int(w)} for s, w in zip(sources, norm)]

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
        return random.choice(items)
    r = random.uniform(0, total)
    acc = 0.0
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

        # IP rotation
        self.proxy: ProxyProvider = NullProxyProvider() if self.cfg.proxy_backend == "null" else PIAProxyProvider(self.cfg.piactl_path, self.cfg.pia_regions)
        self.rotation_task: Optional[asyncio.Task] = None

        # NEW: load referrers from env once
        self.referrers = _build_referrers_from_env()

    async def run(self):
        loop = asyncio.get_running_loop()
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(self._graceful_stop(s)))

        async with async_playwright() as pw:
            # Launch engines once
            browsers = {}
            for b in self.cfg.browsers_enabled:
                try:
                    if b == "chromium":
                        browsers[b] = await pw.chromium.launch(headless=True)
                    elif b == "firefox":
                        browsers[b] = await pw.firefox.launch(headless=True)
                    elif b == "webkit":
                        browsers[b] = await pw.webkit.launch(headless=True)
                except Exception as e:
                    debug_print(self.cfg.debug, f"Failed to launch {b}: {e}")

            device_pool = build_device_pool(self.cfg.device_mix)

            # start proxy (optional)
            try:
                await self.proxy.start()
            except Exception as e:
                debug_print(self.cfg.debug, f"Proxy start error: {e}")

            if self.cfg.rotate_ip_interval_sec > 0 and self.proxy.rotation_enabled():
                self.rotation_task = asyncio.create_task(self._rotation_loop())

            # schedule sessions
            tasks = []
            try:
                tasks.append(asyncio.create_task(self._schedule_loop(browsers, pw, device_pool)))
                await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
            finally:
                if self.rotation_task:
                    self.rotation_task.cancel()
                    with contextlib.suppress(Exception):
                        await self.rotation_task
                await asyncio.gather(*(b.close() for b in browsers.values()), return_exceptions=True)
                try:
                    await self.proxy.stop()
                except Exception:
                    pass

    async def _rotation_loop(self):
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(self.cfg.rotate_ip_interval_sec)
                await self.proxy.rotate()
        except asyncio.CancelledError:
            return

    async def _schedule_loop(self, browsers: dict, pw, device_pool):
        # Spread session starts uniformly across each minute with jitter
        interval = max(60.0 / max(self.cfg.sessions_per_minute, 0.1), 0.25)
        debug_print(self.cfg.debug, f"Start interval ≈ {interval:.2f}s for {self.cfg.sessions_per_minute} sessions/min")
        started_total = 0
        while not self.stop_event.is_set():
            # Check kill switch
            if self.cfg.kill_switch_file:
                try:
                    if os.path.exists(self.cfg.kill_switch_file):
                        debug_print(self.cfg.debug, "Kill switch present; draining…")
                        break
                except Exception:
                    pass

            # smoke limit
            if self.smoke_limit is not None and started_total >= self.smoke_limit:
                break

            await asyncio.sleep(interval * random.uniform(0.85, 1.15))
            # choose engine
            if not browsers:
                continue
            engine = random.choice(list(browsers.keys()))
            browser = browsers[engine]
            await self.sem.acquire()
            self.session_counter += 1
            started_total += 1
            asyncio.create_task(self._run_session(self.session_counter, browser, pw, device_pool), name=f"session-{self.session_counter}")

        # Drain: wait for ongoing
        while self.sem._value < self.cfg.max_concurrency:
            await asyncio.sleep(0.5)

    def _choose_referrer_for_session(self) -> Optional[str]:
        if not self.referrers:
            return None
        picked = _weighted_pick(self.referrers, key="weight") or {}
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
                referrer_url=ref,  # NEW: pass per-session referrer (None for direct)
            )
            await s.run()
        except Exception as e:
            debug_print(self.cfg.debug, f"[session {sid}] error: {e}")
        finally:
            self.sem.release()

    async def _graceful_stop(self, sig):
        debug_print(self.cfg.debug, f"Signal {sig} received: draining…")
        self.stop_event.set()
