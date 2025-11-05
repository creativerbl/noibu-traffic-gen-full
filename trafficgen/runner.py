
import asyncio
import signal
import random
import contextlib
import os
import time
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

        # Health + telemetry
        self.restart_event = asyncio.Event()
        self._pending_restart_reason: Optional[str] = None
        self._consecutive_failures = 0
        self._scheduler_cooldown_until = 0.0
        self._scheduler_cooldown_logged = False
        self.metrics: Dict[str, int] = {
            "started": 0,
            "completed": 0,
            "failed": 0,
            "timeouts": 0,
            "browser_restarts": 0,
        }

        # Tunables with environment overrides
        self._browser_failure_threshold = self._parse_int_env(
            "BROWSER_FAILURE_THRESHOLD", default=5, minimum=1
        )
        self._scheduler_failure_threshold = self._parse_int_env(
            "SCHEDULER_FAILURE_THRESHOLD", default=10, minimum=1
        )
        self._scheduler_cooldown_seconds = self._parse_float_env(
            "SCHEDULER_COOLDOWN_SECONDS", default=15.0, minimum=1.0
        )

        avg_minutes = max(float(self.cfg.avg_session_minutes or 1.0), 0.5)
        default_timeout = max(60.0, avg_minutes * 120.0)
        self._session_timeout = self._parse_float_env(
            "SESSION_MAX_SECONDS", default=default_timeout, minimum=30.0
        )

        default_refresh_sessions = max(
            50, int(self.cfg.sessions_per_minute * avg_minutes * 2) or 50
        )
        self._browser_session_refresh_limit = self._parse_int_env(
            "BROWSER_MAX_SESSIONS", default=default_refresh_sessions, minimum=0
        )
        default_refresh_minutes = max(15.0, avg_minutes * 4.0)
        refresh_minutes = self._parse_float_env(
            "BROWSER_MAX_MINUTES", default=default_refresh_minutes, minimum=0.0
        )
        self._browser_refresh_seconds = refresh_minutes * 60.0 if refresh_minutes > 0 else 0.0

        self._sessions_since_launch_success = 0
        self._browser_launched_at = 0.0

    @staticmethod
    def _parse_int_env(key: str, default: int, minimum: int) -> int:
        raw = os.getenv(key)
        try:
            if raw is None:
                return max(default, minimum)
            return max(int(raw), minimum)
        except Exception:
            return max(default, minimum)

    @staticmethod
    def _parse_float_env(key: str, default: float, minimum: float) -> float:
        raw = os.getenv(key)
        try:
            if raw is None:
                return max(default, minimum)
            return max(float(raw), minimum)
        except Exception:
            return max(default, minimum)

    async def run(self):
        loop = asyncio.get_running_loop()
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(self._graceful_stop(s)))

        headless = True  # Chromium-only; headless by default
        async with async_playwright() as pw:
            device_pool = build_device_pool(self.cfg.device_mix)
            while not self.stop_event.is_set():
                browser = await pw.chromium.launch(headless=headless)
                self._on_browser_launch()
                try:
                    await self._schedule_loop(browser, pw, device_pool)
                finally:
                    with contextlib.suppress(Exception):
                        await browser.close()
                self._log_metrics_snapshot("cycle complete")
                if self.stop_event.is_set():
                    break
                if not self.restart_event.is_set():
                    break
                self.metrics["browser_restarts"] += 1
                reason = self._pending_restart_reason or "unspecified"
                debug_print(
                    self.cfg.debug,
                    f"Restarting browser #{self.metrics['browser_restarts']} (reason: {reason})",
                )
                self._pending_restart_reason = None
                self.restart_event.clear()
                await asyncio.sleep(random.uniform(1.0, 3.0))

    def _on_browser_launch(self):
        self._sessions_since_launch_success = 0
        self._browser_launched_at = time.monotonic()
        self._consecutive_failures = 0
        self._scheduler_cooldown_until = 0.0
        self._scheduler_cooldown_logged = False
        debug_print(self.cfg.debug, "Browser launched and health counters reset")

    def _request_restart(self, reason: str):
        if not self.restart_event.is_set():
            self._pending_restart_reason = reason
            debug_print(self.cfg.debug, f"Browser restart requested: {reason}")
            self.restart_event.set()

    def _register_session_result(self, success: bool, timed_out: bool = False):
        if success:
            self.metrics["completed"] += 1
            self._consecutive_failures = 0
            self._sessions_since_launch_success += 1
            if self._scheduler_cooldown_until > 0:
                self._scheduler_cooldown_until = 0.0
                self._scheduler_cooldown_logged = False
                debug_print(self.cfg.debug, "Scheduler cooldown reset after success")
            self._maybe_schedule_browser_rotation()
        else:
            self.metrics["failed"] += 1
            self._consecutive_failures += 1
            if timed_out:
                self.metrics["timeouts"] += 1
            if self._consecutive_failures >= self._browser_failure_threshold:
                self._request_restart(
                    f"{self._consecutive_failures} consecutive session failures"
                )
            if self._consecutive_failures >= self._scheduler_failure_threshold:
                self._open_scheduler_circuit()

    def _maybe_schedule_browser_rotation(self):
        if self.restart_event.is_set():
            return
        if self._browser_session_refresh_limit > 0 and self._sessions_since_launch_success >= self._browser_session_refresh_limit:
            self._request_restart(
                f"rotating browser after {self._sessions_since_launch_success} successful sessions"
            )
            return
        if self._browser_refresh_seconds > 0 and (time.monotonic() - self._browser_launched_at) >= self._browser_refresh_seconds:
            self._request_restart(
                f"rotating browser after {self._browser_refresh_seconds/60:.1f} minutes"
            )

    def _open_scheduler_circuit(self):
        until = time.monotonic() + self._scheduler_cooldown_seconds
        if until <= self._scheduler_cooldown_until:
            return
        self._scheduler_cooldown_until = until
        self._scheduler_cooldown_logged = False
        debug_print(
            self.cfg.debug,
            f"Scheduler cooldown engaged for {self._scheduler_cooldown_seconds:.1f}s",
        )

    def _scheduler_circuit_active(self) -> bool:
        if self._scheduler_cooldown_until <= 0:
            return False
        now = time.monotonic()
        if now >= self._scheduler_cooldown_until:
            self._scheduler_cooldown_until = 0.0
            self._scheduler_cooldown_logged = False
            debug_print(self.cfg.debug, "Scheduler cooldown cleared")
            return False
        if not self._scheduler_cooldown_logged:
            remaining = self._scheduler_cooldown_until - now
            debug_print(
                self.cfg.debug,
                f"Scheduler backing off for {remaining:.1f}s after failures",
            )
            self._scheduler_cooldown_logged = True
        return True

    def _log_metrics_snapshot(self, context: str):
        debug_print(
            self.cfg.debug,
            f"{context}: started={self.metrics['started']} completed={self.metrics['completed']} "
            f"failed={self.metrics['failed']} timeouts={self.metrics['timeouts']} "
            f"browser_restarts={self.metrics['browser_restarts']}",
        )

    async def _schedule_loop(self, browser, pw, device_pool):
        interval = max(60.0 / max(self.cfg.sessions_per_minute, 0.1), 0.25)
        debug_print(self.cfg.debug, f"Start interval ≈ {interval:.2f}s for {self.cfg.sessions_per_minute} sessions/min")
        started_total = 0
        while not self.stop_event.is_set():
            if self.restart_event.is_set():
                debug_print(self.cfg.debug, "Restart requested; pausing scheduling")
                break
            if self.cfg.kill_switch_file:
                try:
                    if os.path.exists(self.cfg.kill_switch_file):
                        debug_print(self.cfg.debug, "Kill switch present; draining…")
                        break
                except Exception:
                    pass
            if self.smoke_limit is not None and started_total >= self.smoke_limit:
                break
            if self._scheduler_circuit_active():
                await asyncio.sleep(min(interval, 1.0))
                continue
            await asyncio.sleep(interval * random.uniform(0.85, 1.15))
            if self.stop_event.is_set() or self.restart_event.is_set():
                break
            await self.sem.acquire()
            self.session_counter += 1
            started_total += 1
            self.metrics["started"] += 1
            if self.metrics["started"] % 25 == 0:
                self._log_metrics_snapshot("trafficgen metrics")
            asyncio.create_task(
                self._run_session(self.session_counter, browser, pw, device_pool),
                name=f"session-{self.session_counter}",
            )
        while self.sem._value < self.cfg.max_concurrency:
            await asyncio.sleep(0.5)
        self._log_metrics_snapshot("drain complete")

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
        success = False
        timed_out = False
        record_metrics = True
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
            try:
                await asyncio.wait_for(s.run(), timeout=self._session_timeout)
                success = True
            except asyncio.TimeoutError:
                timed_out = True
                debug_print(
                    self.cfg.debug,
                    f"[session {sid}] timed out after {self._session_timeout:.1f}s",
                )
        except asyncio.CancelledError:
            record_metrics = False
            raise
        except Exception as e:
            debug_print(self.cfg.debug, f"[session {sid}] error: {e}")
        finally:
            if record_metrics:
                self._register_session_result(success, timed_out)
            self.sem.release()

    async def _graceful_stop(self, sig):
        debug_print(self.cfg.debug, f"Signal {sig} received: draining…")
        self.stop_event.set()
