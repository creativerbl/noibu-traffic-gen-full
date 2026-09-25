# trafficgen/runner.py — schedules ab-test sessions.
#
# Two scheduling modes:
#   BACK_TO_BACK=1  next session starts 2-5s after a slot frees up
#   BACK_TO_BACK=0  one session every 60/SESSIONS_PER_MINUTE seconds (±jitter)
# MAX_CONCURRENCY caps how many run at once (1 = strictly one at a time).
import asyncio
import contextlib
import os
import random
import signal
from dataclasses import dataclass

from playwright.async_api import async_playwright

from trafficgen.session import Session
from trafficgen.utils import debug_print


def _f(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        return max(float(os.getenv(name, str(default))), minimum)
    except ValueError:
        return default


@dataclass
class RunnerConfig:
    origin: str
    debug: bool
    headless: bool
    max_concurrency: int
    session_max_seconds: float
    browser_per_session: bool
    back_to_back: bool
    gap_min_s: float
    gap_max_s: float
    sessions_per_minute: float
    jitter: float

    @classmethod
    def from_env(cls) -> "RunnerConfig":
        gap_min = _f("BACK_TO_BACK_GAP_MIN_S", 2.0)
        return cls(
            origin=os.getenv("ORIGIN", "https://noibudemo.com").rstrip("/"),
            debug=os.getenv("DEBUG", "1") == "1",
            headless=os.getenv("HEADLESS", "1") != "0",
            max_concurrency=max(1, int(_f("MAX_CONCURRENCY", 1, 1))),
            session_max_seconds=_f("SESSION_MAX_SECONDS", 600, 30),
            browser_per_session=os.getenv("BROWSER_PER_SESSION", "1") == "1",
            back_to_back=os.getenv("BACK_TO_BACK", "1") == "1",
            gap_min_s=gap_min,
            gap_max_s=max(gap_min, _f("BACK_TO_BACK_GAP_MAX_S", 5.0)),
            sessions_per_minute=_f("SESSIONS_PER_MINUTE", 0.0833333, 1e-6),
            jitter=min(_f("SCHEDULER_JITTER", 0.15), 0.9),
        )


class Runner:
    def __init__(self, cfg: RunnerConfig):
        self.cfg = cfg
        # Created in run(): on Python 3.9, asyncio primitives made outside the
        # running loop bind to a different loop and fail under asyncio.run().
        self.stop_event: asyncio.Event = None
        self.sem: asyncio.Semaphore = None
        self.session_counter = 0
        self.consecutive_errors = 0  # back-off so a broken setup can't spin

    def log(self, msg: str):
        debug_print(self.cfg.debug, msg)

    async def _launch(self, pw):
        return await pw.chromium.launch(
            headless=self.cfg.headless,
            args=["--disable-cache", "--disable-application-cache",
                  "--disk-cache-size=0", "--aggressive-cache-discard"],
        )

    async def run(self):
        self.stop_event = asyncio.Event()
        self.sem = asyncio.Semaphore(self.cfg.max_concurrency)
        loop = asyncio.get_running_loop()
        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, self._request_stop)
        async with async_playwright() as pw:
            shared = None if self.cfg.browser_per_session else await self._launch(pw)
            try:
                await self._schedule_loop(pw, shared)
            finally:
                if shared is not None:
                    with contextlib.suppress(Exception):
                        await shared.close()

    def _request_stop(self):
        if not self.stop_event.is_set():
            self.log("Stop requested: letting running sessions finish… (Ctrl+C again to force)")
            self.stop_event.set()
            # A second Ctrl+C falls through to Python's default handler.
            with contextlib.suppress(Exception):
                asyncio.get_running_loop().remove_signal_handler(signal.SIGINT)

    async def _sleep_or_stop(self, seconds: float) -> bool:
        """Sleep; return True if a stop was requested meanwhile."""
        if self.stop_event.is_set():
            return True
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def _schedule_loop(self, pw, shared_browser):
        c = self.cfg
        interval = 60.0 / c.sessions_per_minute
        if c.back_to_back:
            self.log(f"BACK_TO_BACK=1: next session starts {c.gap_min_s:.0f}-{c.gap_max_s:.0f}s "
                     f"after a slot frees (max {c.max_concurrency} at once)")
        else:
            self.log(f"Start interval ≈ {interval:.0f}s (jitter ±{c.jitter:.0%}), "
                     f"max {c.max_concurrency} at once")
        tasks = set()
        first = True
        while not self.stop_event.is_set():
            if c.back_to_back:
                await self.sem.acquire()  # waits for the previous session to end
                wait = random.uniform(1.0, 3.0) if first else random.uniform(c.gap_min_s, c.gap_max_s)
                if self.consecutive_errors:
                    wait = max(wait, min(300.0, 10.0 * 2 ** (self.consecutive_errors - 1)))
                    self.log(f"{self.consecutive_errors} session error(s) in a row; waiting {wait:.0f}s")
                if await self._sleep_or_stop(wait):
                    self.sem.release()
                    break
            else:
                wait = random.uniform(1.0, 3.0) if first else interval * random.uniform(1 - c.jitter, 1 + c.jitter)
                if await self._sleep_or_stop(wait):
                    break
                await self.sem.acquire()
            first = False
            self.session_counter += 1
            t = asyncio.create_task(self._run_session(self.session_counter, pw, shared_browser))
            tasks.add(t)
            t.add_done_callback(tasks.discard)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_session(self, sid: int, pw, shared_browser):
        own = None
        try:
            browser = shared_browser
            if browser is None:
                own = browser = await self._launch(pw)
                self.log(f"[S{sid}] fresh Chromium process for this session")
            s = Session(sid, browser, self.cfg.origin, debug=self.cfg.debug)
            await asyncio.wait_for(s.run(), timeout=self.cfg.session_max_seconds)
            self.consecutive_errors = 0
        except asyncio.TimeoutError:
            self.log(f"[S{sid}] timed out after {self.cfg.session_max_seconds:.0f}s")
        except Exception as e:
            self.consecutive_errors += 1
            self.log(f"[S{sid}] error: {type(e).__name__}: {str(e).splitlines()[0]}")
        finally:
            if own is not None:
                with contextlib.suppress(Exception):
                    await own.close()
            self.sem.release()
