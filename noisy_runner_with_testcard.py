# noisy_runner_with_testcard.py — normalized referrer weights + UTM sources
import os, asyncio, re, math
from playwright.async_api import TimeoutError as PWTimeoutError
from trafficgen.runner import Runner, RunnerConfig
from trafficgen.session import Session, SEL_TIMEOUT, ALLOW_NAV_TIMEOUT
from trafficgen.utils import ExponentialBackoff, same_origin, think

ORIGIN = os.getenv("ORIGIN", "https://noibu.mybigcommerce.com").rstrip("/")
SESSIONS_PER_MIN = float(os.getenv("SESSIONS_PER_MINUTE", "25"))
AVG_SESSION_MIN  = float(os.getenv("AVG_SESSION_MINUTES", "1"))
EXTRA_FIXED_WAIT_SEC = float(os.getenv("EXTRA_FIXED_WAIT_SEC", "5"))
CHECKOUT_COMPLETE_RATE = float(os.getenv("CHECKOUT_COMPLETE_RATE", "0.3"))

def _parse_csv(s):
    return [x.strip() for x in (s or "").split(",") if x and x.strip()]

def _normalize_to_100(weights):
    vals = [max(0.0, float(w) if str(w).strip() else 0.0) for w in weights]
    total = sum(vals)
    if total <= 0:
        # all zero or invalid -> uniform 1s
        vals = [1.0 for _ in weights]
        total = float(len(vals))
    scaled = [ (v * 100.0) / total for v in vals ]  # exact floats summing to 100
    # round with largest remainders strategy
    floored = [int(math.floor(x)) for x in scaled]
    remainder = 100 - sum(floored)
    # compute fractional parts
    fracs = [(i, scaled[i] - floored[i]) for i in range(len(scaled))]
    fracs.sort(key=lambda t: t[1], reverse=True)
    for i in range(remainder):
        idx = fracs[i % len(fracs)][0]
        floored[idx] += 1
    return floored

def build_referrers_from_env():
    sources = _parse_csv(os.getenv("REFERRER_SOURCES", ""))
    weights = _parse_csv(os.getenv("REFERRER_WEIGHTS", ""))
    if not sources:
        sources = ["direct", "https://www.google.com", "https://www.bing.com"]
    if not weights or len(weights) != len(sources):
        # default weights aligned to the above
        defaults = [50, 25, 15] + [5] * max(0, len(sources) - 3)
        weights = [str(w) for w in defaults[:len(sources)]]
    norm = _normalize_to_100(weights)
    items = []
    for s, w in zip(sources, norm):
        items.append({"source": s, "weight": int(w)})
    return items

REFERRERS = build_referrers_from_env()

def log(*a): print(*a, flush=True)

# Optional: confirm chosen referrer at session start
_orig_run = Session.run
async def _run_with_logs(self):
    try:
        await _orig_run(self)
    except Exception as e:
        raise
Session.run = _run_with_logs

# --- Runner config (Chromium-only) + referrers ---
cfg = RunnerConfig(
    origin=ORIGIN,
    allowlist_roots=[ORIGIN],
    sessions_per_minute=SESSIONS_PER_MIN,
    avg_session_minutes=AVG_SESSION_MIN,
    max_concurrency=int(SESSIONS_PER_MIN*AVG_SESSION_MIN)+10,
    global_qps_cap=6.0,
    kill_switch_file=None,
    allow_checkout=True,
    checkout_complete_rate=CHECKOUT_COMPLETE_RATE,
    device_mix=[
        {"name":"iphone-14","weight":1.0},
        {"name":"android-pixel","weight":1.0},
        {"name":"desktop-chrome","weight":1.0},
    ],
    locales=["en-US","en-CA","en-GB","fr-CA"],
    timezones=["America/Toronto","America/New_York","America/Vancouver","Europe/London"],
    flows=[{"type":"scripted","steps":[
        {"action":"open_random_category"},
        {"action":"open_random_pdp","count":2},
        {"action":"add_to_cart"},
        {"action":"view_cart"},
        {"action":"start_checkout"},
    ]}],
    think_times={"page_min_ms":800,"page_max_ms":2200,"scroll_min_ms":200,"scroll_max_ms":700},
    smoke=False,
    debug=True,
    referrers=REFERRERS,
)

def main():
    log(f"BOOT: noisy runner @ {SESSIONS_PER_MIN}/min, avg {AVG_SESSION_MIN}min, settle {EXTRA_FIXED_WAIT_SEC}s; origin={ORIGIN}")
    try:
        asyncio.run(Runner(cfg).run())
    except KeyboardInterrupt:
        log("SIGINT: graceful shutdown")

if __name__ == "__main__":
    main()
