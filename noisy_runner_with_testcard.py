# noisy_runner_with_testcard.py — Chromium-only runner + UTM sources + normalized weights
import os, asyncio, math
from trafficgen.runner import Runner, RunnerConfig

ORIGIN = os.getenv("ORIGIN", "https://noibu.mybigcommerce.com").rstrip("/")
SESSIONS_PER_MIN = float(os.getenv("SESSIONS_PER_MINUTE", "25"))
AVG_SESSION_MIN  = float(os.getenv("AVG_SESSION_MINUTES", "1"))
EXTRA_FIXED_WAIT_SEC = float(os.getenv("EXTRA_FIXED_WAIT_SEC", "5"))
CHECKOUT_COMPLETE_RATE = float(os.getenv("CHECKOUT_COMPLETE_RATE", "0.3"))

def _parse_csv(s):
    return [x.strip() for x in (s or "").split(",")]

def _normalize_to_100(weights):
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

def build_referrers_from_env():
    sources = [s for s in _parse_csv(os.getenv("REFERRER_SOURCES", "")) if s and s.strip()]
    weights = [w for w in _parse_csv(os.getenv("REFERRER_WEIGHTS", "")) if w and w.strip()]
    if not sources:
        sources = ["direct", "https://www.google.com", "https://www.bing.com"]
    if not weights or len(weights) != len(sources):
        defaults = [50, 25, 15] + [5] * max(0, len(sources) - 3)
        weights = [str(w) for w in defaults[:len(sources)]]
    norm = _normalize_to_100(weights)
    return [{"source": s, "weight": int(w)} for s, w in zip(sources, norm)]

REFERRERS = build_referrers_from_env()

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
    print(f"BOOT: noisy runner @ {SESSIONS_PER_MIN}/min, avg {AVG_SESSION_MIN}min; origin={ORIGIN}", flush=True)
    try:
        asyncio.run(Runner(cfg).run())
    except KeyboardInterrupt:
        print("SIGINT: graceful shutdown", flush=True)

if __name__ == "__main__":
    main()
