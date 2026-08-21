
# noibu-traffic-gen.py — Chromium-only runner with .env-driven referrers & devices
import os, asyncio, math
from pathlib import Path
from dotenv import load_dotenv  # 👈 ensure .env is loaded into process env
from trafficgen.runner import Runner, RunnerConfig
from trafficgen.utils import load_yaml_files

# Load .env from the current working directory (repo root)
# Set override=False so exported shell vars still take precedence if set.
load_dotenv(override=False)

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
        total = float(len(vals) or 1.0)
    scaled = [ (v * 100.0) / total for v in vals ]
    floored = [int(math.floor(x)) for x in scaled]
    remainder = 100 - sum(floored)
    fracs = [(i, scaled[i] - floored[i]) for i in range(len(scaled))]
    fracs.sort(key=lambda t: t[1], reverse=True)
    for i in range(max(0, remainder)):
        idx = fracs[i % len(fracs)][0]
        floored[idx] += 1
    return floored

def _load_flows_from_disk() -> list:
    """Load flow YAMLs. FLOWS_ONLY=a,b restricts loading to those file stems.

    Used by the broken-checkout branch (FLOWS_ONLY=broken-checkout) so the
    scenario runs in isolation instead of competing with the other flows.
    """
    flow_dir = Path(__file__).parent / "trafficgen" / "flows"
    flow_files = sorted(flow_dir.glob("*.yaml"))
    only = [x.strip() for x in (os.getenv("FLOWS_ONLY", "") or "").split(",") if x.strip()]
    if only:
        wanted = {o[:-5] if o.endswith(".yaml") else o for o in only}
        flow_files = [p for p in flow_files if p.stem in wanted]
        if not flow_files:
            raise SystemExit(
                f"FLOWS_ONLY={','.join(only)} matched no files in {flow_dir}. "
                f"Available: {', '.join(sorted(p.stem for p in flow_dir.glob('*.yaml')))}"
            )
        print(f"FLOWS_ONLY active: {', '.join(p.stem for p in flow_files)}", flush=True)
    flows = load_yaml_files([str(p) for p in flow_files])
    return [f for f in flows if f]

def build_referrers_from_env():
    sources = [s for s in _parse_csv(os.getenv("REFERRER_SOURCES", "")) if s and s.strip()]
    weights = [w for w in _parse_csv(os.getenv("REFERRER_WEIGHTS", "")) if w and w.strip()]
    if not sources:
        sources = ["direct", "google", "bing"]
    if not weights or len(weights) != len(sources):
        defaults = [60, 25, 15] + [5] * max(0, len(sources) - 3)
        weights = [str(w) for w in defaults[:len(sources)]]
    norm = _normalize_to_100(weights)
    return [{"source": s, "weight": int(w)} for s, w in zip(sources, norm)]

def build_device_mix_from_env():
    raw = os.getenv("DEVICE_MIX", "")
    pairs = [p for p in _parse_csv(raw) if p]
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
        if not name:
            continue
        out.append({"name": name, "weight": w})
    if not out:
        out = [
            {"name":"iphone-safari", "weight":15},
            {"name":"iphone-chrome", "weight":10},
            {"name":"android-chrome","weight":20},
            {"name":"desktop-chrome","weight":25},
            {"name":"desktop-edge",  "weight":10},
            {"name":"desktop-safari","weight":10},
            {"name":"desktop-firefox","weight":10},
        ]
    return out

def main():
    origin = os.getenv("ORIGIN", "https://noibu.mybigcommerce.com").rstrip("/")
    sessions_per_min = float(os.getenv("SESSIONS_PER_MINUTE", "25"))
    avg_session_min  = float(os.getenv("AVG_SESSION_MINUTES", "1"))
    checkout_rate    = float(os.getenv("CHECKOUT_COMPLETE_RATE", "0.30"))
    flows = _load_flows_from_disk()
    if not flows:
        flows = [{"type":"scripted","steps":[
            {"action":"open_random_category"},
            {"repeat":{"times":2,"steps":[
                {"action":"open_random_pdp"},
                {"action":"pdp_explore"},
                {"action":"pdp_decision","add_to_cart_weight":0.7,"bounce_weight":1.0},
            ]}},
            {"action":"view_cart"},
            {"action":"start_checkout"},
        ]}]

    cfg = RunnerConfig(
        origin=origin,
        allowlist_roots=[origin],
        sessions_per_minute=sessions_per_min,
        avg_session_minutes=avg_session_min,
        max_concurrency=int(os.getenv("MAX_CONCURRENCY", str(int(sessions_per_min*avg_session_min)+10))),
        global_qps_cap=float(os.getenv("GLOBAL_QPS_CAP", "6.0")),
        allow_checkout=True,
        checkout_complete_rate=checkout_rate,
        device_mix=build_device_mix_from_env(),
        locales=[x for x in _parse_csv(os.getenv("LOCALES", "en-US,en-CA,en-GB,fr-CA")) if x],
        timezones=[x for x in _parse_csv(os.getenv("TIMEZONES", "America/Toronto,America/New_York,America/Vancouver,Europe/London")) if x],
        flows=flows,
        think_times={"page_min_ms":800,"page_max_ms":2200,"scroll_min_ms":200,"scroll_max_ms":700},
        smoke=False,
        debug=True,
        referrers=build_referrers_from_env(),
    )

    print(f">> Running noibu-traffic-gen.py …")
    print(f"BOOT: {sessions_per_min}/min, avg {avg_session_min}min; origin={origin}", flush=True)
    try:
        asyncio.run(Runner(cfg).run())
    except KeyboardInterrupt:
        print("SIGINT: graceful shutdown", flush=True)

if __name__ == "__main__":
    main()
