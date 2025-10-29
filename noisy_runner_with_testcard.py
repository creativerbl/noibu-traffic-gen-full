
# noisy_runner_with_testcard.py — updated device mix
from trafficgen.runner import Runner, RunnerConfig
import asyncio, os

ORIGIN = os.getenv("ORIGIN", "https://noibu.mybigcommerce.com").rstrip("/")

cfg = RunnerConfig(
    origin=ORIGIN,
    allowlist_roots=[ORIGIN],
    sessions_per_minute=25,
    avg_session_minutes=1.0,
    max_concurrency=35,
    global_qps_cap=6.0,
    kill_switch_file=None,
    allow_checkout=True,
    checkout_complete_rate=0.3,
    device_mix=[
        {"name":"iphone-safari","weight":15},
        {"name":"iphone-chrome","weight":10},
        {"name":"android-chrome","weight":20},
        {"name":"desktop-chrome","weight":25},
        {"name":"desktop-edge","weight":10},
        {"name":"desktop-safari","weight":10},
        {"name":"desktop-firefox","weight":10},
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
)

if __name__ == "__main__":
    asyncio.run(Runner(cfg).run())
