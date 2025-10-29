# noisy_runner_with_testcard.py
# Direct-run friendly:
# - Reads ORIGIN, SESSIONS_PER_MINUTE, AVG_SESSION_MINUTES, EXTRA_FIXED_WAIT_SEC, CHECKOUT_COMPLETE_RATE from env
# - BigCommerce TEST card defaults (env-overridable)
# - Loud logs + DOMContentLoaded waits + FIXED settle after each nav/click
# - Safe scrolling
# - Weighted category nav
# - NEW: click_all_home_sample_products — visits every home-page "[Sample] ..." PDP

import os, asyncio, re
from playwright.async_api import TimeoutError as PWTimeoutError
from trafficgen.runner import Runner, RunnerConfig
from trafficgen.session import Session, SEL_TIMEOUT, ALLOW_NAV_TIMEOUT
from trafficgen.utils import ExponentialBackoff, same_origin, think

# ---------- Config from env (with safe defaults) ----------
ORIGIN = os.getenv("ORIGIN", "https://noibu.mybigcommerce.com").rstrip("/")
SESSIONS_PER_MIN = float(os.getenv("SESSIONS_PER_MINUTE", "25"))
AVG_SESSION_MIN  = float(os.getenv("AVG_SESSION_MINUTES", "1"))
EXTRA_FIXED_WAIT_SEC = float(os.getenv("EXTRA_FIXED_WAIT_SEC", "5"))  # fixed settle after nav/click
CHECKOUT_COMPLETE_RATE = float(os.getenv("CHECKOUT_COMPLETE_RATE", "0.3"))

# Referrer envs (comma-separated lists)
def _parse_csv(s):
    return [x.strip() for x in (s or "").split(",") if x and x.strip()]

def build_referrers_from_env():
    sources = _parse_csv(os.getenv("REFERRER_SOURCES", ""))
    weights = _parse_csv(os.getenv("REFERRER_WEIGHTS", ""))
    # Defaults: Direct highest, google, bing, then others
    if not sources:
        sources = ["direct", "https://www.google.com", "https://www.bing.com"]
    if not weights or len(weights) != len(sources):
        # default weights aligned to the above
        defaults = [50, 25, 15] + [5] * max(0, len(sources) - 3)
        weights = [str(w) for w in defaults[:len(sources)]]
    items = []
    for s, w in zip(sources, weights):
        try:
            wt = float(w)
        except ValueError:
            wt = 1.0
        items.append({"source": s, "weight": wt})
    return items

REFERRERS = build_referrers_from_env()

def log(*a): print(*a, flush=True)

# ============================================================
# Loud logging + DOM-ready waits + FIXED settle + safe scrolling
# ============================================================

# 1) Wrap Session.run to log start/end of each session
_orig_run = Session.run
async def _run_with_logs(self):
    log(f"[S{self.id}] START (tz={self.tz})")
    try:
        await _orig_run(self)
        log(f"[S{self.id}] END (ok)")
    except Exception as e:
        log(f"[S{self.id}] END (err) {type(e).__name__}: {e}")
        raise
Session.run = _run_with_logs

# 2) Always wait DOMContentLoaded on navigations + fixed settle
async def _guarded_goto_dom(self, url: str):
    if not same_origin(url, self.allowlist):
        log(f"[S{self.id}] SKIP non-allowlisted: {url}")
        return
    await self.global_qps.wait()
    backoff = ExponentialBackoff()
    while True:
        try:
            log(f"[S{self.id}] GOTO {url}")
            resp = await self.page.goto(url, timeout=ALLOW_NAV_TIMEOUT, wait_until="domcontentloaded")
            try:
                await self.page.wait_for_load_state("domcontentloaded", timeout=SEL_TIMEOUT)
            except Exception:
                pass
            # fixed settle
            try:
                await asyncio.sleep(EXTRA_FIXED_WAIT_SEC)
            except Exception:
                pass
            return resp
        except Exception as e:
            log(f"[S{self.id}] GOTO RETRY ({backoff.attempts}) {type(e).__name__}: {e}")
            await backoff.wait()
            if backoff.attempts > 5:
                raise
Session._guarded_goto = _guarded_goto_dom

# 3) Click wrappers that wait for nav or settle to DOMContentLoaded + fixed settle
async def _click_and_wait_dom(self, locator):
    await self.global_qps.wait()
    nav_fired = False
    try:
        async with self.page.expect_navigation(wait_until="domcontentloaded", timeout=ALLOW_NAV_TIMEOUT):
            await locator.first.click(timeout=SEL_TIMEOUT)
        nav_fired = True
    except PWTimeoutError:
        pass
    except Exception:
        pass
    try:
        await self.page.wait_for_load_state("domcontentloaded", timeout=SEL_TIMEOUT)
    except Exception:
        pass
    # fixed settle
    try:
        await asyncio.sleep(EXTRA_FIXED_WAIT_SEC)
    except Exception:
        pass

async def _click_by_text_dom(self, text: str, exact: bool = False):
    log(f"[S{self.id}] CLICK text={text!r}")
    await _click_and_wait_dom(self, self.page.get_by_text(text, exact=exact))
Session._click_by_text = _click_by_text_dom

async def _click_role_dom(self, role: str, name: str | None = None, exact: bool = False):
    log(f"[S{self.id}] CLICK role={role} name={name}")
    loc = self.page.get_by_role(role, name=name, exact=exact) if name else self.page.get_by_role(role)
    await _click_and_wait_dom(self, loc)
Session._click_role = _click_role_dom

async def _click_selector_dom(self, sel: str):
    log(f"[S{self.id}] CLICK sel={sel}")
    await _click_and_wait_dom(self, self.page.locator(sel))
Session._click_selector = _click_selector_dom

async def _scroll_page_logged(self):
    # Same as base but with log + settle
    try:
        await self.page.wait_for_selector("body", timeout=SEL_TIMEOUT)
    except Exception:
        log(f"[S{self.id}] SCROLL: body missing")
        return
    try:
        height = await self.page.evaluate("() => Math.max(document.documentElement.scrollHeight || 0, document.body?.scrollHeight || 0, 2000)")
    except Exception:
        height = 2000
    steps = max(3, min(8, int(height/800)))
    log(f"[S{self.id}] SCROLL height≈{height} steps={steps}")
    for _ in range(steps):
        try:
            await self.page.mouse.wheel(0, height/steps)
        except Exception:
            break
        await think(self.think_cfg["scroll_min_ms"], self.think_cfg["scroll_max_ms"])
Session._scroll_page = _scroll_page_logged

# 5) Log every step execution
_orig_exec = Session._execute_step
async def _exec_with_logs(self, step: dict):
    kind = step.get("action")
    log(f"[S{self.id}] STEP {kind} -> {step}")
    return await _orig_exec(self, step)
Session._execute_step = _exec_with_logs

# ============================================================
# Actions (weighted nav + PDP/cart/checkout + NEW home Sample clicks)
# ============================================================

# Weighted biases for category/content (higher = more frequent)
NAV_WEIGHTS = {
    "Kitchen": 3.0,
    "Shop All": 2.0,
    "Bath": 1.5,
    "Garden": 1.2,
    "Publications": 0.7,
    "Utility": 0.6,
}
NAV_CATEGORIES = list(NAV_WEIGHTS.keys())

def weighted_choice(labels, weights_map):
    import random
    weights = [max(0.0, float(weights_map.get(lbl, 1.0))) for lbl in labels]
    total = sum(weights)
    if total <= 0:
        return random.choice(labels)
    r, acc = random.uniform(0, total), 0.0
    for lbl, w in zip(labels, weights):
        acc += w
        if r <= acc:
            return lbl
    return labels[-1]

# --- NEW: click every home-page product whose title starts with "[Sample]" or "Sample" ---
SAMPLE_START_RE = re.compile(r"^\s*\[?\s*sample", re.I)

async def _go_home(self):
    await self._guarded_goto(ORIGIN + "/")
Session._go_home = _go_home

async def _click_all_home_sample_products(self):
    # Go home & allow grid to appear
    await self._go_home()
    try:
        await self.page.wait_for_selector(".productGrid, .product-list, .productListing, [data-product-id]", state="visible", timeout=5000)
    except Exception:
        pass

    # Nudge lazy content
    try:
        await self.page.mouse.wheel(0, 1500); await asyncio.sleep(0.3)
        await self.page.mouse.wheel(0, 1500); await asyncio.sleep(0.3)
    except Exception:
        pass

    selectors = [
        "h3.card-title a[data-event-type='product-click']",
        ".card-title a, .product-title a, a.card-title, a.product-title",
        "a[href*='/products/']",
    ]
    seen, urls = set(), []
    for sel in selectors:
        try:
            anchors = await self.page.query_selector_all(sel)
        except Exception:
            anchors = []
        for a in anchors:
            try:
                text = (await a.inner_text()).strip()
            except Exception:
                text = ""
            try:
                aria = (await a.get_attribute("aria-label")) or ""
            except Exception:
                aria = ""
            try:
                href = await a.get_attribute("href")
            except Exception:
                href = None

            label = text or aria
            if not href or not label:
                continue
            if SAMPLE_START_RE.search(label):
                full = (ORIGIN.rstrip("/") + href) if href.startswith("/") else href
                if full not in seen:
                    seen.add(full)
                    urls.append(full)
        if urls:
            break

    if not urls:
        log(f"[S{self.id}] HOME Sample list: none found")
        return

    log(f"[S{self.id}] HOME Sample list: {len(urls)} items")
    for idx, url in enumerate(urls, 1):
        log(f"[S{self.id}] HOME Sample -> [{idx}/{len(urls)}] {url}")
        await self._guarded_goto(url)
        # Light PDP readiness & tiny dwell
        for sel in (".productView", ".productView-details", "button#form-action-addToCart", "form[action*='cart.php']"):
            try:
                await self.page.wait_for_selector(sel, state="visible", timeout=4000)
                break
            except Exception:
                pass
        await asyncio.sleep(0.8)
        # Back home for next
        await self._go_home()
        try:
            await self.page.wait_for_selector(".productGrid, .product-list, .productListing, [data-product-id]", state="visible", timeout=4000)
        except Exception:
            pass
Session._click_all_home_sample_products = _click_all_home_sample_products

# --- Weighted nav/category ---
async def _open_nav_category_weighted(self):
    target = weighted_choice(NAV_CATEGORIES, NAV_WEIGHTS)
    log(f"[S{self.id}] NAV(weighted) -> {target}")
    try:
        await self._click_role("link", name=target)
        return
    except Exception:
        path = {
            "Shop All": "/shop-all/",
            "Bath": "/bath/",
            "Garden": "/garden/",
            "Kitchen": "/kitchen/",
            "Publications": "/publications/",
            "Utility": "/utility/",
        }.get(target, "/shop-all/")
        await self._guarded_goto(ORIGIN + path)
Session._open_nav_category = _open_nav_category_weighted

# ============================================================
# Runner config (Chromium only, loud) WITH REFERRERS
# ============================================================
cfg = RunnerConfig(
    origin=ORIGIN,
    allowlist_roots=[ORIGIN],
    sessions_per_minute=SESSIONS_PER_MIN,
    avg_session_minutes=AVG_SESSION_MIN,
    max_concurrency=int(SESSIONS_PER_MIN*AVG_SESSION_MIN)+10,
    global_qps_cap=6.0,
    rotate_ip_interval_sec=0,
    kill_switch_file=None,
    allow_checkout=True,
    checkout_complete_rate=CHECKOUT_COMPLETE_RATE,
    browsers_enabled=["chromium"],
    device_mix=[
        {"name":"iphone-14","weight":1.0},
        {"name":"android-pixel","weight":1.0},
        {"name":"desktop-chrome","weight":1.0},
    ],
    locales=["en-US","en-CA","en-GB","fr-CA"],
    timezones=["America/Toronto","America/New_York","America/Vancouver","Europe/London"],
    flows=[{"type":"scripted","steps":[
        {"action":"click_all_home_sample_products"},  # visit every Sample PDP from home
        {"action":"open_random_category"},
        {"action":"open_random_pdp","count":2},
        {"action":"add_to_cart"},
        {"action":"view_cart"},
        {"action":"start_checkout"},
    ]}],
    think_times={"page_min_ms":800,"page_max_ms":2200,"scroll_min_ms":200,"scroll_max_ms":700},
    proxy_backend="null", piactl_path="/usr/bin/piactl", pia_regions=[],
    smoke=False,
    debug=True,
    referrers=REFERRERS,  # NEW
)

log(f"BOOT: noisy runner @ {SESSIONS_PER_MIN}/min, avg {AVG_SESSION_MIN}min, settle {EXTRA_FIXED_WAIT_SEC}s; origin={ORIGIN}")
try:
    asyncio.run(Runner(cfg).run())
except KeyboardInterrupt:
    log("SIGINT: graceful shutdown")
