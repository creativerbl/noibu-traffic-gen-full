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

# ---------- TEST CARD (BigCommerce test mode ONLY) ----------
# Public TEST credentials from BigCommerce docs. Never use real cards.
TEST_CARD_NUMBER_DEFAULT = "4111111111111111"
TEST_CARD_EXP_DEFAULT    = "12/30"
TEST_CARD_CVV_DEFAULT    = "123"
TEST_CARD_NAME_DEFAULT   = "Test User"

# Preload env with safe defaults if not already set
os.environ.setdefault("BC_TEST_CARD_NUMBER", TEST_CARD_NUMBER_DEFAULT)
os.environ.setdefault("BC_TEST_EXP", TEST_CARD_EXP_DEFAULT)
os.environ.setdefault("BC_TEST_CVV", TEST_CARD_CVV_DEFAULT)
os.environ.setdefault("BC_TEST_NAME", TEST_CARD_NAME_DEFAULT)

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
    log(f"[S{self.id}] CLICK role={role} name={name!r}")
    loc = self.page.get_by_role(role, name=name, exact=exact) if name else self.page.get_by_role(role)
    await _click_and_wait_dom(self, loc)
Session._click_role = _click_role_dom

async def _click_selector_dom(self, css: str):
    log(f"[S{self.id}] CLICK css={css!r}")
    await _click_and_wait_dom(self, self.page.locator(css))
Session._click_selector = _click_selector_dom

# 4) Safe scroll (avoids document.body null)
async def _scroll_page_logged(self):
    try:
        await self.page.wait_for_selector("body", timeout=SEL_TIMEOUT)
    except Exception:
        log(f"[S{self.id}] SCROLL skip (no body)")
        return
    try:
        height = await self.page.evaluate("""
            () => {
              const doc = document.documentElement;
              const body = document.body;
              const vals = [
                doc?.scrollHeight, body?.scrollHeight,
                doc?.offsetHeight, body?.offsetHeight,
                doc?.clientHeight, body?.clientHeight
              ].filter(v => typeof v === 'number');
              const h = Math.max(...vals, 0);
              return (h && Number.isFinite(h)) ? h : 2000;
            }
        """)
    except Exception:
        height = 2000
    steps = max(3, min(8, int(height/800)))
    log(f"[S{self.id}] SCROLL h≈{height} steps={steps}")
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

# --- PDP open (random tile on grid) ---
async def _open_random_pdp(self, count: int = 1):
    import random
    count = max(1, min(count, 3))
    for _ in range(count):
        grid = self.page.locator("a.card-figure, a.card-title, a.product-title, a[href*='/products/']")
        try:
            n = await grid.count()
        except Exception:
            n = 0
        if n == 0:
            log(f"[S{self.id}] PDP grid: none")
            return
        i = random.randint(0, min(n-1, 10))  # bias to above-the-fold
        log(f"[S{self.id}] PDP grid idx={i}/{n}")
        await _click_and_wait_dom(self, grid.nth(i))
Session._open_random_pdp = _open_random_pdp

# --- Add to cart / view cart / start checkout ---
async def _add_to_cart_dom(self):
    try:
        btn = self.page.get_by_role("button", name=lambda s: "add to cart" in s.lower())
        await _click_and_wait_dom(self, btn)
        log(f"[S{self.id}] ADD TO CART (role)")
        return
    except Exception:
        pass
    try:
        await self._click_selector("button#form-action-addToCart, button[name='add']")
        log(f"[S{self.id}] ADD TO CART (css)")
    except Exception:
        log(f"[S{self.id}] ADD TO CART not found")
Session._add_to_cart = _add_to_cart_dom

async def _view_cart_dom(self):
    try:
        await self._click_role("link", name=lambda s: "cart" in s.lower())
        log(f"[S{self.id}] VIEW CART (role)")
        return
    except Exception:
        await self._guarded_goto(ORIGIN + "/cart.php")
        log(f"[S{self.id}] VIEW CART (direct)")
Session._view_cart = _view_cart_dom

async def _start_checkout_dom(self):
    for loc in [
        self.page.get_by_role("link",   name=lambda s: "checkout" in s.lower()),
        self.page.get_by_role("button", name=lambda s: "checkout" in s.lower()),
    ]:
        try:
            await _click_and_wait_dom(self, loc)
            log(f"[S{self.id}] START CHECKOUT")
            return
        except Exception:
            pass
    try:
        await self._click_selector("a[href*='/checkout'], button[href*='/checkout']")
        log(f"[S{self.id}] START CHECKOUT (css)")
    except Exception:
        log(f"[S{self.id}] START CHECKOUT not found")
Session._start_checkout = _start_checkout_dom

# ============================================================
# Runner config (Chromium only, loud)
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
)

log(f"BOOT: noisy runner @ {SESSIONS_PER_MIN}/min, avg {AVG_SESSION_MIN}min, settle {EXTRA_FIXED_WAIT_SEC}s; origin={ORIGIN}")
try:
    asyncio.run(Runner(cfg).run())
except KeyboardInterrupt:
    log("SIGINT: graceful shutdown")
