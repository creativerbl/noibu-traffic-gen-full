
import asyncio
import os
import random
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urlparse

from trafficgen.utils import think, same_origin, ExponentialBackoff, debug_print

ALLOW_NAV_TIMEOUT = 20000
SEL_TIMEOUT = 15000

def _slug_from_source(src: str) -> str:
    if not src:
        return ""
    s = src.strip().lower()
    if s == "direct":
        return "direct"
    try:
        if "://" in s:
            netloc = urlparse(s).netloc
        else:
            netloc = s
        netloc = re.sub(r"^www\.", "", netloc)
        parts = netloc.split(".")
        if len(parts) >= 2:
            return parts[-2]
        return netloc
    except Exception:
        return re.sub(r"\W+", "", s)

def _parse_kv_csv(env_val: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    s = (env_val or "").strip()
    if not s:
        return result
    for pair in s.split(","):
        if not pair.strip() or ":" not in pair:
            continue
        k, v = pair.split(":", 1)
        k = _slug_from_source(k.strip())
        v = v.strip()
        if k:
            result[k] = v
    return result

class Session:
    def __init__(self,
                 session_id: int,
                 browser,
                 playwright,
                 origin: str,
                 allowlist_roots: List[str],
                 device_context_args: Dict[str, Any],
                 locale: str,
                 timezone_id: str,
                 allow_checkout: bool,
                 checkout_complete_rate: float,
                 flows: List[dict],
                 think_cfg: Dict[str, int],
                 global_qps,
                 debug: bool = False,
                 fault_profile: Optional[dict] = None,
                 referrer_url: Optional[str] = None):
        self.id = session_id
        self.browser = browser
        self.playwright = playwright
        self.origin = origin.rstrip("/")
        self.allowlist = allowlist_roots
        self.ctx_args = device_context_args or {}
        self.locale = locale
        self.tz = timezone_id
        self.allow_checkout = allow_checkout
        self.checkout_rate = float(checkout_complete_rate or 0.0)
        self.flows = flows
        self.think_cfg = think_cfg or {"page_min_ms": 800, "page_max_ms": 3000, "scroll_min_ms": 200, "scroll_max_ms": 1000}
        self.global_qps = global_qps
        self.debug = debug
        self.fault_profile = fault_profile or {}

        # Referrer per session
        self.referrer_url = (referrer_url or "").strip() or None
        if self.referrer_url and self.referrer_url.lower() == "direct":
            self.referrer_url = "direct"

        # UTM env
        self.utm_medium_default = os.getenv("UTM_MEDIUM_DEFAULT", "paid-social")
        self.utm_campaign_default = os.getenv("UTM_CAMPAIGN_DEFAULT", "trafficgen")
        self.utm_mediums = _parse_kv_csv(os.getenv("REFERRER_UTM_MEDIUMS", ""))

        # --- Human-like nav/scroll behavior (all .env driven) ---
        # Wait strategy after goto: 'load' | 'domcontentloaded' | 'networkidle'
        self.wait_until = os.getenv("PAGE_WAIT_UNTIL", "load").strip().lower()
        if self.wait_until not in ("load", "domcontentloaded", "networkidle"):
            self.wait_until = "load"

        # Random settle delay after navigation (ms)
        self.post_nav_settle_min = int(os.getenv("POST_NAV_SETTLE_MIN_MS", "250"))
        self.post_nav_settle_max = int(os.getenv("POST_NAV_SETTLE_MAX_MS", "900"))

        # Probability of scrolling after a pageview (0..1)
        self.scroll_prob = float(os.getenv("SCROLL_PROB", "0.75"))
        # Fraction of total page height to scroll to (min..max)
        self.scroll_depth_min = float(os.getenv("SCROLL_DEPTH_MIN", "0.35"))
        self.scroll_depth_max = float(os.getenv("SCROLL_DEPTH_MAX", "0.95"))
        # Number of scroll steps
        self.scroll_steps_min = int(os.getenv("SCROLL_STEPS_MIN", "2"))
        self.scroll_steps_max = int(os.getenv("SCROLL_STEPS_MAX", "6"))

        self.page = None
        self.context = None

    async def _new_context(self):
        cargs = dict(self.ctx_args)
        cargs["locale"] = self.locale
        cargs["timezone_id"] = self.tz
        cargs.setdefault("ignore_https_errors", True)
        self.context = await self.browser.new_context(**cargs)
        self.page = await self.context.new_page()
        await self._install_faults()

    async def _install_faults(self):
        slow_pct = float(self.fault_profile.get("slow_request_fraction", 0.0))
        if slow_pct <= 0:
            return

        async def route_handler(route):
            if random.random() < slow_pct:
                await asyncio.sleep(random.uniform(0.1, 0.6))
            await route.continue_()
        await self.page.route("**/*", route_handler)

    async def _guarded_goto(self, url: str):
        if not same_origin(url, self.allowlist):
            return
        await self.global_qps.wait()
        backoff = ExponentialBackoff()
        while True:
            try:
                resp = await self.page.goto(url, timeout=ALLOW_NAV_TIMEOUT, wait_until=self.wait_until)
                # Small human-like settle pause after navigation
                settle = random.uniform(self.post_nav_settle_min/1000, self.post_nav_settle_max/1000)
                await asyncio.sleep(settle)
                return resp
            except Exception:
                await backoff.wait()
                if backoff.attempts > 5:
                    raise

    async def _maybe_scroll_page(self):
        """Human-like scrolling: sometimes don't scroll; sometimes partial depth."""
        # decide if we scroll at all
        if random.random() > max(0.0, min(1.0, self.scroll_prob)):
            debug_print(self.debug, f"[S{self.id}] no scroll (randomized)")
            return

        # Ensure <body>
        try:
            await self.page.wait_for_selector("body", timeout=SEL_TIMEOUT)
        except Exception:
            return  # if body never appears, skip

        # Estimate page height safely
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
                  const h = Math.max( ...vals, 0 );
                  return (h && isFinite(h)) ? h : 2000;
                }
            """)
        except Exception:
            height = 2000

        # Pick a random depth and step count
        depth_frac = max(0.0, min(1.0, random.uniform(self.scroll_depth_min, self.scroll_depth_max)))
        target = max(400, height * depth_frac)
        steps = max(1, min(10, random.randint(self.scroll_steps_min, self.scroll_steps_max)))

        for _ in range(steps):
            await self.page.mouse.wheel(0, target/steps)
            await think(self.think_cfg["scroll_min_ms"], self.think_cfg["scroll_max_ms"])

    async def _click_by_text(self, text: str, exact: bool = False):
        await self.global_qps.wait()
        locator = self.page.get_by_text(text, exact=exact)
        await locator.first.click(timeout=SEL_TIMEOUT)

    async def _click_role(self, role: str, name: Optional[str] = None, exact: bool = False):
        await self.global_qps.wait()
        loc = self.page.get_by_role(role, name=name, exact=exact) if name else self.page.get_by_role(role)
        await loc.first.click(timeout=SEL_TIMEOUT)

    async def run(self):
        await self._new_context()
        try:
            flow = random.choice(self.flows or [{}])  # YAML doc
            if not flow:
                return
            await self._run_scripted(flow)
        finally:
            await self.context.close()

    async def _run_scripted(self, flow: dict):
        steps = flow.get("steps", [])
        await self._landing()
        for step in steps:
            await self._execute_step(step)
            await think(self.think_cfg["page_min_ms"], self.think_cfg["page_max_ms"])

    async def _landing(self):
        landing = self.origin + "/"
        if self.referrer_url and self.referrer_url != "direct":
            utm_source = _slug_from_source(self.referrer_url)
            if utm_source and utm_source != "direct":
                utm_medium = self.utm_mediums.get(utm_source, self.utm_medium_default)
                utm_campaign = self.utm_campaign_default
                q = {"utm_source": utm_source, "utm_medium": utm_medium, "utm_campaign": utm_campaign}
                sep = "?" if "?" not in landing else "&"
                landing = landing + sep + urlencode(q)
                debug_print(self.debug, f"[S{self.id}] landing with UTM: {landing}")
            else:
                debug_print(self.debug, f"[S{self.id}] landing direct (invalid source)")
        else:
            debug_print(self.debug, f"[S{self.id}] landing direct")
        await self._guarded_goto(landing)
        await self._maybe_scroll_page()

    async def _execute_step(self, step: dict):
        kind = step.get("action")
        if kind == "search":
            await self._search(step.get("query"))
        elif kind == "open_random_category":
            await self._open_random_category()
        elif kind == "open_random_pdp":
            await self._open_random_pdp(count=int(step.get("count", 1)))
        elif kind == "sort_or_filter":
            await self._sort_or_filter()
        elif kind == "add_to_cart":
            await self._add_to_cart()
        elif kind == "view_cart":
            await self._view_cart()
        elif kind == "start_checkout":
            await self._start_checkout()
        elif kind == "maybe_complete_checkout":
            if self.allow_checkout and random.random() < self.checkout_rate:
                await self._complete_checkout()
        elif kind == "content_page":
            await self._content_page(step.get("slug", ""))
        elif kind == "pause":
            ms = int(step.get("ms", 1000))
            await asyncio.sleep(ms / 1000.0)

    async def _search(self, query: Optional[str]):
        if not query:
            query = random.choice(["shirt", "hat", "bag", "gift", "shoe"])
        try:
            sf = self.page.get_by_placeholder(re.compile("Search", re.I))
            await sf.click(timeout=SEL_TIMEOUT)
            await sf.fill(query, timeout=SEL_TIMEOUT)
            await sf.press("Enter")
        except Exception:
            await self._guarded_goto(f"{self.origin}/search.php?search_query={query}")
        await self._maybe_scroll_page()

    async def _open_random_category(self):
        nav_candidates = self.page.get_by_role("link", name=re.compile("(Shop|All|Men|Women|Accessories|Catalog|Sale|New)", re.I))
        count = await nav_candidates.count()
        if count > 0 and random.random() < 0.7:
            idx = random.randint(0, min(count-1, 5))
            await nav_candidates.nth(idx).click(timeout=SEL_TIMEOUT)
        else:
            cand = random.choice(["/all/", "/categories/", "/shop/"])
            await self._guarded_goto(f"{self.origin}{cand}")
        await self._maybe_scroll_page()

    async def _open_random_pdp(self, count: int = 1):
        count = max(1, min(count, 3))
        for _ in range(count):
            grid = self.page.locator("a.card-figure, a.card-title, a.product-title, a[href*='/products/']")
            try:
                n = await grid.count()
            except Exception:
                n = 0
            if n > 0:
                i = random.randint(0, min(n-1, 15))
                await grid.nth(i).click(timeout=SEL_TIMEOUT)
                await self._maybe_scroll_page()
            else:
                break

    async def _sort_or_filter(self):
        try:
            sort_sel = self.page.locator("select[name='sort'], select#sort, select[name*='Sort']")
            await sort_sel.first.select_option(index=random.randint(0, 2), timeout=SEL_TIMEOUT)
        except Exception:
            pass
        await self._maybe_scroll_page()

    async def _add_to_cart(self):
        try:
            btn = self.page.get_by_role("button", name=re.compile("add to cart", re.I))
            await btn.first.click(timeout=SEL_TIMEOUT)
        except Exception:
            try:
                await self.page.click("button#form-action-addToCart, button[name='add']", timeout=SEL_TIMEOUT)
            except Exception:
                return
        await think(500, 1200)

    async def _view_cart(self):
        try:
            link = self.page.get_by_role("link", name=re.compile("cart|view cart", re.I))
            await link.first.click(timeout=SEL_TIMEOUT)
        except Exception:
            await self._guarded_goto(f"{self.origin}/cart.php")
        await self._maybe_scroll_page()

    async def _start_checkout(self):
        try:
            btn = self.page.get_by_role("link", name=re.compile("checkout", re.I))
            await btn.first.click(timeout=SEL_TIMEOUT)
        except Exception:
            try:
                await self.page.click("a[href*='/checkout']", timeout=SEL_TIMEOUT)
            except Exception:
                return
        await self._maybe_scroll_page()

    async def _content_page(self, slug: str):
        slugs = ["/contact-us/", "/shipping-returns/", "/blog/", "/help/"]
        if slug and slug.startswith("/"):
            slugs.insert(0, slug)
        await self._guarded_goto(self.origin + random.choice(slugs))
        await self._maybe_scroll_page()

    async def _complete_checkout(self):
        card_no = os.getenv("BC_TEST_CARD_NUMBER", "")
        exp = os.getenv("BC_TEST_EXP", "")
        cvv = os.getenv("BC_TEST_CVV", "")
        name = os.getenv("BC_TEST_NAME", "Test User")

        try:
            await self.page.get_by_label(re.compile("email", re.I)).fill(f"demo+{random.randint(1000,9999)}@noibu.com", timeout=SEL_TIMEOUT)
        except Exception:
            pass

        fields = {
            "first name": "Demo",
            "last name": "User",
            "address": "123 Test St",
            "city": "Ottawa",
            "postal": "K1A0B1",
            "zip": "10001",
            "state": "ON",
            "province": "ON",
            "phone": "5551231234",
        }
        for label, val in fields.items():
            try:
                await self.page.get_by_label(re.compile(label, re.I)).fill(val, timeout=5000)
            except Exception:
                continue

        for txt in ["Continue", "Next", "Proceed"]:
            try:
                await self.page.get_by_role("button", name=re.compile(txt, re.I)).click(timeout=5000)
                await asyncio.sleep(1.0)
            except Exception:
                pass

        if not card_no:
            return

        try:
            await self._fill_card_field("number", card_no)
            await self._fill_card_field("name", name)
            await self._fill_card_field("expiry|exp", exp)
            await self._fill_card_field("cvv|cvc", cvv)
        except Exception:
            return

        for lab in ["Pay", "Place Order", "Complete Order"]:
            try:
                await self.page.get_by_role("button", name=re.compile(lab, re.I)).click(timeout=SEL_TIMEOUT)
                await asyncio.sleep(2.0)
                break
            except Exception:
                continue

    async def _fill_card_field(self, label_regex: str, value: str):
        try:
            await self.page.get_by_label(re.compile(label_regex, re.I)).fill(value, timeout=SEL_TIMEOUT)
            return
        except Exception:
            pass
        for frame in self.page.frames:
            try:
                await frame.get_by_placeholder(re.compile(label_regex, re.I)).fill(value, timeout=2000)
                return
            except Exception:
                continue
        try:
            await self.page.fill(f"input[placeholder*='{label_regex}'], input[name*='{label_regex}']", value, timeout=2000)
        except Exception:
            pass
