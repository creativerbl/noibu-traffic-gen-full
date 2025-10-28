import asyncio
import os
import random
import re
from typing import Any, Dict, List, Optional

from trafficgen.utils import think, same_origin, ExponentialBackoff, debug_print

ALLOW_NAV_TIMEOUT = 20000
SEL_TIMEOUT = 15000

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
                 fault_profile: Optional[dict] = None):
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
        # Optional, modest delay on a small fraction of requests
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
                return await self.page.goto(url, timeout=ALLOW_NAV_TIMEOUT, wait_until="domcontentloaded")
            except Exception as e:
                # backoff on 4xx/5xx/429 or timeouts
                await backoff.wait()
                if backoff.attempts > 5:
                    raise

    async def _scroll_page(self):
        # Wait until <body> exists (handles cases where document.body is null briefly)
        try:
            await self.page.wait_for_selector("body", timeout=SEL_TIMEOUT)
        except Exception:
            return  # if body never appears, just skip scrolling

        # Compute a safe page height with multiple fallbacks; default to 2000 px
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
            height = 2000  # last-ditch fallback

        steps = max(3, min(8, int(height / 800)))  # 3–8 steps based on page height
        for _ in range(steps):
            await self.page.mouse.wheel(0, height / steps)
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
            flow = random.choice(self.flows or [{}])  # each is a YAML doc
            if not flow:
                return
            ftype = flow.get("type", "scripted")
            if ftype == "scripted":
                await self._run_scripted(flow)
            elif ftype == "markov":
                await self._run_markov(flow)
            else:
                await self._run_scripted(flow)
        finally:
            await self.context.close()

    async def _run_scripted(self, flow: dict):
        steps = flow.get("steps", [])
        await self._landing()
        for step in steps:
            await self._execute_step(step)
            await think(self.think_cfg["page_min_ms"], self.think_cfg["page_max_ms"])

    async def _run_markov(self, flow: dict):
        states = flow.get("states", {})
        current = flow.get("start")
        max_steps = int(flow.get("max_steps", 10))
        await self._landing()
        for _ in range(max_steps):
            s = states.get(current) or {}
            for step in s.get("actions", []):
                await self._execute_step(step)
                await think(self.think_cfg["page_min_ms"], self.think_cfg["page_max_ms"])
            # choose next
            trans = s.get("transitions", [])
            if not trans:
                break
            weights = [max(float(t.get("weight", 1.0)), 0.0) for t in trans]
            total = sum(weights) or 1.0
            r = random.uniform(0, total)
            up = 0
            chosen = trans[-1]
            for t, w in zip(trans, weights):
                up += w
                if r <= up:
                    chosen = t
                    break
            nxt = chosen.get("to")
            if nxt in (None, "END"):
                break
            current = nxt

    async def _landing(self):
        await self._guarded_goto(self.origin)
        await self._scroll_page()

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
        # try search field
        try:
            sf = self.page.get_by_placeholder(re.compile("Search", re.I))
            await sf.click(timeout=SEL_TIMEOUT)
            await sf.fill(query, timeout=SEL_TIMEOUT)
            await sf.press("Enter")
        except Exception:
            # fallback: query param
            await self._guarded_goto(f"{self.origin}/search.php?search_query={query}")
        await self._scroll_page()

    async def _open_random_category(self):
        # Prefer nav links that look like categories
        nav_candidates = self.page.get_by_role("link", name=re.compile("(Shop|All|Men|Women|Accessories|Catalog|Sale|New)", re.I))
        count = await nav_candidates.count()
        if count > 0 and random.random() < 0.7:
            idx = random.randint(0, min(count-1, 5))
            await nav_candidates.nth(idx).click(timeout=SEL_TIMEOUT)
        else:
            # known BigCommerce paths that often exist
            cand = random.choice(["/all/", "/categories/", "/shop/"])
            await self._guarded_goto(f"{self.origin}{cand}")
        await self._scroll_page()

    async def _open_random_pdp(self, count: int = 1):
        count = max(1, min(count, 3))
        for _ in range(count):
            grid = self.page.locator("a.card-figure, a.card-title, a.product-title, a[href*='/products/']")
            n = await grid.count()
            if n > 0:
                i = random.randint(0, min(n-1, 15))
                await grid.nth(i).click(timeout=SEL_TIMEOUT)
                await self._scroll_page()
            else:
                break

    async def _sort_or_filter(self):
        # Try sort dropdowns
        try:
            sort_sel = self.page.locator("select[name='sort'], select#sort, select[name*='Sort']")
            await sort_sel.first.select_option(index=random.randint(0, 2), timeout=SEL_TIMEOUT)
        except Exception:
            pass
        await self._scroll_page()

    async def _add_to_cart(self):
        # On PDP: click Add to Cart
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
        await self._scroll_page()

    async def _start_checkout(self):
        try:
            btn = self.page.get_by_role("link", name=re.compile("checkout", re.I))
            await btn.first.click(timeout=SEL_TIMEOUT)
        except Exception:
            try:
                await self.page.click("a[href*='/checkout']", timeout=SEL_TIMEOUT)
            except Exception:
                return
        await self._scroll_page()

    async def _content_page(self, slug: str):
        # includes Help/Shipping & Returns/Blog/Contact
        slugs = ["/contact-us/", "/shipping-returns/", "/blog/", "/help/"]
        if slug and slug.startswith("/"):
            slugs.insert(0, slug)
        await self._guarded_goto(self.origin + random.choice(slugs))
        await self._scroll_page()

    async def _complete_checkout(self):
        """
        Best-effort checkout completion for BigCommerce test mode.
        Handles common hosted checkout flows; falls back to abandon if selectors differ.
        """
        card_no = os.getenv("BC_TEST_CARD_NUMBER", "")
        exp = os.getenv("BC_TEST_EXP", "")
        cvv = os.getenv("BC_TEST_CVV", "")
        name = os.getenv("BC_TEST_NAME", "Test User")

        # email + shipping step
        try:
            # Email
            await self.page.get_by_label(re.compile("email", re.I)).fill(f"demo+{random.randint(1000,9999)}@noibu.com", timeout=SEL_TIMEOUT)
        except Exception:
            pass

        # Common shipping fields
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

        # continue/review
        for txt in ["Continue", "Next", "Proceed"]:
            try:
                await self.page.get_by_role("button", name=re.compile(txt, re.I)).click(timeout=5000)
                await asyncio.sleep(1.0)
            except Exception:
                pass

        # Payment (card)
        if not card_no:
            return  # config requires env secrets; otherwise consider abandoned

        # Some checkouts use iframes; attempt common selectors and fallbacks
        try:
            # Try generic card fields (may be in iframes)
            await self._fill_card_field("number", card_no)
            await self._fill_card_field("name", name)
            await self._fill_card_field("expiry|exp", exp)
            await self._fill_card_field("cvv|cvc", cvv)
        except Exception:
            return

        # Place order
        for lab in ["Pay", "Place Order", "Complete Order"]:
            try:
                await self.page.get_by_role("button", name=re.compile(lab, re.I)).click(timeout=SEL_TIMEOUT)
                await asyncio.sleep(2.0)
                break
            except Exception:
                continue

    async def _fill_card_field(self, label_regex: str, value: str):
        # Attempt label targeting; iframes commonly used -> search frames too
        try:
            await self.page.get_by_label(re.compile(label_regex, re.I)).fill(value, timeout=SEL_TIMEOUT)
            return
        except Exception:
            pass
        # search frames for inputs with placeholders
        for frame in self.page.frames:
            try:
                await frame.get_by_placeholder(re.compile(label_regex, re.I)).fill(value, timeout=2000)
                return
            except Exception:
                continue
        # generic inputs
        try:
            await self.page.fill(f"input[placeholder*='{label_regex}'], input[name*='{label_regex}']", value, timeout=2000)
        except Exception:
            pass
