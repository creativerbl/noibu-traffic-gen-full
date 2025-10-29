import asyncio
import os
import random
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse, urljoin

from trafficgen.utils import think, same_origin, ExponentialBackoff, debug_print

ALLOW_NAV_TIMEOUT = 25000
SEL_TIMEOUT = 15000

# ---------------------- helpers: parsing & normalization ----------------------
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

def _parse_kv_csv(env_val: str, key_norm: bool = True) -> Dict[str, str]:
    """Parse 'Kitchen:35,Bath:30' or 'google:organic' -> dict; keys optional normalized."""
    result: Dict[str, str] = {}
    s = (env_val or "").strip()
    if not s:
        return result
    for pair in s.split(","):
        if not pair.strip() or ":" not in pair:
            continue
        k, v = pair.split(":", 1)
        k = k.strip()
        if key_norm:
            k = _normalize_label(k)
        result[k] = v.strip()
    return result

def _parse_prob_csv(env_val: str) -> Dict[str, float]:
    """Parse 'Kitchen:0.65,Bath:0.45' -> {'kitchen':0.65,'bath':0.45}"""
    out: Dict[str, float] = {}
    raw = _parse_kv_csv(env_val, key_norm=True)
    for k, v in raw.items():
        try:
            out[k] = max(0.0, min(1.0, float(v)))
        except Exception:
            continue
    return out

def _normalize_label(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def _is_visible_text(s: str) -> bool:
    return bool((s or "").strip())

# ---------------------- Session class ----------------------
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
        self.utm_mediums = _parse_kv_csv(os.getenv("REFERRER_UTM_MEDIUMS", ""), key_norm=True)

        # Human-like nav/scroll behavior
        self.wait_until = os.getenv("PAGE_WAIT_UNTIL", "load").strip().lower()
        if self.wait_until not in ("load", "domcontentloaded", "networkidle"):
            self.wait_until = "load"
        self.post_nav_settle_min = int(os.getenv("POST_NAV_SETTLE_MIN_MS", "250"))
        self.post_nav_settle_max = int(os.getenv("POST_NAV_SETTLE_MAX_MS", "900"))
        self.scroll_prob = float(os.getenv("SCROLL_PROB", "0.70"))
        self.scroll_depth_min = float(os.getenv("SCROLL_DEPTH_MIN", "0.35"))
        self.scroll_depth_max = float(os.getenv("SCROLL_DEPTH_MAX", "0.90"))
        self.scroll_steps_min = int(os.getenv("SCROLL_STEPS_MIN", "2"))
        self.scroll_steps_max = int(os.getenv("SCROLL_STEPS_MAX", "6"))

        # Top-nav / hotspots
        self.nav_weights = _parse_kv_csv(os.getenv("NAV_CATEGORY_WEIGHTS", ""), key_norm=True)
        self.nav_hotspot_names = [_normalize_label(x) for x in (os.getenv("NAV_HOTSPOT_NAMES","Kitchen,Bath")).split(",") if x.strip()]
        self.nav_hotspot_extra_prob = _parse_prob_csv(os.getenv("NAV_HOTSPOT_EXTRA_CLICK_PROB","Kitchen:0.65,Bath:0.45"))
        self.nav_pause_min = int(os.getenv("NAV_NAVIGATION_PAUSE_MS_MIN","400"))
        self.nav_pause_max = int(os.getenv("NAV_NAVIGATION_PAUSE_MS_MAX","1100"))

        # Coverage pass
        self.coverage_prob = float(os.getenv("COVERAGE_RUN_PROB","0.15"))
        self.coverage_max_clicks = int(os.getenv("COVERAGE_MAX_CLICKS","8"))
        self.coverage_allow = [s.strip() for s in os.getenv("COVERAGE_SELECTOR_ALLOW",".hero a,.promo a,.featured a,.card a,button,.btn").split(",") if s.strip()]
        self.coverage_block = [s.strip() for s in os.getenv("COVERAGE_SELECTOR_BLOCK",'[href*="logout"],[href^="mailto:"],[href^="tel:"],[href*="admin"],.social a').split(",") if s.strip()]

        # Funnel gating
        self.funnel_atc_rate = float(os.getenv("FUNNEL_ADD_TO_CART_RATE","0.30"))
        self.funnel_checkout_rate = float(os.getenv("FUNNEL_CHECKOUT_START_RATE","0.50"))
        self.funnel_max_cart_adds = int(os.getenv("FUNNEL_MAX_CART_ADDS_PER_SESSION","1"))
        self.funnel_max_checkout_starts = int(os.getenv("FUNNEL_MAX_CHECKOUT_STARTS_PER_SESSION","1"))

        # Flags per session
        self.flag_is_atc_session = (random.random() < self.funnel_atc_rate)
        self.flag_should_checkout = (self.flag_is_atc_session and (random.random() < self.funnel_checkout_rate))
        self.did_add_to_cart = 0
        self.did_start_checkout = 0
        self.stop_requested = False

        self.page = None
        self.context = None

    # ---------------------- context & navigation helpers ----------------------
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
                settle = random.uniform(self.post_nav_settle_min/1000, self.post_nav_settle_max/1000)
                await asyncio.sleep(settle)
                return resp
            except Exception:
                await backoff.wait()
                if backoff.attempts > 5:
                    raise

    async def _maybe_scroll_page(self):
        if random.random() > max(0.0, min(1.0, self.scroll_prob)):
            debug_print(self.debug, f"[S{self.id}] no scroll (randomized)")
            return
        try:
            await self.page.wait_for_selector("body", timeout=SEL_TIMEOUT)
        except Exception:
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
                  const h = Math.max( ...vals, 0 );
                  return (h && isFinite(h)) ? h : 2000;
                }
            """)
        except Exception:
            height = 2000
        depth_frac = max(0.0, min(1.0, random.uniform(self.scroll_depth_min, self.scroll_depth_max)))
        target = max(400, height * depth_frac)
        steps = max(1, min(10, random.randint(self.scroll_steps_min, self.scroll_steps_max)))
        for _ in range(steps):
            await self.page.mouse.wheel(0, target/steps)
            await think(self.think_cfg["scroll_min_ms"], self.think_cfg["scroll_max_ms"])

        # ---------------------- flow control ----------------------
    async def run(self):
        await self._new_context()
        try:
            flow = random.choice(self.flows or [{}])
            if not flow:
                return
            await self._run_scripted(flow)
        finally:
            debug_print(self.debug, f"[S{self.id}] summary: atc={self.did_add_to_cart} checkout={self.did_start_checkout}")
            await self.context.close()

    async def _run_scripted(self, flow: dict):
        steps = flow.get("steps", [])
        await self._landing()

        # NEW: Top-nav click-all with hotspot extras
        await self._topnav_click_all_with_hotspots()

        for step in steps:
            if self.stop_requested:
                break
            await self._execute_step(step)
            await think(self.think_cfg["page_min_ms"], self.think_cfg["page_max_ms"])

        # Optional coverage pass
        if (not self.stop_requested) and random.random() < self.coverage_prob:
            await self._coverage_click_pass()

    # ---------------------- landing & steps ----------------------
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

    # ---------------------- top nav logic ----------------------
    async def _query_top_nav_links(self) -> List[Tuple[str, str]]:
        """Return list of (label_normalized, href) for top navigation candidates."""
        sel_list = [
            "header nav a",
            '[role="navigation"] a',
            ".navPages-container a",
            ".navPages a",
            ".header-nav a",
            "nav a",
        ]
        seen = {}
        for sel in sel_list:
            try:
                loc = self.page.locator(sel)
                count = await loc.count()
                for i in range(min(count, 150)):
                    try:
                        el = loc.nth(i)
                        text = (await el.inner_text(timeout=1000)).strip()
                        if not _is_visible_text(text):
                            continue
                        href = await el.get_attribute("href", timeout=500)
                        if not href:
                            continue
                        href = urljoin(self.origin + "/", href)
                        if not same_origin(href, self.allowlist):
                            continue
                        key = _normalize_label(text)
                        cur = seen.get(key)
                        if not cur:
                            seen[key] = href
                        else:
                            if key in self.nav_weights and cur not in self.nav_weights and href in self.nav_weights:
                                seen[key] = href
                    except Exception:
                        continue
            except Exception:
                continue
        items = [(k, v) for k, v in seen.items() if k]
        items = [(k, v) for (k, v) in items if not v.rstrip("/").endswith(self.origin.rstrip("/"))]
        return items

    async def _topnav_click_all_with_hotspots(self):
        """Click ALL discovered categories once, then optional extra clicks for hotspots."""
        links = await self._query_top_nav_links()
        if not links:
            debug_print(self.debug, f"[S{self.id}] top-nav: nothing discovered")
            return
        random.shuffle(links)
        for label_norm, href in links:
            if self.stop_requested:
                break
            await self._click_nav_link(label_norm, href)
        for hot in self.nav_hotspot_names:
            if self.stop_requested:
                break
            prob = self.nav_hotspot_extra_prob.get(hot, 0.0)
            if prob > 0 and random.random() < prob:
                target = next(((ln, h) for (ln, h) in links if ln == _normalize_label(hot)), None)
                if target:
                    await self._click_nav_link(target[0], target[1])

    async def _click_nav_link(self, label_norm: str, href: str):
        debug_print(self.debug, f"[S{self.id}] nav → {label_norm} ({href})")
        await self._guarded_goto(href)
        await self._maybe_scroll_page()
        await asyncio.sleep(random.uniform(self.nav_pause_min/1000, self.nav_pause_max/1000))
        if not self.stop_requested:
            await self._category_micro_behaviors()

    async def _category_micro_behaviors(self):
        await self._sort_or_filter()
        await self._open_random_pdp(count=random.randint(1, 2))

    # ---------------------- actions ----------------------
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
        nav_candidates = self.page.get_by_role("link", name=re.compile("(Shop|All|Men|Women|Accessories|Catalog|Sale|New|Kitchen|Bath)", re.I))
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
            if self.stop_requested:
                break
            grid = self.page.locator("a.card-figure, a.card-title, a.product-title, a[href*='/products/']")
            try:
                n = await grid.count()
            except Exception:
                n = 0
            if n > 0:
                i = random.randint(0, min(n-1, 15))
                await grid.nth(i).click(timeout=SEL_TIMEOUT)
                await self._maybe_scroll_page()
                if self.flag_is_atc_session and self.did_add_to_cart < self.funnel_max_cart_adds:
                    await self._add_to_cart()
                    if self.flag_should_checkout and self.did_start_checkout < self.funnel_max_checkout_starts:
                        await self._view_cart()
                        await self._start_checkout()
                        if self.did_start_checkout:
                            debug_print(self.debug, f"[S{self.id}] checkout reached – pausing flow")
                            self.stop_requested = True
                            return
            else:
                break

    async def _sort_or_filter(self):
        sort_prob = float(os.getenv("CATEGORY_SORT_PROB", "0.30"))
        filter_prob = float(os.getenv("CATEGORY_FILTER_PROB", "0.15"))
        if random.random() < sort_prob:
            try:
                sort_sel = self.page.locator("select[name='sort'], select#sort, select[name*='Sort']")
                await sort_sel.first.select_option(index=random.randint(0, 2), timeout=SEL_TIMEOUT)
            except Exception:
                pass
            await self._maybe_scroll_page()
        if random.random() < filter_prob:
            try:
                filt = self.page.locator("input[type='checkbox'], .facetedSearch-option--checkbox input")
                if await filt.count() > 0:
                    await filt.nth(0).check(timeout=SEL_TIMEOUT)
                    await asyncio.sleep(0.5)
            except Exception:
                pass
            await self._maybe_scroll_page()

    async def _add_to_cart(self):
        if self.did_add_to_cart >= self.funnel_max_cart_adds:
            return
        try:
            btn = self.page.get_by_role("button", name=re.compile("add to cart", re.I))
            await btn.first.click(timeout=SEL_TIMEOUT)
            self.did_add_to_cart += 1
            return
        except Exception:
            pass
        try:
            await self.page.click("button#form-action-addToCart, button[name='add']", timeout=SEL_TIMEOUT)
            self.did_add_to_cart += 1
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
        if self.did_start_checkout >= self.funnel_max_checkout_starts:
            return
        try:
            btn = self.page.get_by_role("link", name=re.compile("checkout", re.I))
            await btn.first.click(timeout=SEL_TIMEOUT)
            self.did_start_checkout += 1
        except Exception:
            try:
                await self.page.click("a[href*='/checkout']", timeout=SEL_TIMEOUT)
                self.did_start_checkout += 1
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
        return

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

    # ---------------------- coverage pass ----------------------
    async def _coverage_click_pass(self):
        try:
            await self.page.wait_for_selector("body", timeout=SEL_TIMEOUT)
        except Exception:
            return
        allow = ", ".join(self.coverage_allow)
        loc = self.page.locator(allow)
        try:
            total = await loc.count()
        except Exception:
            total = 0
        if total == 0:
            return
        indices = list(range(min(total, 100)))
        random.shuffle(indices)
        clicks = 0
        for i in indices:
            if clicks >= self.coverage_max_clicks or self.stop_requested:
                break
            el = loc.nth(i)
            try:
                href = await el.get_attribute("href", timeout=200) or ""
                blocked = any(b in href for b in [s.replace('[href*="','').replace('"]','') for s in self.coverage_block if 'href*="' in s])
                if blocked:
                    continue
            except Exception:
                pass
            try:
                await el.click(timeout=SEL_TIMEOUT)
                clicks += 1
                await self._maybe_scroll_page()
                await asyncio.sleep(random.uniform(0.2, 0.8))
            except Exception:
                continue
