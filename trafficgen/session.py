
# session.py (ready-to-drop) - see chat for full description
import asyncio, os, random, re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse, urljoin
from trafficgen.utils import think, same_origin, ExponentialBackoff, debug_print

ALLOW_NAV_TIMEOUT = 25000
SEL_TIMEOUT = 15000

def _normalize_label(s: str) -> str:
    import re as _re
    return _re.sub(r"\s+", " ", (s or "").strip()).lower()

def _slug_from_source(src: str) -> str:
    if not src: return ""
    s = src.strip().lower()
    if s == "direct": return "direct"
    try:
        netloc = urlparse(s).netloc if "://" in s else s
        netloc = re.sub(r"^www\.", "", netloc)
        parts = netloc.split(".")
        return parts[-2] if len(parts) >= 2 else netloc
    except Exception:
        return re.sub(r"\W+", "", s)

def _parse_kv_csv(env_val: str, normalize_keys: bool = True) -> Dict[str, str]:
    out: Dict[str, str] = {}
    s = (env_val or "").strip()
    if not s: return out
    for pair in s.split(","):
        if ":" not in pair: continue
        k, v = pair.split(":", 1)
        k = _normalize_label(k) if normalize_keys else k.strip()
        out[k] = v.strip()
    return out

def _parse_prob_csv(env_val: str) -> Dict[str, float]:
    raw = _parse_kv_csv(env_val, normalize_keys=True)
    out: Dict[str, float] = {}
    for k, v in raw.items():
        try: out[k] = max(0.0, min(1.0, float(v)))
        except Exception: pass
    return out

class Session:
    def __init__(self, session_id: int, browser, playwright, origin: str, allowlist_roots: List[str],
                 device_context_args: Dict[str, Any], locale: str, timezone_id: str,
                 allow_checkout: bool, checkout_complete_rate: float, flows: List[dict],
                 think_cfg: Dict[str, int], global_qps, debug: bool=False, fault_profile: Optional[dict]=None,
                 referrer_url: Optional[str]=None):
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
        self.think_cfg = think_cfg or {"page_min_ms":800,"page_max_ms":3000,"scroll_min_ms":200,"scroll_max_ms":1000}
        self.global_qps = global_qps
        self.debug = debug
        self.fault_profile = fault_profile or {}

        self.referrer_url = (referrer_url or "").strip() or None
        if self.referrer_url and self.referrer_url.lower() == "direct": self.referrer_url = "direct"

        self.utm_medium_default = os.getenv("UTM_MEDIUM_DEFAULT", "paid-social")
        self.utm_campaign_default = os.getenv("UTM_CAMPAIGN_DEFAULT", "trafficgen")
        self.utm_mediums = _parse_kv_csv(os.getenv("REFERRER_UTM_MEDIUMS", ""), normalize_keys=True)

        self.wait_until = os.getenv("PAGE_WAIT_UNTIL", "load").strip().lower()
        if self.wait_until not in ("load","domcontentloaded","networkidle"): self.wait_until = "load"
        self.post_nav_settle_min = int(os.getenv("POST_NAV_SETTLE_MIN_MS","250"))
        self.post_nav_settle_max = int(os.getenv("POST_NAV_SETTLE_MAX_MS","900"))
        self.scroll_prob = float(os.getenv("SCROLL_PROB","0.70"))
        self.scroll_depth_min = float(os.getenv("SCROLL_DEPTH_MIN","0.35"))
        self.scroll_depth_max = float(os.getenv("SCROLL_DEPTH_MAX","0.90"))
        self.scroll_steps_min = int(os.getenv("SCROLL_STEPS_MIN","2"))
        self.scroll_steps_max = int(os.getenv("SCROLL_STEPS_MAX","6"))

        self.nav_weights = _parse_kv_csv(os.getenv("NAV_CATEGORY_WEIGHTS",""), normalize_keys=True)
        self.nav_hotspot_names = [_normalize_label(x) for x in os.getenv("NAV_HOTSPOT_NAMES","Kitchen,Bath").split(",") if x.strip()]
        self.nav_hotspot_extra_prob = _parse_prob_csv(os.getenv("NAV_HOTSPOT_EXTRA_CLICK_PROB","Kitchen:0.65,Bath:0.45"))
        self.nav_pause_min = int(os.getenv("NAV_NAVIGATION_PAUSE_MS_MIN","400"))
        self.nav_pause_max = int(os.getenv("NAV_NAVIGATION_PAUSE_MS_MAX","1100"))

        self.coverage_prob = float(os.getenv("COVERAGE_RUN_PROB","0.15"))
        self.coverage_max_clicks = int(os.getenv("COVERAGE_MAX_CLICKS","8"))
        self.coverage_allow = [s.strip() for s in os.getenv("COVERAGE_SELECTOR_ALLOW",".hero a,.promo a,.featured a,.card a,button,.btn").split(",") if s.strip()]
        self.coverage_block = [s.strip() for s in os.getenv("COVERAGE_SELECTOR_BLOCK",'[href*="logout"],[href^="mailto:"],[href^="tel:"],[href*="admin"],.social a').split(",") if s.strip()]

        self.funnel_atc_rate = float(os.getenv("FUNNEL_ADD_TO_CART_RATE","0.30"))
        self.funnel_checkout_rate = float(os.getenv("FUNNEL_CHECKOUT_START_RATE","0.50"))
        self.funnel_max_cart_adds = int(os.getenv("FUNNEL_MAX_CART_ADDS_PER_SESSION","1"))
        self.funnel_max_checkout_starts = int(os.getenv("FUNNEL_MAX_CHECKOUT_STARTS_PER_SESSION","1"))
        self.flag_is_atc_session = (random.random() < self.funnel_atc_rate)
        self.flag_should_checkout = (self.flag_is_atc_session and (random.random() < self.funnel_checkout_rate))
        self.did_add_to_cart = 0
        self.did_start_checkout = 0
        self.stop_requested = False

        self.page = None
        self.context = None

    async def _new_context(self):
        cargs = dict(self.ctx_args)
        cargs["locale"] = self.locale
        cargs["timezone_id"] = self.tz
        cargs.setdefault("ignore_https_errors", True)
        self.context = await self.browser.new_context(**cargs)
        self.page = await self.context.new_page()

    async def _guarded_goto(self, url: str):
        if not same_origin(url, self.allowlist): return
        await self.global_qps.wait()
        backoff = ExponentialBackoff()
        while True:
            try:
                await self.page.goto(url, timeout=ALLOW_NAV_TIMEOUT, wait_until=self.wait_until)
                await asyncio.sleep(random.uniform(self.post_nav_settle_min/1000, self.post_nav_settle_max/1000))
                return
            except Exception:
                await backoff.wait()
                if backoff.attempts > 5: raise

    async def _maybe_scroll_page(self):
        if random.random() > max(0.0, min(1.0, self.scroll_prob)):
            debug_print(self.debug, f"[S{self.id}] no scroll (randomized)")
            return
        try: await self.page.wait_for_selector("body", timeout=SEL_TIMEOUT)
        except Exception: return
        try:
            height = await self.page.evaluate("""                () => { const d=document.documentElement,b=document.body;
                const vals=[d.scrollHeight,b.scrollHeight,d.offsetHeight,b.offsetHeight,d.clientHeight,b.clientHeight].filter(v=>typeof v==='number');
                const h=Math.max(...vals,0); return (h && isFinite(h))?h:2000; }""")
        except Exception: height = 2000
        depth_frac = max(0.0, min(1.0, random.uniform(self.scroll_depth_min, self.scroll_depth_max)))
        target = max(400, height * depth_frac)
        steps = max(1, min(10, random.randint(self.scroll_steps_min, self.scroll_steps_max)))
        for _ in range(steps):
            await self.page.mouse.wheel(0, target/steps)
            await think(self.think_cfg["scroll_min_ms"], self.think_cfg["scroll_max_ms"])

    async def run(self):
        await self._new_context()
        try:
            flow = random.choice(self.flows or [{}])
            if not flow: return
            await self._run_scripted(flow)
        finally:
            debug_print(self.debug, f"[S{self.id}] summary: atc={self.did_add_to_cart} checkout={self.did_start_checkout}")
            await self.context.close()

    async def _run_scripted(self, flow: dict):
        steps = flow.get("steps", [])
        await self._landing()
        await self._topnav_click_all_with_hotspots()
        for step in steps:
            if self.stop_requested: break
            await self._execute_step(step)
            await think(self.think_cfg["page_min_ms"], self.think_cfg["page_max_ms"])
        if (not self.stop_requested) and random.random() < self.coverage_prob:
            await self._coverage_click_pass()

    async def _landing(self):
        landing = self.origin + "/"
        if self.referrer_url and self.referrer_url != "direct":
            utm_source = _slug_from_source(self.referrer_url)
            if utm_source and utm_source != "direct":
                utm_medium = self.utm_mediums.get(utm_source, self.utm_medium_default)
                q = {"utm_source": utm_source, "utm_medium": utm_medium, "utm_campaign": self.utm_campaign_default}
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
        if kind == "open_random_category":
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

    async def _query_top_nav_links(self) -> List[Tuple[str, any]]:
        selectors = ["header nav a","[role=\"navigation\"] a",".navPages-container a",".navPages a",".header-nav a","nav a"]
        seen = {}
        for sel in selectors:
            try:
                loc = self.page.locator(sel)
                cnt = await loc.count()
                for i in range(min(cnt, 150)):
                    el = loc.nth(i)
                    try:
                        text = (await el.inner_text(timeout=800)).strip()
                        if not text: continue
                        key = _normalize_label(text)
                        href = await el.get_attribute("href", timeout=300) or ""
                        if not href: continue
                        url = urljoin(self.origin + "/", href)
                        if not same_origin(url, self.allowlist): continue
                        if key and key not in seen: seen[key] = el
                    except Exception: continue
            except Exception: continue
        out = []
        for key, el in seen.items():
            try:
                href = await el.get_attribute("href", timeout=200) or ""
                if href.rstrip("/").endswith(self.origin.rstrip("/")): continue
            except Exception: pass
            out.append((key, el))
        return out

    async def _topnav_click_all_with_hotspots(self):
        links = await self._query_top_nav_links()
        if not links:
            debug_print(self.debug, f"[S{self.id}] top-nav: none found"); return
        random.shuffle(links)
        for label_norm, el in links:
            if self.stop_requested: break
            await self._click_nav_el(label_norm, el)
        for hot in self.nav_hotspot_names:
            if self.stop_requested: break
            label = _normalize_label(hot)
            prob = self.nav_hotspot_extra_prob.get(label, 0.0)
            if prob > 0 and random.random() < prob:
                target = next(((ln, e) for (ln, e) in links if ln == label), None)
                if target: await self._click_nav_el(target[0], target[1])

    async def _click_nav_el(self, label_norm: str, el):
        try:
            box = await el.bounding_box()
            if box: await self.page.mouse.move(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
            await el.click(timeout=SEL_TIMEOUT)
            debug_print(self.debug, f"[S{self.id}] nav click → {label_norm}")
        except Exception:
            try:
                href = await el.get_attribute("href", timeout=500)
                if href:
                    url = urljoin(self.origin + "/", href)
                    debug_print(self.debug, f"[S{self.id}] nav goto (fallback) → {label_norm} ({url})")
                    await self._guarded_goto(url)
            except Exception: return
        await self._maybe_scroll_page()
        await asyncio.sleep(random.uniform(self.nav_pause_min/1000, self.nav_pause_max/1000))
        if not self.stop_requested: await self._category_micro_behaviors()

    async def _category_micro_behaviors(self):
        await self._sort_or_filter()
        await self._open_random_pdp(count=random.randint(1, 2))

    async def _open_random_category(self):
        nav_candidates = self.page.get_by_role("link", name=re.compile("(Shop|All|Kitchen|Bath|Accessories|Sale|New)", re.I))
        count = await nav_candidates.count()
        if count > 0 and random.random() < 0.7:
            idx = random.randint(0, min(count-1, 5))
            await nav_candidates.nth(idx).click(timeout=SEL_TIMEOUT)
        else:
            await self._guarded_goto(f"{self.origin}/categories/")
        await self._maybe_scroll_page()

    async def _open_random_pdp(self, count: int = 1):
        count = max(1, min(count, 3))
        for _ in range(count):
            if self.stop_requested: break
            grid = self.page.locator("a.card-figure, a.card-title, a.product-title, a[href*='/products/']")
            try: n = await grid.count()
            except Exception: n = 0
            if n > 0:
                i = random.randint(0, min(n-1, 15))
                await grid.nth(i).click(timeout=SEL_TIMEOUT)
                await self._maybe_scroll_page()
                if self.flag_is_atc_session and self.did_add_to_cart < self.funnel_max_cart_adds:
                    await self._add_to_cart()
                    if self.flag_should_checkout and self.did_start_checkout < self.funnel_max_checkout_starts:
                        await self._view_cart(); await self._start_checkout()
                        if self.did_start_checkout:
                            debug_print(self.debug, f"[S{self.id}] checkout reached – pausing flow")
                            self.stop_requested = True; return
            else: break

    async def _sort_or_filter(self):
        sort_prob = float(os.getenv("CATEGORY_SORT_PROB","0.30"))
        filter_prob = float(os.getenv("CATEGORY_FILTER_PROB","0.15"))
        if random.random() < sort_prob:
            try:
                sel = self.page.locator("select[name='sort'], select#sort, select[name*='Sort']")
                await sel.first.select_option(index=random.randint(0, 2), timeout=SEL_TIMEOUT)
            except Exception: pass
            await self._maybe_scroll_page()
        if random.random() < filter_prob:
            try:
                filt = self.page.locator("input[type='checkbox'], .facetedSearch-option--checkbox input")
                if await filt.count() > 0:
                    await filt.nth(0).check(timeout=SEL_TIMEOUT); await asyncio.sleep(0.5)
            except Exception: pass
            await self._maybe_scroll_page()

    async def _add_to_cart(self):
        if self.did_add_to_cart >= self.funnel_max_cart_adds: return
        try:
            btn = self.page.get_by_role("button", name=re.compile("add to cart", re.I))
            await btn.first.click(timeout=SEL_TIMEOUT); self.did_add_to_cart += 1; return
        except Exception: pass
        try:
            await self.page.click("button#form-action-addToCart, button[name='add']", timeout=SEL_TIMEOUT); self.did_add_to_cart += 1
        except Exception: return
        await think(500, 1200)

    async def _view_cart(self):
        try:
            link = self.page.get_by_role("link", name=re.compile("cart|view cart", re.I))
            await link.first.click(timeout=SEL_TIMEOUT)
        except Exception:
            await self._guarded_goto(f"{self.origin}/cart.php")
        await self._maybe_scroll_page()

    async def _start_checkout(self):
        if self.did_start_checkout >= self.funnel_max_checkout_starts: return
        try:
            btn = self.page.get_by_role("link", name=re.compile("checkout", re.I))
            await btn.first.click(timeout=SEL_TIMEOUT); self.did_start_checkout += 1
        except Exception:
            try:
                await self.page.click("a[href*='/checkout']", timeout=SEL_TIMEOUT); self.did_start_checkout += 1
            except Exception: return
        await self._maybe_scroll_page()

    async def _content_page(self, slug: str):
        slugs = ["/contact-us/","/shipping-returns/","/blog/","/help/"]
        if slug and slug.startswith("/"): slugs.insert(0, slug)
        await self._guarded_goto(self.origin + random.choice(slugs)); await self._maybe_scroll_page()

    async def _coverage_click_pass(self):
        try: await self.page.wait_for_selector("body", timeout=SEL_TIMEOUT)
        except Exception: return
        allow = ", ".join(self.coverage_allow)
        loc = self.page.locator(allow)
        try: total = await loc.count()
        except Exception: total = 0
        if total == 0: return
        idxs = list(range(min(total, 100))); random.shuffle(idxs)
        clicks = 0
        for i in idxs:
            if clicks >= self.coverage_max_clicks or self.stop_requested: break
            el = loc.nth(i)
            try:
                href = await el.get_attribute("href", timeout=200) or ""
                for b in self.coverage_block:
                    if "href*=" in b:
                        needle = b.split('href*="',1)[1].rstrip('"]')
                        if needle in href: raise Exception("blocked")
                await el.click(timeout=SEL_TIMEOUT); clicks += 1
                await self._maybe_scroll_page(); await asyncio.sleep(random.uniform(0.2,0.8))
            except Exception: continue
