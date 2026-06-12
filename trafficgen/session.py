# trafficgen/session.py (ready-to-drop)
import asyncio
import contextlib
import os
import random
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse, urljoin

from trafficgen.utils import (
    biased_index,
    ExponentialBackoff,
    choose_weighted,
    debug_print,
    same_origin,
    think,
    weighted_value,
)
from trafficgen import attribution
from trafficgen import checkout as checkout_engine

ALLOW_NAV_TIMEOUT = 25000
SEL_TIMEOUT = 15000

def _normalize_label(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def _slug_from_source(src: str) -> str:
    if not src:
        return ""
    s = src.strip().lower()
    if s == "direct":
        return "direct"
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
    if not s:
        return out
    for pair in s.split(","):
        if ":" not in pair:
            continue
        k, v = pair.split(":", 1)
        k = _normalize_label(k) if normalize_keys else k.strip()
        out[k] = v.strip()
    return out

def _parse_list_csv(s: str) -> List[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]

def _parse_float_csv(s: str) -> List[float]:
    out: List[float] = []
    for x in _parse_list_csv(s):
        try:
            out.append(float(x))
        except Exception:
            continue
    return out

def _parse_prob_csv(env_val: str) -> Dict[str, float]:
    raw = _parse_kv_csv(env_val, normalize_keys=True)
    out: Dict[str, float] = {}
    for k, v in raw.items():
        try:
            out[k] = max(0.0, min(1.0, float(v)))
        except Exception:
            pass
    return out

def _parse_weight_overrides(env_val: str) -> Dict[str, float]:
    raw = _parse_kv_csv(env_val, normalize_keys=True)
    out: Dict[str, float] = {}
    for k, v in raw.items():
        try:
            out[k] = max(float(v), 0.0)
        except Exception:
            continue
    return out

def _weighted_choice(items: List[str], weights: List[float]) -> Optional[str]:
    if not items:
        return None
    if not weights or len(weights) != len(items):
        return random.choice(items)
    total = sum(max(0.0, w) for w in weights)
    if total <= 0:
        return random.choice(items)
    r = random.uniform(0, total)
    acc = 0.0
    for it, w in zip(items, weights):
        acc += max(0.0, w)
        if r <= acc:
            return it
    return items[-1]


def _prob_to_fraction(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
    except Exception:
        return None
    if f > 1:
        f = f / 100.0
    return max(0.0, min(1.0, f))

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
                 referrer_url: Optional[str] = None,
                 persona: Optional[Dict[str, Any]] = None,
                 may_place_order=None):
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
        self.persona = persona if isinstance(persona, dict) else None
        self.may_place_order = may_place_order
        self.flow_weight_overrides = _parse_weight_overrides(os.getenv("FLOW_WEIGHTS", ""))

        # UTM source (legacy env choice supplied by Runner)
        self.referrer_url = (referrer_url or "").strip() or None
        if self.referrer_url and self.referrer_url.lower() == "direct":
            self.referrer_url = "direct"

        # UTM/env
        self.utm_medium_default = os.getenv("UTM_MEDIUM_DEFAULT", "paid-social")
        self.utm_campaign_default = os.getenv("UTM_CAMPAIGN_DEFAULT", "trafficgen")
        self.utm_mediums = _parse_kv_csv(os.getenv("REFERRER_UTM_MEDIUMS", ""), normalize_keys=True)

        # NEW: explicit header URLs; weights reuse existing REFERRER_WEIGHTS
        self.ref_hdr_urls = _parse_list_csv(os.getenv("REFERRER_HEADER_URLS", ""))
        self.ref_hdr_weights = _parse_float_csv(os.getenv("REFERRER_WEIGHTS", ""))

        # Human-like behavior
        self.wait_until = os.getenv("PAGE_WAIT_UNTIL", "load").strip().lower()
        if self.wait_until not in ("load","domcontentloaded","networkidle"):
            self.wait_until = "load"
        self.post_nav_settle_min = int(os.getenv("POST_NAV_SETTLE_MIN_MS","250"))
        self.post_nav_settle_max = int(os.getenv("POST_NAV_SETTLE_MAX_MS","900"))
        self.scroll_prob = float(os.getenv("SCROLL_PROB","0.70"))
        self.scroll_depth_min = float(os.getenv("SCROLL_DEPTH_MIN","0.35"))
        self.scroll_depth_max = float(os.getenv("SCROLL_DEPTH_MAX","0.90"))
        self.scroll_steps_min = int(os.getenv("SCROLL_STEPS_MIN","2"))
        self.scroll_steps_max = int(os.getenv("SCROLL_STEPS_MAX","6"))

        # Post-load pauses
        self.micro_pause_min_ms = int(os.getenv("PAGE_MICRO_PAUSE_MIN_MS","90"))
        self.micro_pause_max_ms = int(os.getenv("PAGE_MICRO_PAUSE_MAX_MS","280"))
        self.idle_after_page_prob = float(os.getenv("PAGE_IDLE_PROB","0.14"))
        self.idle_after_page_min_ms = int(os.getenv("PAGE_IDLE_MIN_MS","1400"))
        self.idle_after_page_max_ms = int(os.getenv("PAGE_IDLE_MAX_MS","5200"))

        # Per-step jitter
        self.step_jitter_pause_min = int(os.getenv("STEP_JITTER_PAUSE_MIN_MS","120"))
        self.step_jitter_pause_max = int(os.getenv("STEP_JITTER_PAUSE_MAX_MS","600"))
        self.step_jitter_scroll_prob = float(os.getenv("STEP_JITTER_SCROLL_PROB","0.35"))
        self.step_jitter_scroll_depth_min = float(os.getenv("STEP_JITTER_SCROLL_DEPTH_MIN","0.08"))
        self.step_jitter_scroll_depth_max = float(os.getenv("STEP_JITTER_SCROLL_DEPTH_MAX","0.45"))
        self.step_jitter_scroll_steps_min = int(os.getenv("STEP_JITTER_SCROLL_STEPS_MIN","1"))
        self.step_jitter_scroll_steps_max = int(os.getenv("STEP_JITTER_SCROLL_STEPS_MAX","3"))

        # Tile hover heatmaps
        self.tile_hover_prob = float(os.getenv("CATEGORY_TILE_HOVER_PROB","0.6"))
        self.tile_hover_count_min = int(os.getenv("CATEGORY_TILE_HOVER_MIN","2"))
        self.tile_hover_count_max = int(os.getenv("CATEGORY_TILE_HOVER_MAX","5"))
        self.tile_hover_dwell_min_ms = int(os.getenv("CATEGORY_TILE_HOVER_DWELL_MIN_MS","160"))
        self.tile_hover_dwell_max_ms = int(os.getenv("CATEGORY_TILE_HOVER_DWELL_MAX_MS","520"))

        # Top-nav & hotspots
        self.nav_weights = _parse_kv_csv(os.getenv("NAV_CATEGORY_WEIGHTS",""), normalize_keys=True)
        self.nav_hotspot_names = [_normalize_label(x) for x in os.getenv("NAV_HOTSPOT_NAMES","Kitchen,Bath").split(",") if x.strip()]
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
        # Persona funnel probabilities take precedence over FUNNEL_* env rates.
        persona_funnel = (self.persona or {}).get("funnel")
        if isinstance(persona_funnel, dict):
            self.flag_bounce = (random.random() < float(persona_funnel.get("bounce", 0.0) or 0.0))
            atc_rate = float(persona_funnel.get("add_to_cart", self.funnel_atc_rate) or 0.0)
            checkout_start_rate = float(persona_funnel.get("checkout_start", self.funnel_checkout_rate) or 0.0)
        else:
            self.flag_bounce = False
            atc_rate = self.funnel_atc_rate
            checkout_start_rate = self.funnel_checkout_rate
        self.flag_is_atc_session = (not self.flag_bounce) and (random.random() < atc_rate)
        self.flag_should_checkout = (self.flag_is_atc_session and (random.random() < checkout_start_rate))
        self.did_add_to_cart = 0
        self.did_start_checkout = 0
        self.did_complete_checkout = 0
        self.stop_requested = False
        self._apply_flow_weight_overrides()
        self._apply_persona_flow_overrides()
        self._flow_weights_available = any(isinstance(f, dict) and "weight" in f for f in self.flows)

        # Search
        self.search_terms = _parse_list_csv(os.getenv(
            "SEARCH_TERMS",
            "faucet,sink,shower,towel,mirror,lighting,vanity,fixture,soap,kitchen,bathroom,storage,rug,mat",
        )) or ["sale", "new", "gift"]

        self.page = None
        self.context = None

    async def _new_context(self):
        cargs = dict(self.ctx_args)
        cargs["locale"] = self.locale
        cargs["timezone_id"] = self.tz
        cargs.setdefault("ignore_https_errors", True)
        # 3-layer no-cache treatment (all sessions): CloudFront varies cache
        # on Referer; cached cross-referrer responses broke Noibu loading.
        cargs["service_workers"] = "block"
        self.context = await self.browser.new_context(**cargs)
        await attribution.apply_no_cache(self.context)
        self.page = await self.context.new_page()

    def _apply_flow_weight_overrides(self):
        if not self.flow_weight_overrides:
            return
        for f in self.flows:
            if not isinstance(f, dict):
                continue
            name = _normalize_label(f.get("name") or "")
            if not name:
                continue
            if name in self.flow_weight_overrides:
                f["weight"] = self.flow_weight_overrides[name]

    def _apply_persona_flow_overrides(self):
        overrides = (self.persona or {}).get("flow_overrides")
        if not isinstance(overrides, dict) or not overrides:
            return
        norm = {_normalize_label(str(k)): v for k, v in overrides.items()}
        # Copy flow dicts so per-persona multipliers never mutate shared state.
        self.flows = [dict(f) if isinstance(f, dict) else f for f in self.flows]
        for f in self.flows:
            if not isinstance(f, dict):
                continue
            name = _normalize_label(f.get("name") or "")
            if name not in norm:
                continue
            try:
                mult = max(float(norm[name]), 0.0)
            except Exception:
                continue
            try:
                base = float(f.get("weight", 1) or 1)
            except Exception:
                base = 1.0
            f["weight"] = base * mult

    def _pick_flow(self) -> Optional[dict]:
        if not self.flows:
            return None
        if self._flow_weights_available:
            return choose_weighted(self.flows, key="weight") or self.flows[0]
        return random.choice(self.flows)

    async def _guarded_goto(self, url: str, referer: Optional[str] = None):
        if not same_origin(url, self.allowlist):
            return
        await self.global_qps.wait()
        backoff = ExponentialBackoff()
        while True:
            try:
                await self.page.goto(
                    url,
                    timeout=ALLOW_NAV_TIMEOUT,
                    wait_until=self.wait_until,
                    referer=referer,
                )
                await asyncio.sleep(random.uniform(self.post_nav_settle_min/1000, self.post_nav_settle_max/1000))
                await self._post_load_idle_pause()
                return
            except Exception:
                await backoff.wait()
                if backoff.attempts > 5:
                    raise

    async def _post_load_idle_pause(self):
        pause_ms = random.randint(
            min(self.micro_pause_min_ms, self.micro_pause_max_ms),
            max(self.micro_pause_min_ms, self.micro_pause_max_ms),
        )
        await asyncio.sleep(pause_ms / 1000.0)
        if random.random() < max(0.0, min(1.0, self.idle_after_page_prob)):
            idle_ms = random.randint(
                min(self.idle_after_page_min_ms, self.idle_after_page_max_ms),
                max(self.idle_after_page_min_ms, self.idle_after_page_max_ms),
            )
            debug_print(self.debug, f"[S{self.id}] idle after load for {idle_ms}ms")
            await asyncio.sleep(idle_ms / 1000.0)

    async def _maybe_scroll_page(self,
                                prob: Optional[float] = None,
                                depth_min: Optional[float] = None,
                                depth_max: Optional[float] = None,
                                steps_min: Optional[int] = None,
                                steps_max: Optional[int] = None):
        probability = self.scroll_prob if prob is None else prob
        depth_min = self.scroll_depth_min if depth_min is None else depth_min
        depth_max = self.scroll_depth_max if depth_max is None else depth_max
        steps_min = self.scroll_steps_min if steps_min is None else steps_min
        steps_max = self.scroll_steps_max if steps_max is None else steps_max

        if random.random() > max(0.0, min(1.0, probability)):
            debug_print(self.debug, f"[S{self.id}] no scroll (randomized)")
            return
        try:
            await self.page.wait_for_selector("body", timeout=SEL_TIMEOUT)
        except Exception:
            return
        try:
            height = await self.page.evaluate("""
                () => {
                  const d=document.documentElement,b=document.body;
                  const vals=[d.scrollHeight,b.scrollHeight,d.offsetHeight,b.offsetHeight,d.clientHeight,b.clientHeight].filter(v=>typeof v==='number');
                  const h=Math.max(...vals,0); return (h && isFinite(h))?h:2000;
                }
            """)
        except Exception:
            height = 2000
        depth_frac = max(0.0, min(1.0, random.uniform(depth_min, depth_max)))
        target = max(400, height * depth_frac)
        steps = max(1, min(10, random.randint(steps_min, steps_max)))
        for _ in range(steps):
            await self.page.mouse.wheel(0, target/steps)
            await think(self.think_cfg["scroll_min_ms"], self.think_cfg["scroll_max_ms"])

    async def run(self):
        self.page = None
        self.context = None
        backoff = ExponentialBackoff(base=0.4, factor=1.7, max_wait=3.0)
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            try:
                await self._new_context()
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_exc = exc
                debug_print(self.debug, f"[S{self.id}] new_context failed (attempt {attempt + 1}): {exc}")
                await backoff.wait()
        else:
            if last_exc is not None:
                raise last_exc
            raise RuntimeError("browser context creation failed")
        try:
            if self.persona is not None and self.flag_bounce:
                await self._bounce_session()
                return
            flow = self._pick_flow()
            if not flow:
                return
            await self._run_scripted(flow)
        finally:
            debug_print(self.debug, f"[S{self.id}] summary: atc={self.did_add_to_cart} checkout={self.did_start_checkout} completed={self.did_complete_checkout}")
            if self.context:
                with contextlib.suppress(Exception):
                    await self.context.close()
            self.context = None
            self.page = None

    async def _run_scripted(self, flow: dict):
        steps = flow.get("steps", [])
        await self._landing()
        await self._topnav_click_all_with_hotspots()
        for step in steps:
            if self.stop_requested:
                break
            await self._execute_step(step)
            if self.stop_requested:
                break
            await self._apply_step_jitter()
            if self.stop_requested:
                break
            await think(self.think_cfg["page_min_ms"], self.think_cfg["page_max_ms"])
        if (not self.stop_requested) and random.random() < self.coverage_prob:
            await self._coverage_click_pass()

    async def _bounce_session(self):
        """Persona bounce: land, glance (light/no scroll), dwell 3-10s, leave."""
        landing = attribution.landing_url(self.origin, self.persona)
        referer_hdr = attribution.referer_header(self.persona)
        debug_print(self.debug, f"[S{self.id}] persona '{(self.persona or {}).get('name')}' BOUNCE landing: {landing} | referer={referer_hdr or 'none'}")
        await self._guarded_goto(landing, referer=referer_hdr)
        await self._maybe_scroll_page(prob=0.35, depth_min=0.05, depth_max=0.25, steps_min=1, steps_max=2)
        await asyncio.sleep(random.uniform(3.0, 10.0))

    async def _landing(self):
        # Persona attribution takes precedence over legacy REFERRER_* env logic.
        if self.persona is not None:
            landing = attribution.landing_url(self.origin, self.persona)
            referer_hdr = attribution.referer_header(self.persona)
            debug_print(self.debug, f"[S{self.id}] persona '{self.persona.get('name')}' landing: {landing} | referer={referer_hdr or 'none'}")
            await self._guarded_goto(landing, referer=referer_hdr)
            try:
                ref = await self.page.evaluate("document.referrer")
                debug_print(self.debug, f"[S{self.id}] document.referrer='{ref}'")
            except Exception:
                pass
            await self._maybe_scroll_page()
            return

        landing = self.origin + "/"
        referer_hdr: Optional[str] = None

        # Header source: REFERRER_HEADER_URLS if set, weights reuse REFERRER_WEIGHTS
        if self.ref_hdr_urls:
            chosen = _weighted_choice(self.ref_hdr_urls, self.ref_hdr_weights)
            if chosen:
                chosen = chosen.strip()
                if chosen.lower() != "direct":
                    if chosen.startswith("http://") or chosen.startswith("https://"):
                        referer_hdr = chosen
                    else:
                        slug = _slug_from_source(chosen)
                        referer_hdr = self._default_referrer_url_from_slug(slug)
        else:
            # Fallback header from legacy referrer
            if self.referrer_url and self.referrer_url != "direct":
                if self.referrer_url.startswith("http://") or self.referrer_url.startswith("https://"):
                    referer_hdr = self.referrer_url
                else:
                    referer_hdr = self._default_referrer_url_from_slug(_slug_from_source(self.referrer_url))

        # UTM from legacy vars (keep old behavior)
        if self.referrer_url and self.referrer_url != "direct":
            utm_source = _slug_from_source(self.referrer_url)
            utm_medium = self.utm_mediums.get(utm_source, self.utm_medium_default)
            utm_campaign = self.utm_campaign_default
            q = {"utm_source": utm_source, "utm_medium": utm_medium, "utm_campaign": utm_campaign}
            sep = "?" if "?" not in landing else "&"
            landing = landing + sep + urlencode(q)
            if referer_hdr:
                debug_print(self.debug, f"[S{self.id}] landing with REFERER: {referer_hdr} | {landing}")
            else:
                debug_print(self.debug, f"[S{self.id}] landing (utm only): {landing}")
        else:
            debug_print(self.debug, f"[S{self.id}] landing direct")

        await self._guarded_goto(landing, referer=referer_hdr)

        try:
            ref = await self.page.evaluate("document.referrer")
            debug_print(self.debug, f"[S{self.id}] document.referrer='{ref}'")
        except Exception:
            pass

        await self._maybe_scroll_page()

    def _default_referrer_url_from_slug(self, slug: str) -> str:
        default_map = {
            "google": "https://www.google.com/",
            "bing": "https://www.bing.com/",
            "yahoo": "https://search.yahoo.com/",
            "duckduckgo": "https://duckduckgo.com/",
            "facebook": "https://www.facebook.com/",
            "instagram": "https://www.instagram.com/",
            "tiktok": "https://www.tiktok.com/",
            "linkedin": "https://www.linkedin.com/",
            "reddit": "https://www.reddit.com/",
        }
        return default_map.get(slug, f"https://www.{slug}.com/")

    async def _apply_step_jitter(self):
        pause_ms = random.randint(
            min(self.step_jitter_pause_min, self.step_jitter_pause_max),
            max(self.step_jitter_pause_min, self.step_jitter_pause_max),
        )
        await asyncio.sleep(pause_ms / 1000.0)
        await self._maybe_scroll_page(
            prob=self.step_jitter_scroll_prob,
            depth_min=self.step_jitter_scroll_depth_min,
            depth_max=self.step_jitter_scroll_depth_max,
            steps_min=self.step_jitter_scroll_steps_min,
            steps_max=self.step_jitter_scroll_steps_max,
        )

    def _should_run_step(self, step: dict) -> bool:
        prob = _prob_to_fraction(step.get("probability", step.get("prob")))
        if prob is None:
            return True
        if random.random() <= prob:
            return True
        debug_print(self.debug, f"[S{self.id}] skipping step (probability {prob})")
        return False

    async def _execute_step(self, step: dict):
        if not isinstance(step, dict):
            return
        if not self._should_run_step(step):
            return

        if "choose_one" in step or "choices" in step:
            await self._execute_choose_one(step)
            return
        if "repeat" in step:
            await self._execute_repeat(step)
            return

        await self._execute_action(step)

    async def _execute_action(self, step: dict):
        kind = step.get("action")
        if kind == "open_random_category":
            await self._open_random_category(step)
        elif kind == "category_explore":
            await self._category_explore(step)
        elif kind == "category_hotspot_click":
            await self._category_hotspot_click(step)
        elif kind == "open_random_pdp":
            await self._open_random_pdp(count=int(step.get("count", 1)))
        elif kind == "home_explore":
            await self._home_explore()
        elif kind == "sort_or_filter":
            await self._sort_or_filter()
        elif kind == "add_to_cart":
            await self._add_to_cart()
        elif kind == "pdp_explore":
            await self._pdp_explore(step)
        elif kind == "pdp_decision":
            await self._pdp_decision(step)
        elif kind == "view_cart":
            await self._view_cart()
        elif kind == "cart_edit":
            await self._cart_edit(step)
        elif kind == "start_checkout":
            await self._start_checkout()
        elif kind == "checkout_start":
            await self._checkout_start()
        elif kind == "content_browse":
            await self._content_browse(step)
        elif kind == "content_page":
            await self._content_page(step.get("slug",""))
        elif kind == "search":
            await self._search(step)
        elif kind == "search_result_explore":
            await self._search_result_explore()
        elif kind == "exit_session":
            debug_print(self.debug, f"[S{self.id}] exit_session requested")
            self.stop_requested = True

    async def _execute_choose_one(self, step: dict):
        choices = step.get("choose_one") or step.get("choices") or []
        if not isinstance(choices, list) or not choices:
            return
        choice = None
        if all(isinstance(c, dict) for c in choices):
            choice = choose_weighted(choices, key="weight")
        if choice is None:
            choice = random.choice(choices)
        if isinstance(choice, dict) and choice.get("steps"):
            for sub in choice.get("steps", []):
                if self.stop_requested:
                    break
                await self._execute_step(sub)
        elif isinstance(choice, list):
            for sub in choice:
                if self.stop_requested:
                    break
                await self._execute_step(sub)
        elif isinstance(choice, dict):
            await self._execute_step(choice)

    async def _execute_repeat(self, step: dict):
        repeat_spec = step.get("repeat")
        steps = step.get("steps", [])
        count = 0
        if isinstance(repeat_spec, int):
            count = repeat_spec
        elif isinstance(repeat_spec, dict):
            count = int(repeat_spec.get("times", repeat_spec.get("count", 1)))
            steps = repeat_spec.get("steps", steps)
        count = max(1, min(count or 1, 10))
        for _ in range(count):
            if self.stop_requested:
                break
            for sub in steps:
                if self.stop_requested:
                    break
                await self._execute_step(sub)

    def _extract_category_spec(self, step: Optional[dict]) -> Optional[dict]:
        if not isinstance(step, dict):
            return None
        if "category" in step:
            val = step.get("category")
            if isinstance(val, dict):
                name = val.get("name") or val.get("label")
                return {"name": name, "url": val.get("url")}
            if isinstance(val, str) and val.strip():
                return {"name": val}
        if isinstance(step.get("category_name"), str) and step.get("category_name", "").strip():
            return {"name": step.get("category_name")}
        cats = step.get("categories")
        if isinstance(cats, list) and cats:
            choice = None
            if all(isinstance(c, dict) for c in cats):
                choice = choose_weighted(cats, key="weight") or cats[0]
            else:
                names = [c for c in cats if isinstance(c, str) and c.strip()]
                if names:
                    choice = random.choice(names)
            if isinstance(choice, dict):
                name = choice.get("name") or choice.get("label")
                return {"name": name, "url": choice.get("url")}
            if isinstance(choice, str):
                return {"name": choice}
        return None

    def _match_nav_link(self, links: List[Tuple[str, any]], target_norm: str):
        for label_norm, el in links:
            if label_norm == target_norm:
                return el
        for label_norm, el in links:
            if target_norm in label_norm or label_norm in target_norm:
                return el
        return None

    def _choose_weighted_nav_link(self, links: List[Tuple[str, any]]):
        weighted = []
        for label_norm, el in links:
            weight = float(self.nav_weights.get(label_norm, 1.0)) if self.nav_weights else 1.0
            weighted.append({"label": label_norm, "el": el, "weight": weight})
        choice = choose_weighted(weighted, key="weight") if weighted else None
        return choice.get("el") if isinstance(choice, dict) else None

    async def _query_top_nav_links(self) -> List[Tuple[str, any]]:
        selectors = [
            "header nav a",
            '[role="navigation"] a',
            ".navPages-container a",
            ".navPages a",
            ".header-nav a",
            "nav a",
        ]
        seen: Dict[str, any] = {}
        for sel in selectors:
            try:
                loc = self.page.locator(sel)
                count = await loc.count()
                for i in range(min(count, 150)):
                    el = loc.nth(i)
                    try:
                        text = (await el.inner_text(timeout=800)).strip()
                        if not text:
                            continue
                        key = _normalize_label(text)
                        href = await el.get_attribute("href", timeout=300) or ""
                        if not href:
                            continue
                        url = urljoin(self.origin + "/", href)
                        if not same_origin(url, self.allowlist):
                            continue
                        if key and key not in seen:
                            seen[key] = el
                    except Exception:
                        continue
            except Exception:
                continue
        out = []
        for key, el in seen.items():
            try:
                href = await el.get_attribute("href", timeout=200) or ""
                if href.rstrip("/").endswith(self.origin.rstrip("/")):
                    continue
            except Exception:
                pass
            out.append((key, el))
        return out

    async def _topnav_click_all_with_hotspots(self):
        links = await self._query_top_nav_links()
        if not links:
            debug_print(self.debug, f"[S{self.id}] top-nav: none found")
            return
        random.shuffle(links)
        for label_norm, el in links:
            if self.stop_requested:
                break
            await self._click_nav_el(label_norm, el)
        for hot in self.nav_hotspot_names:
            if self.stop_requested:
                break
            label = _normalize_label(hot)
            prob = self.nav_hotspot_extra_prob.get(label, 0.0)
            if prob > 0 and random.random() < prob:
                target = next(((ln, e) for (ln, e) in links if ln == label), None)
                if target:
                    await self._click_nav_el(target[0], target[1])

    async def _click_nav_el(self, label_norm: str, el):
        try:
            box = await el.bounding_box()
            if box:
                await self.page.mouse.move(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
            await el.click(timeout=SEL_TIMEOUT)
            debug_print(self.debug, f"[S{self.id}] nav click → {label_norm}")
        except Exception:
            try:
                href = await el.get_attribute("href", timeout=500)
                if href:
                    url = urljoin(self.origin + "/", href)
                    debug_print(self.debug, f"[S{self.id}] nav goto (fallback) → {label_norm} ({url})")
                    await self._guarded_goto(url)
            except Exception:
                return
        await self._maybe_scroll_page()
        await asyncio.sleep(random.uniform(self.nav_pause_min/1000, self.nav_pause_max/1000))
        if not self.stop_requested:
            await self._category_micro_behaviors()

    async def _category_micro_behaviors(self):
        await self._sort_or_filter()
        await self._open_random_pdp(count=random.randint(1, 2))

    def _home_scroll_depth(self) -> float:
        buckets = [
            {"depth": 0.20, "weight": 18},
            {"depth": 0.50, "weight": 40},
            {"depth": 0.80, "weight": 28},
            {"depth": 1.00, "weight": 14},
        ]
        depth = weighted_value(
            buckets,
            value_key="depth",
            weight_key="weight",
            default=0.5,
            jitter=(-0.06, 0.08),
            clamp_min=0.05,
            clamp_max=1.1,
        )
        try:
            return float(depth)
        except Exception:
            return 0.5

    async def _scroll_to_depth(self, depth: float):
        try:
            await self.page.wait_for_selector("body", timeout=SEL_TIMEOUT)
        except Exception:
            return
        try:
            metrics = await self.page.evaluate("""
                () => {
                  const d=document.documentElement,b=document.body;
                  const vals=[d.scrollHeight,b.scrollHeight,d.offsetHeight,b.offsetHeight,d.clientHeight,b.clientHeight].filter(v=>typeof v==='number');
                  const height=Math.max(...vals,0)||2000;
                  return {height, y: window.scrollY || 0};
                }
            """)
        except Exception:
            metrics = {"height": 2000, "y": 0}
        target = max(400, metrics.get("height", 2000) * depth)
        current = float(metrics.get("y", 0) or 0.0)
        delta = target - current
        steps = max(1, min(8, random.randint(2, 5)))
        distance = delta / steps if steps else delta
        for _ in range(steps):
            await self.page.mouse.wheel(0, distance)
            await think(self.think_cfg["scroll_min_ms"], self.think_cfg["scroll_max_ms"])

    async def _maybe_click_home_cta(self) -> bool:
        selectors = [
            ".hero a, .hero button, .hero-cta a, .hero-cta button, .banner a, .banner button, .jumbotron a, .jumbotron button",
            ".featured-products a, .featured-collection a, .featured a, .featured-collections a",
        ]
        if random.random() > 0.55:
            return False
        for sel in selectors:
            loc = self.page.locator(sel)
            try:
                count = await loc.count()
            except Exception:
                count = 0
            if count <= 0:
                continue
            idx = random.randint(0, min(count - 1, 3))
            try:
                await loc.nth(idx).click(timeout=SEL_TIMEOUT)
                await self._maybe_scroll_page(
                    prob=0.65,
                    depth_min=0.12,
                    depth_max=0.35,
                    steps_min=1,
                    steps_max=3,
                )
                return True
            except Exception:
                continue
        return False

    async def _home_explore(self):
        await self._guarded_goto(self.origin + "/")
        segments = random.randint(1, 3)
        last_depth = 0.0
        for i in range(segments):
            target_depth = self._home_scroll_depth()
            if i > 0 and random.random() < 0.35:
                target_depth = max(0.05, last_depth - random.uniform(0.08, 0.3))
            await self._scroll_to_depth(target_depth)
            last_depth = target_depth
        if random.random() < 0.6:
            await self._maybe_click_home_cta()

    def _pick_search_term(self, step: Optional[dict]) -> str:
        if isinstance(step, dict):
            terms_spec = step.get("terms")
            if isinstance(terms_spec, list) and terms_spec:
                if all(isinstance(t, dict) for t in terms_spec):
                    weighted = []
                    for t in terms_spec:
                        term_val = t.get("term") or t.get("value") or t.get("text")
                        if term_val:
                            weighted.append({"term": str(term_val), "weight": float(t.get("weight", 1.0) or 0.0)})
                    choice = choose_weighted(weighted, key="weight") if weighted else None
                    if isinstance(choice, dict) and choice.get("term"):
                        return str(choice["term"])
                else:
                    str_terms = [str(t) for t in terms_spec if str(t).strip()]
                    if str_terms:
                        return random.choice(str_terms)
        return random.choice(self.search_terms)

    async def _find_search_input(self):
        selectors = [
            "input[type='search']",
            "input[name*='search' i]",
            "input[placeholder*='search' i]",
            "input[aria-label*='search' i]",
            "form[role='search'] input",
            "form[action*='search' i] input",
        ]
        for sel in selectors:
            loc = self.page.locator(sel)
            try:
                count = await loc.count()
            except Exception:
                count = 0
            if count <= 0:
                continue
            for i in range(min(count, 3)):
                candidate = loc.nth(i)
                try:
                    if await candidate.is_visible(timeout=SEL_TIMEOUT):
                        return candidate
                except Exception:
                    continue
        toggles = [
            "button[aria-label*='search' i]",
            "button:has-text('Search')",
            "a[aria-label*='search' i]",
            "a[href*='search']",
        ]
        for sel in toggles:
            toggle = self.page.locator(sel).first
            try:
                if await toggle.is_visible(timeout=SEL_TIMEOUT):
                    await toggle.click(timeout=SEL_TIMEOUT)
                    break
            except Exception:
                continue
        for sel in selectors:
            loc = self.page.locator(sel)
            try:
                count = await loc.count()
            except Exception:
                count = 0
            if count <= 0:
                continue
            for i in range(min(count, 3)):
                candidate = loc.nth(i)
                try:
                    if await candidate.is_visible(timeout=SEL_TIMEOUT):
                        return candidate
                except Exception:
                    continue
        return None

    async def _submit_search_form(self, input_el):
        if input_el is None:
            return
        try:
            await input_el.press("Enter", timeout=SEL_TIMEOUT)
            return
        except Exception:
            pass
        try:
            form = input_el.locator("xpath=ancestor::form[1]")
            buttons = form.locator("button[type='submit'],input[type='submit']")
            if await buttons.count() > 0:
                await buttons.first.click(timeout=SEL_TIMEOUT)
                return
        except Exception:
            pass
        try:
            buttons = self.page.locator("button[aria-label*='search' i],button[type='submit'][name*='search' i]")
            if await buttons.count() > 0:
                await buttons.first.click(timeout=SEL_TIMEOUT)
        except Exception:
            return

    async def _search(self, step: Optional[dict] = None):
        term = self._pick_search_term(step)
        input_el = await self._find_search_input()
        if input_el is None:
            debug_print(self.debug, f"[S{self.id}] search input not found")
            return
        try:
            await input_el.click(timeout=SEL_TIMEOUT)
            await input_el.fill(term, timeout=SEL_TIMEOUT)
            debug_print(self.debug, f"[S{self.id}] search → '{term}'")
        except Exception as exc:
            debug_print(self.debug, f"[S{self.id}] search fill failed: {exc}")
            return
        await self._submit_search_form(input_el)
        with contextlib.suppress(Exception):
            await self.page.wait_for_load_state(self.wait_until, timeout=ALLOW_NAV_TIMEOUT)
            await asyncio.sleep(random.uniform(self.post_nav_settle_min/1000, self.post_nav_settle_max/1000))
        await self._maybe_scroll_page(prob=0.85, depth_min=0.12, depth_max=0.32, steps_min=1, steps_max=3)

    async def _search_result_explore(self):
        await self._maybe_scroll_page(prob=0.95, depth_min=0.18, depth_max=0.45, steps_min=1, steps_max=3)
        branch = random.random()
        if branch < 0.6:
            await self._click_category_tiles(random.randint(1, 2))
        else:
            await self._apply_category_filters(random.randint(1, 2))
            await self._maybe_scroll_page(prob=0.75, depth_min=0.1, depth_max=0.35, steps_min=1, steps_max=3)
            if random.random() < 0.35:
                await self._randomize_category_sort()

    async def _open_random_category(self, step: Optional[dict] = None):
        spec = self._extract_category_spec(step)
        target_raw = spec.get("name") if isinstance(spec, dict) else ""
        target_name = _normalize_label(target_raw or "") if isinstance(spec, dict) and spec.get("name") else None
        target_url = (spec.get("url") or "").strip() if isinstance(spec, dict) else ""
        if target_url:
            dest = urljoin(self.origin + "/", target_url)
            await self._guarded_goto(dest)
            await self._maybe_scroll_page()
            return

        links = await self._query_top_nav_links()
        chosen_el = None
        if target_name and links:
            chosen_el = self._match_nav_link(links, target_name)
        if chosen_el is None and links:
            chosen_el = self._choose_weighted_nav_link(links)
        if chosen_el:
            await self._click_category_element(chosen_el)
            return

        if target_raw:
            try:
                target_candidates = self.page.get_by_role("link", name=re.compile(re.escape(target_raw), re.I))
                tcount = await target_candidates.count()
                if tcount > 0:
                    idx = biased_index(tcount, focus=4)
                    await target_candidates.nth(idx).click(timeout=SEL_TIMEOUT)
                    await self._maybe_scroll_page()
                    return
            except Exception:
                pass

        nav_candidates = self.page.get_by_role("link", name=re.compile("(Shop|All|Kitchen|Bath|Accessories|Sale|New)", re.I))
        count = await nav_candidates.count()
        if count > 0 and random.random() < 0.7:
            idx = random.randint(0, min(count-1, 5))
            await nav_candidates.nth(idx).click(timeout=SEL_TIMEOUT)
        else:
            await self._guarded_goto(f"{self.origin}/categories/")
        await self._maybe_scroll_page()

    async def _click_category_element(self, el):
        try:
            await el.click(timeout=SEL_TIMEOUT)
        except Exception:
            try:
                href = await el.get_attribute("href", timeout=500) or ""
                if href:
                    await self._guarded_goto(urljoin(self.origin + "/", href))
            except Exception:
                return
        await self._maybe_scroll_page()

    async def _open_product_link(self) -> bool:
        """Open a random product page robustly.

        Try a human-like click on a VISIBLE product link first (themes such
        as Dawn render a hidden 0x0 duplicate of every card link, which makes
        index-based clicks hang on visibility checks). If the click fails for
        any reason, fall back to harvesting hrefs and navigating directly —
        the clmod3-proven path. Never raises; returns success.
        """
        base = "a[href*='/products/']"
        click_sel = "a.card-figure:visible, a.card-title:visible, a.product-title:visible, a[href*='/products/']:visible"
        try:
            vis = self.page.locator(click_sel)
            n = await vis.count()
            if n > 0:
                i = random.randint(0, min(n - 1, 15))
                await vis.nth(i).click(timeout=6000)
                await self.page.wait_for_load_state("load", timeout=ALLOW_NAV_TIMEOUT)
                if "/products/" in self.page.url:
                    return True
        except Exception:
            debug_print(self.debug, f"[S{self.id}] product tile click failed; goto fallback")
        # Fallback: harvest hrefs and navigate directly.
        try:
            hrefs = await self.page.eval_on_selector_all(
                base,
                "els => [...new Set(els.map(a => a.getAttribute('href')).filter(h => h && h.includes('/products/')))]",
            )
        except Exception:
            hrefs = []
        if not hrefs:
            return False
        href = random.choice(hrefs)
        url = href if href.startswith("http") else self.origin.rstrip("/") + href
        try:
            await self._guarded_goto(url)
            debug_print(self.debug, f"[S{self.id}] pdp via goto → {url}")
            return "/products/" in self.page.url
        except Exception:
            return False

    async def _open_random_pdp(self, count: int = 1):
        count = max(1, min(count, 3))
        for _ in range(count):
            if self.stop_requested:
                break
            if await self._open_product_link():
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

    async def _apply_category_filters(self, count: int):
        count = max(0, min(count, 4))
        if count <= 0:
            return
        selectors = [
            ".facetedSearch-option--checkbox input",
            "input[type='checkbox'][name*='filter']",
            ".facetedSearch input[type='checkbox']",
            "input[type='checkbox']",
        ]
        for sel in selectors:
            loc = self.page.locator(sel)
            try:
                total = await loc.count()
            except Exception:
                total = 0
            if total <= 0:
                continue
            picks = set()
            for _ in range(count):
                if len(picks) >= total:
                    break
                attempt = 0
                idx = None
                while attempt < 4:
                    candidate = biased_index(total, focus=10)
                    if candidate not in picks:
                        idx = candidate
                        break
                    attempt += 1
                if idx is None:
                    continue
                picks.add(idx)
                try:
                    await loc.nth(idx).check(timeout=SEL_TIMEOUT)
                    await asyncio.sleep(random.uniform(0.2, 0.8))
                except Exception:
                    continue
            break
        await self._maybe_scroll_page()

    async def _randomize_category_sort(self):
        selectors = [
            "select[name='sort']",
            "select#sort",
            "select[name*='Sort']",
            "select[data-sort]",
        ]
        for sel in selectors:
            dropdown = self.page.locator(sel).first
            try:
                options = await dropdown.locator("option").count()
            except Exception:
                options = 0
            if options <= 1:
                continue
            try:
                idx = biased_index(options, focus=4)
                await dropdown.select_option(index=idx, timeout=SEL_TIMEOUT)
                await self._maybe_scroll_page()
                return
            except Exception:
                continue

    async def _maybe_paginate_category(self) -> bool:
        if random.random() < 0.4:
            return False
        selectors = [
            "a[rel='next']",
            "button[aria-label*='next' i]",
            ".pagination a[aria-label*='next' i]",
            ".pagination-item--next a",
        ]
        for sel in selectors:
            loc = self.page.locator(sel)
            try:
                count = await loc.count()
            except Exception:
                count = 0
            if count <= 0:
                continue
            idx = biased_index(count, focus=3)
            try:
                await loc.nth(idx).click(timeout=SEL_TIMEOUT)
                await self.page.wait_for_load_state(self.wait_until, timeout=ALLOW_NAV_TIMEOUT)
                await asyncio.sleep(random.uniform(self.post_nav_settle_min/1000, self.post_nav_settle_max/1000))
                await self._post_load_idle_pause()
                await self._maybe_scroll_page()
                return True
            except Exception:
                continue
        numeric = self.page.locator(".pagination a, nav[aria-label*='pagination' i] a")
        try:
            count = await numeric.count()
        except Exception:
            count = 0
        if count <= 0:
            return False
        idx = biased_index(min(count, 6), focus=3)
        try:
            await numeric.nth(idx).click(timeout=SEL_TIMEOUT)
            await self.page.wait_for_load_state(self.wait_until, timeout=ALLOW_NAV_TIMEOUT)
            await asyncio.sleep(random.uniform(self.post_nav_settle_min/1000, self.post_nav_settle_max/1000))
            await self._post_load_idle_pause()
            await self._maybe_scroll_page()
            return True
        except Exception:
            return False

    async def _hover_category_tiles(self, count: int):
        count = max(1, min(count, 8))
        selector = "a.card-figure, a.card-title, a.product-title, a[href*='/products/']"
        grid = self.page.locator(selector)
        try:
            total = await grid.count()
        except Exception:
            total = 0
        if total <= 0:
            return
        seen: set = set()
        dwell_min = min(self.tile_hover_dwell_min_ms, self.tile_hover_dwell_max_ms)
        dwell_max = max(self.tile_hover_dwell_min_ms, self.tile_hover_dwell_max_ms)
        for _ in range(count):
            idx = biased_index(min(total, 60), focus=8)
            if idx in seen:
                continue
            seen.add(idx)
            try:
                el = grid.nth(idx)
                await el.hover(timeout=SEL_TIMEOUT)
                await asyncio.sleep(random.uniform(dwell_min/1000, dwell_max/1000))
            except Exception:
                continue

    async def _click_category_tiles(self, count: int):
        count = max(1, min(count, 3))
        visited: set = set()
        selector = "a.card-figure:visible, a.card-title:visible, a.product-title:visible, a[href*='/products/']:visible"
        for i in range(count):
            grid = self.page.locator(selector)
            try:
                total = await grid.count()
            except Exception:
                total = 0
            if total <= 0:
                break
            choice = None
            attempts = 0
            while attempts < 5:
                idx = biased_index(min(total, 40))
                if idx not in visited:
                    choice = idx
                    break
                attempts += 1
            if choice is None:
                choice = 0
            visited.add(choice)
            try:
                await grid.nth(choice).click(timeout=6000)
                await self._maybe_scroll_page(prob=0.85, depth_min=0.12, depth_max=0.35, steps_min=1, steps_max=3)
            except Exception:
                # Click failed (hidden/overlaid tile): goto-fallback keeps the
                # session alive instead of burning the selector timeout budget.
                if not await self._open_product_link():
                    continue
                await self._maybe_scroll_page(prob=0.85, depth_min=0.12, depth_max=0.35, steps_min=1, steps_max=3)
            if i < count - 1:
                with contextlib.suppress(Exception):
                    await self.page.go_back(timeout=ALLOW_NAV_TIMEOUT, wait_until=self.wait_until)
                    await asyncio.sleep(random.uniform(self.post_nav_settle_min/1000, self.post_nav_settle_max/1000))
                    await self._post_load_idle_pause()
                    await self._maybe_scroll_page(prob=0.65, depth_min=0.08, depth_max=0.22, steps_min=1, steps_max=2)

    async def _category_explore(self, step: dict):
        await self._open_random_category(step)
        if self.stop_requested:
            return
        filters_to_apply = random.randint(0, 2)
        if filters_to_apply > 0:
            await self._apply_category_filters(filters_to_apply)
        await self._randomize_category_sort()
        await self._maybe_paginate_category()
        hover_count = random.randint(
            min(self.tile_hover_count_min, self.tile_hover_count_max),
            max(self.tile_hover_count_min, self.tile_hover_count_max),
        )
        if random.random() < max(0.0, min(1.0, self.tile_hover_prob)):
            await self._hover_category_tiles(hover_count)
        await self._click_category_tiles(random.randint(1, 3))

    async def _category_hotspot_click(self, step: dict):
        if any(k in (step or {}) for k in ("category", "categories", "category_name")):
            await self._open_random_category(step)
        selectors = [
            ".category-hero a, .category-hero button, .collection-hero a, .collection-hero button",
            ".category-banner a, .category-banner button, .collection-banner a, .collection-banner button",
            ".category-promo a, .category-promo button, .promo-banner a, .promo-tile a, .promo a",
        ]
        for sel in selectors:
            loc = self.page.locator(sel)
            try:
                count = await loc.count()
            except Exception:
                count = 0
            if count <= 0:
                continue
            idx = biased_index(min(count, 6), focus=4)
            try:
                await loc.nth(idx).click(timeout=SEL_TIMEOUT)
                await self._maybe_scroll_page(prob=0.75, depth_min=0.18, depth_max=0.4, steps_min=1, steps_max=3)
                return
            except Exception:
                continue
        await self._maybe_scroll_page(prob=0.4, depth_min=0.1, depth_max=0.3, steps_min=1, steps_max=2)

    async def _sort_or_filter(self):
        sort_prob = float(os.getenv("CATEGORY_SORT_PROB","0.30"))
        filter_prob = float(os.getenv("CATEGORY_FILTER_PROB","0.15"))
        if random.random() < sort_prob:
            await self._randomize_category_sort()
        if random.random() < filter_prob:
            await self._apply_category_filters(1)

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

    async def _pdp_view_media(self):
        selectors = [
            ".productView-thumbnail img",
            ".productView-thumbnails img",
            ".productView-image--thumb img",
            ".productView img",
            "[data-image-gallery] img",
        ]
        for sel in selectors:
            loc = self.page.locator(sel)
            try:
                total = await loc.count()
            except Exception:
                total = 0
            if total <= 0:
                continue
            taps = random.randint(1, min(3, total))
            visited = set()
            for _ in range(taps):
                idx = biased_index(min(total, 12), focus=4)
                if idx in visited:
                    continue
                visited.add(idx)
                with contextlib.suppress(Exception):
                    await loc.nth(idx).click(timeout=SEL_TIMEOUT)
                    await asyncio.sleep(random.uniform(0.15, 0.5))
            break
        try:
            zoom_btns = self.page.get_by_role("button", name=re.compile("zoom", re.I))
            zcount = await zoom_btns.count()
        except Exception:
            zcount = 0
        if zcount > 0 and random.random() < 0.55:
            idx = biased_index(min(zcount, 4), focus=2)
            with contextlib.suppress(Exception):
                await zoom_btns.nth(idx).click(timeout=SEL_TIMEOUT)
                await asyncio.sleep(random.uniform(0.3, 0.9))
                await self.page.keyboard.press("Escape")

    async def _pdp_select_variant(self):
        dropdowns = [
            "form select[name*='option']",
            "form select[id*='option']",
            "form select[name*='attribute']",
            "form select",
        ]
        for sel in dropdowns:
            loc = self.page.locator(sel)
            try:
                total = await loc.count()
            except Exception:
                total = 0
            if total <= 0:
                continue
            for i in range(total):
                dropdown = loc.nth(i)
                try:
                    opts = await dropdown.locator("option").count()
                except Exception:
                    opts = 0
                if opts <= 1:
                    continue
                try:
                    idx = random.randint(1, opts - 1)
                    await dropdown.select_option(index=idx, timeout=SEL_TIMEOUT)
                    await asyncio.sleep(random.uniform(0.2, 0.5))
                except Exception:
                    continue
                return
        radio_selectors = [
            "input[type='radio'][name*='option']",
            ".form-radio input[type='radio']",
            "input[type='radio'][name*='attribute']",
        ]
        for sel in radio_selectors:
            loc = self.page.locator(sel)
            try:
                total = await loc.count()
            except Exception:
                total = 0
            if total <= 0:
                continue
            idx = biased_index(min(total, 12), focus=5)
            with contextlib.suppress(Exception):
                await loc.nth(idx).check(timeout=SEL_TIMEOUT)
                await asyncio.sleep(random.uniform(0.2, 0.5))
                return

    async def _pdp_scroll_to_reviews_or_description(self):
        candidates: List[Any] = []
        try:
            links = self.page.get_by_role("link", name=re.compile("(review|rating|description|details|specs)", re.I))
            lcount = await links.count()
            for i in range(min(lcount, 5)):
                candidates.append(links.nth(i))
        except Exception:
            pass
        selectors = [
            "#tab-description, #description, [id*='Description']",
            "#tab-reviews, #reviews, [id*='Review']",
            ".productView-description, .productView-details",
        ]
        for sel in selectors:
            loc = self.page.locator(sel)
            try:
                count = await loc.count()
            except Exception:
                count = 0
            if count > 0:
                candidates.append(loc.first)
        for target in candidates:
            with contextlib.suppress(Exception):
                await target.scroll_into_view_if_needed(timeout=SEL_TIMEOUT)
                await self._maybe_scroll_page(
                    prob=0.95,
                    depth_min=0.18,
                    depth_max=0.45,
                    steps_min=1,
                    steps_max=3,
                )
                return
        await self._scroll_to_depth(random.uniform(0.35, 0.8))

    async def _pdp_click_related_product(self):
        selectors = [
            "section.related-products a",
            "[data-related-products] a",
            "[data-recommended-products] a",
            ".productRelated a",
            ".upsell-products a",
        ]
        for sel in selectors:
            loc = self.page.locator(sel)
            try:
                total = await loc.count()
            except Exception:
                total = 0
            if total <= 0:
                continue
            idx = biased_index(min(total, 10), focus=4)
            el = loc.nth(idx)
            try:
                await el.scroll_into_view_if_needed(timeout=SEL_TIMEOUT)
                await asyncio.sleep(random.uniform(0.1, 0.3))
                await el.click(timeout=SEL_TIMEOUT)
                await self._maybe_scroll_page(
                    prob=0.8,
                    depth_min=0.1,
                    depth_max=0.3,
                    steps_min=1,
                    steps_max=2,
                )
                return
            except Exception:
                continue

    async def _pdp_explore(self, step: Optional[dict] = None):
        await self._pdp_view_media()
        await self._pdp_select_variant()
        await self._pdp_scroll_to_reviews_or_description()
        related_prob = float((step or {}).get("related_click_prob", 0.35))
        if random.random() < max(0.0, min(1.0, related_prob)):
            await self._pdp_click_related_product()

    async def _pdp_decision(self, step: Optional[dict] = None):
        step = step or {}
        outcomes = [
            {"outcome": "add_to_cart", "weight": float(step.get("add_to_cart_weight", step.get("add_weight", 0.6)) or 0.0)},
            {"outcome": "bounce", "weight": float(step.get("bounce_weight", 1.0) or 0.0)},
        ]
        choice = choose_weighted(outcomes, key="weight") or outcomes[0]
        outcome = (choice or {}).get("outcome", "bounce")
        if outcome == "add_to_cart":
            await self._add_to_cart()
        else:
            debug_print(self.debug, f"[S{self.id}] pdp_decision → bounce (ending session)")
            self.stop_requested = True

    async def _view_cart(self):
        try:
            link = self.page.get_by_role("link", name=re.compile("cart|view cart", re.I))
            await link.first.click(timeout=SEL_TIMEOUT)
        except Exception:
            await self._guarded_goto(f"{self.origin}/cart.php")
        await self._maybe_scroll_page()

    async def _cart_edit(self, step: Optional[dict] = None):
        remove_prob = float((step or {}).get("remove_prob", 0.18))
        try:
            items = self.page.locator(".cart-item, [data-cart-item], tr.cart-item")
            count = await items.count()
        except Exception:
            count = 0
        if count <= 0:
            return
        target_idx = biased_index(min(count, 6), focus=4)
        row = items.nth(target_idx)
        qty_locators = row.locator("input[name*='qty'], input[name='qty[]'], input[type='number']")
        try:
            qty_count = await qty_locators.count()
        except Exception:
            qty_count = 0
        if qty_count > 0:
            qty_input = qty_locators.first
            try:
                current_val = await qty_input.input_value(timeout=800)
            except Exception:
                current_val = ""
            try:
                new_qty = random.randint(1, 3)
                if str(current_val).isdigit():
                    if int(current_val) == new_qty and new_qty < 3:
                        new_qty += 1
                await qty_input.fill(str(new_qty), timeout=SEL_TIMEOUT)
                with contextlib.suppress(Exception):
                    await qty_input.press("Enter", timeout=SEL_TIMEOUT)
                await asyncio.sleep(random.uniform(0.2, 0.6))
            except Exception:
                pass
        if random.random() < max(0.0, min(1.0, remove_prob)):
            selectors = [
                "button[aria-label*='remove']",
                "button[name='action'][value='delete']",
                "button[name='delete']",
                "button:has-text('Remove')",
                "a:has-text('Remove')",
                "a.cart-remove",
            ]
            for sel in selectors:
                target = row.locator(sel)
                try:
                    if await target.count() > 0:
                        await target.first.click(timeout=SEL_TIMEOUT)
                        break
                except Exception:
                    continue
        await self._maybe_scroll_page(prob=0.4, depth_min=0.08, depth_max=0.2, steps_min=1, steps_max=2)

    async def _persona_checkout(self):
        """Persona-driven checkout: proceed, then complete or abandon.

        Completion requires the persona's checkout_complete roll AND the
        global order rate limiter (may_place_order). Denied/failed rolls
        abandon at a weighted random stage instead.
        """
        if self.did_start_checkout >= self.funnel_max_checkout_starts:
            return
        if not self.flag_should_checkout or self.did_add_to_cart <= 0:
            return
        funnel = (self.persona or {}).get("funnel") or {}
        if not await checkout_engine.proceed_to_checkout(self.page, debug=self.debug):
            return
        self.did_start_checkout += 1
        complete = random.random() < float(funnel.get("checkout_complete", 0.0) or 0.0)
        forced_stage: Optional[str] = None
        if complete:
            allowed = True
            if self.may_place_order is not None:
                allowed = await self.may_place_order()
            if not allowed:
                debug_print(self.debug, f"[S{self.id}] order rate limit active; abandoning at payment instead")
                complete = False
                forced_stage = "payment"
        if complete:
            identity = checkout_engine.random_identity()
            card = {
                "number": os.getenv("CARD_NUMBER", "4111111111111111"),
                "expiry": os.getenv("CARD_EXPIRY", "01/30"),
                "cvv": os.getenv("CARD_CVV", "989"),
            }
            if await checkout_engine.complete_checkout(self.page, identity, card, debug=self.debug):
                self.did_complete_checkout += 1
                debug_print(self.debug, f"[S{self.id}] checkout COMPLETED")
            else:
                debug_print(self.debug, f"[S{self.id}] checkout completion failed")
        else:
            stage = forced_stage
            if stage is None:
                picked = choose_weighted([
                    {"stage": "customer", "weight": 0.2},
                    {"stage": "shipping", "weight": 0.3},
                    {"stage": "payment", "weight": 0.5},
                ], key="weight") or {}
                stage = picked.get("stage", "payment")
            await checkout_engine.abandon_checkout(self.page, stage, debug=self.debug)
        self.stop_requested = True

    async def _start_checkout(self):
        if self.did_start_checkout >= self.funnel_max_checkout_starts:
            return
        if self.persona is not None:
            await self._persona_checkout()
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

    async def _checkout_start(self):
        await self._start_checkout()
        if self.did_start_checkout:
            debug_print(self.debug, f"[S{self.id}] checkout_start requested; ending session after checkout entry")
            self.stop_requested = True

    async def _content_page(self, slug: str):
        slugs = ["/contact-us/","/shipping-returns/","/blog/","/help/"]
        if slug and slug.startswith("/"):
            slugs.insert(0, slug)
        await self._guarded_goto(self.origin + random.choice(slugs))
        await self._maybe_scroll_page()

    async def _content_browse(self, step: Optional[dict] = None):
        pages = step.get("pages") if isinstance(step, dict) else None
        if not isinstance(pages, list) or not pages:
            pages = ["/about-us/", "/contact-us/", "/shipping-returns/", "/blog/", "/help/"]
        unique_pages = [p for p in pages if isinstance(p, str) and p.startswith("/")]
        random.shuffle(unique_pages)
        visit_count = random.randint(1, min(2, max(1, len(unique_pages))))
        for slug in unique_pages[:visit_count]:
            if self.stop_requested:
                break
            await self._content_page(slug)

    async def _footer_explore(self, step: Optional[dict] = None):
        try:
            await self._scroll_to_depth(1.05)
        except Exception:
            with contextlib.suppress(Exception):
                await self.page.mouse.wheel(0, 2000)
        selectors = step.get("selectors") if isinstance(step, dict) else None
        if not isinstance(selectors, list) or not selectors:
            selectors = ["footer a[href]", "footer nav a[href]", "footer li a[href]"]
        for sel in selectors:
            loc = self.page.locator(sel)
            try:
                count = await loc.count()
            except Exception:
                count = 0
            if count <= 0:
                continue
            idx = random.randint(0, min(count - 1, 8))
            try:
                await loc.nth(idx).click(timeout=SEL_TIMEOUT)
                await self._maybe_scroll_page(prob=0.6, depth_min=0.15, depth_max=0.4, steps_min=1, steps_max=3)
                return
            except Exception:
                continue
        await self._maybe_scroll_page(prob=0.4, depth_min=0.05, depth_max=0.15, steps_min=1, steps_max=2)

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
                for b in self.coverage_block:
                    if "href*=" in b:
                        needle = b.split('href*="',1)[1].rstrip('"]')
                        if needle in href:
                            raise Exception("blocked")
                await el.click(timeout=SEL_TIMEOUT)
                clicks += 1
                await self._maybe_scroll_page()
                await asyncio.sleep(random.uniform(0.2, 0.8))
            except Exception:
                continue
