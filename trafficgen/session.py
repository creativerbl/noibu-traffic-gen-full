# trafficgen/session.py — one ab-test shopper session.
#
# 1. Land on the store (referrer header + matching UTM tags).
# 2. Add 1-3 random products from /shop-all/.
# 3. Header CART -> "View Cart" -> /cart.php.
# 4. Sticky checkout banner shown  -> click it, complete checkout with the
#    test card.  Only the primary "Check out" button -> end the session.
#
# Selectors are fixed and were verified on noibudemo.com (Cornerstone theme).
import atexit
import contextlib
import os
import random
import time
from typing import List, Optional
from urllib.parse import urlencode

from trafficgen import checkout as checkout_engine
from trafficgen.devices import context_args
from trafficgen.utils import debug_print, think, weighted_choice

NAV_TIMEOUT_MS = 25_000

# Referer header sent on the landing request for each REFERRER_SOURCES entry.
REFERER_URLS = {
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


def _csv(name: str, default: str = "") -> List[str]:
    return [x.strip() for x in os.getenv(name, default).split(",") if x.strip()]


def _floats(name: str) -> List[float]:
    out = []
    for x in _csv(name):
        try:
            out.append(float(x))
        except ValueError:
            out.append(0.0)
    return out


def _kv(name: str) -> dict:
    out = {}
    for pair in _csv(name):
        if ":" in pair:
            k, v = pair.split(":", 1)
            out[k.strip().lower()] = v.strip()
    return out


# ── A/B outcome tally ────────────────────────────────────────────────────────

class _ABTally:
    """Process-wide outcome counter.

    Prints an [AB SUMMARY] every AB_SUMMARY_EVERY finished sessions (default
    5), or when AB_SUMMARY_MINUTES (default 30) have passed since the last
    one, and a final summary when the process exits.
    """
    OUTCOMES = ("control", "sticky_ordered", "sticky_order_failed",
                "sticky_no_checkout", "no_cart", "no_items")

    def __init__(self):
        self.started = 0
        self.counts = {k: 0 for k in self.OUTCOMES}
        self.by_engine = {}  # engine -> [control, sticky]
        self.t0 = time.time()
        self.last_print = self.t0
        self.every = max(1, int(os.getenv("AB_SUMMARY_EVERY", "5")))
        self.minutes = max(1.0, float(os.getenv("AB_SUMMARY_MINUTES", "30")))
        atexit.register(lambda: self.started and self.print_summary("final"))

    def start(self):
        self.started += 1

    def record(self, outcome: str, engine: str = ""):
        self.counts[outcome] += 1
        if engine and (outcome == "control" or outcome.startswith("sticky")):
            row = self.by_engine.setdefault(engine, [0, 0])
            row[0 if outcome == "control" else 1] += 1
        finished = sum(self.counts.values())
        if finished % self.every == 0 or (time.time() - self.last_print) >= self.minutes * 60:
            self.print_summary()

    def print_summary(self, tag: str = ""):
        c = self.counts
        finished = sum(c.values())
        sticky = c["sticky_ordered"] + c["sticky_order_failed"] + c["sticky_no_checkout"]
        decided = c["control"] + sticky  # sessions that actually saw a variant
        pct = lambda n: f"{(100.0 * n / decided):.0f}%" if decided else "-"
        mins = int((time.time() - self.t0) // 60)
        since = time.strftime("%H:%M", time.localtime(self.t0))
        print("\n".join([
            f"[AB SUMMARY{(' ' + tag) if tag else ''}] {self.started} started, "
            f"{finished} finished since {since} ({mins // 60}h{mins % 60:02d}m)",
            f"  variant split : control {c['control']} ({pct(c['control'])}) | "
            f"sticky {sticky} ({pct(sticky)})",
            f"  sticky detail : ordered {c['sticky_ordered']} | order not confirmed "
            f"{c['sticky_order_failed']} | checkout didn't load {c['sticky_no_checkout']}",
            f"  no variant    : never reached cart {c['no_cart']} | nothing added "
            f"{c['no_items']} | unfinished/timed out {self.started - finished}",
            "  by browser    : " + (" | ".join(
                f"{e} control {v[0]} / sticky {v[1]}" for e, v in sorted(self.by_engine.items()))
                or "-"),
        ]), flush=True)
        self.last_print = time.time()


AB_TALLY = _ABTally()


# ── session ──────────────────────────────────────────────────────────────────

class Session:
    LISTING_PATHS = ["/shop-all/", "/shop-all/?page=2"]
    PRODUCT_LINK_SEL = "article.card .card-title a"
    ADD_TO_CART_SEL = "#form-action-addToCart"
    ADDED_MODAL_SEL = "#previewModal.open"
    ADDED_MODAL_CLOSE_SEL = "#previewModal .modal-close"
    HEADER_CART_SEL = "a[data-cart-preview]"
    VIEW_CART_SEL = "#cart-preview-dropdown .previewCartAction-viewCart a"
    STICKY_SEL = "[data-cart-sticky-checkout]:not([hidden]) a[data-sticky-checkout-now-action]"
    PRIMARY_SEL = "a[data-primary-checkout-now-action]"
    FLAG_KEY = "3393-desktop-cart-sticky-checkout-cta"

    def __init__(self, session_id: int, browser, engine: str, origin: str, debug: bool = True):
        self.id = session_id
        self.browser = browser
        self.engine = engine
        self.origin = origin.rstrip("/")
        self.debug = debug
        self.context = None
        self.page = None

        self.products_min = max(1, int(os.getenv("AB_TEST_PRODUCTS_MIN", "1")))
        self.products_max = max(self.products_min, int(os.getenv("AB_TEST_PRODUCTS_MAX", "3")))
        self.sticky_wait_ms = int(os.getenv("AB_TEST_STICKY_WAIT_MS", "6000"))
        self.settle_min_ms = int(os.getenv("POST_NAV_SETTLE_MIN_MS", "1500"))
        self.settle_max_ms = max(self.settle_min_ms, int(os.getenv("POST_NAV_SETTLE_MAX_MS", "3500")))
        self.locale = random.choice(_csv("LOCALES", "en-US"))
        self.timezone = random.choice(_csv("TIMEZONES", "America/Toronto"))

    def log(self, msg: str):
        debug_print(self.debug, f"[S{self.id}] {msg}")

    async def _log_page(self, why: str):
        """URL + title when a step fails, so the log shows what page it was on."""
        title = ""
        with contextlib.suppress(Exception):
            title = await self.page.title()
        self.log(f"ab: {why} | url={self.page.url} | title={title!r}")

    # ── lifecycle ────────────────────────────────────────────────────────────

    async def run(self):
        self.context = await self.browser.new_context(
            **context_args(self.engine),
            locale=self.locale,
            timezone_id=self.timezone,
            ignore_https_errors=True,
            service_workers="block",
            # Stop CloudFront from serving a cached page fetched under a
            # different Referer (that broke Noibu loading in the past).
            extra_http_headers={"Cache-Control": "no-cache, no-store, must-revalidate",
                                "Pragma": "no-cache"},
        )
        # Automation flag: Playwright sets navigator.webdriver=true in every
        # engine; a real shopper's browser reports false.
        await self.context.add_init_script(
            "Object.defineProperty(Navigator.prototype, 'webdriver', {get: () => false});")
        self.page = await self.context.new_page()
        try:
            await self._land()
            await self._ab_test_checkout()
        finally:
            with contextlib.suppress(Exception):
                await self.context.close()

    async def _goto(self, url: str, referer: Optional[str] = None):
        await self.page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="load", referer=referer)
        await think(self.settle_min_ms, self.settle_max_ms)

    async def _scroll_a_bit(self):
        for _ in range(random.randint(1, 3)):
            await self.page.mouse.wheel(0, random.randint(250, 600))
            await think(200, 700)

    # ── landing ──────────────────────────────────────────────────────────────

    async def _land(self):
        """Pick one traffic source; send its Referer and matching UTM tags."""
        source = (weighted_choice(_csv("REFERRER_SOURCES", "direct"), _floats("REFERRER_WEIGHTS"))
                  or "direct").lower()
        url, referer = self.origin + "/", None
        if source != "direct":
            referer = REFERER_URLS.get(source)
            medium = _kv("REFERRER_UTM_MEDIUMS").get(source, os.getenv("UTM_MEDIUM_DEFAULT", "referral"))
            url += "?" + urlencode({"utm_source": source, "utm_medium": medium,
                                    "utm_campaign": os.getenv("UTM_CAMPAIGN_DEFAULT", "trafficgen")})
        self.log(f"landing ({self.engine}): source={source} referer={referer or 'none'} | {url}")
        await self._goto(url, referer=referer)
        await self._scroll_a_bit()

    # ── products ─────────────────────────────────────────────────────────────

    async def _product_urls(self) -> List[str]:
        """Product URLs from the Shop All listing (both pages)."""
        urls: List[str] = []
        for path in self.LISTING_PATHS:
            await self._goto(self.origin + path)
            try:
                await self.page.wait_for_selector(self.PRODUCT_LINK_SEL, timeout=15_000)
            except Exception:
                await self._log_page(f"no product cards on {path}")
                continue
            hrefs = await self.page.eval_on_selector_all(
                self.PRODUCT_LINK_SEL, "els => els.map(a => a.href)")
            urls.extend(h for h in hrefs if h and h not in urls)
        return urls

    async def _pick_required_options(self):
        """Products with required options (swatches, sizes) won't add until
        one is chosen: take the first choice of every required radio group
        and the first real option of every required select."""
        form = "form[data-cart-item-add]"
        names = await self.page.eval_on_selector_all(
            f"{form} input[type=radio][required]",
            "els => [...new Set(els.map(e => e.name))]")
        for name in names:
            radio = self.page.locator(f'{form} input[type=radio][name="{name}"]').first
            rid = await radio.get_attribute("id")
            label = self.page.locator(f'label[for="{rid}"]') if rid else None
            with contextlib.suppress(Exception):
                if label is not None and await label.count():
                    await label.first.click(timeout=5_000)
                else:
                    await radio.check(timeout=5_000, force=True)
                await think(300, 700)
        selects = self.page.locator(f"{form} select[required]")
        for i in range(await selects.count()):
            sel = selects.nth(i)
            with contextlib.suppress(Exception):
                values = await sel.eval_on_selector_all(
                    "option", "os => os.map(o => o.value).filter(v => v)")
                if values:
                    await sel.select_option(values[0], timeout=5_000)
                    await think(300, 700)
        if names:
            await think(800, 1500)  # let the theme re-price / re-enable the button

    async def _close_added_modal(self):
        """The "added to cart" modal's backdrop covers the header CART link."""
        try:
            await self.page.locator(self.ADDED_MODAL_SEL).wait_for(state="visible", timeout=5_000)
        except Exception:
            return
        try:
            await self.page.locator(self.ADDED_MODAL_CLOSE_SEL).first.click(timeout=5_000)
        except Exception:
            with contextlib.suppress(Exception):
                await self.page.keyboard.press("Escape")
        with contextlib.suppress(Exception):
            await self.page.locator(".modal-background").wait_for(state="hidden", timeout=5_000)

    async def _add_product(self, url: str) -> bool:
        """Open a PDP and add it; True once POST /remote/v1/cart/add returns."""
        await self._goto(url)
        try:
            btn = self.page.locator(self.ADD_TO_CART_SEL)
            await btn.wait_for(state="visible", timeout=10_000)
            await self._pick_required_options()
            await think(600, 1500)
            async with self.page.expect_response(
                lambda r: "/remote/v1/cart/add" in r.url and r.request.method == "POST",
                timeout=15_000,
            ):
                await btn.click()
            await think(1200, 2200)
            await self._close_added_modal()
            return True
        except Exception as e:
            await self._log_page(f"add-to-cart failed ({type(e).__name__})")
            return False

    # ── cart ─────────────────────────────────────────────────────────────────

    async def _open_cart_via_preview(self) -> bool:
        """Header CART -> preview dropdown -> "View Cart" -> /cart.php."""
        try:
            await self.page.locator(self.HEADER_CART_SEL).first.click(timeout=10_000)
            view = self.page.locator(self.VIEW_CART_SEL).first
            await view.wait_for(state="visible", timeout=8_000)
            await think(400, 1100)
            await view.click()
            await self.page.wait_for_url("**/cart.php**", timeout=15_000)
            return True
        except Exception as e:
            await self._log_page(f"cart preview -> View Cart failed ({type(e).__name__}); goto /cart.php")
            with contextlib.suppress(Exception):
                await self._goto(f"{self.origin}/cart.php")
            return "cart.php" in self.page.url

    async def _flag_diagnostics(self) -> dict:
        """The theme asks window.NoibuFeatureFlag for FLAG_KEY and falls back
        to "original" (control) if the SDK isn't there within 1s."""
        with contextlib.suppress(Exception):
            return await self.page.evaluate("""(key) => {
                const ff = window.NoibuFeatureFlag;
                let v = 'no-sdk';
                try { if (ff) v = ff.getClient().getStringValue(key, 'original'); }
                catch (e) { v = 'error: ' + e; }
                // collect-core.js only loads the flag SDK (collect-ff.js) when
                // its embedded NOIBUJS_CONFIG has a feature_flag_key.
                const cfg = window.NOIBUJS_CONFIG || {};
                const ffJs = performance.getEntriesByType('resource')
                    .some(e => e.name.includes('collect-ff'));
                return { sdk: !!ff, flag: v, vw: innerWidth,
                         cfgKey: !!cfg.feature_flag_key, ffJs,
                         webdriver: navigator.webdriver };
            }""", self.FLAG_KEY)
        return {}

    # ── scenario ─────────────────────────────────────────────────────────────

    async def _ab_test_checkout(self):
        AB_TALLY.start()
        want = random.randint(self.products_min, self.products_max)

        urls = await self._product_urls()
        random.shuffle(urls)
        added = 0
        for url in urls:
            if added >= want:
                break
            if await self._add_product(url):
                added += 1
                self.log(f"ab: added {added}/{want} ← {url}")
        if added == 0:
            self.log("ab: nothing added to cart; ending")
            AB_TALLY.record("no_items")
            return

        if not await self._open_cart_via_preview():
            self.log("ab: could not reach cart page; ending")
            AB_TALLY.record("no_cart")
            return

        sticky = self.page.locator(self.STICKY_SEL).first
        try:
            await sticky.wait_for(state="visible", timeout=self.sticky_wait_ms)
        except Exception:
            primary = await self.page.locator(self.PRIMARY_SEL).first.is_visible()
            diag = await self._flag_diagnostics()
            self.log(
                f"ab result: variant=control engine={self.engine} items={added} "
                f"primary_button={'yes' if primary else 'no'} "
                f"flag_sdk={'yes' if diag.get('sdk') else 'no'} flag={diag.get('flag')} "
                f"cfg_ff_key={'yes' if diag.get('cfgKey') else 'no'} "
                f"collect_ff_js={'yes' if diag.get('ffJs') else 'no'} "
                f"webdriver={diag.get('webdriver')} vw={diag.get('vw')} -> exit")
            AB_TALLY.record("control", self.engine)
            return

        self.log("ab: sticky checkout banner visible -> checkout")
        await think(500, 1400)
        await sticky.click()
        try:
            await self.page.wait_for_url("**/checkout**", timeout=30_000)
        except Exception:
            await self._log_page("did not reach checkout after sticky click")
            AB_TALLY.record("sticky_no_checkout", self.engine)
            return
        ok = await checkout_engine.complete_checkout(
            self.page, checkout_engine.random_identity(),
            checkout_engine.default_card(), debug=self.debug)
        self.log(f"ab result: variant=sticky engine={self.engine} items={added} order_placed={ok}")
        AB_TALLY.record("sticky_ordered" if ok else "sticky_order_failed", self.engine)
