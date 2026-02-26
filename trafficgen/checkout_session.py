# trafficgen/checkout_session.py
# Single-flow checkout session: Homepage → Orbit Terrarium PDP → Add to Cart → Checkout → Place Order

import asyncio
import os
import random
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urlparse

from trafficgen.utils import think, ExponentialBackoff, debug_print

ALLOW_NAV_TIMEOUT = 30000
SEL_TIMEOUT = 15000
CHECKOUT_SEL_TIMEOUT = 20000

# ── Random data pools ──

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
    "Christopher", "Betty", "Andrew", "Dorothy", "Joshua", "Sandra",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas",
    "Taylor", "Moore", "Jackson", "Martin", "Lee", "Thompson", "White", "Harris",
    "Clark", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
]

STREET_NAMES = [
    "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine St", "Elm St",
    "Washington Ave", "Park Blvd", "Lake Dr", "River Rd", "Hill St",
    "Forest Ave", "Sunset Blvd", "Broadway", "Market St", "Church St",
]

US_CITIES = [
    {"city": "New York", "state": "New York", "state_code": "NY", "zip": "10001"},
    {"city": "Los Angeles", "state": "California", "state_code": "CA", "zip": "90012"},
    {"city": "Chicago", "state": "Illinois", "state_code": "IL", "zip": "60601"},
    {"city": "Houston", "state": "Texas", "state_code": "TX", "zip": "77001"},
    {"city": "Phoenix", "state": "Arizona", "state_code": "AZ", "zip": "85001"},
    {"city": "Philadelphia", "state": "Pennsylvania", "state_code": "PA", "zip": "19101"},
    {"city": "San Antonio", "state": "Texas", "state_code": "TX", "zip": "78201"},
    {"city": "San Diego", "state": "California", "state_code": "CA", "zip": "92101"},
    {"city": "Dallas", "state": "Texas", "state_code": "TX", "zip": "75201"},
    {"city": "Austin", "state": "Texas", "state_code": "TX", "zip": "78701"},
    {"city": "Denver", "state": "Colorado", "state_code": "CO", "zip": "80201"},
    {"city": "Seattle", "state": "Washington", "state_code": "WA", "zip": "98101"},
    {"city": "Boston", "state": "Massachusetts", "state_code": "MA", "zip": "02101"},
    {"city": "Nashville", "state": "Tennessee", "state_code": "TN", "zip": "37201"},
    {"city": "Portland", "state": "Oregon", "state_code": "OR", "zip": "97201"},
    {"city": "Atlanta", "state": "Georgia", "state_code": "GA", "zip": "30301"},
    {"city": "Miami", "state": "Florida", "state_code": "FL", "zip": "33101"},
    {"city": "Minneapolis", "state": "Minnesota", "state_code": "MN", "zip": "55401"},
    {"city": "Charlotte", "state": "North Carolina", "state_code": "NC", "zip": "28201"},
    {"city": "Raleigh", "state": "North Carolina", "state_code": "NC", "zip": "27601"},
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com"]


def _random_email():
    first = random.choice(FIRST_NAMES).lower()
    last = random.choice(LAST_NAMES).lower()
    num = random.randint(1, 9999)
    return f"{first}.{last}{num}@{random.choice(EMAIL_DOMAINS)}"


def _random_address():
    city = random.choice(US_CITIES)
    return {
        "first_name": random.choice(FIRST_NAMES),
        "last_name": random.choice(LAST_NAMES),
        "address": f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
        "city": city["city"],
        "state": city["state"],
        "state_code": city["state_code"],
        "zip": city["zip"],
        "phone": f"{random.randint(200, 999)}{random.randint(200, 999)}{random.randint(1000, 9999)}",
    }


def _slug_from_source(src):
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


def _parse_kv_csv(env_val, normalize_keys=True):
    out = {}
    for pair in (env_val or "").strip().split(","):
        if ":" not in pair:
            continue
        k, v = pair.split(":", 1)
        k = k.strip().lower() if normalize_keys else k.strip()
        out[k] = v.strip()
    return out


class CheckoutSession:
    """Runs one complete checkout flow:
    Homepage → Orbit Terrarium PDP → Add to Cart → Checkout → Place Order
    """

    def __init__(self, session_id, browser, playwright, origin, device_context_args,
                 locale, timezone_id, global_qps, debug=False, referrer_url=None):
        self.id = session_id
        self.browser = browser
        self.playwright = playwright
        self.origin = origin.rstrip("/")
        self.ctx_args = device_context_args or {}
        self.locale = locale
        self.tz = timezone_id
        self.global_qps = global_qps
        self.debug = debug
        self.referrer_url = (referrer_url or "").strip() or None
        if self.referrer_url and self.referrer_url.lower() == "direct":
            self.referrer_url = None

        # UTM config
        self.utm_medium_default = os.getenv("UTM_MEDIUM_DEFAULT", "paid-social")
        self.utm_campaign_default = os.getenv("UTM_CAMPAIGN_DEFAULT", "trafficgen")
        self.utm_mediums = _parse_kv_csv(os.getenv("REFERRER_UTM_MEDIUMS", ""))

        # Human-like timing
        self.wait_until = os.getenv("PAGE_WAIT_UNTIL", "load").strip().lower()
        if self.wait_until not in ("load", "domcontentloaded", "networkidle"):
            self.wait_until = "load"
        self.post_nav_settle_min = int(os.getenv("POST_NAV_SETTLE_MIN_MS", "2500"))
        self.post_nav_settle_max = int(os.getenv("POST_NAV_SETTLE_MAX_MS", "5000"))
        self.scroll_prob = float(os.getenv("SCROLL_PROB", "0.70"))
        self.scroll_depth_min = float(os.getenv("SCROLL_DEPTH_MIN", "0.35"))
        self.scroll_depth_max = float(os.getenv("SCROLL_DEPTH_MAX", "0.90"))
        self.scroll_steps_min = int(os.getenv("SCROLL_STEPS_MIN", "2"))
        self.scroll_steps_max = int(os.getenv("SCROLL_STEPS_MAX", "6"))

        self.page = None
        self.context = None

    # ── Browser context ──

    async def _new_context(self):
        cargs = dict(self.ctx_args)
        cargs["locale"] = self.locale
        cargs["timezone_id"] = self.tz
        cargs.setdefault("ignore_https_errors", True)
        self.context = await self.browser.new_context(**cargs)
        self.page = await self.context.new_page()

    async def _guarded_goto(self, url, referer=None):
        await self.global_qps.wait()
        backoff = ExponentialBackoff()
        while True:
            try:
                await self.page.goto(
                    url, timeout=ALLOW_NAV_TIMEOUT,
                    wait_until=self.wait_until, referer=referer,
                )
                await asyncio.sleep(random.uniform(
                    self.post_nav_settle_min / 1000, self.post_nav_settle_max / 1000))
                return
            except Exception:
                await backoff.wait()
                if backoff.attempts > 5:
                    raise

    async def _maybe_scroll(self):
        if random.random() > self.scroll_prob:
            return
        try:
            await self.page.wait_for_selector("body", timeout=SEL_TIMEOUT)
        except Exception:
            return
        try:
            height = await self.page.evaluate("""
                () => {
                    const d = document.documentElement, b = document.body;
                    const vals = [d.scrollHeight, b.scrollHeight, d.offsetHeight,
                                  b.offsetHeight, d.clientHeight, b.clientHeight]
                                 .filter(v => typeof v === 'number');
                    const h = Math.max(...vals, 0);
                    return (h && isFinite(h)) ? h : 2000;
                }
            """)
        except Exception:
            height = 2000
        depth = random.uniform(self.scroll_depth_min, self.scroll_depth_max)
        target = max(400, height * depth)
        steps = random.randint(self.scroll_steps_min, self.scroll_steps_max)
        for _ in range(steps):
            await self.page.mouse.wheel(0, target / steps)
            await think(200, 700)

    def _build_landing_url(self):
        landing = self.origin + "/"
        referer_hdr = None
        if self.referrer_url:
            slug = _slug_from_source(self.referrer_url)
            if self.referrer_url.startswith("http"):
                referer_hdr = self.referrer_url
            else:
                default_map = {
                    "google": "https://www.google.com/",
                    "bing": "https://www.bing.com/",
                    "yahoo": "https://search.yahoo.com/",
                    "facebook": "https://www.facebook.com/",
                    "instagram": "https://www.instagram.com/",
                    "reddit": "https://www.reddit.com/",
                }
                referer_hdr = default_map.get(slug, f"https://www.{slug}.com/")
            utm_source = slug
            utm_medium = self.utm_mediums.get(utm_source, self.utm_medium_default)
            q = {"utm_source": utm_source, "utm_medium": utm_medium,
                 "utm_campaign": self.utm_campaign_default}
            landing += "?" + urlencode(q)
        return landing, referer_hdr

    # ══════════════════════════════════════════════
    # Flow steps
    # ══════════════════════════════════════════════

    async def _step_go_to_homepage(self):
        landing, referer_hdr = self._build_landing_url()
        debug_print(self.debug, f"[S{self.id}] step 1 → homepage: {landing}")
        await self._guarded_goto(landing, referer=referer_hdr)
        await self._maybe_scroll()

    async def _step_click_orbit_terrarium(self):
        debug_print(self.debug, f"[S{self.id}] step 2 → finding Orbit Terrarium - Large")

        # Scroll down the homepage to reveal products
        for _ in range(3):
            await self.page.mouse.wheel(0, 500)
            await think(300, 600)

        # Try multiple selector strategies
        product_selectors = [
            'a:has-text("[Sample] Orbit Terrarium - Large")',
            'a:has-text("Orbit Terrarium - Large")',
            '.card-title a:has-text("Orbit Terrarium")',
            '.card a:has-text("Orbit Terrarium")',
            'a[href*="orbit-terrarium-large"]',
        ]

        for sel in product_selectors:
            try:
                loc = self.page.locator(sel).first
                if await loc.is_visible(timeout=3000):
                    await loc.scroll_into_view_if_needed()
                    await think(500, 1000)
                    await loc.click(timeout=SEL_TIMEOUT)
                    debug_print(self.debug, f"[S{self.id}] clicked Orbit Terrarium (sel: {sel})")
                    await asyncio.sleep(random.uniform(
                        self.post_nav_settle_min / 1000, self.post_nav_settle_max / 1000))
                    return
            except Exception:
                continue

        # Fallback: find by role
        try:
            link = self.page.get_by_role("link", name=re.compile(r"Orbit Terrarium.*Large", re.I))
            await link.first.scroll_into_view_if_needed()
            await think(500, 1000)
            await link.first.click(timeout=SEL_TIMEOUT)
            debug_print(self.debug, f"[S{self.id}] clicked Orbit Terrarium (by role)")
            await asyncio.sleep(random.uniform(
                self.post_nav_settle_min / 1000, self.post_nav_settle_max / 1000))
            return
        except Exception:
            pass

        # Last resort: navigate directly to the product page
        debug_print(self.debug, f"[S{self.id}] direct nav to product page (fallback)")
        await self._guarded_goto(f"{self.origin}/orbit-terrarium-large/")

    async def _step_add_to_cart(self):
        debug_print(self.debug, f"[S{self.id}] step 3 → Add to Cart")
        await think(500, 1500)

        # Try by role first
        try:
            btn = self.page.get_by_role("button", name=re.compile("add to cart", re.I))
            await btn.first.click(timeout=SEL_TIMEOUT)
            debug_print(self.debug, f"[S{self.id}] Add to Cart clicked")
            await think(1000, 2000)
            return
        except Exception:
            pass

        # Try specific BigCommerce selectors
        for sel in [
            "#form-action-addToCart",
            "button[name='add']",
            "input[name='add']",
            ".productView-details button[type='submit']",
            "button:has-text('Add to Cart')",
        ]:
            try:
                await self.page.click(sel, timeout=5000)
                debug_print(self.debug, f"[S{self.id}] Add to Cart clicked (sel: {sel})")
                await think(1000, 2000)
                return
            except Exception:
                continue

        raise Exception("Could not find Add to Cart button")

    async def _step_proceed_to_checkout(self):
        debug_print(self.debug, f"[S{self.id}] step 4 → Proceed to Checkout (popup)")
        await think(1000, 2000)

        # Wait for the cart popup/modal and find checkout button
        checkout_selectors = [
            'a:has-text("Proceed to Checkout")',
            'a:has-text("Check out")',
            'a:has-text("Checkout")',
            '.previewCartAction a[href*="checkout"]',
            '.previewCart a[href*="checkout"]',
            '#previewModal a[href*="checkout"]',
            '.modal a[href*="checkout"]',
            'a.button--primary[href*="checkout"]',
            '[data-test="proceed-to-checkout"]',
            'a[href*="/checkout"]',
        ]

        for sel in checkout_selectors:
            try:
                loc = self.page.locator(sel).first
                if await loc.is_visible(timeout=3000):
                    await loc.click(timeout=SEL_TIMEOUT)
                    debug_print(self.debug, f"[S{self.id}] Proceed to Checkout clicked (sel: {sel})")
                    await asyncio.sleep(random.uniform(
                        self.post_nav_settle_min / 1000, self.post_nav_settle_max / 1000))
                    return
            except Exception:
                continue

        # Fallback: link by role
        try:
            btn = self.page.get_by_role("link", name=re.compile(r"check.?out|proceed", re.I))
            await btn.first.click(timeout=SEL_TIMEOUT)
            debug_print(self.debug, f"[S{self.id}] Proceed to Checkout clicked (by role)")
            await asyncio.sleep(random.uniform(
                self.post_nav_settle_min / 1000, self.post_nav_settle_max / 1000))
            return
        except Exception:
            pass

        # Last resort: navigate directly
        debug_print(self.debug, f"[S{self.id}] direct nav to /checkout (fallback)")
        await self._guarded_goto(f"{self.origin}/checkout")

    async def _step_fill_email(self):
        email = _random_email()
        debug_print(self.debug, f"[S{self.id}] step 5-6 → email: {email}")

        await self.page.wait_for_load_state("load", timeout=ALLOW_NAV_TIMEOUT)
        await think(1500, 3000)

        # Find and fill email
        for sel in [
            "#email",
            'input[data-test="customer-email"]',
            'input[type="email"]',
            'input[name="email"]',
        ]:
            try:
                loc = self.page.locator(sel).first
                if await loc.is_visible(timeout=5000):
                    await loc.click()
                    await loc.fill(email)
                    debug_print(self.debug, f"[S{self.id}] email filled: {email}")
                    break
            except Exception:
                continue
        else:
            raise Exception("Could not find email field")

        await think(500, 1000)
        await self._click_continue_button("customer")

    async def _step_fill_shipping(self):
        addr = _random_address()
        debug_print(self.debug, f"[S{self.id}] step 7 → shipping: {addr['first_name']} {addr['last_name']}, "
                    f"{addr['address']}, {addr['city']}, {addr['state_code']} {addr['zip']}")

        await think(1500, 3000)

        # Wait for shipping form
        await self._wait_for_any_selector([
            "#firstNameInput",
            'input[data-test="firstNameInput"]',
        ], timeout=CHECKOUT_SEL_TIMEOUT)

        await think(500, 1000)

        # Country → US
        await self._try_select_country("US")
        await think(300, 600)

        # Fill address fields
        await self._fill_field(
            ["#firstNameInput", 'input[data-test="firstNameInput"]', 'input[name="firstName"]'],
            addr["first_name"], "first name")

        await self._fill_field(
            ["#lastNameInput", 'input[data-test="lastNameInput"]', 'input[name="lastName"]'],
            addr["last_name"], "last name")

        await self._fill_field(
            ["#addressLine1Input", 'input[data-test="addressLine1Input"]', 'input[name="addressLine1"]'],
            addr["address"], "address")

        await self._fill_field(
            ["#cityInput", 'input[data-test="cityInput"]', 'input[name="city"]'],
            addr["city"], "city")

        await self._try_select_state(addr["state_code"], addr["state"])

        await self._fill_field(
            ["#postCodeInput", 'input[data-test="postCodeInput"]', 'input[name="postCode"]'],
            addr["zip"], "zip")

        # Phone (optional on some stores)
        try:
            await self._fill_field(
                ["#phoneInput", 'input[data-test="phoneInput"]', 'input[name="phone"]'],
                addr["phone"], "phone")
        except Exception:
            debug_print(self.debug, f"[S{self.id}] phone field not found (optional)")

        await think(500, 1000)
        await self._click_continue_button("shipping")

        # Handle shipping method selection if it appears
        await self._handle_shipping_method()

    async def _handle_shipping_method(self):
        debug_print(self.debug, f"[S{self.id}] → checking for shipping method selection")
        await think(2000, 4000)

        try:
            shipping_opts = self.page.locator(
                '.shippingOption-desc, [data-test="shipping-option"], '
                '.form-checklist-item--shippingOption')
            count = await shipping_opts.count()
            if count > 0:
                debug_print(self.debug, f"[S{self.id}] {count} shipping options found")
                # First option is usually pre-selected; click it to be sure
                first_radio = self.page.locator(
                    'input[name="shippingOption"], .shippingOption-desc input[type="radio"]').first
                try:
                    if not await first_radio.is_checked(timeout=2000):
                        await first_radio.click(timeout=5000)
                except Exception:
                    pass
                await think(500, 1000)
                await self._click_continue_button("shipping-method")
                return
        except Exception:
            pass

        debug_print(self.debug, f"[S{self.id}] no separate shipping method step")

    async def _step_fill_payment(self):
        debug_print(self.debug, f"[S{self.id}] step 8-9 → payment")
        await think(2000, 4000)

        # Wait for payment section
        await self._wait_for_any_selector([
            '#checkout-payment-heading',
            '.checkout-step--payment',
            '[data-test="payment"]',
            '.paymentMethod',
        ], timeout=CHECKOUT_SEL_TIMEOUT)

        await think(500, 1500)

        # Select Test Payment Provider
        await self._select_test_payment_provider()
        await think(1000, 2000)

        # Card number: 4111 1111 1111 1111
        await self._fill_card_field(
            ["#ccNumber", 'input[data-test="credit-card-number-input"]',
             'input[name="ccNumber"]', 'input[id*="ccNumber"]'],
            "4111111111111111", "card number")

        # Expiry: 01/30
        await self._fill_card_field(
            ["#ccExpiry", 'input[data-test="credit-card-expiry-input"]',
             'input[name="ccExpiry"]', 'input[id*="ccExpiry"]'],
            "0130", "expiry")

        # CVV: 989
        await self._fill_card_field(
            ["#ccCvv", 'input[data-test="credit-card-cvv-input"]',
             'input[name="ccCvv"]', 'input[id*="ccCvv"]'],
            "989", "cvv")

        # Name on card (use the same random name from address)
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        try:
            await self._fill_card_field(
                ["#ccName", 'input[data-test="credit-card-name-input"]',
                 'input[name="ccName"]', 'input[id*="ccName"]'],
                name, "name on card")
        except Exception:
            debug_print(self.debug, f"[S{self.id}] name on card not found (optional)")

        debug_print(self.debug, f"[S{self.id}] payment details filled")

    async def _step_place_order(self):
        debug_print(self.debug, f"[S{self.id}] step 9 → Place Order")
        await think(500, 1500)

        for sel in [
            'button[data-test="payment-submit-button"]',
            'button:has-text("Place Order")',
            '#checkout-payment-continue',
            '.checkout-step--payment button[type="submit"]',
        ]:
            try:
                loc = self.page.locator(sel).first
                if await loc.is_visible(timeout=3000):
                    await loc.click(timeout=SEL_TIMEOUT)
                    debug_print(self.debug, f"[S{self.id}] Place Order clicked")
                    await think(3000, 5000)
                    return
            except Exception:
                continue

        # Fallback by role
        try:
            btn = self.page.get_by_role("button", name=re.compile("place order", re.I))
            await btn.first.click(timeout=SEL_TIMEOUT)
            debug_print(self.debug, f"[S{self.id}] Place Order clicked (by role)")
            await think(3000, 5000)
            return
        except Exception:
            pass

        raise Exception("Could not find Place Order button")

    # ══════════════════════════════════════════════
    # Helpers
    # ══════════════════════════════════════════════

    async def _fill_field(self, selectors, value, label):
        for sel in selectors:
            try:
                loc = self.page.locator(sel).first
                if await loc.is_visible(timeout=3000):
                    await loc.click()
                    await loc.fill(value)
                    await think(100, 300)
                    return
            except Exception:
                continue
        raise Exception(f"Could not find {label} field")

    async def _fill_card_field(self, selectors, value, label):
        """Fill a card field, handling potential iframes for PCI compliance."""
        # Try directly on page first
        for sel in selectors:
            try:
                loc = self.page.locator(sel).first
                if await loc.is_visible(timeout=3000):
                    await loc.click()
                    await loc.press_sequentially(value, delay=random.randint(30, 80))
                    await think(200, 500)
                    debug_print(self.debug, f"[S{self.id}] {label} filled")
                    return
            except Exception:
                continue

        # Try within iframes (payment fields are often in iframes)
        for frame in self.page.frames:
            if frame == self.page.main_frame:
                continue
            for sel in selectors:
                try:
                    loc = frame.locator(sel).first
                    if await loc.is_visible(timeout=2000):
                        await loc.click()
                        await loc.press_sequentially(value, delay=random.randint(30, 80))
                        await think(200, 500)
                        debug_print(self.debug, f"[S{self.id}] {label} filled (iframe)")
                        return
                except Exception:
                    continue

        raise Exception(f"Could not find {label} field")

    async def _try_select_country(self, country_code):
        for sel in ["#countryCodeInput", 'select[data-test="countryCodeInput"]', 'select[name="countryCode"]']:
            try:
                loc = self.page.locator(sel).first
                if await loc.is_visible(timeout=3000):
                    await loc.select_option(value=country_code, timeout=5000)
                    debug_print(self.debug, f"[S{self.id}] country: {country_code}")
                    await think(500, 1000)
                    return
            except Exception:
                continue
        debug_print(self.debug, f"[S{self.id}] country selector not found (may default to US)")

    async def _try_select_state(self, state_code, state_name):
        for sel in [
            "#provinceCodeInput", 'select[data-test="provinceCodeInput"]',
            'select[name="provinceCode"]', "#provinceInput", 'select[name="province"]',
        ]:
            try:
                loc = self.page.locator(sel).first
                if await loc.is_visible(timeout=3000):
                    tag = await loc.evaluate("el => el.tagName.toLowerCase()")
                    if tag == "select":
                        try:
                            await loc.select_option(value=state_code, timeout=3000)
                        except Exception:
                            await loc.select_option(label=state_name, timeout=3000)
                    else:
                        await loc.fill(state_name)
                    debug_print(self.debug, f"[S{self.id}] state: {state_code}")
                    await think(200, 500)
                    return
            except Exception:
                continue
        debug_print(self.debug, f"[S{self.id}] state selector not found")

    async def _select_test_payment_provider(self):
        debug_print(self.debug, f"[S{self.id}] selecting Test Payment Provider")

        for sel in [
            'label:has-text("Test Payment Provider")',
            '.paymentMethod label:has-text("Test")',
            'input[value*="test"]',
            '[data-test="payment-method-test"]',
        ]:
            try:
                loc = self.page.locator(sel).first
                if await loc.is_visible(timeout=5000):
                    await loc.click(timeout=SEL_TIMEOUT)
                    debug_print(self.debug, f"[S{self.id}] Test Payment Provider selected")
                    return
            except Exception:
                continue

        # Try by text
        try:
            loc = self.page.get_by_text("Test Payment Provider", exact=False)
            await loc.first.click(timeout=SEL_TIMEOUT)
            debug_print(self.debug, f"[S{self.id}] Test Payment Provider selected (by text)")
            return
        except Exception:
            pass

        debug_print(self.debug, f"[S{self.id}] Test Payment Provider not found (may be pre-selected or only option)")

    async def _click_continue_button(self, section):
        await think(500, 1000)

        # Section-specific data-test selectors (BigCommerce convention)
        section_buttons = {
            "customer": [
                'button[data-test="customer-continue-button"]',
                '#checkout-customer-continue',
            ],
            "shipping": [
                'button[data-test="shipping-continue-button"]',
                '#checkout-shipping-continue',
            ],
            "shipping-method": [
                'button[data-test="shipping-continue-button"]',
            ],
            "billing": [
                'button[data-test="billing-continue-button"]',
            ],
        }

        for sel in section_buttons.get(section, []):
            try:
                loc = self.page.locator(sel).first
                if await loc.is_visible(timeout=3000):
                    await loc.click(timeout=SEL_TIMEOUT)
                    debug_print(self.debug, f"[S{self.id}] Continue clicked ({section})")
                    await think(1500, 3000)
                    return
            except Exception:
                continue

        # Generic: find visible Continue button
        try:
            buttons = self.page.get_by_role("button", name=re.compile(r"^continue$", re.I))
            count = await buttons.count()
            for i in range(count):
                btn = buttons.nth(i)
                try:
                    if await btn.is_visible(timeout=1000):
                        await btn.click(timeout=SEL_TIMEOUT)
                        debug_print(self.debug, f"[S{self.id}] Continue clicked ({section}, generic)")
                        await think(1500, 3000)
                        return
                except Exception:
                    continue
        except Exception:
            pass

        # Last resort: any submit button in checkout
        try:
            btn = self.page.locator(
                '.checkout-form button[type="submit"], .form-actions button').first
            if await btn.is_visible(timeout=3000):
                await btn.click(timeout=SEL_TIMEOUT)
                debug_print(self.debug, f"[S{self.id}] Continue clicked ({section}, fallback)")
                await think(1500, 3000)
                return
        except Exception:
            pass

        debug_print(self.debug, f"[S{self.id}] Continue button not found for {section}")

    async def _wait_for_any_selector(self, selectors, timeout=15000):
        deadline = asyncio.get_event_loop().time() + timeout / 1000
        while asyncio.get_event_loop().time() < deadline:
            for sel in selectors:
                try:
                    loc = self.page.locator(sel).first
                    if await loc.is_visible(timeout=500):
                        return sel
                except Exception:
                    continue
            await asyncio.sleep(0.5)
        return None

    # ══════════════════════════════════════════════
    # Main entry
    # ══════════════════════════════════════════════

    async def run(self):
        await self._new_context()
        try:
            await self._step_go_to_homepage()
            await self._step_click_orbit_terrarium()
            await self._step_add_to_cart()
            await self._step_proceed_to_checkout()
            await self._step_fill_email()
            await self._step_fill_shipping()
            await self._step_fill_payment()
            await self._step_place_order()
            debug_print(self.debug, f"[S{self.id}] ORDER PLACED SUCCESSFULLY")
        except Exception as e:
            debug_print(self.debug, f"[S{self.id}] flow error: {e}")
        finally:
            debug_print(self.debug, f"[S{self.id}] session complete")
            try:
                await self.context.close()
            except Exception:
                pass
