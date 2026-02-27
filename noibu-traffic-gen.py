#!/usr/bin/env python3
"""
Noibu Traffic Generator – Single-flow checkout runner.

Navigates to noibudemo.com, adds "Orbit Terrarium - Large" to cart,
completes checkout with random identity/address, then resets.
Rate-limited to 1 order per minute with randomised device, browser & referrer.
"""

import asyncio
import os
import random
import signal
import string
import sys
import time
import json

from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PwTimeout

# ── load .env ────────────────────────────────────────────────────────────────
load_dotenv()

ORIGIN = os.getenv("ORIGIN", "https://noibudemo.com")
PRODUCT_PATH = os.getenv("PRODUCT_PATH", "/orbit-terrarium-large/")
MIN_INTERVAL_S = int(os.getenv("MIN_INTERVAL_SECONDS", "60"))
HEADLESS = os.getenv("HEADLESS", "0") == "1"
DEBUG = os.getenv("DEBUG", "0") == "1"

# Card details
CARD_NUMBER = os.getenv("CARD_NUMBER", "4111111111111111")
CARD_EXPIRY = os.getenv("CARD_EXPIRY", "01/30")
CARD_CVV = os.getenv("CARD_CVV", "989")

# ── device profiles ──────────────────────────────────────────────────────────

DEVICE_PROFILES = [
    {
        "name": "Desktop Chrome",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "viewport": {"width": 1920, "height": 1080},
        "device_scale_factor": 1,
        "is_mobile": False,
        "has_touch": False,
    },
    {
        "name": "Desktop Firefox",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "viewport": {"width": 1440, "height": 900},
        "device_scale_factor": 1,
        "is_mobile": False,
        "has_touch": False,
    },
    {
        "name": "Desktop Edge",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
        "viewport": {"width": 1536, "height": 864},
        "device_scale_factor": 1,
        "is_mobile": False,
        "has_touch": False,
    },
    {
        "name": "Desktop Safari",
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
        "viewport": {"width": 1440, "height": 900},
        "device_scale_factor": 2,
        "is_mobile": False,
        "has_touch": False,
    },
    {
        "name": "iPhone Safari",
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
        "viewport": {"width": 390, "height": 844},
        "device_scale_factor": 3,
        "is_mobile": True,
        "has_touch": True,
    },
    {
        "name": "Android Chrome",
        "user_agent": "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
        "viewport": {"width": 412, "height": 915},
        "device_scale_factor": 2.625,
        "is_mobile": True,
        "has_touch": True,
    },
]

# ── referrer sources ─────────────────────────────────────────────────────────

REFERRERS = [
    {"source": "google", "url": "https://www.google.com/", "utm_medium": "organic"},
    {"source": "facebook", "url": "https://www.facebook.com/", "utm_medium": "paid-social"},
    {"source": "instagram", "url": "https://www.instagram.com/", "utm_medium": "social"},
    {"source": "tiktok", "url": "https://www.tiktok.com/", "utm_medium": "social"},
    {"source": "direct", "url": "", "utm_medium": "none"},
    {"source": "bing", "url": "https://www.bing.com/", "utm_medium": "organic"},
    {"source": "linkedin", "url": "https://www.linkedin.com/", "utm_medium": "social"},
    {"source": "reddit", "url": "https://www.reddit.com/", "utm_medium": "social"},
]

# ── locales / timezones ──────────────────────────────────────────────────────

LOCALES = ["en-US", "en-CA", "en-GB"]
TIMEZONES = [
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
]

# ── random data generators ───────────────────────────────────────────────────

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Daniel", "Karen", "Matthew",
    "Lisa", "Anthony", "Nancy", "Mark", "Betty", "Chris", "Emily",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
]

US_ADDRESSES = [
    {"street": "123 Main St", "city": "New York", "state": "NY", "zip": "10001"},
    {"street": "456 Oak Ave", "city": "Los Angeles", "state": "CA", "zip": "90001"},
    {"street": "789 Pine Rd", "city": "Chicago", "state": "IL", "zip": "60601"},
    {"street": "321 Elm St", "city": "Houston", "state": "TX", "zip": "77001"},
    {"street": "654 Maple Dr", "city": "Phoenix", "state": "AZ", "zip": "85001"},
    {"street": "987 Cedar Ln", "city": "Philadelphia", "state": "PA", "zip": "19101"},
    {"street": "147 Birch Way", "city": "San Antonio", "state": "TX", "zip": "78201"},
    {"street": "258 Walnut St", "city": "San Diego", "state": "CA", "zip": "92101"},
    {"street": "369 Spruce Ave", "city": "Dallas", "state": "TX", "zip": "75201"},
    {"street": "741 Ash Blvd", "city": "Portland", "state": "OR", "zip": "97201"},
    {"street": "852 Willow Ct", "city": "Denver", "state": "CO", "zip": "80201"},
    {"street": "963 Poplar St", "city": "Seattle", "state": "WA", "zip": "98101"},
    {"street": "111 Cherry Ln", "city": "Miami", "state": "FL", "zip": "33101"},
    {"street": "222 Peach Dr", "city": "Atlanta", "state": "GA", "zip": "30301"},
    {"street": "333 Olive Way", "city": "Boston", "state": "MA", "zip": "02101"},
]


def _rand_email() -> str:
    first = random.choice(FIRST_NAMES).lower()
    last = random.choice(LAST_NAMES).lower()
    num = random.randint(10, 9999)
    domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com"])
    return f"{first}.{last}{num}@{domain}"


def _rand_phone() -> str:
    area = random.randint(200, 999)
    mid = random.randint(200, 999)
    last = random.randint(1000, 9999)
    return f"{area}{mid}{last}"


def _rand_identity() -> dict:
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    addr = random.choice(US_ADDRESSES)
    return {
        "email": _rand_email(),
        "first_name": first,
        "last_name": last,
        "phone": _rand_phone(),
        **addr,
    }


# ── helpers ──────────────────────────────────────────────────────────────────

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def dbg(msg: str):
    if DEBUG:
        log(f"  DEBUG: {msg}")


async def human_delay(lo: float = 0.8, hi: float = 2.5):
    """Random pause to simulate human think-time."""
    await asyncio.sleep(random.uniform(lo, hi))


async def slow_type(locator, text: str, delay_lo: int = 40, delay_hi: int = 120):
    """Type text character-by-character with random delays."""
    for ch in text:
        await locator.press_sequentially(ch, delay=random.randint(delay_lo, delay_hi))
        await asyncio.sleep(random.uniform(0.01, 0.05))


async def scroll_down(page, steps: int = 3, step_px: int = 300):
    """Scroll the page down in increments."""
    for _ in range(steps):
        await page.mouse.wheel(0, step_px + random.randint(-50, 100))
        await asyncio.sleep(random.uniform(0.3, 0.8))


async def dump_page_debug(page, label: str):
    """Save screenshot + HTML dump for debugging."""
    try:
        safe = label.replace(" ", "_").replace("#", "")
        await page.screenshot(path=f"debug_{safe}.png", full_page=True)
        log(f"  Screenshot saved: debug_{safe}.png")

        # Dump all visible input/select/button/label/iframe elements
        elements = await page.evaluate("""() => {
            const results = [];
            const sels = 'input,select,button,label,iframe,a[href*="checkout"],a[href*="cart"],.form-checklist-item,[data-test],[class*="payment"],[class*="credit"],[id*="cc"],[id*="card"]';
            document.querySelectorAll(sels).forEach(el => {
                const rect = el.getBoundingClientRect();
                if (rect.width > 0 && rect.height > 0) {
                    results.push({
                        tag: el.tagName.toLowerCase(),
                        id: el.id || '',
                        name: el.getAttribute('name') || '',
                        type: el.getAttribute('type') || '',
                        class: el.className ? el.className.toString().slice(0, 100) : '',
                        dataTest: el.getAttribute('data-test') || '',
                        text: el.textContent ? el.textContent.trim().slice(0, 80) : '',
                        value: el.value ? el.value.slice(0, 40) : '',
                        href: el.getAttribute('href') || '',
                        src: el.getAttribute('src') || '',
                    });
                }
            });
            return results;
        }""")
        with open(f"debug_{safe}_elements.json", "w") as f:
            json.dump(elements, f, indent=2)
        log(f"  Element dump saved: debug_{safe}_elements.json ({len(elements)} elements)")
    except Exception as e:
        log(f"  Debug dump failed: {e}")


# ── main flow ────────────────────────────────────────────────────────────────

async def run_order(browser, order_num: int) -> bool:
    """Execute one full order flow. Returns True on success."""

    # Pick random device, referrer, locale, timezone
    device = random.choice(DEVICE_PROFILES)
    referrer = random.choice(REFERRERS)
    locale = random.choice(LOCALES)
    tz = random.choice(TIMEZONES)
    identity = _rand_identity()

    log(f"[Order #{order_num}] Device: {device['name']} | "
        f"Referrer: {referrer['source']} | Locale: {locale}")

    # Build context options
    ctx_opts = {
        "user_agent": device["user_agent"],
        "viewport": device["viewport"],
        "device_scale_factor": device["device_scale_factor"],
        "is_mobile": device["is_mobile"],
        "has_touch": device["has_touch"],
        "locale": locale,
        "timezone_id": tz,
    }

    # Set HTTP referer header if not direct
    if referrer["url"]:
        ctx_opts["extra_http_headers"] = {"Referer": referrer["url"]}

    context = await browser.new_context(**ctx_opts)
    page = await context.new_page()
    page.set_default_timeout(30_000)

    try:
        # ── Step 1: Land on homepage ──
        landing_url = ORIGIN
        if referrer["source"] != "direct":
            landing_url += (
                f"?utm_source={referrer['source']}"
                f"&utm_medium={referrer['utm_medium']}"
                f"&utm_campaign=trafficgen"
            )
        log(f"[Order #{order_num}] Step 1: Landing on {ORIGIN}")
        await page.goto(landing_url, wait_until="load")
        await human_delay(2, 4)

        # ── Step 2: Scroll down and click on Orbit Terrarium ──
        log(f"[Order #{order_num}] Step 2: Scrolling and clicking product")
        await scroll_down(page, steps=random.randint(3, 6))
        await human_delay(1, 2)

        # Try to find the product link
        product_link = page.locator(f'a[href*="orbit-terrarium-large"]').first
        try:
            await product_link.scroll_into_view_if_needed(timeout=10_000)
            await human_delay(0.5, 1.5)
            await product_link.click()
        except PwTimeout:
            # Fallback: navigate directly to product page
            dbg("Product link not found on homepage, navigating directly")
            await page.goto(f"{ORIGIN}{PRODUCT_PATH}", wait_until="load")

        await page.wait_for_load_state("load")
        await human_delay(2, 4)
        log(f"[Order #{order_num}] Step 2: On product page")

        # ── Step 3: Add to Cart ──
        log(f"[Order #{order_num}] Step 3: Adding to cart")
        await scroll_down(page, steps=random.randint(1, 3), step_px=200)
        await human_delay(0.8, 1.5)

        add_btn = page.locator('#form-action-addToCart, [data-button-type="add-cart"], input[value="Add to Cart"], button:has-text("Add to Cart")').first
        await add_btn.scroll_into_view_if_needed()
        await human_delay(0.5, 1)
        await add_btn.click()
        await human_delay(2, 4)
        log(f"[Order #{order_num}] Step 3: Added to cart")

        # ── Step 4: Proceed to checkout (popup or navigate) ──
        log(f"[Order #{order_num}] Step 4: Proceeding to checkout")

        # Try the modal/popup "Proceed to Checkout" or "Check out" button
        checkout_btn = page.locator(
            'a:has-text("Proceed to Checkout"), '
            'a:has-text("Check out"), '
            'a:has-text("Checkout"), '
            'a[href*="/checkout"], '
            '.previewCart a[href*="checkout"], '
            '[data-preview-checkout-button]'
        ).first

        try:
            await checkout_btn.wait_for(state="visible", timeout=8_000)
            await human_delay(0.5, 1.5)
            await checkout_btn.click()
        except PwTimeout:
            # Fallback: go directly to checkout
            dbg("Checkout button not found in popup, navigating directly")
            await page.goto(f"{ORIGIN}/checkout", wait_until="load")

        # Wait for checkout page to load (React-based, may take a moment)
        await page.wait_for_load_state("networkidle", timeout=30_000)
        await human_delay(2, 4)
        log(f"[Order #{order_num}] Step 4: On checkout page")

        # ── Step 5: Enter email ──
        log(f"[Order #{order_num}] Step 5: Entering email")
        email_input = page.locator('#email, input[data-test="customer-email"], input[name="email"], input[type="email"]').first
        await email_input.wait_for(state="visible", timeout=15_000)
        await human_delay(0.5, 1)
        await email_input.click()
        await slow_type(email_input, identity["email"])
        await human_delay(0.5, 1)

        # Click "Continue" after email
        continue_btn = page.locator(
            '#checkout-customer-continue, '
            'button:has-text("Continue"), '
            '[data-test="customer-continue-button"]'
        ).first
        await continue_btn.click()
        await human_delay(2, 4)
        log(f"[Order #{order_num}] Step 5: Email entered - {identity['email']}")

        # ── Step 6: Fill shipping address ──
        log(f"[Order #{order_num}] Step 6: Filling shipping address")

        # Wait for shipping form to appear
        first_name_field = page.locator(
            '#firstNameInput, '
            'input[name="firstName"], '
            'input[data-test="firstNameInput"], '
            '#checkout-shipping-address input[name="firstName"]'
        ).first
        await first_name_field.wait_for(state="visible", timeout=15_000)
        await human_delay(0.5, 1)

        # Fill the shipping form fields
        async def fill_field(selectors: str, value: str):
            field = page.locator(selectors).first
            try:
                await field.wait_for(state="visible", timeout=5_000)
                await field.click()
                await field.fill("")  # clear existing
                await slow_type(field, value, delay_lo=30, delay_hi=80)
                await human_delay(0.3, 0.7)
            except PwTimeout:
                dbg(f"Field not found: {selectors}")

        await fill_field(
            '#firstNameInput, input[name="firstName"], input[data-test="firstNameInput"]',
            identity["first_name"]
        )
        await fill_field(
            '#lastNameInput, input[name="lastName"], input[data-test="lastNameInput"]',
            identity["last_name"]
        )
        await fill_field(
            '#addressLine1Input, input[name="address1"], input[data-test="addressLine1Input"]',
            identity["street"]
        )
        await fill_field(
            '#cityInput, input[name="city"], input[data-test="cityInput"]',
            identity["city"]
        )

        # Country MUST be selected before State (BigCommerce renders
        # the state dropdown dynamically based on the chosen country).
        country_select = page.locator(
            '#countryCodeInput, '
            'select[name="countryCode"], '
            'select[data-test="countryCodeInput"]'
        ).first
        try:
            await country_select.wait_for(state="visible", timeout=5_000)
            await country_select.select_option(value="US")
            await human_delay(1, 2)  # wait for state dropdown to populate
        except (PwTimeout, Exception):
            dbg("Country select not found or already set")

        # State/Province - handle as dropdown or text input
        state_selector = (
            '#provinceCodeInput, '
            'select[name="stateOrProvince"], '
            'select[name="stateOrProvinceCode"], '
            'select[data-test="provinceCodeInput"], '
            '#provinceInput'
        )
        state_el = page.locator(state_selector).first
        try:
            await state_el.wait_for(state="visible", timeout=8_000)
            tag = await state_el.evaluate("el => el.tagName.toLowerCase()")
            if tag == "select":
                # Try matching by value first (abbreviation), then by label (full name)
                try:
                    await state_el.select_option(value=identity["state"])
                except Exception:
                    await state_el.select_option(label=identity["state"])
                await human_delay(0.3, 0.7)
            else:
                # It's a text input
                await state_el.click()
                await state_el.fill("")
                await slow_type(state_el, identity["state"], delay_lo=30, delay_hi=80)
                await human_delay(0.3, 0.7)
        except (PwTimeout, Exception) as exc:
            dbg(f"State/Province field not found or failed: {exc}")

        await fill_field(
            '#postCodeInput, input[name="postalCode"], input[data-test="postCodeInput"]',
            identity["zip"]
        )

        await fill_field(
            '#phoneInput, input[name="phone"], input[data-test="phoneInput"]',
            identity["phone"]
        )

        await human_delay(1, 2)

        # Click Continue on shipping
        shipping_continue = page.locator(
            '#checkout-shipping-continue, '
            'button[data-test="shipping-continue-button"], '
            '#checkout-shipping-options button:has-text("Continue"), '
            'form[data-test="checkout-shipping-form"] button:has-text("Continue")'
        ).first

        try:
            await shipping_continue.wait_for(state="visible", timeout=10_000)
            await shipping_continue.click()
        except PwTimeout:
            # Try any visible Continue button
            all_continues = page.locator('button:has-text("Continue")')
            count = await all_continues.count()
            for i in range(count):
                btn = all_continues.nth(i)
                if await btn.is_visible():
                    await btn.click()
                    break

        await human_delay(3, 5)
        log(f"[Order #{order_num}] Step 6: Shipping filled - {identity['street']}, {identity['city']}, {identity['state']}")

        # If there's a shipping method step, click continue again
        try:
            shipping_method_continue = page.locator(
                '#checkout-shipping-continue, '
                'button[data-test="shipping-continue-button"]'
            ).first
            if await shipping_method_continue.is_visible():
                await human_delay(1, 2)
                await shipping_method_continue.click()
                await human_delay(2, 4)
                dbg("Clicked shipping method continue")
        except Exception:
            pass

        # ── Step 7: Select test payment provider ──
        log(f"[Order #{order_num}] Step 7: Selecting payment method")

        # Debug: capture the payment section state
        await dump_page_debug(page, f"order{order_num}_payment_step")

        # Also dump iframe info
        frames = page.frames
        for i, frame in enumerate(frames):
            url = frame.url
            if url and url != "about:blank":
                dbg(f"Frame[{i}]: name={frame.name!r} url={url}")

        # Look for "Test Payment Provider" option - try multiple approaches
        payment_selected = False
        # Approach 1: Look for a radio/label with text matching test payment
        for selector in [
            'label:has-text("Test Payment")',
            'label:has-text("Test Gateway")',
            '.form-checklist-item:has-text("Test")',
            '[data-test*="payment"] label',
            'input[type="radio"][name*="payment"]',
            '.checkout-step--payment label',
            '#checkout-payment-continue',
        ]:
            try:
                el = page.locator(selector).first
                if await el.is_visible(timeout=2_000):
                    await el.click()
                    payment_selected = True
                    dbg(f"Selected payment via: {selector}")
                    await human_delay(1, 2)
                    break
            except Exception:
                continue

        if not payment_selected:
            dbg("No payment method radio found, may be auto-selected or single option")

        # ── Step 8: Enter card details ──
        log(f"[Order #{order_num}] Step 8: Entering card details")
        await human_delay(1, 2)

        # Debug: capture state after payment selection
        await dump_page_debug(page, f"order{order_num}_card_entry_step")

        card_filled = False

        # Attempt 1: Direct inputs on the main page (BigCommerce test provider)
        CC_NUMBER_SELECTORS = [
            '#ccNumber', 'input[name="ccNumber"]',
            'input[data-test="credit-card-number-input"]',
            'input[name="credit_card_number"]',
            'input[id*="ccNumber"]', 'input[id*="cardNumber"]',
            'input[autocomplete="cc-number"]',
            'input[placeholder*="Card Number"]',
            'input[placeholder*="card number"]',
        ]
        CC_EXPIRY_SELECTORS = [
            '#ccExpiry', 'input[name="ccExpiry"]',
            'input[data-test="credit-card-expiry-input"]',
            'input[name="expiration"]',
            'input[id*="ccExpiry"]', 'input[id*="cardExpiry"]',
            'input[autocomplete="cc-exp"]',
            'input[placeholder*="MM"]',
        ]
        CC_CVV_SELECTORS = [
            '#ccCvv', 'input[name="ccCvv"]',
            'input[data-test="credit-card-cvv-input"]',
            'input[name="cvv"]', 'input[name="cvc"]',
            'input[id*="ccCvv"]', 'input[id*="cardCvv"]',
            'input[autocomplete="cc-csc"]',
            'input[placeholder*="CVV"]',
        ]
        CC_NAME_SELECTORS = [
            '#ccName', 'input[name="ccName"]',
            'input[data-test="credit-card-name-input"]',
            'input[id*="ccName"]',
            'input[autocomplete="cc-name"]',
            'input[placeholder*="Name on"]',
        ]

        async def find_and_fill(selectors_list, value, label="field"):
            """Try each selector until one works."""
            for sel in selectors_list:
                try:
                    loc = page.locator(sel).first
                    if await loc.is_visible(timeout=1_500):
                        await loc.click()
                        await loc.fill("")
                        await slow_type(loc, value)
                        await human_delay(0.3, 0.7)
                        dbg(f"Filled {label} via: {sel}")
                        return True
                except Exception:
                    continue
            return False

        # Try direct inputs on the page
        cc_ok = await find_and_fill(CC_NUMBER_SELECTORS, CARD_NUMBER, "card number")
        if cc_ok:
            await find_and_fill(CC_EXPIRY_SELECTORS, CARD_EXPIRY, "expiry")
            await find_and_fill(CC_CVV_SELECTORS, CARD_CVV, "cvv")
            full_name = f"{identity['first_name']} {identity['last_name']}"
            await find_and_fill(CC_NAME_SELECTORS, full_name, "name on card")
            card_filled = True
            dbg("Filled card details via direct page inputs")

        # Attempt 2: Look inside iframes
        # BigCommerce hosted payment uses SEPARATE iframes per field,
        # so we must search across ALL iframes for each field individually.
        if not card_filled:
            full_name = f"{identity['first_name']} {identity['last_name']}"
            non_main_frames = [f for f in page.frames if f != page.main_frame]
            dbg(f"Checking {len(non_main_frames)} child frames for card inputs...")

            # Define what to look for in each field category
            iframe_field_map = [
                ("card number", CARD_NUMBER, [
                    'input[name="cardnumber"]', 'input[id*="card-number"]',
                    'input[name="credit-card-number"]', 'input[autocomplete="cc-number"]',
                    'input[placeholder*="Card Number"]', 'input[placeholder*="card number"]',
                    'input[id*="ccNumber"]', 'input[data-test="credit-card-number-input"]',
                    'input',  # last resort: only input in the frame
                ]),
                ("expiry", CARD_EXPIRY, [
                    'input[name="exp-date"]', 'input[name="expiry"]',
                    'input[autocomplete="cc-exp"]', 'input[placeholder*="MM"]',
                    'input[placeholder*="Expir"]', 'input[id*="ccExpiry"]',
                    'input[data-test="credit-card-expiry-input"]',
                    'input',
                ]),
                ("name on card", full_name, [
                    'input[name="ccName"]', 'input[autocomplete="cc-name"]',
                    'input[placeholder*="Name"]', 'input[id*="ccName"]',
                    'input[data-test="credit-card-name-input"]',
                    'input',
                ]),
                ("cvv", CARD_CVV, [
                    'input[name="cvc"]', 'input[name="cvv"]',
                    'input[autocomplete="cc-csc"]', 'input[placeholder*="CVV"]',
                    'input[placeholder*="CVC"]', 'input[id*="ccCvv"]',
                    'input[data-test="credit-card-cvv-input"]',
                    'input',
                ]),
            ]

            fields_filled = 0
            used_frames = set()  # track which frames we already filled

            for label, value, selectors in iframe_field_map:
                filled_this = False
                for frame in non_main_frames:
                    if id(frame) in used_frames:
                        continue
                    for sel in selectors:
                        try:
                            loc = frame.locator(sel).first
                            if await loc.is_visible(timeout=2_000):
                                await loc.click()
                                await loc.fill("")
                                await slow_type(loc, value)
                                await human_delay(0.3, 0.6)
                                used_frames.add(id(frame))
                                fields_filled += 1
                                filled_this = True
                                dbg(f"  Filled {label} in frame url={frame.url[:80]}")
                                break
                        except Exception:
                            continue
                    if filled_this:
                        break
                if not filled_this:
                    dbg(f"  Could not find {label} in any iframe")

            if fields_filled >= 3:  # card number + expiry + cvv at minimum
                card_filled = True
                dbg(f"Filled {fields_filled}/4 card fields across iframes")

        if not card_filled:
            log(f"[Order #{order_num}] WARNING: Could not find card input fields")
            await dump_page_debug(page, f"order{order_num}_card_FAILED")

        await human_delay(1, 2)
        log(f"[Order #{order_num}] Step 8: Card details {'entered' if card_filled else 'FAILED'}")

        # ── Step 9: Place order ──
        log(f"[Order #{order_num}] Step 9: Placing order")

        PLACE_ORDER_SELECTORS = [
            '#checkout-payment-continue',
            'button[data-test="place-order-button"]',
            'button:has-text("Place Order")',
            'input[value="Place Order"]',
            'button:has-text("Complete Order")',
            'button:has-text("Submit Order")',
            '.checkout-step--payment button[type="submit"]',
        ]

        order_clicked = False
        for sel in PLACE_ORDER_SELECTORS:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=3_000):
                    await human_delay(0.5, 1.5)
                    await btn.click()
                    order_clicked = True
                    dbg(f"Clicked place order via: {sel}")
                    break
            except Exception:
                continue

        if not order_clicked:
            log(f"[Order #{order_num}] WARNING: Could not find Place Order button")
            await dump_page_debug(page, f"order{order_num}_placeorder_FAILED")

        # Wait for order confirmation
        await human_delay(5, 8)

        # Check for confirmation
        try:
            await page.wait_for_url("**/order-confirmation**", timeout=30_000)
            log(f"[Order #{order_num}] ✓ ORDER PLACED SUCCESSFULLY")
            return True
        except PwTimeout:
            # Check page content for success indicators
            content = await page.content()
            if any(w in content.lower() for w in ["thank you", "order confirmation", "order number", "order-confirmation"]):
                log(f"[Order #{order_num}] ✓ ORDER PLACED SUCCESSFULLY")
                return True
            else:
                log(f"[Order #{order_num}] ✗ Order placement uncertain - no confirmation detected")
                return False

    except PwTimeout as e:
        log(f"[Order #{order_num}] ✗ TIMEOUT: {e}")
        return False
    except Exception as e:
        log(f"[Order #{order_num}] ✗ ERROR: {e}")
        return False
    finally:
        await context.close()


# ── main loop ────────────────────────────────────────────────────────────────

async def main():
    log("=" * 60)
    log("Noibu Traffic Generator - Single Flow Checkout")
    log(f"Target: {ORIGIN}")
    log(f"Product: {PRODUCT_PATH}")
    log(f"Rate: max 1 order / {MIN_INTERVAL_S}s")
    log(f"Headless: {HEADLESS} | Debug: {DEBUG}")
    log("=" * 60)

    shutdown = asyncio.Event()

    def _signal_handler(sig, frame):
        log("Shutdown signal received, finishing current order...")
        shutdown.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    order_num = 0
    stats = {"success": 0, "failed": 0}

    async with async_playwright() as pw:
        while not shutdown.is_set():
            # Launch fresh browser for each order (resilience + fingerprint rotation)
            launch_opts = {"headless": HEADLESS}
            if not HEADLESS:
                launch_opts["slow_mo"] = 100  # 100ms delay between actions for visibility
            browser = await pw.chromium.launch(**launch_opts)
            order_num += 1
            cycle_start = time.monotonic()

            try:
                success = await run_order(browser, order_num)
                if success:
                    stats["success"] += 1
                else:
                    stats["failed"] += 1
            except Exception as e:
                log(f"[Order #{order_num}] ✗ UNHANDLED: {e}")
                stats["failed"] += 1
            finally:
                await browser.close()

            log(f"Stats: {stats['success']} succeeded, {stats['failed']} failed "
                f"out of {order_num} total")

            # Rate limit: ensure at least MIN_INTERVAL_S between orders
            elapsed = time.monotonic() - cycle_start
            remaining = MIN_INTERVAL_S - elapsed
            if remaining > 0 and not shutdown.is_set():
                log(f"Waiting {remaining:.0f}s before next order...")
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # Normal - timeout means we waited the full duration

    log("=" * 60)
    log(f"Shutdown complete. Final stats: {stats['success']} succeeded, "
        f"{stats['failed']} failed out of {order_num} total")
    log("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
