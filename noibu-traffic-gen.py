#!/usr/bin/env python3
"""
Noibu Traffic Generator – Randomised multi-product checkout runner.

Navigates to noibudemo.com, picks 1-3 random products from the homepage,
adds them to cart, completes checkout with random identity/address, then resets.
Rate-limited to 1 order per 5 minutes with randomised device, browser & referrer.
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
MIN_INTERVAL_S = int(os.getenv("MIN_INTERVAL_SECONDS", "300"))
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

        # ── Generate Noibu helpcode ──
        helpcode = None

        def _on_dialog(dialog):
            nonlocal helpcode
            # The prompt dialog contains the helpcode as its default value
            helpcode = dialog.default_value or dialog.message
            asyncio.ensure_future(dialog.accept())

        page.on("dialog", _on_dialog)

        try:
            helpcode_btn = page.locator(
                'button:has-text("Generate Helpcode"), '
                'a:has-text("Generate Helpcode"), '
                '[class*="helpcode"], '
                'button:has-text("Generate Help")'
            ).first
            if await helpcode_btn.is_visible(timeout=5_000):
                await helpcode_btn.click()
                await human_delay(1, 2)
                if helpcode:
                    log(f"[Order #{order_num}] Noibu Helpcode: {helpcode}")
                else:
                    dbg("Helpcode button clicked but no dialog appeared")
        except Exception:
            dbg("Helpcode button not found, skipping")

        page.remove_listener("dialog", _on_dialog)

        # ── Step 2: Browse homepage and add random products to cart ──
        num_products = random.randint(1, 3)
        log(f"[Order #{order_num}] Step 2: Will add {num_products} random product(s)")

        products_added = []
        for prod_idx in range(num_products):
            # Scroll down the homepage to reveal product sections
            await scroll_down(page, steps=random.randint(3, 7))
            await human_delay(1, 2)

            # Collect all product card links on the homepage
            # BigCommerce product cards are <a> inside .card or article elements
            product_cards = page.locator(
                '.card a.card-figure__link, '
                '.card a.card-title a, '
                '.card .card-body a, '
                'article.card a[href*="/"], '
                '.productGrid .card a[href*="/"], '
                '.card-figure a[href], '
                '.card a[href*="/"]'
            )
            card_count = await product_cards.count()
            dbg(f"  Found {card_count} product card links on homepage")

            if card_count == 0:
                log(f"[Order #{order_num}] WARNING: No product cards found on homepage")
                break

            # Pick a random card, avoiding ones we already added
            attempts = 0
            picked = False
            while attempts < 10:
                idx = random.randint(0, card_count - 1)
                card = product_cards.nth(idx)
                try:
                    href = await card.get_attribute("href") or ""
                    card_text = (await card.inner_text()).strip()[:60]
                except Exception:
                    href, card_text = "", ""
                    attempts += 1
                    continue

                # Skip if we already added this product
                if href and href not in products_added:
                    picked = True
                    break
                attempts += 1

            if not picked:
                dbg("  Could not find a new product to add, using whatever is available")
                idx = random.randint(0, card_count - 1)
                card = product_cards.nth(idx)
                href = await card.get_attribute("href") or ""
                card_text = ""

            # Scroll to and click the chosen product
            log(f"[Order #{order_num}]   Product {prod_idx + 1}/{num_products}: {card_text or href}")
            try:
                await card.scroll_into_view_if_needed(timeout=5_000)
                await human_delay(0.5, 1.5)
                await card.click()
            except Exception:
                # Fallback: navigate directly to the product URL
                if href:
                    full_url = href if href.startswith("http") else f"{ORIGIN}{href}"
                    dbg(f"  Click failed, navigating directly to {full_url}")
                    await page.goto(full_url, wait_until="load")
                else:
                    log(f"[Order #{order_num}]   WARNING: Could not navigate to product")
                    continue

            await page.wait_for_load_state("load")
            await human_delay(2, 4)

            # Browse the PDP a bit
            await scroll_down(page, steps=random.randint(1, 3), step_px=200)
            await human_delay(0.8, 1.5)

            # Add to cart
            add_btn = page.locator(
                '#form-action-addToCart, '
                '[data-button-type="add-cart"], '
                'input[value="Add to Cart"], '
                'button:has-text("Add to Cart")'
            ).first
            try:
                await add_btn.scroll_into_view_if_needed()
                await human_delay(0.5, 1)
                await add_btn.click()
                await human_delay(2, 4)
                products_added.append(href)
                log(f"[Order #{order_num}]   Added to cart ({len(products_added)}/{num_products})")
            except Exception as e:
                log(f"[Order #{order_num}]   WARNING: Could not add to cart: {e}")

            # If more products to add, dismiss any cart popup and go back to homepage
            if prod_idx < num_products - 1:
                # Try to close cart preview popup if visible
                try:
                    close_btn = page.locator(
                        '.previewCart .modal-close, '
                        '[data-close], '
                        'button[aria-label="Close"]'
                    ).first
                    if await close_btn.is_visible(timeout=2_000):
                        await close_btn.click()
                        await human_delay(0.5, 1)
                except Exception:
                    pass

                # Navigate back to homepage for the next product
                log(f"[Order #{order_num}]   Returning to homepage for next product...")
                await page.goto(ORIGIN, wait_until="load")
                await human_delay(2, 3)

        if not products_added:
            log(f"[Order #{order_num}] ✗ No products added to cart, skipping checkout")
            return False

        log(f"[Order #{order_num}] Step 2: Done - added {len(products_added)} product(s)")

        # ── Step 3: Proceed to checkout (popup or navigate) ──
        log(f"[Order #{order_num}] Step 3: Proceeding to checkout")

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
        log(f"[Order #{order_num}] Step 3: On checkout page")

        # ── Step 4: Enter email ──
        log(f"[Order #{order_num}] Step 4: Entering email")
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
        log(f"[Order #{order_num}] Step 4: Email entered - {identity['email']}")

        # ── Step 5: Fill shipping address ──
        log(f"[Order #{order_num}] Step 5: Filling shipping address")

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
        log(f"[Order #{order_num}] Step 5: Shipping filled - {identity['street']}, {identity['city']}, {identity['state']}")

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

        # ── Step 6: Select test payment provider ──
        log(f"[Order #{order_num}] Step 6: Selecting payment method")

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

        # ── Step 7: Enter card details ──
        log(f"[Order #{order_num}] Step 7: Entering card details")
        await human_delay(1, 2)

        # Debug: capture state after payment selection
        await dump_page_debug(page, f"order{order_num}_card_entry_step")

        card_filled = False

        # Attempt 1: Direct inputs on the main page (BigCommerce test provider)
        CC_NUMBER_SELECTORS = [
            '#ccNumber', '#card-number', 'input[name="ccNumber"]',
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
        # BigCommerce hosted payment uses SEPARATE iframes per field.
        # Each iframe has ONE visible input (the real field) plus hidden
        # autocomplete trap inputs (tabindex="-1", opacity:0). We must
        # find the visible input in each iframe and identify it by its
        # attributes (id, autocomplete, aria-label, placeholder).
        if not card_filled:
            full_name = f"{identity['first_name']} {identity['last_name']}"
            non_main_frames = [f for f in page.frames if f != page.main_frame]
            dbg(f"Checking {len(non_main_frames)} child frames for card inputs...")

            fields_filled = 0
            field_values = {
                "card number": CARD_NUMBER,
                "expiry": CARD_EXPIRY,
                "cvv": CARD_CVV,
                "name on card": full_name,
            }
            filled_fields = set()

            for frame in non_main_frames:
                try:
                    # Find the PRIMARY visible input — skip hidden autocomplete traps
                    visible_input = frame.locator(
                        'input:not([tabindex="-1"]):not([type="hidden"])'
                    ).first
                    try:
                        if not await visible_input.is_visible(timeout=2_000):
                            continue
                    except Exception:
                        continue

                    # Read attributes to identify which field this is
                    autocomplete = (await visible_input.get_attribute("autocomplete") or "").lower()
                    input_id = (await visible_input.get_attribute("id") or "").lower()
                    aria_label = (await visible_input.get_attribute("aria-label") or "").lower()
                    placeholder = (await visible_input.get_attribute("placeholder") or "").lower()
                    ident = f"{autocomplete} {input_id} {aria_label} {placeholder}"
                    dbg(f"  Frame visible input: id={input_id!r} autocomplete={autocomplete!r} aria-label={aria_label!r}")

                    if any(k in ident for k in ["cc-number", "card-number", "card number", "credit card"]):
                        field_key = "card number"
                    elif any(k in ident for k in ["cc-exp", "expir", "card-expiry"]):
                        field_key = "expiry"
                    elif any(k in ident for k in ["cc-csc", "cvv", "cvc", "card-code", "security code"]):
                        field_key = "cvv"
                    elif any(k in ident for k in ["cc-name", "card-name", "cardholder", "name on"]):
                        field_key = "name on card"
                    else:
                        dbg(f"  Unknown field, skipping: {ident}")
                        continue

                    if field_key in filled_fields:
                        continue

                    value = field_values[field_key]
                    await visible_input.click()
                    await visible_input.fill("")
                    await slow_type(visible_input, value)
                    await human_delay(0.3, 0.6)
                    filled_fields.add(field_key)
                    fields_filled += 1
                    dbg(f"  Filled {field_key} in frame (id={input_id!r})")

                except Exception as e:
                    dbg(f"  Frame error: {e}")

            if fields_filled >= 3:  # card number + expiry + cvv at minimum
                card_filled = True
                dbg(f"Filled {fields_filled}/4 card fields across iframes")

        if not card_filled:
            log(f"[Order #{order_num}] WARNING: Could not find card input fields")
            await dump_page_debug(page, f"order{order_num}_card_FAILED")

        await human_delay(1, 2)
        log(f"[Order #{order_num}] Step 7: Card details {'entered' if card_filled else 'FAILED'}")

        # ── Step 8: Place order ──
        log(f"[Order #{order_num}] Step 8: Placing order")

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
    log("Noibu Traffic Generator - Random Multi-Product Checkout")
    log(f"Target: {ORIGIN}")
    log(f"Rate: 1 order / {MIN_INTERVAL_S}s ({MIN_INTERVAL_S // 60} min)")
    log(f"Products per order: 1-3 (randomised)")
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
