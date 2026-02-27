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

from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PwTimeout

# ── load .env ────────────────────────────────────────────────────────────────
load_dotenv()

ORIGIN = os.getenv("ORIGIN", "https://noibudemo.com")
PRODUCT_PATH = os.getenv("PRODUCT_PATH", "/orbit-terrarium-large/")
MIN_INTERVAL_S = int(os.getenv("MIN_INTERVAL_SECONDS", "60"))
HEADLESS = os.getenv("HEADLESS", "1") == "1"
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

        # State/Province - handle as dropdown or input
        state_select = page.locator(
            '#provinceCodeInput, '
            'select[name="stateOrProvince"], '
            'select[data-test="provinceCodeInput"], '
            'select[name="stateOrProvinceCode"]'
        ).first
        try:
            await state_select.wait_for(state="visible", timeout=5_000)
            await state_select.select_option(value=identity["state"])
            await human_delay(0.3, 0.7)
        except (PwTimeout, Exception):
            # Try as text input
            await fill_field(
                '#provinceInput, input[name="stateOrProvince"]',
                identity["state"]
            )

        await fill_field(
            '#postCodeInput, input[name="postalCode"], input[data-test="postCodeInput"]',
            identity["zip"]
        )

        # Country - select United States
        country_select = page.locator(
            '#countryCodeInput, '
            'select[name="countryCode"], '
            'select[data-test="countryCodeInput"]'
        ).first
        try:
            await country_select.wait_for(state="visible", timeout=5_000)
            await country_select.select_option(value="US")
            await human_delay(0.3, 0.7)
        except (PwTimeout, Exception):
            dbg("Country select not found or already set")

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

        # Look for "Test Payment Provider" option
        test_payment = page.locator(
            'label:has-text("Test Payment"), '
            'input[value*="test"], '
            '[data-test="payment-method-radio"]:has-text("Test"), '
            '.form-checklist-item:has-text("Test Payment Provider")'
        ).first

        try:
            await test_payment.wait_for(state="visible", timeout=10_000)
            await test_payment.click()
            await human_delay(1, 2)
            dbg("Selected test payment provider")
        except PwTimeout:
            dbg("Test payment provider radio not found, may be auto-selected")

        # ── Step 8: Enter card details ──
        log(f"[Order #{order_num}] Step 8: Entering card details")

        # Card number - may be in an iframe (common for PCI-compliant gateways)
        # Try direct input first, then iframe
        card_filled = False

        # Attempt 1: Direct inputs (test payment providers often use plain inputs)
        try:
            cc_input = page.locator(
                '#ccNumber, '
                'input[name="ccNumber"], '
                'input[data-test="credit-card-number-input"], '
                'input[name="credit_card_number"], '
                'input[id*="ccNumber"]'
            ).first
            await cc_input.wait_for(state="visible", timeout=8_000)
            await cc_input.click()
            await slow_type(cc_input, CARD_NUMBER)
            await human_delay(0.3, 0.7)

            # Expiry
            exp_input = page.locator(
                '#ccExpiry, '
                'input[name="ccExpiry"], '
                'input[data-test="credit-card-expiry-input"], '
                'input[name="expiration"], '
                'input[id*="ccExpiry"]'
            ).first
            await exp_input.click()
            await slow_type(exp_input, CARD_EXPIRY)
            await human_delay(0.3, 0.7)

            # CVV
            cvv_input = page.locator(
                '#ccCvv, '
                'input[name="ccCvv"], '
                'input[data-test="credit-card-cvv-input"], '
                'input[name="cvv"], '
                'input[id*="ccCvv"]'
            ).first
            await cvv_input.click()
            await slow_type(cvv_input, CARD_CVV)
            await human_delay(0.3, 0.7)

            # Name on card (if present)
            try:
                name_input = page.locator(
                    '#ccName, '
                    'input[name="ccName"], '
                    'input[data-test="credit-card-name-input"], '
                    'input[id*="ccName"]'
                ).first
                if await name_input.is_visible():
                    await name_input.click()
                    full_name = f"{identity['first_name']} {identity['last_name']}"
                    await slow_type(name_input, full_name)
                    await human_delay(0.3, 0.7)
            except Exception:
                pass

            card_filled = True
            dbg("Filled card details via direct inputs")
        except PwTimeout:
            dbg("Direct card inputs not found, trying iframes")

        # Attempt 2: Iframe-based card inputs
        if not card_filled:
            try:
                frames = page.frames
                for frame in frames:
                    cc_in_frame = frame.locator('input[name="cardnumber"], input[id*="card-number"]').first
                    try:
                        if await cc_in_frame.is_visible():
                            await cc_in_frame.click()
                            await slow_type(cc_in_frame, CARD_NUMBER)

                            exp_in_frame = frame.locator('input[name="exp-date"], input[id*="expiry"]').first
                            await exp_in_frame.click()
                            await slow_type(exp_in_frame, CARD_EXPIRY)

                            cvv_in_frame = frame.locator('input[name="cvc"], input[id*="cvv"]').first
                            await cvv_in_frame.click()
                            await slow_type(cvv_in_frame, CARD_CVV)

                            card_filled = True
                            dbg("Filled card details via iframe")
                            break
                    except Exception:
                        continue
            except Exception:
                pass

        if not card_filled:
            log(f"[Order #{order_num}] WARNING: Could not find card input fields")

        await human_delay(1, 2)
        log(f"[Order #{order_num}] Step 8: Card details entered")

        # ── Step 9: Place order ──
        log(f"[Order #{order_num}] Step 9: Placing order")
        place_order_btn = page.locator(
            '#checkout-payment-continue, '
            'button[data-test="place-order-button"], '
            'button:has-text("Place Order"), '
            'input[value="Place Order"], '
            '#checkout-payment-continue'
        ).first

        await place_order_btn.wait_for(state="visible", timeout=10_000)
        await human_delay(0.5, 1.5)
        await place_order_btn.click()

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
            browser = await pw.chromium.launch(headless=HEADLESS)
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
