# trafficgen/checkout.py
"""BigCommerce checkout engine, ported from the clmod3 single-file generator.

The selectors, waits, fallbacks and per-iframe payment-field logic below are
battle-tested against https://noibudemo.com (BigCommerce + Noibu). Do NOT
"clean up" or simplify them without re-validating a live checkout completion.

Key invariants carried over from clmod3:
  * Country MUST be selected before State (BigCommerce renders the state
    dropdown dynamically based on the chosen country).
  * BigCommerce hosted payments render each card field in a SEPARATE iframe.
    Each iframe has exactly one visible input (the real field) plus hidden
    autocomplete-trap inputs (tabindex="-1", opacity:0) that must be skipped.
  * After order confirmation we linger 5-10s so Noibu captures the page.

Public API:
    random_identity() -> dict
    proceed_to_checkout(page, debug=False) -> bool
    complete_checkout(page, identity, card, debug=False) -> bool
    abandon_checkout(page, stage, debug=False)
    capture_helpcode(page, debug=False) -> str | None
"""

import asyncio
import os
import random
import time
from typing import Optional
from urllib.parse import urlparse

from playwright.async_api import TimeoutError as PwTimeout

# ── random data pools (ported verbatim from clmod3) ─────────────────────────

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


def random_identity() -> dict:
    """Random shopper identity: email, name, phone, US address."""
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


def default_card() -> dict:
    """Test card from env (BigCommerce test gateway defaults from clmod3)."""
    return {
        "number": os.getenv("CARD_NUMBER", "4111111111111111"),
        "expiry": os.getenv("CARD_EXPIRY", "01/30"),
        "cvv": os.getenv("CARD_CVV", "989"),
    }


# ── helpers (ported from clmod3) ─────────────────────────────────────────────

def _log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _dbg(debug: bool, msg: str):
    if debug:
        _log(f"  DEBUG: {msg}")


async def _human_delay(lo: float = 0.8, hi: float = 2.5):
    """Random pause to simulate human think-time."""
    await asyncio.sleep(random.uniform(lo, hi))


async def _slow_type(locator, text: str, delay_lo: int = 40, delay_hi: int = 120):
    """Type text character-by-character with random delays."""
    for ch in text:
        await locator.press_sequentially(ch, delay=random.randint(delay_lo, delay_hi))
        await asyncio.sleep(random.uniform(0.01, 0.05))


def _origin_of(page) -> str:
    try:
        u = urlparse(page.url)
        if u.scheme and u.netloc:
            return f"{u.scheme}://{u.netloc}"
    except Exception:
        pass
    return os.getenv("ORIGIN", "https://noibudemo.com").rstrip("/")


# ── Noibu helpcode ───────────────────────────────────────────────────────────

async def capture_helpcode(page, debug: bool = False) -> Optional[str]:
    """Generate a Noibu helpcode via window.NOIBUJS.requestHelpCode(true).

    Auto-accepts the prompt/alert dialog Noibu may raise and returns the
    helpcode string, or None if Noibu is not loaded / call failed.
    """
    helpcode: Optional[str] = None

    def _on_dialog(dialog):
        nonlocal helpcode
        helpcode = dialog.default_value or dialog.message
        asyncio.ensure_future(dialog.accept())

    page.on("dialog", _on_dialog)
    try:
        result = await page.evaluate("""() => {
            if (window.NOIBUJS && typeof window.NOIBUJS.requestHelpCode === 'function') {
                return window.NOIBUJS.requestHelpCode(true);
            }
            return null;
        }""")
        await _human_delay(1, 2)
        if result:
            helpcode = result
        if helpcode:
            _log(f"Noibu Helpcode: {helpcode}")
        else:
            _dbg(debug, "NOIBUJS.requestHelpCode returned null/empty")
    except Exception as e:
        _dbg(debug, f"Helpcode generation failed: {e}")
    finally:
        try:
            page.remove_listener("dialog", _on_dialog)
        except Exception:
            pass
    return helpcode


# ── cart → checkout ──────────────────────────────────────────────────────────

async def proceed_to_checkout(page, debug: bool = False) -> bool:
    """From cart/cart-popup, get onto the checkout page (clmod3 Step 3)."""
    try:
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
            await _human_delay(0.5, 1.5)
            await checkout_btn.click()
        except PwTimeout:
            # Fallback: go directly to checkout
            _dbg(debug, "Checkout button not found in popup, navigating directly")
            await page.goto(f"{_origin_of(page)}/checkout", wait_until="load")

        # Wait for checkout page to load (React-based, may take a moment)
        try:
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except PwTimeout:
            pass
        await _human_delay(2, 4)
        if "/checkout" not in page.url:
            _dbg(debug, f"Not on checkout page (url={page.url})")
            return False
        _dbg(debug, "On checkout page")
        return True
    except Exception as e:
        _dbg(debug, f"proceed_to_checkout failed: {e}")
        return False


# ── checkout stages (refactored from clmod3 run_order so abandonment can
#    share the exact same battle-tested internals) ───────────────────────────

async def _stage_customer(page, identity: dict, debug: bool = False) -> bool:
    """Enter customer email and continue (clmod3 Step 4)."""
    email_input = page.locator('#email, input[data-test="customer-email"], input[name="email"], input[type="email"]').first
    await email_input.wait_for(state="visible", timeout=15_000)
    await _human_delay(0.5, 1)
    await email_input.click()
    await _slow_type(email_input, identity["email"])
    await _human_delay(0.5, 1)

    # Click "Continue" after email
    continue_btn = page.locator(
        '#checkout-customer-continue, '
        'button:has-text("Continue"), '
        '[data-test="customer-continue-button"]'
    ).first
    await continue_btn.click()
    await _human_delay(2, 4)
    _log(f"Checkout: email entered - {identity['email']}")
    return True


async def _stage_shipping(page, identity: dict, debug: bool = False) -> bool:
    """Fill shipping address with COUNTRY-BEFORE-STATE ordering (clmod3 Step 5)."""
    # Wait for shipping form to appear
    first_name_field = page.locator(
        '#firstNameInput, '
        'input[name="firstName"], '
        'input[data-test="firstNameInput"], '
        '#checkout-shipping-address input[name="firstName"]'
    ).first
    await first_name_field.wait_for(state="visible", timeout=15_000)
    await _human_delay(0.5, 1)

    # Fill the shipping form fields
    async def fill_field(selectors: str, value: str):
        field = page.locator(selectors).first
        try:
            await field.wait_for(state="visible", timeout=5_000)
            await field.click()
            await field.fill("")  # clear existing
            await _slow_type(field, value, delay_lo=30, delay_hi=80)
            await _human_delay(0.3, 0.7)
        except PwTimeout:
            _dbg(debug, f"Field not found: {selectors}")

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
        await _human_delay(1, 2)  # wait for state dropdown to populate
    except (PwTimeout, Exception):
        _dbg(debug, "Country select not found or already set")

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
            await _human_delay(0.3, 0.7)
        else:
            # It's a text input
            await state_el.click()
            await state_el.fill("")
            await _slow_type(state_el, identity["state"], delay_lo=30, delay_hi=80)
            await _human_delay(0.3, 0.7)
    except (PwTimeout, Exception) as exc:
        _dbg(debug, f"State/Province field not found or failed: {exc}")

    await fill_field(
        '#postCodeInput, input[name="postalCode"], input[data-test="postCodeInput"]',
        identity["zip"]
    )

    await fill_field(
        '#phoneInput, input[name="phone"], input[data-test="phoneInput"]',
        identity["phone"]
    )

    await _human_delay(1, 2)

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

    await _human_delay(3, 5)
    _log(f"Checkout: shipping filled - {identity['street']}, {identity['city']}, {identity['state']}")

    # If there's a shipping method step, click continue again
    try:
        shipping_method_continue = page.locator(
            '#checkout-shipping-continue, '
            'button[data-test="shipping-continue-button"]'
        ).first
        if await shipping_method_continue.is_visible():
            await _human_delay(1, 2)
            await shipping_method_continue.click()
            await _human_delay(2, 4)
            _dbg(debug, "Clicked shipping method continue")
    except Exception:
        pass
    return True


async def _stage_payment(page, identity: dict, card: dict, debug: bool = False) -> bool:
    """Select payment method and enter card details (clmod3 Steps 6 + 7).

    Returns True if card fields were filled (>=3 of number/expiry/cvv/name).
    """
    # ── Select test payment provider ──
    # Dump iframe info (debug)
    frames = page.frames
    for i, frame in enumerate(frames):
        url = frame.url
        if url and url != "about:blank":
            _dbg(debug, f"Frame[{i}]: name={frame.name!r} url={url}")

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
                _dbg(debug, f"Selected payment via: {selector}")
                await _human_delay(1, 2)
                break
        except Exception:
            continue

    if not payment_selected:
        _dbg(debug, "No payment method radio found, may be auto-selected or single option")

    # ── Enter card details ──
    await _human_delay(1, 2)

    card_number = card.get("number", "4111111111111111")
    card_expiry = card.get("expiry", "01/30")
    card_cvv = card.get("cvv", "989")

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
                    await _slow_type(loc, value)
                    await _human_delay(0.3, 0.7)
                    _dbg(debug, f"Filled {label} via: {sel}")
                    return True
            except Exception:
                continue
        return False

    # Try direct inputs on the page
    cc_ok = await find_and_fill(CC_NUMBER_SELECTORS, card_number, "card number")
    if cc_ok:
        await find_and_fill(CC_EXPIRY_SELECTORS, card_expiry, "expiry")
        await find_and_fill(CC_CVV_SELECTORS, card_cvv, "cvv")
        full_name = f"{identity['first_name']} {identity['last_name']}"
        await find_and_fill(CC_NAME_SELECTORS, full_name, "name on card")
        card_filled = True
        _dbg(debug, "Filled card details via direct page inputs")

    # Attempt 2: Look inside iframes
    # BigCommerce hosted payment uses SEPARATE iframes per field.
    # Each iframe has ONE visible input (the real field) plus hidden
    # autocomplete trap inputs (tabindex="-1", opacity:0). We must
    # find the visible input in each iframe and identify it by its
    # attributes (id, autocomplete, aria-label, placeholder).
    if not card_filled:
        full_name = f"{identity['first_name']} {identity['last_name']}"
        non_main_frames = [f for f in page.frames if f != page.main_frame]
        _dbg(debug, f"Checking {len(non_main_frames)} child frames for card inputs...")

        fields_filled = 0
        field_values = {
            "card number": card_number,
            "expiry": card_expiry,
            "cvv": card_cvv,
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
                _dbg(debug, f"  Frame visible input: id={input_id!r} autocomplete={autocomplete!r} aria-label={aria_label!r}")

                if any(k in ident for k in ["cc-number", "card-number", "card number", "credit card"]):
                    field_key = "card number"
                elif any(k in ident for k in ["cc-exp", "expir", "card-expiry"]):
                    field_key = "expiry"
                elif any(k in ident for k in ["cc-csc", "cvv", "cvc", "card-code", "security code"]):
                    field_key = "cvv"
                elif any(k in ident for k in ["cc-name", "card-name", "cardholder", "name on"]):
                    field_key = "name on card"
                else:
                    _dbg(debug, f"  Unknown field, skipping: {ident}")
                    continue

                if field_key in filled_fields:
                    continue

                value = field_values[field_key]
                await visible_input.click()
                await visible_input.fill("")
                await _slow_type(visible_input, value)
                await _human_delay(0.3, 0.6)
                filled_fields.add(field_key)
                fields_filled += 1
                _dbg(debug, f"  Filled {field_key} in frame (id={input_id!r})")

            except Exception as e:
                _dbg(debug, f"  Frame error: {e}")

        if fields_filled >= 3:  # card number + expiry + cvv at minimum
            card_filled = True
            _dbg(debug, f"Filled {fields_filled}/4 card fields across iframes")

    if not card_filled:
        _log("Checkout WARNING: Could not find card input fields")

    await _human_delay(1, 2)
    _log(f"Checkout: card details {'entered' if card_filled else 'FAILED'}")
    return card_filled


async def _place_order(page, debug: bool = False) -> bool:
    """Submit the order and wait for confirmation (clmod3 Step 8)."""
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
                await _human_delay(0.5, 1.5)
                await btn.click()
                order_clicked = True
                _dbg(debug, f"Clicked place order via: {sel}")
                break
        except Exception:
            continue

    if not order_clicked:
        _log("Checkout WARNING: Could not find Place Order button")

    # Wait for order confirmation
    await _human_delay(5, 8)

    # Check for confirmation
    try:
        await page.wait_for_url("**/order-confirmation**", timeout=30_000)
        _log("Checkout: ORDER PLACED SUCCESSFULLY")
        await _human_delay(5, 10)  # linger so Noibu records the confirmation page
        return True
    except PwTimeout:
        # Check page content for success indicators
        content = await page.content()
        if any(w in content.lower() for w in ["thank you", "order confirmation", "order number", "order-confirmation"]):
            _log("Checkout: ORDER PLACED SUCCESSFULLY")
            await _human_delay(5, 10)
            return True
        else:
            _log("Checkout: order placement uncertain - no confirmation detected")
            return False


# ── public flows ─────────────────────────────────────────────────────────────

async def complete_checkout(page, identity: dict, card: dict, debug: bool = False) -> bool:
    """Run the full checkout: email → shipping → payment → place order.

    `page` must already be on the checkout page (see proceed_to_checkout).
    `identity` from random_identity(); `card` is {number, expiry, cvv}.
    Returns True when the order confirmation page was reached.
    """
    try:
        await _stage_customer(page, identity, debug=debug)
        await _stage_shipping(page, identity, debug=debug)
        await _stage_payment(page, identity, card, debug=debug)
        return await _place_order(page, debug=debug)
    except PwTimeout as e:
        _log(f"Checkout TIMEOUT: {e}")
        return False
    except Exception as e:
        _log(f"Checkout ERROR: {e}")
        return False


ABANDON_STAGES = ("customer", "shipping", "payment")


async def abandon_checkout(page, stage: str, debug: bool = False):
    """Perform checkout steps up to and including `stage`, then stop.

    stage: "customer" (email only), "shipping" (email + address),
    or "payment" (email + address + card details, but never place order).
    Shares the exact same stage internals as complete_checkout.
    """
    stage = (stage or "customer").strip().lower()
    if stage not in ABANDON_STAGES:
        stage = "customer"
    identity = random_identity()
    try:
        await _stage_customer(page, identity, debug=debug)
        if stage in ("shipping", "payment"):
            await _stage_shipping(page, identity, debug=debug)
        if stage == "payment":
            await _stage_payment(page, identity, default_card(), debug=debug)
    except PwTimeout as e:
        _dbg(debug, f"abandon_checkout({stage}) timeout: {e}")
    except Exception as e:
        _dbg(debug, f"abandon_checkout({stage}) error: {e}")
    # Hesitate like a real abandoner, then just walk away.
    await _human_delay(2, 6)
    _log(f"Checkout: abandoned at {stage} stage")
