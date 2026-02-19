# Execution Plan: Homepage Element Coverage, 3 Hotspots, and 10+ Orders/Hour

## Goal

Transform the traffic generator to produce three distinct behaviors:

1. **3 Homepage Hotspots** — concentrated click clusters on navbar, mid-page, and product-grid areas
2. **Full Element Coverage** — at least a few clicks on every single clickable element / product listing on the homepage
3. **10+ Completed Orders per Hour** — end-to-end checkout using test card `4111 1111 1111 1111` via BigCommerce test payment gateway

---

## Part A: Site Element Map (from live browsing)

### Homepage Sections (top to bottom)

| # | Section | Key Elements |
|---|---------|-------------|
| 1 | **Top Nav Bar** | Logo, Shop All, Bath, Garden, Kitchen, Publications, Utility, Search, Account dropdown, Compare, Cart icon |
| 2 | **Hero Banner** | Background image, "Hero Banner Title" heading, subtitle text, **"Shop now" CTA button** → `/brands/common-good/` |
| 3 | **Secondary Banner** | Image link → `/brands/common-good/` |
| 4 | **Featured Products** (4 cards) | 3 Plant Bundle ($100, Choose Options), Birds Of Paradise ($55, ATC), African Fig ($70, ATC), The Planter ($55, Choose Options) |
| 5 | **Most Popular Products** (12 cards) | Able Brewing System ($225), Tiered Wire Basket ($119.95), Canvas Laundry Cart ($200), Chemex Coffeemaker ($49.50), Fog Linen Towel ($49), 1L Le Parfait Jar ($7), Utility Caddy ($45.95), African Fig ($70), 3 Plant Bundle ($100), Spray Bottle ($15), Orbit Terrarium Large ($109), Oak Cheese Grater ($34.95) |
| 6 | **New Products** (~11 cards) | ZZ Plant ($80), Spray Bottle ($15), Snake Plant ($109.99), Sansevieria ($45), Pothos ($49.95), Palm ($35), Monstera ($39), Jade ($40), Fiddle Leaf Fig ($45), Dracaena ($137), Chinese Evergreen ($30) |
| 7 | **"Last Chance" / Promo section** | Section banner + "Generate Helpcode?" button |
| 8 | **Lifestyle Blog** | "Your first blog post!" link → `/your-first-blog-post/` |
| 9 | **Footer** | Quick Links (Help, Shipping & Returns, Blog, Contact Us), Popular Brands (OFS, Common Good, Sagaform), Category links, Newsletter signup form, Social icons, Sitemap |

### Products That Can Be Added to Cart Directly (no options required)

These are the products we'll use for the order flow — they have a direct "Add to Cart" button on their PDP:

| Product | Price | URL |
|---------|-------|-----|
| Birds Of Paradise | $55.00 | `/birds-of-paradise/` |
| African Fig | $70.00 | `/african-fig/` |
| [Sample] Able Brewing System | $225.00 | `/able-brewing-system/` |
| [Sample] Tiered Wire Basket | $119.95 | `/tiered-wire-basket/` |
| [Sample] Canvas Laundry Cart | $200.00 | `/canvas-laundry-cart/` |
| [Sample] Chemex Coffeemaker 3 Cup | $49.50 | `/chemex-coffeemaker-3-cup/` |
| [Sample] Utility Caddy | $45.95 | `/utility-caddy/` |
| [Sample] Dustpan & Brush | $34.95 | `/dustpan-brush/` |
| [Sample] Oak Cheese Grater | $34.95 | `/oak-cheese-grater/` |
| [Sample] Smith Journal 13 | $25.00 | `/smith-journal-13/` |
| Spray Bottle | $15.00 | `/spray-bottle/` |
| [Sample] Orbit Terrarium - Small | $89.00 | `/orbit-terrarium-small/` |
| [Sample] Orbit Terrarium - Large | $109.00 | `/orbit-terrarium-large/` |
| [Sample] Laundry Detergent | $29.95 | `/laundry-detergent/` |

### Products That Require Option Selection

| Product | Required Options |
|---------|-----------------|
| 3 Plant Bundle ($100) | Plant 1, Plant 2, Plant 3 dropdowns |
| 1 L Le Parfait Jar ($7) | Size/option dropdown |
| The Planter by Rustic Roots ($55) | Option dropdown |
| Fog Linen Chambray Towel ($49) | Option selection |

---

## Part B: Three Hotspot Zones

We define 3 specific page locations that will receive concentrated repeated clicks to create visible hotspot clusters in heatmap data:

### Hotspot 1: Navigation Bar — "Kitchen" link
- **Why:** Most popular category (35% weight in current config), top of page
- **Selector:** `header nav a` matching text "Kitchen", or `a[href="/kitchen/"]`
- **Target clicks:** Every session clicks this at least once; 65% of sessions click it a second time
- **Expected heatmap effect:** Bright red spot on the Kitchen nav link

### Hotspot 2: Mid-Page — Hero Banner "Shop now" CTA button
- **Why:** Prominent CTA in the hero section, middle-of-viewport on scroll
- **Selector:** `.heroCarousel-action` or `a.button` with text "Shop now"
- **Target clicks:** 70% of sessions click this button
- **Expected heatmap effect:** Strong cluster on the hero CTA

### Hotspot 3: Product Grid Below — First "Featured Product" card image (Birds Of Paradise)
- **Why:** First direct-ATC product in the featured grid, below the fold
- **Selector:** `a[href="/birds-of-paradise/"] img` or the product card `.card-figure` linking to Birds Of Paradise
- **Target clicks:** 60% of sessions click this product card
- **Expected heatmap effect:** Prominent spot in the product grid area

### Implementation

Add a new method `_homepage_hotspot_clicks()` that:
1. Navigates to homepage (already there from landing)
2. Clicks "Kitchen" in the nav bar (always)
3. Scrolls to hero section, clicks "Shop now" (70% probability)
4. Scrolls to featured products, clicks Birds Of Paradise card (60% probability)
5. After each click, navigates back to homepage to maintain heatmap on the same URL
6. Adds coordinate jitter (random offset within element bounding box) for natural-looking clusters

---

## Part C: Full Element Coverage Pass

### Goal
Every clickable element on the homepage receives at least a few clicks across the session population. This ensures no element is "dark" on the heatmap.

### Implementation: New `_homepage_full_coverage()` method

Replace the existing `_coverage_click_pass()` with a structured homepage coverage approach:

**Phase 1 — Nav bar links** (all 6 category links + account + compare + cart)
- Click each nav link, then navigate back to homepage
- Selectors: `header nav a`, `a[href="/account.php"]`, `a[href="/compare"]`, `a[href="/cart.php"]`

**Phase 2 — Hero section**
- Click hero banner image/link
- Click "Shop now" button
- Click secondary banner image

**Phase 3 — Featured Products section** (4 cards)
- For each card: click the product image, click the product title link, click the "Add to Cart" or "Choose Options" button, click the wishlist icon
- Selectors: `.card-figure a`, `.card-title a`, `.card-body .button`, `a[data-wishlist]`

**Phase 4 — Most Popular Products** (12 cards)
- Same pattern: image, title, action button, wishlist per card

**Phase 5 — New Products** (~11 cards)
- Same pattern per card

**Phase 6 — Blog section**
- Click blog post title link

**Phase 7 — Footer**
- Click each Quick Link (Help, Shipping & Returns, Blog, Contact Us)
- Click each Brand link (OFS, Common Good, Sagaform, View All)
- Click each Category link in footer
- Click Newsletter submit button (without entering email — just the click registers)
- Click Sitemap link

**Session distribution:**
- Not every session does the full coverage pass (that would be unrealistic)
- 20% of sessions run the full coverage pass
- Each element gets clicked by ~5% of sessions minimum (with the 25 sessions/min rate, that's ~75 clicks/hour on even the least-clicked element)
- Navigate back to homepage after each click to keep heatmap data on the homepage URL

---

## Part D: Completed Orders Flow (10+ per hour)

### BigCommerce Checkout Architecture

BigCommerce uses an **Optimized One-Page Checkout** built with React (checkout-js). The checkout page at `/checkout` has these sections:

1. **Customer** — email field (guest checkout)
2. **Shipping** — name, address, phone fields + shipping method selection
3. **Billing** — same as shipping or separate
4. **Payment** — credit card in an embedded iframe (BigCommerce payments) or direct fields (test gateway)

The **Test Payment Gateway** accepts card `4111 1111 1111 1111` with any expiry/CVV. The cardholder name should be set to any value (or `"success"` for explicit test mode).

### Order Completion Flow (per session)

```
1. Navigate to a direct-ATC product PDP (e.g., /birds-of-paradise/)
2. Click "Add to Cart" button
3. Wait for cart confirmation
4. Navigate to /cart.php
5. Click "Check out" / proceed to checkout link
6. On /checkout page:
   a. Fill email: testuser+{session_id}@noibudemo.com
   b. Fill First Name: "Test"
   c. Fill Last Name: "User{session_id}"
   d. Fill Address: "979 Bank Street"
   e. Fill City: "Ottawa"
   f. Select Country: "Canada" or "United States"
   g. Select State/Province: "Ontario" or appropriate
   h. Fill Postal Code: "K2A 0E8" (or "10001" for US)
   i. Fill Phone: "6135551234"
   j. Select shipping method (first available)
   k. Continue to payment
   l. Fill card number: 4111 1111 1111 1111
   m. Fill expiry: 12/30
   n. Fill CVV: 123
   o. Fill cardholder name: "Test User"
   p. Click "Place Order"
7. Wait for order confirmation page
8. Log order completion
```

### Implementation: New `_complete_order()` method in session.py

**Selector strategy** (BigCommerce checkout uses `data-test` attributes and standard form IDs):

```python
# Customer section
email_field = '#email or [data-test="customer-email-input"] or input[type="email"]'

# Shipping address fields — use multiple selector strategies with fallbacks:
# BigCommerce checkout-js uses id="firstNameInput", id="lastNameInput", etc.
# Also try: input[name="firstName"], [data-test="firstNameInput"]
first_name = '#firstNameInput, input[name="firstName"], [data-test="addressLine1Input-firstName"]'
last_name  = '#lastNameInput, input[name="lastName"]'
address1   = '#addressLine1Input, input[name="address1"]'
city       = '#cityInput, input[name="city"]'
postal     = '#postCodeInput, input[name="postalCode"]'
phone      = '#phoneInput, input[name="phone"]'
country    = '#countryCodeInput, select[name="countryCode"]'
state      = '#provinceInput, select[name="province"], input[name="province"]'

# Shipping method
shipping_option = '.shippingOption-desc, [data-test="shipping-option"]'

# Payment — BigCommerce test gateway may render direct fields or iframe
# For test gateway: look for credit card fields
card_number = '#ccNumber, input[name="ccNumber"], [data-test="credit-card-number-input"]'
card_expiry = '#ccExpiry, input[name="ccExpiry"], [data-test="credit-card-expiry-input"]'
card_cvv    = '#ccCvv, input[name="ccCvv"], [data-test="credit-card-cvv-input"]'
card_name   = '#ccName, input[name="ccName"], [data-test="credit-card-name-input"]'

# If payment is in an iframe:
payment_iframe = 'iframe[title*="payment"], iframe[src*="bigcommerce.com/pay"]'

# Place order button
place_order = '#checkout-payment-continue, [data-test="payment-submit-button"], button:has-text("Place Order")'
```

**Handling iframes:** BigCommerce payment fields may be inside an iframe. The method must:
1. Check if payment fields exist on the main page
2. If not, locate the payment iframe and switch context: `frame = page.frame_locator('iframe[title*="payment"]')`
3. Fill fields within the iframe context

### Rate Calculation

- Target: 10 orders per hour = 1 order every 6 minutes
- At 25 sessions/minute = 1,500 sessions/hour
- Order rate: 10/1500 = 0.67% of sessions
- Implementation: Set a new env var `ORDER_COMPLETE_RATE=0.007` (slightly above to account for failures)
- Alternative: Use a dedicated order timer that triggers an order-completion session every 6 minutes regardless of the random session flow

**Recommended approach: Dedicated order scheduler.** Rather than probabilistic gating (which could under/overshoot), add a separate scheduling loop in `runner.py` that spawns an order-completion session every 6 minutes. This guarantees exactly 10/hour.

### New env vars

```env
# Order completion
ORDER_TARGET_PER_HOUR=10
ORDER_PRODUCT_URLS=/birds-of-paradise/,/african-fig/,/able-brewing-system/,/chemex-coffeemaker-3-cup/,/utility-caddy/,/dustpan-brush/,/oak-cheese-grater/,/spray-bottle/,/orbit-terrarium-small/,/orbit-terrarium-large/
ORDER_EMAIL_TEMPLATE=testuser+{sid}@noibudemo.com
ORDER_FIRST_NAME=Test
ORDER_LAST_NAME=User
ORDER_ADDRESS=979 Bank Street
ORDER_CITY=Ottawa
ORDER_COUNTRY=CA
ORDER_STATE=ON
ORDER_POSTAL=K2A 0E8
ORDER_PHONE=6135551234
ORDER_CARD_NUMBER=4111111111111111
ORDER_CARD_EXPIRY=12/30
ORDER_CARD_CVV=123
ORDER_CARD_NAME=Test User
```

---

## Part E: Session Architecture Changes

### Current Flow (every session)
```
Landing → Click ALL nav items (with micro-behaviors each) → Scripted flow → Coverage pass (15%)
```

### New Flow (3 session types)

**Type 1: Homepage Heatmap Session (40% of sessions)**
```
Landing on homepage →
  _homepage_hotspot_clicks() [3 hotspots with high probability] →
  _homepage_full_coverage() [20% of these do full pass] →
  Browse 1-2 category pages →
  Open 1-2 PDPs →
  Maybe scroll, think, leave
```

**Type 2: Journey/Funnel Session (53% of sessions)**
```
Landing on homepage →
  Click 1-3 nav items (weighted, not ALL) →
  Execute one of the YAML flows (randomly selected):
    - category-browse (40%)
    - pdp-deep-dive (25%)
    - add-to-cart-checkout-start (25%)
    - content-read (10%)
```

**Type 3: Order Completion Session (scheduled, ~7% / 10 per hour)**
```
Landing on homepage →
  _homepage_hotspot_clicks() [for heatmap contribution] →
  Navigate to random direct-ATC product →
  Add to cart →
  Go to cart →
  Proceed to checkout →
  Fill customer info (guest) →
  Fill shipping address →
  Select shipping method →
  Fill payment (4111 1111 1111 1111) →
  Place Order →
  Confirm order success →
  Log completion
```

---

## Part F: File-by-File Changes

### 1. `.env` — Add new configuration

```env
# --- Homepage hotspots ---
HOTSPOT_1_SELECTOR=a[href="/kitchen/"]
HOTSPOT_1_PROB=1.0
HOTSPOT_2_SELECTOR=.heroCarousel-action
HOTSPOT_2_TEXT=Shop now
HOTSPOT_2_PROB=0.70
HOTSPOT_3_SELECTOR=a[href="/birds-of-paradise/"] .card-figure__link, a[href="/birds-of-paradise/"]
HOTSPOT_3_PROB=0.60

# --- Session type mix ---
SESSION_TYPE_WEIGHTS=heatmap:40,journey:53,order:7

# --- Order completion ---
ORDER_TARGET_PER_HOUR=10
ORDER_PRODUCT_URLS=/birds-of-paradise/,/african-fig/,/able-brewing-system/,/chemex-coffeemaker-3-cup/,/utility-caddy/,/dustpan-brush/
ORDER_EMAIL_TEMPLATE=testuser+{sid}@noibudemo.com
ORDER_FIRST_NAME=Test
ORDER_LAST_NAME=User
ORDER_ADDRESS=979 Bank Street
ORDER_CITY=Ottawa
ORDER_COUNTRY=CA
ORDER_STATE=ON
ORDER_POSTAL=K2A 0E8
ORDER_PHONE=6135551234
ORDER_CARD_NUMBER=4111111111111111
ORDER_CARD_EXPIRY=12/30
ORDER_CARD_CVV=123
ORDER_CARD_NAME=Test User

# --- Reduce nav clicks from ALL to 1-3 ---
NAV_MAX_CLICKS=3
```

### 2. `trafficgen/runner.py` — Add order scheduler + session types

Changes:
- Add `_order_schedule_loop()` — spawns one order session every `3600 / ORDER_TARGET_PER_HOUR` seconds
- Modify `_run_session()` to accept a `session_type` parameter ("heatmap", "journey", or "order")
- In `_schedule_loop()`, randomly pick session type based on `SESSION_TYPE_WEIGHTS` (excluding order, which has its own scheduler)
- Run both loops concurrently in `run()`

### 3. `trafficgen/session.py` — Major changes

**New methods to add:**

a. `_homepage_hotspot_clicks()`
   - Click Kitchen nav link (100% prob), navigate back
   - Click "Shop now" hero CTA (70% prob), navigate back
   - Click Birds Of Paradise product card (60% prob), navigate back
   - Each click uses bounding-box jitter: random offset within element bounds

b. `_homepage_full_coverage()`
   - Systematic click of every element on homepage (described in Part C)
   - Navigate back after each click
   - 20% of heatmap sessions run this

c. `_click_with_jitter(element)`
   - Get bounding box
   - Calculate random x,y within bounds (not center)
   - `page.mouse.click(x, y)`

d. `_complete_order()`
   - Full checkout flow (described in Part D)
   - Navigate to random direct-ATC product
   - Add to cart → Cart → Checkout
   - Fill all form fields with test data
   - Handle iframe payment fields
   - Click Place Order
   - Verify confirmation page
   - Return success/failure

e. `_fill_checkout_field(selector, value)`
   - Helper that tries multiple selector strategies
   - Handles both direct fields and iframe fields
   - Types with realistic per-character delay

**Modified methods:**

f. `_topnav_click_all_with_hotspots()` → `_topnav_click_limited()`
   - Click only 1-3 nav items (randomly selected with weights) instead of all
   - Keep hotspot extra-click logic for Kitchen/Bath

g. `_run_scripted()` → accept session_type parameter
   - For "heatmap": run `_homepage_hotspot_clicks()` + maybe `_homepage_full_coverage()`+ light browse
   - For "journey": run limited nav clicks + random YAML flow
   - For "order": run `_homepage_hotspot_clicks()` + `_complete_order()`

### 4. `noibu-traffic-gen.py` — Load YAML flows + new config

Changes:
- Load all 5 YAML flow files from `trafficgen/flows/` using `load_yaml_files()`
- Add flow weights to config
- Parse new env vars (ORDER_*, HOTSPOT_*, SESSION_TYPE_WEIGHTS, NAV_MAX_CLICKS)
- Pass session type weights and order config into RunnerConfig

### 5. `trafficgen/devices.py` — No changes needed

### 6. `trafficgen/utils.py` — No changes needed

---

## Part G: Implementation Order

| Step | Task | Files |
|------|------|-------|
| 1 | Add `_click_with_jitter()` utility to session.py | session.py |
| 2 | Add `_homepage_hotspot_clicks()` method | session.py |
| 3 | Add `_homepage_full_coverage()` method | session.py |
| 4 | Add `_complete_order()` + `_fill_checkout_field()` methods | session.py |
| 5 | Refactor `_topnav_click_all_with_hotspots` → `_topnav_click_limited` | session.py |
| 6 | Refactor `_run_scripted()` to support session types | session.py |
| 7 | Add session type selection + order scheduler to runner.py | runner.py |
| 8 | Load YAML flows + new env vars in entry point | noibu-traffic-gen.py |
| 9 | Update `.env` with new configuration | .env |
| 10 | End-to-end test: run with SMOKE=1 and verify hotspots, coverage, and order completion | manual test |

---

## Part H: Expected Outcomes

### Heatmaps
- **3 bright hotspots** on homepage: Kitchen nav link (top), "Shop now" CTA (middle), Birds Of Paradise card (bottom-ish)
- **Full coverage** — every product card, every nav link, every footer link, blog link, hero banner all show at least some click heat
- **Natural scroll depth gradients** from existing scroll simulation
- **Click jitter** creates realistic spread patterns instead of center-point dots

### Journeys
- **Diverse entry sources** (10 referrers with weighted distribution) — unchanged
- **Varied path depths** — bounce sessions (heatmap-only), browse sessions (1-3 categories), funnel sessions (PDP → ATC → checkout → order)
- **Multiple flow types** — category browse, PDP deep-dive, content reading, checkout flows
- **Realistic session depth** — 2-5 pages per session instead of 20-40

### Orders
- **10+ completed orders per hour** guaranteed by dedicated scheduler
- **Varied products** — randomly picks from 6+ direct-ATC products
- **Full funnel visibility** — PDP → ATC → Cart → Checkout → Payment → Confirmation
- **Unique customer data** per order (email includes session ID)

---

## Part I: Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Checkout form selectors change | Use 3-4 fallback selectors per field; log failures |
| Payment iframe detection fails | Try both direct fields and iframe; implement retry with different strategy |
| Order rate drops below 10/hour | Dedicated scheduler (not probabilistic); retry failed orders |
| Cart state leaks between sessions | Each session uses isolated browser context (already implemented) |
| BigCommerce rate limits checkout | Space orders 6 min apart; use exponential backoff on failures |
| Navigate-back breaks page state | Use `page.goto(origin)` instead of `page.go_back()` for reliability |
