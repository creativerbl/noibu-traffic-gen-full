# Noibu traffic generator — `ab-test` branch

Drives synthetic shoppers into the cart-page checkout A/B test on
[noibudemo.com](https://noibudemo.com) (feature flag
`3393-desktop-cart-sticky-checkout-cta`).

## What one session does

1. **Lands** on the store from one weighted traffic source (`REFERRER_SOURCES`):
   sends that source's `Referer` header and matching `utm_source`/`utm_medium`.
2. **Adds 1–3 random products.** Product URLs come from `/shop-all/` (pages 1–2);
   each PDP's `#form-action-addToCart` is clicked (first choice of any required
   option is picked first) and the add counts once `POST /remote/v1/cart/add`
   returns. The "added to cart" modal is closed after each add.
3. **Opens the cart** via header **CART** → **View Cart** (falls back to `/cart.php`).
4. **Checks the variant** on the cart page:
   - **Sticky banner** (`[data-cart-sticky-checkout]:not([hidden]) a[data-sticky-checkout-now-action]`)
     → clicks it and completes checkout with the test card in `.env`.
   - **Primary button only** (`a[data-primary-checkout-now-action]`) → ends the session.

Every session runs desktop Chrome at 1920×1080 in a fresh browser (the banner
only renders at ≥801px).

## Run

```bash
chmod +x ./run_with_venv.sh
./run_with_venv.sh
```

Ctrl+C once lets running sessions finish and prints a final summary; Ctrl+C
again forces exit.

## Logs

Per session:

```
[DEBUG] [S12] landing: source=google referer=https://www.google.com/ | https://noibudemo.com/?utm_source=google&utm_medium=organic&utm_campaign=trafficgen
[DEBUG] [S12] ab: added 1/2 ← https://noibudemo.com/dustpan-brush/
[DEBUG] [S12] ab result: variant=control items=2 primary_button=yes flag_sdk=no flag=no-sdk vw=1920 -> exit
```

`flag_sdk` / `flag` show whether `window.NoibuFeatureFlag` loaded and what it
returned — the theme falls back to control when the SDK is missing.

Every `AB_SUMMARY_EVERY` sessions (or `AB_SUMMARY_MINUTES`), and on exit:

```
[AB SUMMARY] 20 started, 20 finished since 14:02 (0h41m)
  variant split : control 11 (58%) | sticky 8 (42%)
  sticky detail : ordered 7 | order not confirmed 1 | checkout didn't load 0
  no variant    : never reached cart 1 | nothing added 0 | unfinished/timed out 0
```

## Configuration (`.env`)

| Setting | Default | Meaning |
| --- | --- | --- |
| `BACK_TO_BACK` | `1` | 1 = next session starts `BACK_TO_BACK_GAP_MIN_S`–`MAX_S` s after a slot frees; 0 = use `SESSIONS_PER_MINUTE` |
| `SESSIONS_PER_MINUTE` | `0.0833333` | Only when `BACK_TO_BACK=0` (0.0833333 = 5/hour), ± `SCHEDULER_JITTER` |
| `MAX_CONCURRENCY` | `1` | Sessions running at once |
| `SESSION_MAX_SECONDS` | `600` | Kill a stuck session after this long |
| `HEADLESS` | `1` | 0 shows the browser window |
| `BROWSER_PER_SESSION` | `1` | 1 = new Chromium process per session; 0 = shared browser, fresh context per session |
| `POST_NAV_SETTLE_MIN_MS` / `MAX_MS` | `2500` / `5000` | Pause after each page load |
| `AB_TEST_PRODUCTS_MIN` / `MAX` | `1` / `3` | Products added per session |
| `AB_TEST_STICKY_WAIT_MS` | `6000` | How long to wait for the sticky banner before counting control |
| `AB_SUMMARY_EVERY` / `AB_SUMMARY_MINUTES` | `5` / `30` | Summary cadence |
| `REFERRER_SOURCES` / `REFERRER_WEIGHTS` | | Traffic-source mix (`direct` = no Referer, no UTM) |
| `REFERRER_UTM_MEDIUMS`, `UTM_MEDIUM_DEFAULT`, `UTM_CAMPAIGN_DEFAULT` | | UTM tagging |
| `LOCALES`, `TIMEZONES` | | Picked at random per session |
| `CARD_NUMBER`, `CARD_EXPIRY`, `CARD_CVV` | | Test card for sticky-variant orders |

Each sticky-variant session places a real test order — keep volume in mind
when running back-to-back or with `MAX_CONCURRENCY` > 1.

## Layout

```
noibu-traffic-gen.py     entry point (loads .env)
trafficgen/runner.py     scheduling, browser launch, timeouts
trafficgen/session.py    one shopper session + [AB SUMMARY] tally
trafficgen/checkout.py   BigCommerce card checkout (email → shipping → payment → place order)
trafficgen/devices.py    desktop Chrome 1920×1080 profile
trafficgen/utils.py      small helpers
run_with_venv.sh         creates .venv, installs deps + Chromium, runs
```
