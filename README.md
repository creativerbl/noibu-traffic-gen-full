# 🧠 Noibu Traffic Generator

A realistic web traffic simulation tool for generating synthetic user sessions on eCommerce storefronts (e.g., Noibu demo store). It helps validate **referrer/source attribution**, **journeys**, **heatmaps**, and **basic funnel** behavior inside Noibu.

---

## 🚀 What it does
- Launches **Playwright / Chromium** sessions that behave like humans (random waits, optional scrolling, real clicks).
- Lands with **true HTTP Referer** (from `.env`) and optional **UTM** tags.
- Clicks through **top navigation** (extra focus on **Kitchen** and **Bath** by default), opens **PDPs**, and follows a **light funnel**:

<img width="1329" height="941" alt="image" src="https://github.com/user-attachments/assets/e93680d3-389e-42ca-8279-b436b55e5640" />

<img width="1333" height="895" alt="image" src="https://github.com/user-attachments/assets/ab67c87d-a6e2-410e-a3b4-9c52d46b75d8" />

---

## 🔧 Quick Start (with .sh)
> The repo ships with a helper script. Make it executable and run it.

```bash
chmod +x ./run_with_venv.sh 
./run_with_venv.sh 
```


The script will:
1) Install Playwright’s Chromium browser (if missing).  
2) Read your `.env` for configuration.  
3) Launch the traffic generator (`noibu-traffic-gen.py`).

> **Tip:** If you change `.env`, just re-run the script.

---

## ⚙️ Configure via `.env`

### 🌐 Referrers (HTTP Referer) & UTM
These control how sessions *arrive* and how UTMs are tagged.

| Variable | Purpose |
| --- | --- |
| `REFERRER_HEADER_URLS` | **New.** Full URLs (or `direct`) used as the **true HTTP Referer** on first navigation. |
| `REFERRER_WEIGHTS` | Weights applied to the header mix (same weights also used for legacy UTM source mix). |
| `REFERRER_SOURCES` | Legacy source list for **UTM** `utm_source` (e.g., `google`, `facebook`, `direct`). |
| `REFERRER_UTM_MEDIUMS` | Per-source medium mapping (e.g., `google:organic,facebook:paid-social`). |
| `UTM_CAMPAIGN_DEFAULT` | Default `utm_campaign` (e.g., `trafficgen`). |
| `UTM_MEDIUM_DEFAULT` | Fallback `utm_medium` when not mapped. |

**Example:**
```env
REFERRER_SOURCES=direct,google,bing,yahoo,duckduckgo,facebook,instagram,tiktok,linkedin,reddit
REFERRER_WEIGHTS=10,35,5,5,5,15,13,12,5,5

# Real HTTP Referer sources (URLs). Direct = no Referer header.
REFERRER_HEADER_URLS=direct,https://www.google.com/,https://www.bing.com/,https://search.yahoo.com/,https://duckduckgo.com/,https://www.facebook.com/,https://www.instagram.com/,https://www.tiktok.com/,https://www.linkedin.com/,https://www.reddit.com/

# UTM behavior
REFERRER_UTM_MEDIUMS=google:organic,bing:organic,yahoo:organic,duckduckgo:organic,facebook:paid-social,instagram:paid-social,tiktok:paid-social
UTM_CAMPAIGN_DEFAULT=trafficgen
UTM_MEDIUM_DEFAULT=paid-social
```

### 👤 Devices & session behavior (common knobs)
```env
# Page waits & scrolling
PAGE_WAIT_UNTIL=load                 # load | domcontentloaded | networkidle
SCROLL_PROB=0.70                     # 70% of sessions scroll
SCROLL_DEPTH_MIN=0.35                # 35%–90% of page height
SCROLL_DEPTH_MAX=0.90
SCROLL_STEPS_MIN=2
SCROLL_STEPS_MAX=6
POST_NAV_SETTLE_MIN_MS=250
POST_NAV_SETTLE_MAX_MS=900

# Top-nav hotspots
NAV_HOTSPOT_NAMES=Kitchen,Bath
NAV_HOTSPOT_EXTRA_CLICK_PROB=Kitchen:0.65,Bath:0.45
NAV_NAVIGATION_PAUSE_MS_MIN=400
NAV_NAVIGATION_PAUSE_MS_MAX=1100

# Funnel
FUNNEL_ADD_TO_CART_RATE=0.30         # ~30% add to cart
FUNNEL_CHECKOUT_START_RATE=0.50      # ~50% of ATC sessions start checkout
```

### 🎯 Flows
Each flow YAML in `trafficgen/flows/*.yaml` can include a `weight` to bias selection.

| Variable | Purpose |
| --- | --- |
| `FLOW_WEIGHTS` | Optional overrides for flow weights (by flow `name`), e.g., `category-browse:40,checkout-complete:10`. |

---

## ▶️ What to expect in logs
With `DEBUG=1` set in `.env`, you’ll see lines like:
```
[S12] landing with REFERER: https://www.google.com/ | https://noibu.mybigcommerce.com/?utm_source=google&utm_medium=organic&utm_campaign=trafficgen
[S12] document.referrer='https://www.google.com/'
[S12] nav click → kitchen
[S12] nav click → bath
[S12] summary: atc=1 checkout=1
```

---

## 📁 Layout
```
trafficgen/
 ├─ runner.py            # Session launcher/orchestration
 ├─ session.py           # Real clicks, referrer/UTM handling, human-like behavior
 ├─ utils.py             # Helpers: wait, backoff, same_origin, logging
noibu-traffic-gen.py     # Entry point
noibu-traffic-gen.sh     # Helper script to install/run (chmod +x and execute)
.env                     # Configuration
```

---

## 🧩 Requirements
- Python 3.9+
- Playwright (installed by the `.sh` script)
- Chromium (installed by the `.sh` script)

---

## 📄 License
MIT

---

## Branch: `broken-checkout`

This branch runs **one scenario only** — a low-frequency probe that walks a
shopper into a checkout that cannot complete, so Noibu captures the failure
and the retry behaviour around it.

**What one session does**

1. Lands on the store (legacy referrer/UTM attribution — personas are off).
2. Adds **3 random products** to the cart, re-entering a category listing
   before each one so every add is a fresh navigation.
3. Views the cart and proceeds to checkout.
4. Fills email + shipping, then selects an **offline payment method** —
   randomly **Bank Deposit** or **Cash on Delivery**. These have no card
   iframes, so the failure surfaces on order submission.
5. Clicks **Place Order**. When it fails, waits **5–20s** and clicks again,
   for **1–7 retries** (so 2–8 total submissions), then leaves.

**Cadence and devices**

* `SESSIONS_PER_MINUTE=0.0166667` → one session per hour (±5% jitter).
* `MAX_CONCURRENCY=1` → hourly sessions can never overlap.
* `DEVICE_MIX=desktop-chrome:50,android-chrome:50` → Chrome only, ~50/50
  desktop and mobile.

**Knobs** (all in `.env`)

| Variable | Default | Meaning |
| --- | --- | --- |
| `BROKEN_CHECKOUT_PRODUCTS` | `3` | Products added before checkout |
| `BROKEN_CHECKOUT_PAYMENT_METHODS` | `bank,cod` | Pool picked from per session |
| `BROKEN_CHECKOUT_RETRY_MIN` / `_MAX` | `1` / `7` | Retries *after* the first failure |
| `BROKEN_CHECKOUT_RETRY_WAIT_MIN_S` / `_MAX_S` | `5` / `20` | Pause between attempts |
| `FLOWS_ONLY` | `broken-checkout` | Restricts flow loading to these YAML stems |
| `SCHEDULER_JITTER` | `0.15` | Fraction of jitter on the scheduling interval |
| `SCHEDULER_START_IMMEDIATELY` | `0` | `1` fires the first session at boot instead of one interval later |

If the order ever *does* go through, the loop stops after that submission —
the retry count is a ceiling, not a fixed number of clicks.

**Note:** `SESSIONS_PER_MINUTE` below `0.1` used to be silently clamped by the
scheduler to a 10-minute interval. That floor is fixed on this branch, so
sub-hourly rates now mean what they say.
