# Noibu Traffic Generator - Project Knowledge Base

This document captures the complete architecture, patterns, and configuration of the Noibu Traffic Generator so that it can be rebuilt from scratch or adapted for new projects.

## What This Project Does

Generates realistic synthetic eCommerce user sessions against a target storefront (e.g., noibudemo.com on BigCommerce). The tool uses Playwright/Chromium to launch automated browser sessions that simulate real humans browsing, searching, adding to cart, and checking out. The purpose is to validate Noibu's analytics platform: referrer/source attribution, user journeys, heatmaps, and funnel metrics.

## Tech Stack

- **Language:** Python 3.9+
- **Browser Automation:** Playwright 1.48.0 (Chromium only)
- **CLI:** Typer 0.12.5
- **Data Validation:** Pydantic 2.8.2
- **Flow Definitions:** PyYAML 6.0.2
- **Config:** python-dotenv 1.0.1
- **Retry Logic:** Tenacity 8.5.0
- **No build step** - pure Python, no compilation needed

## Architecture Overview

```
Entry Point (noibu-traffic-gen.py)
  ├── Loads .env configuration
  ├── Parses referrer sources, device mix, flows from YAML
  ├── Builds RunnerConfig dataclass
  └── Launches async Runner
        └── Runner.run()
            ├── Launches Playwright Chromium browser
            ├── Creates device pool (mobile/desktop emulation)
            ├── Runs scheduling loop (sessions/minute throttling)
            │   └── Each interval → acquire semaphore → launch session task
            │       ├── Pick device from pool
            │       ├── Create Session instance
            │       └── Session.run()
            │           ├── Create browser context (device profile, locale, timezone)
            │           ├── Pick flow from YAML definitions (weighted random)
            │           └── Execute flow steps sequentially
            └── Health checks, metrics, graceful shutdown
```

## Module Breakdown

### 1. Entry Point (`noibu-traffic-gen.py`, ~130 lines)
- Loads `.env` via python-dotenv
- Helper functions: `_parse_csv()`, `_normalize_to_100()`, `build_referrers_from_env()`, `build_device_mix_from_env()`, `_load_flows_from_disk()`
- Constructs `RunnerConfig` dataclass and passes it to the async `Runner`

### 2. Runner (`trafficgen/runner.py`, ~350 lines)
- **RunnerConfig** dataclass holds all configuration
- **Runner** class manages:
  - Browser lifecycle (launch, restart after N sessions or time)
  - Session scheduling via asyncio (interval = 60 / sessions_per_minute)
  - Concurrency control via asyncio.Semaphore (max_concurrency)
  - Global QPS rate limiting via TokenBucket
  - Metrics tracking: started, completed, failed, timeouts, browser_restarts (logged every 25 sessions)
  - Health/circuit breaker: 5+ consecutive failures → restart browser; 10+ → cooldown
  - Graceful shutdown on SIGINT/SIGTERM
  - Kill switch file check for draining
- **`_weighted_pick()`** - weighted random selection utility

### 3. Session (`trafficgen/session.py`, ~1,560 lines) - The Core Engine
- Creates browser context with device/locale/timezone settings
- Manages HTTP Referer headers and UTM parameter generation
- Parses and executes YAML flow definitions
- **20+ action methods:**

| Action | Description |
|--------|-------------|
| `_landing()` | Land on site with proper HTTP Referer & UTM params |
| `_topnav_click_all_with_hotspots()` | Click all nav categories + weighted hotspot emphasis |
| `_home_explore()` | Home page scrolling with configurable depth |
| `_category_explore()` | Category page navigation and tile hovering |
| `_open_random_pdp()` | Open random product detail page from listings |
| `_pdp_explore()` | Explore product page (images, specs, scroll) |
| `_pdp_decision()` | Weighted decision: add to cart vs. bounce |
| `_add_to_cart()` | Add product to cart (gated by FUNNEL_ADD_TO_CART_RATE) |
| `_view_cart()` | View shopping cart |
| `_cart_edit()` | Modify cart quantities |
| `_start_checkout()` | Start checkout (gated by FUNNEL_CHECKOUT_START_RATE) |
| `_maybe_complete_checkout()` | Complete checkout with test card info |
| `_search()` | Perform search with configurable terms |
| `_search_result_explore()` | Browse search results |
| `_content_browse()` | Browse content/info pages |
| `_content_page()` | Visit specific content pages (about, shipping, contact) |
| `_coverage_click_pass()` | Click random elements for heatmap data generation |
| `_maybe_scroll_page()` | Probabilistic scrolling with depth & step control |
| `_apply_step_jitter()` | Micro-pauses & scrolls between flow steps |
| `_post_load_idle_pause()` | Post-load idle time simulation |

### 4. Devices (`trafficgen/devices.py`, ~86 lines)
- Maps device names to Playwright device profiles
- Generates browser context args (viewport, user agent, mobile flags)
- **Supported devices:**
  - Mobile: iPhone 14 (Safari, Chrome), Pixel 7 (Chrome)
  - Desktop: Chrome, Edge, Safari, Firefox (custom user agents & viewports)

### 5. Utilities (`trafficgen/utils.py`, ~150 lines)
- `TokenBucket` - QPS rate limiting
- `ExponentialBackoff` - Retry with backoff (0.5s → 2x → 10s max)
- `think()` - Async sleep with jitter (human-like pause)
- `same_origin()` - URL allowlist check
- `choose_weighted()` - Weighted random selection
- `weighted_value()` - Pick from bucket with weights & jitter
- `biased_index()` - Biased index selection (favors top of list)
- `load_yaml_files()` - YAML flow file parser
- `debug_print()` - Conditional debug logging (when DEBUG=1)

## Flow Definitions (YAML)

8 predefined user journey flows in `trafficgen/flows/`:

| Flow File | Weight | Steps |
|-----------|--------|-------|
| `pdp-deep-dive.yaml` | 25 | home_explore → category_explore → open_random_pdp → pdp_explore → open_random_pdp → pdp_explore → pdp_decision |
| `category-browse.yaml` | 20 | home_explore → category_explore → category_explore → topnav_click_all_with_hotspots |
| `add-to-cart-checkout-start.yaml` | 20 | home_explore → category_explore → open_random_pdp → add_to_cart → view_cart → start_checkout |
| `content-read.yaml` | 20 | home_explore → content_browse → content_page → content_page |
| `search-to-pdp.yaml` | 15 | home_explore → search → search_result_explore → open_random_pdp → pdp_decision |
| `checkout-complete.yaml` | 15 | home_explore → category_explore → open_random_pdp → add_to_cart → start_checkout → maybe_complete_checkout |
| `info-seeking.yaml` | 15 | home_explore → footer_explore → content_page → content_page |
| `search-only.yaml` | 12 | home_explore → search → search_result_explore |

### YAML Flow Structure
```yaml
name: flow-name
weight: 25
steps:
  - action: home_explore
  - action: category_explore
  - action: open_random_pdp
    params:
      some_param: value
  - action: pdp_decision
```

## Configuration (.env file - 70+ variables)

### Core Traffic Shaping
```
ORIGIN=https://noibudemo.com          # Target eCommerce site
SESSIONS_PER_MINUTE=25                # New sessions per minute
AVG_SESSION_MINUTES=3                 # Average session duration
MAX_CONCURRENCY=100                   # Max parallel browser sessions
GLOBAL_QPS_CAP=6                      # Global queries per second limit
```

### Device Mix (weighted)
```
DEVICE_MIX=iphone-safari:15,iphone-chrome:10,android-chrome:20,desktop-chrome:25,desktop-edge:10,desktop-safari:10,desktop-firefox:10
```

### Traffic Attribution / Referrers
```
REFERRER_SOURCES=google,facebook,instagram,tiktok,direct,bing,yahoo,duckduckgo,linkedin,reddit
REFERRER_WEIGHTS=35,15,13,12,10,5,5,5,5,5
REFERRER_HEADER_URLS=google:https://www.google.com/,facebook:https://www.facebook.com/,instagram:https://www.instagram.com/,...
REFERRER_UTM_MEDIUMS=google:organic,facebook:paid-social,instagram:social,tiktok:social,direct:none,...
UTM_CAMPAIGN_DEFAULT=trafficgen
UTM_MEDIUM_DEFAULT=paid-social
```

### Page Behavior
```
PAGE_WAIT_UNTIL=load                  # load | domcontentloaded | networkidle
SCROLL_PROB=0.7                       # Probability of scrolling (70%)
SCROLL_DEPTH_MIN=0.35                 # Min scroll depth (35% of page)
SCROLL_DEPTH_MAX=0.9                  # Max scroll depth (90%)
SCROLL_STEPS_MIN=2                    # Min scroll segments
SCROLL_STEPS_MAX=6                    # Max scroll segments
POST_NAV_SETTLE_MIN_MS=2500           # Min wait after navigation
POST_NAV_SETTLE_MAX_MS=5000           # Max wait after navigation
```

### Navigation Hotspots
```
NAV_CATEGORY_WEIGHTS=Kitchen:35,Bath:30,Lighting:8,...
NAV_HOTSPOT_NAMES=Kitchen,Bath
NAV_HOTSPOT_EXTRA_CLICK_PROB=Kitchen:0.65,Bath:0.45
```

### Funnel Conversion Rates
```
FUNNEL_ADD_TO_CART_RATE=0.30          # 30% add to cart
FUNNEL_CHECKOUT_START_RATE=0.50       # 50% of ATC sessions start checkout
CHECKOUT_COMPLETE_RATE=0.3            # 30% of checkout starters complete
ALLOW_CHECKOUT=true                   # Enable/disable checkout flow
```

### Coverage / Heatmap
```
COVERAGE_RUN_PROB=0.15                # 15% of sessions run coverage clicks
COVERAGE_MAX_CLICKS=8                 # Max elements per coverage pass
COVERAGE_SELECTOR_ALLOW=.hero a,.featured a,button,...
COVERAGE_SELECTOR_BLOCK=logout,mailto,admin,...
```

### Localization
```
LOCALES=en-US,en-CA,en-GB,fr-CA
TIMEZONES=America/Toronto,America/New_York,America/Vancouver,Europe/London
```

### Checkout Test Card (BigCommerce)
```
BC_TEST_CARD_NUMBER=4111111111111111
BC_TEST_EXP=12/30
BC_TEST_CVV=123
BC_TEST_NAME=Test User
```

### Browser Health
```
BROWSER_MAX_SESSIONS=<auto>           # Default: 50 + 2*SPM*ASM
BROWSER_MAX_MINUTES=<auto>            # Default: 4 * AVG_SESSION_MINUTES
SESSION_MAX_SECONDS=<auto>            # Default: 120 * AVG_SESSION_MINUTES (min 30s)
BROWSER_FAILURE_THRESHOLD=5           # Consecutive failures before browser restart
SCHEDULER_FAILURE_THRESHOLD=10        # Failures before scheduler cooldown
SCHEDULER_COOLDOWN_SECONDS=15         # Cooldown duration
```

### Debug
```
DEBUG=1                               # Enable detailed logging
SMOKE=0                               # Smoke test (only 3 sessions)
```

## Setup & Running

### Shell Script (`run_with_venv.sh`)
```bash
#!/bin/bash
# 1. Detect Python 3.x
# 2. Create virtual environment (.venv/)
# 3. Install: playwright, pydantic, PyYAML, python-dotenv, tenacity, typer
# 4. Install Playwright Chromium browser
# 5. Run: python noibu-traffic-gen.py
```

### Manual Run
```bash
source .venv/bin/activate
python noibu-traffic-gen.py
```

## Key Design Patterns

1. **Weighted Random Selection** - Used everywhere: flows, devices, referrers, categories. All weights are normalized to sum to 100.
2. **Funnel Gating** - Conversion rates control progression: browse → ATC (30%) → checkout start (50%) → checkout complete (30%)
3. **Human Simulation** - Random waits, scroll depths, micro-jitter, think times, idle pauses
4. **Circuit Breaker** - Consecutive failures trigger browser restart, then scheduler cooldown
5. **Token Bucket QPS** - Global rate limiting to avoid overwhelming target site
6. **Graceful Lifecycle** - SIGINT/SIGTERM drain pending sessions; kill switch file check
7. **Browser Rotation** - Restart browser after N sessions or time to prevent memory leaks
8. **YAML-Driven Flows** - User journeys are declarative YAML, making it easy to add/modify flows without code changes

## What Was NOT Implemented

- No Docker containerization
- No automated tests
- No CI/CD pipeline
- No database or persistent state
- No proxy rotation support (proxy module existed but was unused)
- No multi-site support (single ORIGIN)
