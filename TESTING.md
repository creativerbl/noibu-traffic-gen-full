# Supervised local test — unified branch

## Setup
```bash
cd noibu-traffic-gen-unified
cp .env .env.prod-backup && cp .env.test .env
./run_with_venv.sh        # creates .venv, installs deps + Chromium, starts
```
Browser windows will be visible (HEADLESS=0), ~2 sessions/min.

## What to verify (15–20 min run)
1. **Attribution**: log lines like `persona 'google_organic' landing: ...?utm_source=google&utm_medium=organic | referer=https://www.google.com/` and `document.referrer=...`. Direct loyalist sessions land on bare origin with no referer.
2. **No Noibu load errors**: pages load clean; no flood of `Failed to fetch ... csrf-protection-header` in session consoles (the old CloudFront/Referer poisoning symptom).
3. **Bounce sessions**: some sessions land, barely scroll, and close in 3–10s (paid_social/window_shopper most often).
4. **Cart + abandonment**: some sessions add to cart, reach checkout, fill 1–2 stages, then stop (`abandoning at stage=shipping` etc.).
5. **A completed order**: log shows `checkout COMPLETED`; summary line `atc=1 checkout=1 completed=1`. Expect roughly 1 order per few minutes max (ORDER_MIN_INTERVAL_SECONDS=60 + persona rolls).
6. After ~30 min, check Noibu console → Segments: orders should now appear under google/klaviyo/facebook utm_source — not just direct.

## If something breaks
Note the persona name + step from the log line and send it back to Claude with the traceback. Checkout selector issues will show as `checkout completion failed`.

## When satisfied
```bash
cp .env.prod-backup .env   # restore prod traffic shaping
git push origin unified    # publish the branch / open PR
```
