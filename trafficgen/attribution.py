# trafficgen/attribution.py
"""Persona-based traffic attribution that is safe for CloudFront caching.

History: the clmod3 branch removed ALL Referer/UTM attribution because
CloudFront varies its cache on the Referer header, and cached cross-referrer
responses broke Noibu loading. This module re-adds attribution safely:

  * UTM query parameters are ALWAYS used for attribution (they are part of
    the cache key, so they can never serve a stale cross-referrer response).
  * The Referer header is OPTIONAL per persona (referer: true), and every
    session gets the clmod3 3-layer no-cache treatment so a Referer can
    never poison or be poisoned by the CDN cache:
      1. Browser launch args: --disable-cache --disk-cache-size=0 (runner.py)
      2. Context: service_workers="block" (session.py) + no-cache extra
         HTTP headers (apply_no_cache below)
      3. Route interception injecting no-cache headers on every request
         (apply_no_cache below)

Persona schema: see trafficgen/personas.yaml.
"""

import copy
import random
from typing import Dict, List, Optional
from urllib.parse import urlencode

import yaml

# Maps a utm_source slug to a plausible full referrer URL for the
# Referer header (only used when the persona sets `referer: true`).
DEFAULT_REFERER_URLS: Dict[str, str] = {
    "google": "https://www.google.com/",
    "bing": "https://www.bing.com/",
    "yahoo": "https://search.yahoo.com/",
    "duckduckgo": "https://duckduckgo.com/",
    "facebook": "https://www.facebook.com/",
    "instagram": "https://www.instagram.com/",
    "tiktok": "https://www.tiktok.com/",
    "linkedin": "https://www.linkedin.com/",
    "reddit": "https://www.reddit.com/",
    "klaviyo": "https://www.klaviyo.com/",
}


def load_personas(path: str) -> List[dict]:
    """Load persona definitions from a YAML file. Returns [] on failure."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception:
        return []
    personas = data.get("personas") if isinstance(data, dict) else data
    if not isinstance(personas, list):
        return []
    return [p for p in personas if isinstance(p, dict) and p.get("name")]


def choose_persona(personas: List[dict]) -> Optional[dict]:
    """Weighted random pick by `weight`.

    Returns a per-session COPY of the persona. If the persona's utm block
    uses `source_pool`, one source is resolved into `utm.source` here so that
    landing_url() and referer_header() agree for the whole session.
    """
    if not personas:
        return None
    weights = []
    for p in personas:
        try:
            w = max(float(p.get("weight", 0) or 0), 0.0)
        except Exception:
            w = 0.0
        weights.append(w)
    total = sum(weights)
    if total <= 0:
        picked = random.choice(personas)
    else:
        r, acc = random.uniform(0, total), 0.0
        picked = personas[-1]
        for p, w in zip(personas, weights):
            acc += w
            if r <= acc:
                picked = p
                break
    persona = copy.deepcopy(picked)
    utm = persona.get("utm")
    if isinstance(utm, dict):
        pool = utm.pop("source_pool", None)
        if pool and not utm.get("source"):
            utm["source"] = random.choice(list(pool))
    return persona


def landing_url(origin: str, persona: Optional[dict]) -> str:
    """Landing URL for a persona: origin plus utm_* query params (if any).

    UTM params are cache-key safe (unlike the Referer header), so they are
    always applied when the persona defines them.
    """
    base = (origin or "").rstrip("/") + "/"
    utm = (persona or {}).get("utm")
    if not isinstance(utm, dict):
        return base
    q = {}
    if utm.get("source"):
        q["utm_source"] = str(utm["source"])
    if utm.get("medium"):
        q["utm_medium"] = str(utm["medium"])
    if utm.get("campaign"):
        q["utm_campaign"] = str(utm["campaign"])
    if not q:
        return base
    sep = "&" if "?" in base else "?"
    return base + sep + urlencode(q)


def referer_header(persona: Optional[dict]) -> Optional[str]:
    """Full referrer URL (e.g. https://www.google.com/) when the persona
    opts in with `referer: true`; otherwise None (no Referer header)."""
    if not persona or not persona.get("referer"):
        return None
    utm = persona.get("utm") or {}
    source = (utm.get("source") or "").strip().lower()
    if not source:
        return None
    return DEFAULT_REFERER_URLS.get(source, f"https://www.{source}.com/")


async def apply_no_cache(context):
    """Context-level no-cache treatment (layers 2 + 3, ported from clmod3).

    Sends Cache-Control/Pragma no-cache headers on every request and
    intercepts all routes to force cache bypass at the network level.
    CloudFront varies cache on Referer; without this, a cached response
    fetched under one referrer could be served (without Noibu correctly
    loading) to a session with a different referrer.
    """
    await context.set_extra_http_headers({
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
    })

    async def _bypass_cache(route):
        headers = {**route.request.headers}
        headers["Cache-Control"] = "no-cache, no-store"
        headers["Pragma"] = "no-cache"
        await route.continue_(headers=headers)

    await context.route("**/*", _bypass_cache)
