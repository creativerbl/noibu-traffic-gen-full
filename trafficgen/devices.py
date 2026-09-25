"""Desktop browser profiles for the ab-test branch.

The sticky checkout banner only renders at >=801px wide (flag
3393-desktop-cart-sticky-checkout-cta), so every profile is desktop at
1920x1080. Each engine gets a user agent that matches it, so the site sees
a consistent browser rather than, say, Firefox claiming to be Chrome.
"""

VIEWPORT = {"width": 1920, "height": 1080}

PROFILES = {
    "chromium": {
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        ),
        "is_mobile": False,
        "has_touch": False,
        "device_scale_factor": 1.0,
    },
    # Firefox rejects is_mobile/has_touch, so they are left out.
    "firefox": {
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) "
            "Gecko/20100101 Firefox/131.0"
        ),
    },
    "webkit": {
        "user_agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/18.0 Safari/605.1.15"
        ),
        "is_mobile": False,
        "has_touch": False,
        "device_scale_factor": 1.0,
    },
}

ENGINES = tuple(PROFILES)


def context_args(engine: str) -> dict:
    args = dict(PROFILES.get(engine, PROFILES["chromium"]))
    args["viewport"] = dict(VIEWPORT)
    args["screen"] = dict(VIEWPORT)
    return args
