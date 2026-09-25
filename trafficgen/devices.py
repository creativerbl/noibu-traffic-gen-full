"""Browser context profile for the ab-test branch: desktop Chrome, Full HD.

The sticky checkout banner only renders at >=801px wide (flag
3393-desktop-cart-sticky-checkout-cta), so this branch runs desktop only.
"""

DESKTOP_CHROME = {
    "viewport": {"width": 1920, "height": 1080},
    "screen": {"width": 1920, "height": 1080},
    "user_agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122 Safari/537.36"
    ),
    "is_mobile": False,
    "has_touch": False,
    "device_scale_factor": 1.0,
}


def desktop_context_args() -> dict:
    return dict(DESKTOP_CHROME)
