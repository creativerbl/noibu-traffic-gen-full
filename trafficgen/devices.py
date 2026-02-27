
from dataclasses import dataclass
import random
from typing import Optional

@dataclass
class DeviceChoice:
    name: str
    pw_name: Optional[str]

DEVICE_MAP = {
    "iphone-safari": "iPhone 14",
    "iphone-chrome": "iPhone 14",
    "android-chrome": "Pixel 7",
    "desktop-chrome": None,
    "desktop-edge": None,
    "desktop-safari": None,
    "desktop-firefox": None,
}

def build_device_pool(device_mix):
    pool = []
    for item in device_mix:
        name = item.get("name")
        weight = max(float(item.get("weight", 1.0)), 0.0)
        if name not in DEVICE_MAP or weight <= 0:
            continue
        for _ in range(int(weight)):
            pool.append(DeviceChoice(name=name, pw_name=DEVICE_MAP[name]))
    if not pool:
        pool.append(DeviceChoice(name="desktop-chrome", pw_name=None))
    return pool

def pick_device(pool, playwright):
    chosen = random.choice(pool)
    context_args = {}
    if chosen.pw_name:
        context_args.update(playwright.devices.get(chosen.pw_name, {}))
        if chosen.name == "iphone-chrome":
            context_args["user_agent"] = (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "CriOS/120.0.0.0 Mobile/15E148 Safari/604.1"
            )
    else:
        if chosen.name == "desktop-chrome":
            context_args.update({
                "viewport": {"width": 1366, "height": 864},
                "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/122 Safari/537.36",
                "is_mobile": False,
                "has_touch": False,
                "device_scale_factor": 1.0,
            })
        elif chosen.name == "desktop-edge":
            context_args.update({
                "viewport": {"width": 1440, "height": 900},
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120 Safari/537.36 Edg/120",
                "is_mobile": False,
                "has_touch": False,
                "device_scale_factor": 1.0,
            })
        elif chosen.name == "desktop-safari":
            context_args.update({
                "viewport": {"width": 1440, "height": 900},
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                              "Version/17.0 Safari/605.1.15",
                "is_mobile": False,
                "has_touch": False,
                "device_scale_factor": 2.0,
            })
        elif chosen.name == "desktop-firefox":
            context_args.update({
                "viewport": {"width": 1366, "height": 864},
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) "
                              "Gecko/20100101 Firefox/120.0",
                "is_mobile": False,
                "has_touch": False,
                "device_scale_factor": 1.0,
            })
        else:
            context_args.update({"viewport": {"width": 1280, "height": 800}})
    return {"context_args": context_args}
