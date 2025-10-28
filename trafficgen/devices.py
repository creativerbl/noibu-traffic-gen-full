from dataclasses import dataclass
from typing import List, Dict, Any
import random

@dataclass
class DevicePreset:
    name: str
    pw_name: str  # Playwright builtin name
    weight: float

# mapping of our friendly names to Playwright device descriptors
DEVICE_MAP = {
    "iphone-14": "iPhone 14",
    "android-pixel": "Pixel 7",
    "ipad": "iPad (gen 7)",
    "desktop-chrome": None,   # custom
    "desktop-edge": None,     # custom
}

DEFAULTS = [
    DevicePreset("iphone-14", "iPhone 14", 0.30),
    DevicePreset("android-pixel", "Pixel 7", 0.20),
    DevicePreset("ipad", "iPad (gen 7)", 0.10),
    DevicePreset("desktop-chrome", None, 0.25),
    DevicePreset("desktop-edge", None, 0.15),
]

def build_device_pool(config_mix: List[dict]) -> List[DevicePreset]:
    if not config_mix:
        return DEFAULTS
    pool: List[DevicePreset] = []
    for item in config_mix:
        nm = item.get("name")
        wt = float(item.get("weight", 1.0))
        pw_name = DEVICE_MAP.get(nm)
        if nm:
            pool.append(DevicePreset(nm, pw_name, wt))
    return pool

def pick_device(pool: List[DevicePreset], playwright) -> Dict[str, Any]:
    total_w = sum(max(d.weight, 0.0) for d in pool) or 1.0
    r = random.uniform(0, total_w)
    acc = 0.0
    chosen = pool[-1]
    for d in pool:
        acc += max(d.weight, 0.0)
        if r <= acc:
            chosen = d
            break

    # Start from PW builtin descriptor if present
    context_args = {}
    if chosen.pw_name:
        context_args.update(playwright.devices.get(chosen.pw_name, {}))
    else:
        # desktop presets
        if chosen.name == "desktop-chrome":
            context_args.update({
                "viewport": {"width": 1366, "height": 864},
                "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/122 Safari/537.36",
                "device_scale_factor": 1.0,
                "is_mobile": False,
                "has_touch": False
            })
        elif chosen.name == "desktop-edge":
            context_args.update({
                "viewport": {"width": 1440, "height": 900},
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/120 Safari/537.36 Edg/120",
                "device_scale_factor": 1.0,
                "is_mobile": False,
                "has_touch": False
            })
        else:
            context_args.update({"viewport": {"width": 1280, "height": 800}})

    return {"preset": chosen, "context_args": context_args}
