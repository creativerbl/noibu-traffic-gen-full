import asyncio
import os
import random
import shutil
import subprocess
from typing import List, Optional

class ProxyProvider:
    """Interface. For PIA, we manipulate system/VPN outside browser proxies."""
    async def start(self): ...
    async def stop(self): ...
    async def rotate(self): ...
    def rotation_enabled(self) -> bool: return False

class NullProxyProvider(ProxyProvider):
    async def start(self): pass
    async def stop(self): pass
    async def rotate(self): pass
    def rotation_enabled(self) -> bool: return False

class PIAProxyProvider(ProxyProvider):
    """
    Controls Private Internet Access client via `piactl`.
    Assumes host has an authenticated PIA daemon. Docker containers typically
    cannot run `piactl`; prefer host-level execution or host network mode.
    """
    def __init__(self, piactl_path: str, regions: List[str]):
        self.piactl_path = piactl_path
        self.regions = [r for r in regions or [] if r]
        self.current = None

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run([self.piactl_path, *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)

    async def start(self):
        if not shutil.which(self.piactl_path):
            raise RuntimeError(f"piactl not found at {self.piactl_path}")
        # ensure connected
        self._run("connect")

    async def stop(self):
        self._run("disconnect")

    async def rotate(self):
        if not self.regions:
            # reconnect to random
            self._run("disconnect")
            await asyncio.sleep(1.0)
            self._run("connect")
            return
        region = random.choice(self.regions)
        self._run("disconnect")
        await asyncio.sleep(1.0)
        self._run("set", "region", region)
        await asyncio.sleep(0.5)
        self._run("connect")

    def rotation_enabled(self) -> bool:
        return True
