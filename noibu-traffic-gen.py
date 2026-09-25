# noibu-traffic-gen.py — entry point for the ab-test branch.
# Reads .env from the working directory; exported shell variables win.
import asyncio

from dotenv import load_dotenv

load_dotenv(override=False)

from trafficgen.runner import Runner, RunnerConfig  # noqa: E402  (after .env)


def main():
    cfg = RunnerConfig.from_env()
    print(f">> ab-test traffic generator → {cfg.origin}", flush=True)
    try:
        asyncio.run(Runner(cfg).run())
    except KeyboardInterrupt:
        print("Forced exit.", flush=True)


if __name__ == "__main__":
    main()
