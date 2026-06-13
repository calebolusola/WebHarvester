#!/usr/bin/env python3
"""how fast can this machine's ip pull nairaland pages, no db in the way.
handy for comparing ips (home vs vps vs proxy)."""
import asyncio
import time

from impit import AsyncClient
from lxml import html as H

SEED = "https://www.nairaland.com/politics"
CONCURRENCY = 10
N = 150


async def main():
    c = AsyncClient(browser="chrome", http3=False)

    # grab a batch of real thread urls off a board page
    r = await c.get(SEED, timeout=20)
    doc = H.fromstring(r.content)
    urls = []
    for a in doc.xpath("//a/@href"):
        head = a[1:].split("/")[0] if a.startswith("/") else ""
        if head.isdigit():                      # /<id>/<slug> = a thread
            urls.append("https://www.nairaland.com" + a)
    urls = list(dict.fromkeys(urls))[:N]
    print(f"fetching {len(urls)} threads at concurrency {CONCURRENCY} from this ip...")

    sem = asyncio.Semaphore(CONCURRENCY)
    c200 = c429 = cother = cerr = 0

    async def fetch(u):
        nonlocal c200, c429, cother, cerr
        async with sem:
            try:
                resp = await c.get(u, timeout=20)
                if resp.status_code == 200:
                    c200 += 1
                elif resp.status_code == 429:
                    c429 += 1
                else:
                    cother += 1
            except Exception:
                cerr += 1

    t0 = time.time()
    await asyncio.gather(*[fetch(u) for u in urls])
    dt = time.time() - t0

    print(f"\ndone in {dt:.1f}s")
    print(f"  200 OK : {c200}")
    print(f"  429    : {c429}   <- rate-limited")
    print(f"  other  : {cother}")
    print(f"  errors : {cerr}")
    print(f"  => {c200/dt*60:.0f} successful pages/min from THIS ip")


if __name__ == "__main__":
    asyncio.run(main())
