import time

from .config import Config
from .storage import open_backend


def _bar(frac: float, width: int = 30) -> str:
    # plain ascii progress bar, no unicode blocks
    frac = 0.0 if frac < 0 else (1.0 if frac > 1 else frac)
    fill = int(round(frac * width))
    return "[" + "#" * fill + "-" * (width - fill) + "]"


def _dur(secs: float) -> str:
    if secs < 90:
        return f"{secs:.0f}s"
    if secs < 5400:
        return f"{secs / 60:.0f}m"
    if secs < 36 * 3600:
        return f"{secs / 3600:.1f}h"
    return f"{secs / 86400:.1f}d"


async def _frontier(backend) -> dict | None:
    # pending vs handled lives in crawlee's request queue. in the mesh that table
    # sits in the same postgres db, so we can read it. on local sqlite it's a
    # separate file, so the query throws and we just skip the section.
    try:
        r = await backend.query(
            "SELECT COUNT(*) FILTER (WHERE is_handled) AS handled, "
            "COUNT(*) FILTER (WHERE NOT is_handled) AS pending, "
            "COUNT(*) AS total FROM request_queue_records")
        return {"handled": r[0]["handled"], "pending": r[0]["pending"], "total": r[0]["total"]}
    except Exception:
        return None


async def _snapshot(backend) -> dict:
    now = time.time()
    total = await backend.query("SELECT COUNT(*) AS n FROM corpus")
    last60 = await backend.query("SELECT COUNT(*) AS n FROM corpus WHERE fetched_at > $1", now - 60)
    last_text = await backend.query("SELECT COALESCE(SUM(text_len),0) AS n FROM corpus")
    domains = await backend.query(
        "SELECT domain, COUNT(*) AS n FROM corpus GROUP BY domain ORDER BY n DESC LIMIT 12")
    nodes = await backend.query("SELECT node_id, pages_done, rate, ts, status FROM node_heartbeat ORDER BY node_id")
    err_recent = await backend.query("SELECT COUNT(*) AS n FROM crawl_errors WHERE ts > $1", now - 300)
    err_kinds = await backend.query(
        "SELECT kind, COUNT(*) AS n FROM crawl_errors GROUP BY kind ORDER BY n DESC LIMIT 6")
    return {
        "now": now,
        "total": total[0]["n"],
        "last60": last60[0]["n"],
        "text_chars": last_text[0]["n"],
        "domains": domains,
        "nodes": nodes,
        "err_recent": err_recent[0]["n"],
        "err_kinds": err_kinds,
    }


def _render(s: dict) -> str:
    out = []
    out.append(f"corpus pages: {s['total']:,}   text: {s['text_chars']/1e6:.1f}M chars"
               f"   last 60s: {s['last60']} ({s['last60']/60:.1f}/s)")

    fr = s.get("frontier")
    if fr and fr["total"]:
        handled, pending, total = fr["handled"], fr["pending"], fr["total"]
        frac = handled / total if total else 0.0
        out.append("\nfrontier (how far through the discovered set):")
        out.append(f"  {_bar(frac)} {frac*100:5.1f}%")
        out.append(f"  handled {handled:>10,}    pending {pending:>10,}    "
                   f"discovered {total:>10,}")
        # drain = measured deltas between frontier refreshes (real throughput, not
        # the noisy per-node heartbeat sum). pending can rise even while crawling,
        # because each page found enqueues more, so the eta keys off the NET trend.
        d = s.get("drain")
        if d:
            hr, pr = d["handled_rate"], d["pending_rate"]  # pr>0 means pending growing
            if pr < -0.1:
                out.append(f"  crawling {hr:.1f}/s   pending {pr:+.1f}/s   "
                           f"~{_dur(pending / -pr)} to drain")
            elif pr > 0.1:
                out.append(f"  crawling {hr:.1f}/s   pending {pr:+.1f}/s   "
                           f"still expanding (finds links faster than it clears them)")
            else:
                out.append(f"  crawling {hr:.1f}/s   pending ~steady (near break-even)")
        else:
            out.append("  (live rate + eta appear after a few seconds of --watch)")

    out.append("\nnodes (the mesh):")
    if not s["nodes"]:
        out.append("  (none reporting yet)")
    for n in s["nodes"]:
        age = s["now"] - (n["ts"] or 0)
        stale = age >= 20
        live = "live" if not stale else f"stale {int(age)}s"
        # a stale node's last-reported rate is frozen, not current. zero it so it
        # doesn't look like a dead node is still pulling pages.
        rate = 0.0 if stale else (n["rate"] or 0)
        out.append(f"  {n['node_id']:<20} done {n['pages_done']:>9,}  "
                   f"{rate:6.1f}/s  [{n['status']}, {live}]")

    if s["domains"]:
        out.append("\ntop domains:")
        for d in s["domains"]:
            out.append(f"  {d['n']:>9,}  {d['domain']}")

    out.append(f"\nerrors (last 5m): {s['err_recent']}")
    for e in s["err_kinds"]:
        out.append(f"  {e['n']:>7,}  {e['kind']}")
    return "\n".join(out)


async def show_status(cfg: Config, watch: bool, export: str | None):
    backend = await open_backend(cfg)
    try:
        if export:
            await _export(backend, export)
            return
        if not watch:
            snap = await _snapshot(backend)
            snap["frontier"], snap["drain"] = await _frontier(backend), None
            print(_render(snap))
            return
        import asyncio
        from collections import deque
        # the frontier count is a full scan of millions of rows, far heavier than
        # the rest. refresh it on a slower beat so --watch doesn't hammer the db
        # the whole mesh leans on, while rates/totals still tick every 2s.
        # keep ~60s of samples and measure the rate end-to-end across them: at a
        # page or two a second a single 10s gap is pure noise (the net pending
        # trend flips sign), a minute-long window is steady.
        hist = deque(maxlen=7)
        frontier, frontier_t, drain = None, 0.0, None
        while True:
            snap = await _snapshot(backend)
            if snap["now"] - frontier_t >= 10:
                new = await _frontier(backend)
                if new:
                    hist.append((snap["now"], new["handled"], new["pending"]))
                    t0, h0, p0 = hist[0]
                    dt = snap["now"] - t0
                    if len(hist) >= 2 and dt > 0:
                        drain = {"handled_rate": (new["handled"] - h0) / dt,
                                 "pending_rate": (new["pending"] - p0) / dt}
                frontier, frontier_t = new, snap["now"]
            snap["frontier"], snap["drain"] = frontier, drain
            print("\033[2J\033[H", end="")  # clear screen
            print(f"crawler status  db={cfg.db}  {time.strftime('%H:%M:%S')}\n")
            print(_render(snap))
            await asyncio.sleep(2)
    finally:
        await backend.close()


async def _export(backend, path: str):
    import json
    rows = await backend.query(
        "SELECT url, domain, node_id, fetched_at, title, lang, extractor, "
        "text_len, text, structure FROM corpus")
    with open(path, "w") as f:
        for r in rows:
            struct = r.get("structure")
            if isinstance(struct, str):
                try:
                    r["structure"] = json.loads(struct)
                except Exception:
                    pass
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    print(f"exported {len(rows)} records to {path}")
