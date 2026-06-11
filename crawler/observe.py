import time

from .config import Config
from .storage import open_backend


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

    out.append("\nnodes (the mesh):")
    if not s["nodes"]:
        out.append("  (none reporting yet)")
    for n in s["nodes"]:
        age = s["now"] - (n["ts"] or 0)
        live = "live" if age < 20 else f"stale {int(age)}s"
        out.append(f"  {n['node_id']:<20} done {n['pages_done']:>9,}  "
                   f"{(n['rate'] or 0):6.1f}/s  [{n['status']}, {live}]")

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
            print(_render(await _snapshot(backend)))
            return
        import asyncio
        while True:
            snap = await _snapshot(backend)
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
