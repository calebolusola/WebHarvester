import argparse
import asyncio

from . import config as configmod


def _split_commas(values):
    # --proxy-tier "a,b" --proxy-tier "c" -> (("a","b"), ("c",))
    return tuple(tuple(v.split(",")) for v in values) if values else None


def _overrides(args) -> dict:
    # only keys the user actually set (non-None) so toml/env still win otherwise
    m = {
        "base_dir": args.base_dir,
        "db": args.db,
        "node_id": args.node_id,
        "seeds": tuple(args.seed) if args.seed else None,
        "strategy": args.strategy,
        "allowed_domains": tuple(args.allow_domain) if args.allow_domain else None,
        "include": tuple(args.include) if args.include else None,
        "exclude": tuple(args.exclude) if args.exclude else None,
        "max_depth": args.max_depth,
        "max_pages": args.max_pages,
        "concurrency": args.concurrency,
        "max_concurrency": args.max_concurrency,
        "rate": args.rate,
        "parse_workers": args.parse_workers,
        "render": args.render,
        "flush_max_records": args.flush_records,
        "flush_max_seconds": args.flush_seconds,
        "store_html": args.store_html,
        "store_generic": args.store_generic,
        "unlogged_corpus": args.unlogged_corpus,
        "retries": args.retries,
        "max_session_rotations": args.max_session_rotations,
        "session_pool_size": args.session_pool_size,
        "keep_alive": args.keep_alive,
        "throttle": args.throttle,
        "domain_base_delay": args.domain_delay,
        "browser": args.browser,
        "proxies": tuple(args.proxy) if args.proxy else None,
        "proxy_tiers": _split_commas(args.proxy_tier),
        "proxies_file": args.proxy_file,
        "fresh": args.fresh,
    }
    if args.no_robots:
        m["respect_robots"] = False
    return {k: v for k, v in m.items() if v is not None}


def _add_crawl_flags(p):
    p.add_argument("--config", help="toml config file")
    p.add_argument("--seed", action="append", help="starting url, repeatable")
    p.add_argument("--base-dir", help="where runtime data lives (default the SSD)")
    p.add_argument("--db", help="'sqlite' (default, on SSD) or a postgres dsn for the mesh")
    p.add_argument("--node-id", help="name for this machine in observability (default hostname)")
    p.add_argument("--strategy", choices=["all", "same-domain", "same-hostname", "same-origin"])
    p.add_argument("--same-site", dest="strategy", action="store_const", const="same-domain",
                   help="shortcut for --strategy same-domain (stay on the seed's site)")
    p.add_argument("--allow-domain", action="append", help="bound crawl to these domains, repeatable")
    p.add_argument("--include", action="append", help="url glob that must match, repeatable")
    p.add_argument("--exclude", action="append", help="url glob that must not match, repeatable")
    p.add_argument("--max-depth", type=int)
    p.add_argument("--max-pages", type=int)
    p.add_argument("--concurrency", type=int)
    p.add_argument("--max-concurrency", type=int)
    p.add_argument("--rate", type=int, help="requests/min cap, 0 = uncapped")
    p.add_argument("--parse-workers", type=int, help="extraction process pool size, 0 = all cores")
    p.add_argument("--render", action="store_const", const=True, default=None,
                   help="adaptive playwright fallback for js-heavy pages")
    p.add_argument("--flush-records", type=int)
    p.add_argument("--flush-seconds", type=float)
    p.add_argument("--store-html", action="store_const", const=True, default=None)
    p.add_argument("--store-generic", action="store_const", const=True, default=None,
                   help="on sites with their own extractor, also store pages it can't parse")
    p.add_argument("--unlogged-corpus", action="store_const", const=True, default=None)
    p.add_argument("--retries", type=int)
    p.add_argument("--max-session-rotations", type=int)
    p.add_argument("--session-pool-size", type=int)
    p.add_argument("--keep-alive", action="store_const", const=True, default=None,
                   help="mesh: wait for peers instead of exiting when queue empties")
    p.add_argument("--no-throttle", dest="throttle", action="store_const", const=False, default=None,
                   help="disable per-domain 429/crawl-delay backoff (will storm strict sites)")
    p.add_argument("--domain-delay", type=float, help="starting per-domain gap in seconds (default 1.0)")
    p.add_argument("--no-robots", action="store_true", help="ignore robots.txt (careful)")
    p.add_argument("--browser", help="impit impersonation target (default chrome), or 'none'")
    p.add_argument("--proxy", action="append", help="proxy url to rotate, repeatable")
    p.add_argument("--proxy-tier", action="append", help="tier of comma-separated proxies, repeatable")
    p.add_argument("--proxy-file", help="file of proxy urls, one per line")
    p.add_argument("--fresh", action="store_const", const=True, default=None, help="wipe state, start over")


async def _cmd_crawl(args):
    from .crawl import run_crawl
    cfg = configmod.load(args.config, **_overrides(args))
    if not cfg.seeds and cfg.fresh:
        raise SystemExit("--fresh with no --seed would crawl nothing. pass --seed URL")
    await run_crawl(cfg)


async def _cmd_status(args):
    from .observe import show_status
    cfg = configmod.load(args.config, **_overrides_min(args))
    await show_status(cfg, watch=args.watch, export=args.export)


async def _cmd_initdb(args):
    from .storage import open_backend
    cfg = configmod.load(args.config, **_overrides_min(args))
    backend = await open_backend(cfg)
    await backend.init_schema()
    await backend.close()
    print(f"schema ready on {cfg.db}")


async def _cmd_reconcile(args):
    # re-enqueue urls that previously failed so a future run retries them
    from crawlee.configuration import Configuration
    from crawlee.storages import RequestQueue
    from .storage import build_storage_client, open_backend
    cfg = configmod.load(args.config, **_overrides_min(args))
    backend = await open_backend(cfg)
    failed = await backend.query("SELECT DISTINCT url FROM crawl_errors")
    await backend.close()
    urls = [r["url"] for r in failed if r["url"] and r["url"].startswith("http")]
    if not urls:
        print("nothing to reconcile")
        return
    configuration = Configuration(storage_dir=cfg.storage_dir, purge_on_start=False)
    rq = await RequestQueue.open(configuration=configuration, storage_client=build_storage_client(cfg))
    await rq.add_requests(urls)
    print(f"re-enqueued {len(urls)} previously-failed urls")


# crawlee keeps the depth inside the stored request json, not in its own column
_DEPTH = "COALESCE((data::jsonb #>> '{user_data,__crawlee,crawlDepth}')::int, 0)"


async def _cmd_reopen(args):
    # mark already-handled pages at the edge of a depth-capped crawl as pending
    # again. crawlee drops links past max_depth, so those pages never queued their
    # children. refetching them with a higher (or no) cap picks the links back up,
    # and dedup still skips every page that's already been crawled.
    import asyncpg
    from .storage import _asyncpg_dsn
    cfg = configmod.load(args.config, **_overrides_min(args))
    if not cfg.is_postgres:
        raise SystemExit("reopen only works against the postgres mesh for now")
    con = await asyncpg.connect(_asyncpg_dsn(cfg.db), command_timeout=None)
    try:
        depth = args.depth
        if depth is None:
            depth = await con.fetchval(f"SELECT MAX({_DEPTH}) FROM request_queue_records")
        n = await con.fetchval(
            f"SELECT COUNT(*) FROM request_queue_records WHERE is_handled AND {_DEPTH} >= $1", depth)
        print(f"{n:,} handled pages at depth >= {depth}")
        if args.dry_run or not n:
            return
        async with con.transaction():
            res = await con.execute(
                "UPDATE request_queue_records "
                "SET is_handled = false, time_blocked_until = NULL, client_key = NULL "
                f"WHERE is_handled AND {_DEPTH} >= $1", depth)
            # crawlee decides a queue is empty from the counters on its metadata
            # row, not the records. rebuild them or nodes think there's no work.
            await con.execute(
                "UPDATE request_queues q SET "
                "pending_request_count = (SELECT COUNT(*) FROM request_queue_records r "
                "  WHERE r.request_queue_id = q.request_queue_id AND NOT r.is_handled), "
                "handled_request_count = (SELECT COUNT(*) FROM request_queue_records r "
                "  WHERE r.request_queue_id = q.request_queue_id AND r.is_handled), "
                "total_request_count = (SELECT COUNT(*) FROM request_queue_records r "
                "  WHERE r.request_queue_id = q.request_queue_id)")
        print(f"reopened {int(res.split()[-1]):,} pages. restart the crawlers to pick them up")
    finally:
        await con.close()


def _overrides_min(args):
    # status/init-db/reconcile only need to locate the data
    m = {"base_dir": getattr(args, "base_dir", None), "db": getattr(args, "db", None)}
    return {k: v for k, v in m.items() if v is not None}


def main():
    parser = argparse.ArgumentParser(prog="crawler",
                                     description="distributed, resumable, set-and-forget web crawler")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("crawl", help="start or resume a crawl")
    _add_crawl_flags(p)
    p.set_defaults(func=_cmd_crawl)

    p = sub.add_parser("status", help="peek at progress (whole mesh)")
    p.add_argument("--config")
    p.add_argument("--base-dir")
    p.add_argument("--db")
    p.add_argument("--watch", action="store_true", help="live refresh")
    p.add_argument("--export", help="dump corpus to this jsonl file")
    p.set_defaults(func=_cmd_status)

    p = sub.add_parser("init-db", help="create corpus/errors/heartbeat tables")
    p.add_argument("--config")
    p.add_argument("--base-dir")
    p.add_argument("--db")
    p.set_defaults(func=_cmd_initdb)

    p = sub.add_parser("reconcile", help="re-enqueue previously-failed urls")
    p.add_argument("--config")
    p.add_argument("--base-dir")
    p.add_argument("--db")
    p.set_defaults(func=_cmd_reconcile)

    p = sub.add_parser("reopen", help="re-queue pages at the depth edge so a deeper crawl can continue")
    p.add_argument("--config")
    p.add_argument("--base-dir")
    p.add_argument("--db")
    p.add_argument("--depth", type=int, help="reopen handled pages at this depth or deeper (default: the deepest reached)")
    p.add_argument("--dry-run", action="store_true", help="just count, change nothing")
    p.set_defaults(func=_cmd_reopen)

    args = parser.parse_args()
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
