import asyncio
import os
import time
from concurrent.futures import ProcessPoolExecutor

from crawlee import ConcurrencySettings, Glob
from crawlee.configuration import Configuration
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.http_clients import ImpitHttpClient
from crawlee.proxy_configuration import ProxyConfiguration
from crawlee.sessions import SessionPool

from .config import Config
from .corpus import BufferedCorpusWriter
from .extract import extract_record
from .storage import build_storage_client, open_backend


def _http_client(cfg: Config):
    # impit speaks a real browser's TLS + headers. first line of anti-blocking:
    # most captchas/blocks come from looking like a bot, not from solving puzzles.
    if cfg.browser.lower() == "none":
        return ImpitHttpClient()
    # http3=False on purpose: the http3/QUIC path in impit was throwing
    # intermittent connect errors under load. http2/1.1 is steadier.
    return ImpitHttpClient(browser=cfg.browser, http3=False)


def _proxy(cfg: Config):
    if cfg.proxy_tiers:
        return ProxyConfiguration(tiered_proxy_urls=[list(t) for t in cfg.proxy_tiers])
    if cfg.proxies:
        return ProxyConfiguration(proxy_urls=list(cfg.proxies))
    return None


# filesystems that don't give sqlite the locking/fsync it needs -> corruption
BAD_FS = {"exfat", "msdos", "vfat", "fat", "fat32", "smbfs", "nfs", "cifs"}


def _fstype(path: str) -> str | None:
    import re
    import subprocess
    real = os.path.realpath(path)
    while real and not os.path.exists(real):
        real = os.path.dirname(real)
    try:
        out = subprocess.run(["/sbin/mount"], capture_output=True, text=True, timeout=5).stdout
    except Exception:
        return None
    best_mp, best_fs = "", None
    for line in out.splitlines():
        m = re.match(r"\S+ on (.+?) \(([^,)]+)", line)
        if not m:
            continue
        mp, fs = m.group(1), m.group(2).strip().lower()
        if real == mp or real.startswith(mp.rstrip("/") + "/"):
            if len(mp) >= len(best_mp):
                best_mp, best_fs = mp, fs
    return best_fs


def _guard_filesystem(cfg: Config):
    if cfg.is_postgres:
        return  # db is remote, local fs doesn't matter for the frontier
    fs = _fstype(cfg.base_dir)
    if fs in BAD_FS:
        raise SystemExit(
            f"\nrefusing to run: {cfg.base_dir} is on a '{fs}' filesystem.\n"
            f"sqlite (the frontier + corpus) needs real file locking/fsync, which\n"
            f"{fs} does not provide -> the database corrupts mid-write ('disk image\n"
            f"is malformed'). that is exactly what just happened.\n\n"
            f"fix one of:\n"
            f"  - reformat the SSD to APFS (mac-native). for the mesh this is fine:\n"
            f"    only the host mac touches the disk, the dell talks to postgres over\n"
            f"    the LAN, never the filesystem.\n"
            f"  - or point --base-dir at a local APFS path (e.g. ~/crawler-data).\n"
        )


def _safe_stats(crawler):
    # statistics.state raises if the run died before stats loaded. don't let
    # that mask the real error or crash shutdown
    try:
        st = crawler.statistics.state
        return st.requests_finished, st.requests_failed
    except Exception:
        return 0, 0


def _domain_globs(cfg: Config):
    # turn allowed_domains into include globs so enqueue_links enforces bounding
    globs = [Glob(f"*://{d}/*") for d in cfg.allowed_domains]
    globs += [Glob(f"*://*.{d}/*") for d in cfg.allowed_domains]
    return globs


_throttle_patched = False


def _patch_throttle_overflow():
    # crawlee's ThrottlingRequestManager computes base_delay * 2**(count-1) BEFORE
    # capping to max_delay. on a long 429 streak (an IP penalty box) that int
    # overflows -> "Python int too large to convert to C int", and the backoff
    # turns into useless retry spam. clamp the streak so the math can't overflow.
    global _throttle_patched
    if _throttle_patched:
        return
    from crawlee.request_loaders import ThrottlingRequestManager
    _orig = ThrottlingRequestManager.record_domain_delay

    def _safe(self, url, *, retry_after=None):
        state = self._get_domain_state(url)
        if state is not None and state.consecutive_429_count > 24:
            state.consecutive_429_count = 24   # 2**24 * base_delay stays in timedelta range
        return _orig(self, url, retry_after=retry_after)

    ThrottlingRequestManager.record_domain_delay = _safe
    _throttle_patched = True


def _throttle_domains(cfg: Config) -> list[str]:
    # which domains to apply per-domain backoff to. explicit bound wins,
    # else derive from the seed hostnames
    from urllib.parse import urlsplit
    if cfg.allowed_domains:
        return list(cfg.allowed_domains)
    seen = []
    for s in cfg.seeds:
        h = (urlsplit(s).hostname or "").lower()
        if h and h not in seen:
            seen.append(h)
    return seen


async def run_crawl(cfg: Config):
    _guard_filesystem(cfg)
    os.makedirs(cfg.storage_dir, exist_ok=True)

    configuration = Configuration(
        storage_dir=cfg.storage_dir,
        purge_on_start=bool(cfg.fresh),
        log_level="WARNING",  # no per-page logging on the hot path
    )
    storage_client = build_storage_client(cfg)

    backend = await open_backend(cfg)
    await backend.init_schema()
    if cfg.fresh:
        await backend.reset()   # --fresh wipes corpus too, not just the frontier
    writer = BufferedCorpusWriter(backend, cfg.flush_max_records, cfg.flush_max_seconds)
    writer.start()

    # parsing/extraction runs across all cores, off the event loop
    n_cores = cfg.parse_workers or os.cpu_count() or 4
    pool = ProcessPoolExecutor(max_workers=n_cores)

    concurrency = ConcurrencySettings(
        desired_concurrency=cfg.concurrency,
        max_concurrency=cfg.max_concurrency,
        max_tasks_per_minute=cfg.rate if cfg.rate > 0 else float("inf"),
    )

    kwargs = dict(
        configuration=configuration,
        storage_client=storage_client,
        concurrency_settings=concurrency,
        max_request_retries=cfg.retries,
        max_requests_per_crawl=cfg.max_pages,
        max_crawl_depth=cfg.max_depth,
        respect_robots_txt_file=cfg.respect_robots,
        # self-healing: rotate fingerprint+cookies on blocks, keep going
        use_session_pool=True,
        session_pool=SessionPool(max_pool_size=cfg.session_pool_size),
        max_session_rotations=cfg.max_session_rotations,
        retry_on_blocked=True,
        keep_alive=cfg.keep_alive,
    )
    proxy = _proxy(cfg)
    if proxy is not None:
        kwargs["proxy_configuration"] = proxy

    # per-domain backoff that honors 429 + robots crawl-delay. stops the 429 storm.
    if cfg.throttle and (domains := _throttle_domains(cfg)):
        from datetime import timedelta
        from crawlee.request_loaders import ThrottlingRequestManager
        from crawlee.storages import RequestQueue
        _patch_throttle_overflow()
        queue = await RequestQueue.open(configuration=configuration, storage_client=storage_client)
        kwargs["request_manager"] = ThrottlingRequestManager(
            inner=queue,
            domains=domains,
            request_manager_opener=RequestQueue.open,
            base_delay=timedelta(seconds=cfg.domain_base_delay),
            max_delay=timedelta(seconds=cfg.domain_max_delay),
        )

    if cfg.render:
        # adaptive: cheap http first, real chromium only when js changes the page
        from crawlee.crawlers import AdaptivePlaywrightCrawler
        crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
            playwright_crawler_specific_kwargs={"headless": True}, **kwargs)
    else:
        crawler = BeautifulSoupCrawler(http_client=_http_client(cfg), **kwargs)

    include = [Glob(g) for g in cfg.include] + _domain_globs(cfg)
    exclude = [Glob(g) for g in cfg.exclude]
    strategy = cfg.strategy

    @crawler.router.default_handler
    async def handler(context: BeautifulSoupCrawlingContext):
        # discover links cheaply (crawlee handles depth, strategy, dedup, globs)
        await context.enqueue_links(
            strategy=strategy,
            include=include or None,
            exclude=exclude or None,
        )
        # heavy lifting (parse + structure) goes to the process pool.
        # in render mode the rendered DOM is in parsed_content, not the static body
        if cfg.render and getattr(context, "parsed_content", None) is not None:
            html = str(context.parsed_content).encode("utf-8", "replace")
        else:
            html = await context.http_response.read()
        loop = asyncio.get_running_loop()
        record = await loop.run_in_executor(
            pool, extract_record, context.request.url, html,
            context.http_response.status_code, cfg.store_html,
        )
        record["domain"] = (context.request.url.split("/")[2] if "//" in context.request.url else "")
        record["node_id"] = cfg.node_id
        record["fetched_at"] = time.time()
        await writer.add(record)

    @crawler.failed_request_handler
    async def on_failed(context, error):
        # log and move on, never crash the run. this is the set-and-forget part
        writer.add_error(context.request.url, cfg.node_id, type(error).__name__, error)

    # heartbeat so observability sees this node live, without hot-path logging
    hb_stop = asyncio.Event()

    async def heartbeat():
        last_done, last_t = 0, time.monotonic()
        while not hb_stop.is_set():
            await asyncio.wait([asyncio.create_task(hb_stop.wait())], timeout=5)
            done, _ = _safe_stats(crawler)
            now = time.monotonic()
            rate = (done - last_done) / max(0.001, now - last_t)
            last_done, last_t = done, now
            try:
                await backend.heartbeat(cfg.node_id, done, rate, "running")
            except Exception:
                pass

    hb_task = asyncio.create_task(heartbeat())

    print(f"[{cfg.node_id}] crawling. db={cfg.db} mode={'render' if cfg.render else 'http'} "
          f"strategy={strategy} concurrency={cfg.concurrency} cores={n_cores} "
          f"robots={cfg.respect_robots}")
    print("ctrl-c to stop. state is durable, rerun to resume.\n")

    try:
        await crawler.run(list(cfg.seeds))
    finally:
        hb_stop.set()
        await hb_task
        await writer.stop()           # flush remaining corpus before exit
        done, _ = _safe_stats(crawler)
        try:
            await backend.heartbeat(cfg.node_id, done, 0, "stopped")
        except Exception:
            pass
        await backend.close()
        pool.shutdown(wait=True, cancel_futures=True)

    done, failed = _safe_stats(crawler)
    print(f"\n[{cfg.node_id}] done. finished={done} "
          f"failed={failed} corpus_written={writer.written}")

