import os
import socket
from dataclasses import dataclass, field, replace

try:
    import tomllib
except ModuleNotFoundError:        # python < 3.11 (e.g. ubuntu 22.04)
    import tomli as tomllib

DEFAULT_BASE_DIR = "/Volumes/MainSSD/CRAWLER"


@dataclass(frozen=True)
class Config:
    # where all runtime data lives. on the SSD by default
    base_dir: str = DEFAULT_BASE_DIR

    # frontier / shared-state backend.
    #   "sqlite"  -> a local file under base_dir, single machine, zero infra
    #   a dsn     -> postgresql+asyncpg://user:pass@host:5432/crawler  (mesh)
    db: str = "sqlite"

    # who am i. shows up in observability so you can tell the dell from the mac
    node_id: str = field(default_factory=socket.gethostname)

    # what to follow
    seeds: tuple = ()
    strategy: str = "all"          # all | same-domain | same-hostname | same-origin
    allowed_domains: tuple = ()    # extra bounding on top of strategy
    include: tuple = ()            # url globs that must match
    exclude: tuple = ()            # url globs that must not match
    max_depth: int | None = None
    max_pages: int | None = None

    # performance
    concurrency: int = 50          # desired in-flight requests
    max_concurrency: int = 200
    rate: int = 0                  # requests/min cap, 0 = uncapped
    parse_workers: int = 0         # process pool size for extraction, 0 = cpu_count
    render: bool = False           # adaptive playwright fallback for js pages

    # corpus buffered writer (keeps DB writes off the hot path)
    flush_max_records: int = 500   # flush when buffer hits this many
    flush_max_seconds: float = 2.0 # ...or this long since last flush
    store_html: bool = False       # also keep raw gzipped html
    unlogged_corpus: bool = False  # postgres UNLOGGED corpus table, faster, less durable

    # self healing / set-and-forget
    retries: int = 8
    max_session_rotations: int = 12
    session_pool_size: int = 1000
    keep_alive: bool = False       # mesh: set true so a node waits for peers instead of exiting on empty
    respect_robots: bool = False

    # politeness: per-domain backoff that honors HTTP 429 + robots crawl-delay.
    # this is what stops the "429 storm". on a single site with one IP it
    # effectively serializes that domain, so use proxies if you want speed there.
    throttle: bool = True
    domain_base_delay: float = 1.0   # starting per-domain gap (seconds)
    domain_max_delay: float = 60.0   # cap after repeated 429 backoff

    # anti blocking
    browser: str = "chrome"        # impit impersonation target, or "none"
    proxies: tuple = ()
    proxy_tiers: tuple = ()        # tuple of tuples, escalates when blocked

    fresh: bool = False            # wipe state and start over

    @property
    def storage_dir(self) -> str:
        return os.path.join(self.base_dir, "storage")

    @property
    def is_postgres(self) -> bool:
        return self.db != "sqlite" and "postgres" in self.db

    @property
    def connection_string(self) -> str | None:
        # None lets crawlee's SqlStorageClient default to sqlite in storage_dir
        if self.db == "sqlite":
            return None
        return self.db


def _coerce(value, kind):
    if kind is bool and isinstance(value, str):
        return value.lower() in ("1", "true", "yes", "on")
    if kind in (tuple,) and isinstance(value, list):
        return tuple(value)
    return value


def load(path: str | None = None, **overrides) -> Config:
    # precedence: defaults < toml file < env (CRAWLER_*) < explicit overrides (cli)
    data: dict = {}

    if path and os.path.exists(path):
        with open(path, "rb") as f:
            data.update(tomllib.load(f))

    fields = {f.name: f.type for f in Config.__dataclass_fields__.values()}
    for name in fields:
        env = os.environ.get("CRAWLER_" + name.upper())
        if env is not None:
            data[name] = env

    for k, v in overrides.items():
        if v is not None:
            data[k] = v

    # normalize list-ish + bool-ish values coming from toml/env/cli
    clean = {}
    for k, v in data.items():
        if k not in Config.__dataclass_fields__:
            continue
        if k in ("seeds", "allowed_domains", "include", "exclude", "proxies", "proxy_tiers") and isinstance(v, list):
            v = tuple(tuple(x) if isinstance(x, list) else x for x in v)
        if isinstance(v, str) and Config.__dataclass_fields__[k].type is bool:
            v = v.lower() in ("1", "true", "yes", "on")
        clean[k] = v

    cfg = Config(**clean)
    if not cfg.node_id:
        cfg = replace(cfg, node_id=socket.gethostname())
    return cfg
