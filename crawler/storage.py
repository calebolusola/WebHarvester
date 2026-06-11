import json
import os
import time

from crawlee.storage_clients import SqlStorageClient

from .config import Config


def _sqlite_engine(path: str):
    # our own engine so we can set pragmas crawlee won't. critically mmap_size=0:
    # SQLite mmaps the db by default, and on an external/USB SSD an I/O hiccup on
    # an mmap'd page is an unrecoverable SIGBUS (bus error) that kills the process.
    # plain read()/write() turn the same hiccup into a normal, retryable error.
    import os as _os

    from sqlalchemy import event
    from sqlalchemy.ext.asyncio import create_async_engine

    _os.makedirs(_os.path.dirname(path), exist_ok=True)
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")

    # crawlee normally sets these pragmas itself, but ONLY when it creates the
    # engine from a connection_string. by passing our own engine we bypass that,
    # so we re-apply crawlee's tuning here, with the one change that matters:
    # mmap_size=0 instead of its 256MB (the 256MB mmap is what SIGBUS'd on the SSD).
    @event.listens_for(engine.sync_engine, "connect")
    def _pragmas(dbapi_con, _rec):
        cur = dbapi_con.cursor()
        cur.execute("PRAGMA mmap_size=0")          # THE fix: no mmap -> no SIGBUS
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA cache_size=100000")    # 100MB page cache (in RAM, safe)
        cur.execute("PRAGMA temp_store=MEMORY")
        cur.execute("PRAGMA foreign_keys=ON")      # crawlee's schema uses FKs
        cur.execute("PRAGMA busy_timeout=60000")
        cur.close()

    return engine


def build_storage_client(cfg: Config) -> SqlStorageClient:
    # this backs crawlee's request queue = the shared frontier + dedup.
    # postgres dsn -> shared across the mesh. sqlite -> a file on the SSD with
    # mmap disabled (see _sqlite_engine) so a USB hiccup can't SIGBUS the crawl.
    if cfg.is_postgres:
        return SqlStorageClient(connection_string=cfg.connection_string)
    return SqlStorageClient(engine=_sqlite_engine(os.path.join(cfg.storage_dir, "crawlee.db")))


def _asyncpg_dsn(dsn: str) -> str:
    # crawlee/sqlalchemy want postgresql+asyncpg://, asyncpg.connect wants postgresql://
    return dsn.replace("+asyncpg", "")


# ---- our own tables (corpus, errors, heartbeat) -------------------------------
# kept separate from crawlee's frontier tables. written through the buffered
# writer so DB latency never touches the crawl hot path.

PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS corpus (
    url          TEXT PRIMARY KEY,
    domain       TEXT,
    node_id      TEXT,
    fetched_at   DOUBLE PRECISION,
    http_status  INTEGER,
    title        TEXT,
    lang         TEXT,
    extractor    TEXT,
    text         TEXT,
    text_len     INTEGER,
    structure    JSONB,
    content_hash TEXT,
    html         BYTEA
);
CREATE INDEX IF NOT EXISTS idx_corpus_domain ON corpus(domain);
CREATE INDEX IF NOT EXISTS idx_corpus_fetched ON corpus(fetched_at);
CREATE INDEX IF NOT EXISTS idx_corpus_hash ON corpus(content_hash);

CREATE TABLE IF NOT EXISTS crawl_errors (
    id       BIGSERIAL PRIMARY KEY,
    url      TEXT,
    node_id  TEXT,
    ts       DOUBLE PRECISION,
    kind     TEXT,
    message  TEXT
);
CREATE INDEX IF NOT EXISTS idx_errors_ts ON crawl_errors(ts);

CREATE TABLE IF NOT EXISTS node_heartbeat (
    node_id     TEXT PRIMARY KEY,
    ts          DOUBLE PRECISION,
    pages_done  BIGINT,
    rate        DOUBLE PRECISION,
    status      TEXT
);
"""

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS corpus (
    url          TEXT PRIMARY KEY,
    domain       TEXT,
    node_id      TEXT,
    fetched_at   REAL,
    http_status  INTEGER,
    title        TEXT,
    lang         TEXT,
    extractor    TEXT,
    text         TEXT,
    text_len     INTEGER,
    structure    TEXT,
    content_hash TEXT,
    html         BLOB
);
CREATE INDEX IF NOT EXISTS idx_corpus_domain ON corpus(domain);
CREATE INDEX IF NOT EXISTS idx_corpus_fetched ON corpus(fetched_at);
CREATE INDEX IF NOT EXISTS idx_corpus_hash ON corpus(content_hash);

CREATE TABLE IF NOT EXISTS crawl_errors (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    url      TEXT, node_id TEXT, ts REAL, kind TEXT, message TEXT
);
CREATE INDEX IF NOT EXISTS idx_errors_ts ON crawl_errors(ts);

CREATE TABLE IF NOT EXISTS node_heartbeat (
    node_id TEXT PRIMARY KEY, ts REAL, pages_done INTEGER, rate REAL, status TEXT
);
"""

CORPUS_COLS = ["url", "domain", "node_id", "fetched_at", "http_status", "title",
               "lang", "extractor", "text", "text_len", "structure",
               "content_hash", "html"]


class Backend:
    """Talks to corpus/errors/heartbeat. Two impls: postgres and sqlite."""

    async def init_schema(self): ...
    async def reset(self): ...
    async def write_corpus(self, rows: list[dict]): ...
    async def write_errors(self, rows: list[tuple]): ...
    async def heartbeat(self, node_id, pages_done, rate, status): ...
    async def query(self, sql, *args) -> list: ...
    async def close(self): ...


class PgBackend(Backend):
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._dsn = _asyncpg_dsn(cfg.db)
        self.pool = None

    async def open(self):
        import asyncpg

        async def _init(con):
            # so we can hand jsonb a plain json string (incl. via COPY)
            await con.set_type_codec(
                "jsonb", encoder=lambda v: v, decoder=lambda v: v, schema="pg_catalog")

        # pool sized to cover concurrent flushers/queries without starving
        self.pool = await asyncpg.create_pool(self._dsn, min_size=2, max_size=16, init=_init)
        return self

    async def init_schema(self):
        async with self.pool.acquire() as con:
            await con.execute(PG_SCHEMA)
            if self.cfg.unlogged_corpus:
                # faster bulk loads, corpus not crash-durable (frontier still is)
                await con.execute("ALTER TABLE corpus SET UNLOGGED")

    async def reset(self):
        async with self.pool.acquire() as con:
            await con.execute("TRUNCATE corpus, crawl_errors, node_heartbeat")

    async def write_corpus(self, rows):
        records = [tuple(_pg_value(c, r.get(c)) for c in CORPUS_COLS) for r in rows]
        async with self.pool.acquire() as con:
            # COPY is the fastest path into postgres. land into a temp table then
            # upsert so re-seen urls don't blow up on the primary key
            await con.execute("CREATE TEMP TABLE IF NOT EXISTS _corpus_in "
                              "(LIKE corpus INCLUDING DEFAULTS) ON COMMIT DELETE ROWS")
            async with con.transaction():
                await con.copy_records_to_table("_corpus_in", records=records, columns=CORPUS_COLS)
                await con.execute(
                    "INSERT INTO corpus SELECT * FROM _corpus_in "
                    "ON CONFLICT (url) DO NOTHING"
                )

    async def write_errors(self, rows):
        async with self.pool.acquire() as con:
            await con.executemany(
                "INSERT INTO crawl_errors (url, node_id, ts, kind, message) "
                "VALUES ($1,$2,$3,$4,$5)", rows)

    async def heartbeat(self, node_id, pages_done, rate, status):
        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO node_heartbeat (node_id, ts, pages_done, rate, status) "
                "VALUES ($1,$2,$3,$4,$5) ON CONFLICT (node_id) DO UPDATE SET "
                "ts=EXCLUDED.ts, pages_done=EXCLUDED.pages_done, rate=EXCLUDED.rate, status=EXCLUDED.status",
                node_id, time.time(), pages_done, rate, status)

    async def query(self, sql, *args):
        async with self.pool.acquire() as con:
            rows = await con.fetch(sql, *args)
            return [dict(r) for r in rows]

    async def close(self):
        if self.pool:
            await self.pool.close()


class SqliteBackend(Backend):
    def __init__(self, cfg: Config):
        self.cfg = cfg
        # NOT under storage_dir: crawlee's purge_on_start would unlink it mid-write
        self.path = os.path.join(cfg.base_dir, "corpus.db")
        self.db = None

    async def open(self):
        import aiosqlite
        os.makedirs(self.cfg.base_dir, exist_ok=True)
        self.db = await aiosqlite.connect(self.path)
        await self.db.execute("PRAGMA mmap_size=0")   # no SIGBUS on external-SSD I/O hiccup
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA synchronous=NORMAL")
        return self

    async def init_schema(self):
        await self.db.executescript(SQLITE_SCHEMA)
        await self.db.commit()

    async def reset(self):
        for t in ("corpus", "crawl_errors", "node_heartbeat"):
            await self.db.execute(f"DELETE FROM {t}")
        await self.db.commit()

    async def write_corpus(self, rows):
        ph = ",".join("?" * len(CORPUS_COLS))
        records = [tuple(_sqlite_value(c, r.get(c)) for c in CORPUS_COLS) for r in rows]
        await self.db.executemany(
            f"INSERT OR IGNORE INTO corpus ({','.join(CORPUS_COLS)}) VALUES ({ph})", records)
        await self.db.commit()

    async def write_errors(self, rows):
        await self.db.executemany(
            "INSERT INTO crawl_errors (url, node_id, ts, kind, message) VALUES (?,?,?,?,?)", rows)
        await self.db.commit()

    async def heartbeat(self, node_id, pages_done, rate, status):
        await self.db.execute(
            "INSERT INTO node_heartbeat (node_id, ts, pages_done, rate, status) VALUES (?,?,?,?,?) "
            "ON CONFLICT(node_id) DO UPDATE SET ts=excluded.ts, pages_done=excluded.pages_done, "
            "rate=excluded.rate, status=excluded.status",
            (node_id, time.time(), pages_done, rate, status))
        await self.db.commit()

    async def query(self, sql, *args):
        # translate $1..$n placeholders to ? for sqlite callers that share sql
        for i in range(9, 0, -1):
            sql = sql.replace(f"${i}", "?")
        cur = await self.db.execute(sql, args)
        cols = [d[0] for d in cur.description]
        rows = await cur.fetchall()
        return [dict(zip(cols, r)) for r in rows]

    async def close(self):
        if self.db:
            await self.db.close()


def _pg_value(col, val):
    if col == "structure" and val is not None and not isinstance(val, str):
        return json.dumps(val, ensure_ascii=False)
    return val


def _sqlite_value(col, val):
    if col == "structure" and val is not None and not isinstance(val, str):
        return json.dumps(val, ensure_ascii=False)
    return val


async def open_backend(cfg: Config) -> Backend:
    backend = PgBackend(cfg) if cfg.is_postgres else SqliteBackend(cfg)
    await backend.open()
    return backend
