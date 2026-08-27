# WebHarvester

A recursive, resumable web crawler for building text corpora. Give it a seed URL,
it follows links outward and harvests clean, structured text. Built to run
unattended for days, spread across more than one machine, and survive dropped
connections, power blips, flaky drives, and sites that fight back with rate
limiting.

Built to scrape conversational text for research and model training. Main target
is Nairaland, the Reddit of Nigeria: millions of threads of Nigerian English
conversation, which is exactly the kind of text underrepresented in public
datasets and not available for download anywhere.

## Design priorities (in order)

1. **Performance** - saturate the connection, use every core, never sit idle on a database.
2. **Resumability** - safe to kill at any moment, resumes from exactly where it stopped.
3. **Set and forget** - runs for days unsupervised, recovers from its own failures.
4. **Distribution** - multiple machines can work the same crawl without stepping on each other.
5. **Corpus quality** - clean text with conversation structure preserved, not an HTML dump.

## Built on Crawlee, without reinventing the frontier

The genuinely hard parts of a crawler (URL dedup, the request queue, robots.txt,
session/cookie management, retry and backoff, browser fingerprinting) are solved
better by [Crawlee](https://crawlee.dev/python/) than I'd solve them on a first
pass. So WebHarvester is built on Crawlee, and the actual engineering here is the
glue: keeping the database off the hot path, the distribution model, extraction
quality, and surviving real world failure modes.

## Architecture

### Database off the hot path

The core performance decision: **the database is never touched synchronously
while crawling.** A naive crawler writes each page inline in the request handler,
and that write latency lands directly on throughput.

Writes split into two classes:

- **Frontier/dedup state** (tiny: a URL, a couple of status flags). Lives in
  Crawlee's request queue, shared and atomic, this is what makes the crawl
  resumable and stops two machines from crawling the same page.
- **The page corpus** (large: cleaned text + thread structure). Never written
  inline. The handler drops the extracted record on an in-memory queue and
  returns. A background task drains it in batches via Postgres `COPY` or batched
  SQLite `executemany`, flushing on size or time, whichever comes first.

Handler returns in microseconds. If the writer falls behind, the queue applies
backpressure, the one moment I actually want the crawl to slow down. Tradeoff:
a hard crash loses the unflushed buffer (~100 records, so a second or two of
pages). Acceptable for a research corpus. See `crawler/corpus.py` and
`crawler/storage.py`.

### Using all the cores

HTML parsing is the only CPU-bound work in an otherwise IO-bound program. Run it
inline and one core stalls the event loop. Extraction runs in a
`ProcessPoolExecutor` instead: the loop stays free for network IO, parsing fans
out across every core.

### Storage

Crawlee 1.x's `SqlStorageClient` is the keystone: same code path for SQLite or
PostgreSQL.

- **Single machine:** SQLite on local disk, zero infrastructure.
- **Mesh:** PostgreSQL on one machine, every node connects over LAN.

Same backend also stores the corpus, error log, and per-node heartbeats, so
observability is just SQL queries against the live database.

### Distribution: shared frontier, no coordinator

No leader, no message queue. Every node points at the same PostgreSQL frontier
and claims work atomically. Because the request queue dedups by URL, adding a
node is just pointing a second machine at the same connection string.

Honest caveat: distribution helps against many different sites or when nodes
have distinct IPs. It does not help against a single site that rate limits by
IP if your nodes share one router, i.e one public IP. More nodes there just means
more requests from the same IP and more rate limiting. Learned this the hard way,
see below.

### Resumability

Comes free from the durable frontier: kill the process, rerun the same command,
Crawlee picks up pending requests. `--fresh` is the only flag that wipes state.
`run.sh` is a tiny supervisor that reruns the crawl on abnormal exit and stops
cleanly on a normal finish or Ctrl-C, so a crash becomes a ten second pause
instead of an end of run.

### Politeness and anti-blocking

Two layers. For *looking* like a real client: Crawlee's `impit` HTTP client with
browser impersonation, matching TLS/header fingerprint to real Chrome (most
blocks come from looking like a bot, not from failing a puzzle). For *behaving*
politely: a per-domain throttle honoring HTTP 429 and robots.txt crawl-delay,
with exponential backoff per domain. No crawler solves captchas here, the goal
is to avoid triggering them.

### Extraction: structure, not soup

Lives in `crawler/extract/`:

- **Generic**: readability-style extractor for arbitrary pages, keeps headings,
  paragraphs, lists, strips scripts/nav/boilerplate.
- **Site-aware (Nairaland)**: reconstructs the conversation, each post's author,
  position, timestamp, body, plus the quote graph (who quoted whom, pointing at
  the exact replied-to post). Stored as JSON alongside flattened text.

Adding a site is one `extract(tree) -> dict` function registered in
`crawler/extract/registry.py`.

### Observability without slowing the crawl

At hundreds of pages/sec, logging every page becomes the bottleneck. The hot
path is silent instead. Each node writes a heartbeat every few seconds; `status`
queries the database for totals, throughput, top domains, errors, per-node
breakdown. Reads the database, so it works from any machine and adds no load.

## Problems hit, and fixes

Real issues from running this against a real site on real hardware.

**exFAT corrupted SQLite.** External SSD was exFAT. First crawl corrupted the DB
(`database disk image is malformed`) almost immediately, exFAT doesn't give
SQLite the file locking and ordered `fsync` it needs for atomic commits. Local
tests passed only because they ran on APFS. Fix: the crawler now refuses to run
if the data directory is on a known-bad filesystem (exFAT, FAT, network mounts),
and I reformatted the partition to APFS. Mesh nodes reach the DB over TCP, so
this only matters for the host machine.

**SIGBUS after two hours.** Crawlee sets `PRAGMA mmap_size=256MB` on its SQLite
connections. On an external USB SSD, a momentary IO hiccup on a memory-mapped
page is an unrecoverable `SIGBUS` instead of a retryable error. Fix (in
`crawler/storage.py`): build my own SQLAlchemy engine with `mmap_size=0` and
hand it to Crawlee, confirmed via the Crawlee source that a passed-in engine
skips its own pragma setup. Kept the rest of the performance pragmas (cache
size, WAL, temp store).

**429 storm that was self-inflicted.** Nairaland rate limits hard. Logs showed
two compounding problems: Crawlee's autoscaler saw idle workers (they were
waiting on the throttle) and kept adding concurrency, ramping 2 to 30+, each
ramp a bigger burst, each burst tripping more 429s. And rotating sessions on a
429 does nothing when the limit is per-IP, a new session is the same IP, just
wasted churn. Fix: pin concurrency low and equal (`concurrency = max_concurrency
= 2`) so the autoscaler can't stampede, lean on the per-domain backoff honoring
`Retry-After`. Went from lurching and storming to a steady ~100 pages/min at
sub-0.1% error rate. Lesson: a single IP against a strict site has a hard
ceiling, going faster past that needs proxies (supported, just not used here).

**Integer overflow in the throttle.** While 429ing repeatedly, the run started
throwing `Python int too large to convert to C int`. Real bug in Crawlee: it
computes `base_delay * 2 ** (consecutive_429 - 1)` before capping to the max
delay, and a long enough 429 streak overflows what a `timedelta` can hold. Fix:
a small monkeypatch in `crawler/crawl.py` that clamps the streak counter before
the math overflows (worth upstreaming eventually).

**A single IP can't mirror a forum in reasonable time.** Nairaland is roughly
ten million crawlable pages; a polite single IP does 50-100k/day, months for a
full mirror. Real fix was scope, not speed: since thread URLs carry no board
name and the footer links every board, depth-bounded crawling from specific
seed boards (stay on domain, bound depth) turns months into days while still
producing a large, diverse, on-topic corpus. Tuned in `crawler.toml`.

**Quote attribution and lost line breaks.** Extracted threads had null quoted
authors and run-together words. Nairaland puts the quoted author's anchor
*inside* the blockquote (not before it), and uses `<br>` for line breaks, which
`text_content()` silently drops. Fixed attribution to read the anchor inside the
blockquote (and capture the `cite` attribute for the reply graph), and convert
`<br>` to newlines before extracting text.

## Usage

### Setup

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
# only if you want the --render (JavaScript) fallback:
.venv/bin/playwright install chromium
```

Data directory must be on a journaling filesystem (APFS, ext4, NTFS), not exFAT
or FAT, the crawler enforces this.

### Single machine

```sh
.venv/bin/python -m crawler crawl \
  --base-dir /Volumes/MainSSD/CRAWLER --db sqlite \
  --seed https://www.nairaland.com --same-site
```

Resume by rerunning the same command without `--seed` or `--fresh`. For
unattended runs:

```sh
./run.sh    # reads crawler.toml, auto resumes on crash, stops on clean finish or Ctrl-C
```

### Mesh (multiple machines on a LAN)

```sh
DSN="postgresql+asyncpg://crawler:secret@192.168.1.50:5432/crawler"
.venv/bin/python -m crawler init-db --db "$DSN"

# on each machine:
.venv/bin/python -m crawler crawl --db "$DSN" \
  --seed https://www.nairaland.com --same-site --keep-alive
```

`--keep-alive` makes a node wait for peers to enqueue work rather than exiting
when its local queue view empties. Shared frontier guarantees no duplicate
crawling across nodes.

### Configuration

Copy `crawler.example.toml` to `crawler.toml`, edit, run with
`--config crawler.toml`. Precedence: defaults, then file, then `CRAWLER_*` env
vars, then CLI flags.

### Watching it

```sh
.venv/bin/python -m crawler status            # one shot snapshot, whole mesh
.venv/bin/python -m crawler status --watch    # live
.venv/bin/python -m crawler status --export corpus.jsonl
```

### Reading the corpus

```sh
DB=/Volumes/MainSSD/CRAWLER/corpus.db
sqlite3 "$DB" "select count(*), sum(text_len) from corpus"
sqlite3 "$DB" "select text from corpus where extractor='nairaland' order by random() limit 1"
sqlite3 "$DB" "select structure from corpus where extractor='nairaland' limit 1" | python3 -m json.tool
```

## Commands

| command | purpose |
|---------|---------|
| `crawl` | start or resume a crawl |
| `status` | progress of the whole mesh, supports `--watch` and `--export` |
| `init-db` | create the corpus, error, and heartbeat tables |
| `reconcile` | re-enqueue previously failed URLs for another attempt |

## Project layout

```
crawler/
  __main__.py     CLI: crawl, status, init-db, reconcile
  config.py       config dataclass (TOML + env + CLI)
  crawl.py        the crawler: sessions, throttle, process pool, handler, heartbeat
  storage.py      SqlStorageClient wiring + corpus/error/heartbeat backends
  corpus.py       the buffered, off-hot-path corpus writer
  observe.py      status / watch / export
  extract/
    base.py       HTML cleaning + generic readability extractor
    nairaland.py  site-aware thread + quote-graph extractor
    registry.py   domain -> extractor
run.sh            set-and-forget supervisor
crawler.toml      a real, tuned board-scoped Nairaland crawl
crawler.example.toml  annotated template
```

## Honest limitations

- Single IP against a strict site is slow and periodically rate limited. Real
  throughput at scale needs proxies, supported but not used here.
- Corpus loses at most a small buffer of pages on a hard crash. Frontier is
  always durable, corpus trades a tiny loss window for throughput.
- `--max-pages` counts handled requests across the crawl's whole lifetime since
  it is persisted, so set it higher or omit it on resume.
- Site-aware extraction exists for Nairaland only, everything else gets the
  generic extractor.
- Captchas are avoided, not solved. A determined site will still block you.

## Stack

Python, [Crawlee](https://crawlee.dev/python/) for the crawl engine, `impit` for
browser impersonated HTTP, `lxml` for parsing, SQLAlchemy and
`asyncpg`/`aiosqlite` for storage, optional Playwright for JS-heavy pages.