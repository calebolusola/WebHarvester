# WebHarvester

A recursive, resumable web crawler for building text corpora. Give it a seed URL,
it follows links outward and harvests clean, structured text. Built to run
unattended for days, spread across more than one machine, and survive dropped
connections, power blips, flaky drives, and sites that fight back with rate
limiting.

Built to scrape conversational text for research and model training. Main target
is Nairaland, the Reddit of Nigeria: millions of threads of Nigerian English and
Pidgin conversation, the kind of text public datasets barely have. The cleaned
result is on Hugging Face as
[nairaland-conversations](https://huggingface.co/datasets/caleb-mnb/nairaland-conversations).

## Design priorities (in order)

1. **Performance** - saturate the connection, use every core, never sit idle on a database.
2. **Resumability** - safe to kill at any moment, resumes from exactly where it stopped.
3. **Set and forget** - runs for days unsupervised, recovers from its own failures.
4. **Distribution** - multiple machines can work the same crawl without stepping on each other.
5. **Corpus quality** - clean text with conversation structure preserved, not an HTML dump.

## Built on Crawlee, without reinventing the frontier

The hard parts of a crawler (URL dedup, the request queue, robots.txt,
session/cookie management, retry and backoff, browser fingerprinting) are solved
better by [Crawlee](https://crawlee.dev/python/) than I'd solve them on a first
pass. So WebHarvester is built on Crawlee, and the actual engineering here is the
glue: keeping the database off the hot path, the distribution model, extraction
quality, and surviving the things that actually go wrong.

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

Crawlee 1.x's `SqlStorageClient` is what makes this simple: same code path for
SQLite or PostgreSQL.

- **Single machine:** SQLite on local disk, zero infrastructure.
- **Mesh:** PostgreSQL on one machine, every node connects over LAN.

Same backend also stores the corpus, error log, and per-node heartbeats, so
observability is just SQL queries against the live database.

### Distribution: shared frontier, no coordinator

No leader, no message queue. Every node points at the same PostgreSQL frontier
and claims work atomically. Because the request queue dedups by URL, adding a
node is just pointing a second machine at the same connection string.

Caveat: distribution helps against many different sites or when nodes have
their own IPs. Against one site that rate limits by IP, nodes behind the same
router just send more requests from the same IP and get rate limited sooner.
Learned this the hard way. How I actually ran the mesh, and what went wrong, is
in [docs/mesh.md](docs/mesh.md).

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

### Extraction

Lives in `crawler/extract/`:

- **Generic**: readability-style extractor for arbitrary pages, keeps headings,
  paragraphs, lists, strips scripts/nav/boilerplate.
- **Site-aware (Nairaland)**: reconstructs the conversation, each post's author,
  position, timestamp, body, plus the quote graph (who quoted whom, pointing at
  the exact replied-to post). Stored as JSON alongside flattened text.

Adding a site is one `extract(tree) -> dict` function registered in
`crawler/extract/registry.py`.

### Observability without slowing the crawl

Logging every page is mostly noise, and at speed it turns into a bottleneck, so
the hot path is silent. Each node writes a heartbeat every few seconds; `status`
queries the database for totals, throughput, top domains, errors, per-node
breakdown. Reads the database, so it works from any machine and adds no load.

## Things that broke

None of this showed up in testing. All of it came from running against Nairaland
for weeks.

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
producing a large, diverse, on-topic corpus. The first pass ran at depth 4 and
drained in a few weeks. After that I removed the cap and used `reopen` to pick up
from the edge, since pages at the old limit had been handled with their links
thrown away.

**Quote attribution and lost line breaks.** Extracted threads had null quoted
authors and run-together words. Nairaland puts the quoted author's anchor
*inside* the blockquote (not before it), and uses `<br>` for line breaks, which
`text_content()` silently drops. Fixed attribution to read the anchor inside the
blockquote (and capture the `cite` attribute for the reply graph), and convert
`<br>` to newlines before extracting text.

**Every exclude pattern was a no-op.** Found this weeks in, when the corpus had
132k reply-form pages and 13k login pages that were supposed to be excluded.
Crawlee's `Glob` follows file path rules, where `*` stops at a slash, so
`*nairaland.com/login*` can never match a URL. Patterns now compile through
`fnmatch`, where `*` matches anything.

**The forum started cloning itself.** With the depth cap gone,
`www.nairaland.com.` (trailing dot) slipped through as "same domain". To the queue
it's a different host, so nothing dedups against it, and every page on it links
to more of it. Fix: an include pattern that only allows the canonical host.

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

### Mesh (multiple machines, one frontier)

Full setup, including Postgres over Tailscale, is in [docs/mesh.md](docs/mesh.md).
The short version:

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
| `reopen` | re-queue pages at the depth edge so a deeper crawl can continue (`--dry-run` to just count) |

## Releasing the dataset

`scripts/build_release.py` turns the corpus into the public dataset: thread pages
only, deduplicated, usernames and @handles swapped for keyed pseudonyms, contact
details and social links scrubbed, languages tagged per post, written as Parquet
shards with a dataset card. It needs `requirements-release.txt` and a salt file
that stays private. Usage notes are at the top of the script.

The current release: [caleb-mnb/nairaland-conversations](https://huggingface.co/datasets/caleb-mnb/nairaland-conversations)
on Hugging Face, 1.9M thread pages and 34M posts, free for non-commercial research.

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
scripts/
  build_release.py  corpus -> public parquet dataset + card
  start-host.sh     brings postgres and the mac's crawler back after a reboot
  start-db.sh       just postgres
  bootstrap-node.sh sets up a fresh ubuntu box as a mesh node
  bench_ip.py       checks how a given IP fares against the target site
docs/mesh.md      how the multi-machine setup ran, and what broke
run.sh            set-and-forget supervisor
crawler.toml      a real, tuned board-scoped Nairaland crawl
crawler.example.toml  annotated template
```

## Limitations

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