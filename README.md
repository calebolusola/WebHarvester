# WebHarvester

A recursive, resumable web crawler for building high quality text corpora. You
give it a seed URL, it follows links outward, and it harvests clean, structured
text as it goes. It is built to run unattended for days, to spread across more
than one machine, and to survive everything a home setup throws at it: dropped
connections, power blips, flaky external drives, and sites that fight back with
aggressive rate limiting.

I built this to scrape conversational text for research and model training, and
honestly because I had wanted my own crawler for years and finally had the
infrastructure to run one.

---

## Why this exists

The idea is old. I always wanted a crawler that could start from any URL and
recursively pull in everything reachable, forever, and use that to build my own
datasets instead of renting someone else's. Two things always stopped me: I was
on metered, unreliable internet, and I had regular power outages. A crawler that
needs to run for days is a non starter when the connection drops every hour and
the lights go out every afternoon.

That changed. I have fiber and solar now, so a machine can sit there crawling for
a week without me thinking about it. That removed the external constraints, which
meant the only thing left in the way was the engineering. This repo is that
engineering.

The concrete goal was a corpus of real human conversation. My main target is
Nairaland, which is effectively the Reddit of Nigeria: millions of threads of
people arguing, telling stories, and giving advice in Nigerian English. That is
exactly the kind of text that is underrepresented in most public datasets, and it
is the kind of thing you cannot just download.

## What I optimized for

In rough priority order:

1. **Performance.** This is the top priority. The crawler should saturate the
   connection and use every core, not sit idle waiting on a database.
2. **Resumability.** It must be safe to kill at any moment and resume from exactly
   where it stopped. No lost progress, no re-crawling what is already done.
3. **Set and forget.** It should run for days without supervision and recover from
   its own failures.
4. **Distribution.** I have more than one machine. They should be able to work the
   same crawl together without stepping on each other.
5. **Corpus quality.** The output is for training, so it needs clean text with the
   conversation structure preserved, not a dump of HTML and styling.

## Build on Crawlee, do not reinvent the frontier

The first real decision was whether to hand roll the crawler or build on something.
I started hand rolling it (async workers, a SQLite frontier, a domain sharder) and
then stopped, because the genuinely hard parts of a crawler are not the parts that
are fun to write: URL deduplication, the request queue, robots.txt handling,
session and cookie management, retry and backoff policy, and browser fingerprinting
for anti blocking. All of that is solved, and solved better than I would solve it
on a first pass, by [Crawlee](https://crawlee.dev/python/).

So WebHarvester is built on Crawlee. I let it own the frontier, dedup, robots,
retries, and the session pool, and I spend my effort on the things that are
actually specific to this problem: keeping the database off the hot path, the
distribution model, the extraction quality, and surviving real world failure
modes. Most of the interesting code here is the glue and the decisions around
Crawlee, not a reimplementation of it.

## Architecture

### The hot path, and keeping the database off it

The single most important performance decision is this: **the database is never
touched synchronously while crawling.**

A naive crawler writes each page to the database inside the request handler. At a
few hundred pages per second, with the database possibly on another machine across
the LAN, that write latency lands directly on your throughput. It is the classic
mistake.

I split writes into two completely different classes:

- **Frontier and dedup state** (tiny: a URL and a couple of status flags). This has
  to be shared and atomic, because it is what makes the crawl resumable and what
  stops two machines from crawling the same page. It lives in Crawlee's request
  queue. The rows are small and the access is pooled, so it is not the bottleneck.
- **The page corpus** (large: cleaned text plus the thread structure). This is
  never written inline. The request handler does one thing with the extracted
  record: it drops it on an in memory queue and returns. A separate background task
  drains that queue and writes in batches, using PostgreSQL `COPY` (the fastest
  path into Postgres) or a batched `executemany` on SQLite, flushing on a size or
  time trigger, whichever comes first.

The result is that database latency is fully decoupled from crawl throughput. The
handler returns in microseconds. If the writer ever falls behind, the in memory
queue applies backpressure, which is the one moment I actually want the crawl to
slow down. The relevant code is `crawler/corpus.py` (the buffered writer) and the
two flush implementations in `crawler/storage.py`.

The tradeoff is honest: on a hard crash, the unflushed buffer is lost. I made the
buffer small (about 100 records) so the loss window is a second or two of pages,
and I flush on graceful shutdown. For a research corpus, losing the last hundred
pages of a multi hundred thousand page crawl is a non issue.

### Using all the cores

HTML parsing and content extraction are CPU bound, and they are the only CPU bound
work in an otherwise IO bound program. If you run them inline on the event loop,
one core does all the parsing and the loop stalls. So extraction runs in a
`ProcessPoolExecutor`: the event loop stays free for network IO, and parsing
fans out across every core on the machine. On a Machine with many cores, that is a real win, and it
is the only place multicore actually helps a crawler, which is otherwise network
bound.

### Storage, and one backend that does everything

Crawlee 1.x ships a `SqlStorageClient`, which is the keystone of the whole design.
It lets the frontier live in either SQLite or PostgreSQL with the same code path.

- **Single machine:** SQLite on a local disk. Zero infrastructure. You run one
  command and it works.
- **Mesh:** PostgreSQL on one machine, every node connecting over the LAN.

The same backend also stores the corpus, the error log, and per node heartbeats,
so observability is just SQL queries against the live database.

### Distribution: a shared frontier, not a coordinator

The mesh design is deliberately simple. There is no leader, no message queue, no
custom coordination protocol. Every node points at the same PostgreSQL frontier
and claims work from it atomically. Because the request queue dedups by URL, two
machines can hammer the same crawl and never fetch the same page twice. Adding a
node is literally pointing a second machine at the same database connection string.

I want to be precise about what this buys you, because it is easy to oversell.
Distribution helps when you are crawling many different sites, or when each node
has a genuinely different IP. It does not help you go faster against a single site
that rate limits by IP, if your nodes share one home router and therefore one
public IP. In that case more nodes just means more requests from the same IP and
more rate limiting. I learned this the practical way (see below), and the README
would be lying if it promised otherwise.

### Resumability and the supervisor

Resumability comes for free from the durable frontier: kill the process, rerun the
same command, and Crawlee picks up the pending requests. `--fresh` is the only flag
that wipes state.

Set and forget needs one more thing: the crawl has to survive its own crashes
without me watching. That is what `run.sh` is, a tiny supervisor that reruns the
crawl if it exits abnormally and stops cleanly on a normal finish or a Ctrl-C.
Combined with the durable frontier, a crash becomes a ten second pause rather than
an end of run.

### Politeness and anti blocking

Two layers here.

For *looking* like a real client, the crawler uses Crawlee's `impit` HTTP client
with browser impersonation, so the TLS and header fingerprint match a real Chrome.
Most blocks come from looking like a bot, not from failing a puzzle, so this is the
front line. I want to be clear that no crawler "solves" captchas; the honest goal
is to avoid triggering them, and real solving needs a paid service wired in
separately.

For *behaving* politely, there is a per domain throttle that honors HTTP 429 and
robots.txt crawl-delay, with exponential backoff per domain. This is what keeps a
strict site from rate limiting the crawl into the ground, and tuning it correctly
was most of the operational work (again, see below).

### Extraction: structure, not soup

The output is meant for training, so the extractors do real work to preserve
meaning and throw away noise. They live in `crawler/extract/`:

- A **generic** readability style extractor finds the main content of an arbitrary
  page, keeps the headings, paragraphs, and lists, and strips scripts, styling,
  navigation, and boilerplate.
- A **site aware** extractor for Nairaland understands the forum's markup and
  reconstructs the conversation: each post's author, position, timestamp, and body,
  plus the quote graph (who quoted whom, with a pointer to the exact post being
  replied to). That structure is stored as JSON alongside a clean flattened text,
  so you can train on the prose or reconstruct the thread.

Adding a site is writing one `extract(tree) -> dict` function and registering its
domain in `crawler/extract/registry.py`. Everything else (cleaning, the process
pool, the writer) is shared.

### Observability without slowing the crawl

You cannot log every page; at hundreds per second the logging itself becomes the
bottleneck, and the noise is useless. So the hot path is silent, and observability
is pull based. Each node writes a heartbeat (pages done, rate, last seen) every few
seconds, and the `status` command queries the database for totals, throughput, top
domains, recent errors, and a per node breakdown. Because it reads the database, it
works from any machine and adds no load to the crawl. There is a `--watch` mode for
a live view.

## Problems I hit, and how I solved them

This is the part I would actually want to read in someone else's repo. None of these
were in the original plan; all of them came from running the thing against a real
site on real hardware.

### The external SSD was exFAT, and SQLite hates exFAT

I wanted all runtime data on an external SSD. The first real crawl corrupted the
database almost immediately: `database disk image is malformed`. The drive was
exFAT, which is a perfectly good filesystem for moving files around but does not
give SQLite the file locking and ordered `fsync` it depends on for atomic commits.
My local tests passed only because they ran on APFS.

The fix was twofold. First, the crawler now refuses to run if its data directory is
on a known bad filesystem (exFAT, FAT, network mounts), with a message that explains
why instead of silently corrupting. Second, I reformatted a partition to APFS. The
useful realization was that for the mesh, only the host machine ever touches that
filesystem; the other nodes reach the database over TCP, so APFS costs nothing in
cross platform terms.

### A bus error after two hours: SQLite mmap on a USB drive

After fixing the filesystem, a crawl ran cleanly for two hours and then died with a
`SIGBUS`. A bus error is a memory access fault, and the giveaway was that it pointed
at a memory mapped region. Crawlee sets `PRAGMA mmap_size=256MB` on its SQLite
connections for performance. On an external USB SSD, a momentary IO hiccup on a
memory mapped page is an unrecoverable `SIGBUS` that kills the process instantly,
where a normal read would have just returned a retryable error.

The fix is in `crawler/storage.py`: I build my own SQLAlchemy engine with
`mmap_size=0` and hand it to Crawlee. Reading the Crawlee source confirmed that when
you pass it an engine, it skips its own pragma setup entirely, so my setting holds.
I kept the rest of its performance pragmas (cache size, WAL, temp store) and changed
only the one that was killing the process. Plain `read`/`write` turns the same USB
hiccup into a retry instead of a crash.

### The 429 storm, and why it was my own autoscaler

Nairaland rate limits hard. My first sustained runs produced hundreds of "rotating
session" retries and ground to a crawl. The instinct is to blame the site, but the
logs showed the crawler doing it to itself.

Two things were compounding. First, Crawlee's autoscaler saw the workers sitting
idle (they were idle, waiting on the throttle) and concluded it should *add*
concurrency, ramping from 2 up past 30. Every ramp was a bigger burst of
simultaneous requests, every burst tripped more 429s, and the backoff slammed
straight to its ceiling. Second, rotating the session on a 429 does nothing when the
limit is per IP, because a new session is the same IP. It is wasted churn.

The fix was to stop fighting the throttle: pin concurrency low and equal
(`concurrency = max_concurrency = 2`) so the autoscaler cannot stampede, and lean on
the per domain backoff that honors `Retry-After`. After that the consecutive 429
counter stays at one or two and recovers, instead of marching to twenty. The crawl
went from lurching and storming to a steady hundred pages a minute with a sub 0.1%
error rate.

The deeper lesson, which I wrote into the config comments so I do not forget it: a
single IP against a strict site has a hard ceiling no amount of cleverness gets you
past. To actually go faster you need more IPs, which means proxies. Politeness on
one IP is the free, slow option; proxies are the paid, fast option. The crawler
supports proxy pools and tiered proxies for exactly this, but I run it politely on
one IP for now.

### A latent overflow bug in the throttle

While the IP was in a penalty box and 429ing every request, the run started spewing
`Python int too large to convert to C int`. This is a real bug in Crawlee's
throttle: it computes `base_delay * 2 ** (consecutive_429 - 1)` before capping to
the maximum delay, and on a long enough 429 streak that intermediate value overflows
what a `timedelta` can hold. Since the streak counter only resets on a successful
request, a true penalty box drives it past the overflow point.

I cannot patch a dependency in place, so there is a small, surgical monkeypatch in
`crawler/crawl.py` that clamps the streak counter before the math can overflow. It
is the kind of thing I would normally upstream, and the comment says so.

### A single IP cannot mirror a forum in a reasonable time

The honest math: Nairaland is on the order of ten million crawlable pages, and a
polite single IP does roughly fifty to a hundred thousand a day. That is months for
a full mirror. For a research corpus I do not need the entire forum, I need a
representative slice, so the real answer was scope, not speed.

The wrinkle is that Nairaland thread URLs carry no board name, and the footer links
every board, so you cannot cleanly fence the crawl to one topic by URL pattern. The
lever that actually works is crawl depth: seed the boards I care about, stay on the
domain, and bound the depth so the crawl pulls recent threads and their replies
without spidering the entire site through cross links. That turns months into days
and still produces a large, diverse, on topic corpus. The tuned configuration is in
`crawler.toml`.

### Quote attribution and lost line breaks

Reading the extracted threads, two quality issues showed up. Quoted authors were
sometimes null, and post text had words run together where line breaks should be.
Fetching one real thread and looking at the markup explained both: Nairaland puts
the quoted author in an anchor *inside* the blockquote (not before it, which is what
I had assumed), and it uses `<br>` for line breaks, which `text_content()` silently
drops. I fixed the attribution to read the anchor inside the blockquote (and to
capture the `cite` attribute, which points at the exact quoted post, giving a real
reply graph), and I convert `<br>` to newlines before extracting text so the prose
keeps its shape.

## Usage

### Setup

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
# only if you want the --render (JavaScript) fallback:
.venv/bin/playwright install chromium
```

A note that the crawler will enforce: the data directory must be on a journaling
filesystem (APFS, ext4, NTFS), not exFAT or FAT, or SQLite will corrupt.

### Single machine

```sh
.venv/bin/python -m crawler crawl \
  --base-dir /Volumes/MainSSD/CRAWLER --db sqlite \
  --seed https://www.nairaland.com --same-site
```

Resume after any stop by rerunning the same command without `--seed` and without
`--fresh`. For an unattended run, use the supervisor:

```sh
./run.sh    # reads crawler.toml, auto resumes on crash, stops on clean finish or Ctrl-C
```

### Mesh (multiple machines on a LAN)

Stand up PostgreSQL on one machine with its data directory on a real filesystem,
create the schema once, then run the same command on every node pointing at it:

```sh
DSN="postgresql+asyncpg://crawler:secret@192.168.1.50:5432/crawler"
.venv/bin/python -m crawler init-db --db "$DSN"

# on each machine:
.venv/bin/python -m crawler crawl --db "$DSN" \
  --seed https://www.nairaland.com --same-site --keep-alive
```

`--keep-alive` makes a node wait for peers to enqueue work rather than exiting when
its local view of the queue empties. The shared frontier guarantees no node crawls a
URL another already took.

### Configuration

Most runs are easier from a TOML file. Copy `crawler.example.toml` to `crawler.toml`,
edit, and run with `--config crawler.toml`. The example file is heavily commented and
encodes the tuning lessons above (notably the pinned concurrency for strict sites).
Precedence is defaults, then the file, then `CRAWLER_*` environment variables, then
CLI flags.

### Watching it

```sh
.venv/bin/python -m crawler status            # one shot snapshot, whole mesh
.venv/bin/python -m crawler status --watch    # live
.venv/bin/python -m crawler status --export corpus.jsonl
```

### Reading the corpus

The text lands in a `corpus` table (and `corpus.db` on SQLite). Export it to JSONL
with the command above, or query it directly:

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
| `status` | progress of the whole mesh; supports `--watch` and `--export` |
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

- A single IP against a strict site is slow and periodically rate limited. Real
  throughput at scale needs proxies. The crawler supports them; I just run on one IP.
- The corpus loses at most a small buffer of pages on a hard crash. The frontier is
  always durable; the corpus trades a tiny loss window for throughput.
- `--max-pages` counts handled requests across the crawl's whole lifetime, since it
  is persisted, so on resume you set it higher or omit it.
- Site aware extraction exists for Nairaland; everything else gets the generic
  extractor, which is good but not structure aware.
- Captchas are avoided, not solved. A site determined to block you will, and the
  answer there is a paid solving service, which is out of scope.

## Stack

Python, [Crawlee](https://crawlee.dev/python/) for the crawl engine, `impit` for
browser impersonated HTTP, `lxml` for parsing, SQLAlchemy and `asyncpg`/`aiosqlite`
for storage, and optional Playwright for JavaScript heavy pages.
