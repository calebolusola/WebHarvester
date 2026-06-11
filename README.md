# web-crawler

A distributed, resumable, set-and-forget web crawler built on
[Crawlee](https://crawlee.dev/python/). Starts from any seed, follows links
recursively, and harvests clean structured text for building LLM training/research
corpora. Designed to run unattended across multiple machines on a home LAN.

## What it does

- **Recursive crawl** from any seed, with optional site bounding.
- **Resumable at any point.** State is durable. Kill it (power, crash, ctrl-c),
  rerun the same command, it continues from where it stopped.
- **Skips already-crawled pages** via the shared frontier's url dedup.
- **Mesh.** Point several machines at one Postgres and they share a single
  frontier, so they never crawl the same page twice and split the work.
- **Self-healing.** Rotates browser fingerprint + session + proxy tier on blocks
  and errors, and keeps going. Built to run for days.
- **Observability without hot-path logging.** Peek at the whole mesh any time
  with `status`, including a live `--watch`.
- **Clean structured corpus.** Per-site extractors (Nairaland to start) preserve
  thread/reply structure; a generic readability-style fallback handles the rest.
  Styling, scripts, nav and chrome are stripped.
- **Fast.** HTTP-first with browser impersonation, high async concurrency, and
  HTML parsing offloaded across all CPU cores.

## The performance design (how we avoid writing to the DB on every page)

Writes are split into two classes:

1. **Frontier / dedup state** (tiny: url + flags). This must be shared and atomic
   for the mesh to work, so it lives in Crawlee's request queue on Postgres. The
   rows are small and the access is pooled, so it is not the bottleneck.
2. **Page corpus** (big: cleaned text + structure). This is *never* written
   synchronously. The request handler only drops the record on an in-memory
   queue and returns. A background flusher batches them and writes via Postgres
   `COPY` (or SQLite executemany) every N records or T seconds. DB latency is
   fully off the crawl hot path.

On top of that, **HTML parsing + extraction runs in a `ProcessPoolExecutor`**, so
it uses every core instead of blocking the event loop.

## Setup

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
# only if you want --render (js fallback):
.venv/bin/playwright install chromium
```

## Single machine (zero infra)

Uses a SQLite file on the SSD. Nothing else to run.

```sh
.venv/bin/python -m crawler crawl \
  --base-dir /Volumes/SANDISK/CRAWLER --db sqlite \
  --seed https://www.nairaland.com --same-site
```

Resume (just rerun, no seed needed):

```sh
.venv/bin/python -m crawler crawl --base-dir /Volumes/SANDISK/CRAWLER --db sqlite
```

## Mesh (Dell + MacBook on the same LAN)

One machine hosts Postgres, with its data directory on the SSD. Both machines
connect to it over the LAN. SQLite cannot be shared over a network; Postgres can.

### 1. Stand up Postgres on the host machine (data on the SSD)

```sh
initdb -D /Volumes/SANDISK/CRAWLER/pgdata
# in /Volumes/SANDISK/CRAWLER/pgdata/postgresql.conf:
#   listen_addresses = '*'
# in pg_hba.conf, allow your LAN subnet, e.g.:
#   host  crawler  crawler  192.168.1.0/24  scram-sha-256
pg_ctl -D /Volumes/SANDISK/CRAWLER/pgdata -l /Volumes/SANDISK/CRAWLER/pg.log start
createuser crawler --pwprompt
createdb crawler -O crawler
```

### 2. Create the corpus/errors/heartbeat tables once

```sh
DSN="postgresql+asyncpg://crawler:secret@192.168.1.50:5432/crawler"
.venv/bin/python -m crawler init-db --db "$DSN"
```

### 3. Run on each machine (same command, they auto-coordinate)

```sh
# on the macbook AND the dell:
.venv/bin/python -m crawler crawl --db "$DSN" \
  --seed https://www.nairaland.com --same-site --keep-alive
```

`--keep-alive` makes a node wait for peers to enqueue more work instead of exiting
when its local view of the queue empties. The shared frontier guarantees neither
machine crawls a url the other already took. `node_id` defaults to each hostname,
so observability tells the two apart.

## Observability

```sh
.venv/bin/python -m crawler status --db "$DSN"            # one-shot snapshot
.venv/bin/python -m crawler status --db "$DSN" --watch    # live, refreshes
.venv/bin/python -m crawler status --db "$DSN" --export corpus.jsonl
```

Shows total pages, chars harvested, current rate, per-node breakdown (the whole
mesh), top domains, and recent errors. It reads the DB, so it adds no load to the
crawl and works from any machine.

## Config file

Most runs are easier from a TOML file (both machines can share it). Copy
`crawler.example.toml` to `crawler.toml`, edit, then:

```sh
.venv/bin/python -m crawler crawl --config crawler.toml
```

Precedence: defaults < TOML < env (`CRAWLER_*`) < CLI flags.

## Subcommands

| command | purpose |
|---------|---------|
| `crawl` | start or resume a crawl |
| `status` | peek at progress (whole mesh); `--watch`, `--export` |
| `init-db` | create the corpus/errors/heartbeat tables |
| `reconcile` | re-enqueue previously-failed urls for another try |

## Key crawl flags

| flag | meaning |
|------|---------|
| `--seed URL` | starting url, repeatable |
| `--base-dir` | where runtime data lives (default the SSD) |
| `--db` | `sqlite` (default) or a postgres dsn for the mesh |
| `--same-site` | stay on the seed's domain (alias for `--strategy same-domain`) |
| `--strategy` | `all`, `same-domain`, `same-hostname`, `same-origin` |
| `--allow-domain` / `--include` / `--exclude` | finer bounding |
| `--concurrency` / `--max-concurrency` | parallel requests |
| `--rate N` | requests/min cap (politeness), 0 = uncapped |
| `--max-depth` / `--max-pages` | limits (cumulative across resumes) |
| `--parse-workers` | extraction process pool size (0 = all cores) |
| `--render` | adaptive playwright fallback for js-heavy pages |
| `--keep-alive` | mesh: wait for peers instead of exiting on empty queue |
| `--proxy` / `--proxy-tier` | proxy rotation (tiers escalate when blocked) |
| `--no-robots` | ignore robots.txt (careful) |
| `--store-html` | also keep raw gzipped html alongside the text |
| `--fresh` | wipe state and start over |

## Content extraction

Per-domain extractors live in `crawler/extract/`. The Nairaland one parses each
post's author, index, timestamp, body, and quote->reply links, storing the
conversation layout as JSON in the `structure` column while keeping a clean flat
`text` for training. Unknown sites get a generic main-content extractor. To add a
site, write an `extract(tree) -> dict` function and register its domain in
`crawler/extract/registry.py`.

## Notes / trade-offs

- On a hard crash, only the last ~1-2s of unflushed corpus is lost; those urls
  are marked handled so they are not recrawled. `reconcile` exists for the rare
  case that matters. The frontier itself is always durable.
- `--max-pages` counts total handled across the crawl's lifetime (it is
  persisted), so on resume set it higher or omit it.
- No library truly "solves" captchas. The impersonation + session rotation here
  avoids tripping them; real solving needs a paid service wired in separately.
