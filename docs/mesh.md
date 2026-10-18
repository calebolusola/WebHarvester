# Running the crawl across several machines

How I ran WebHarvester as a mesh: one Postgres frontier on my Mac, and a few
other machines crawling into it over Tailscale. The shared frontier is what keeps
two nodes from ever fetching the same page.

## What it looked like

```text
   VPS #1 (own IP) ----------\
   VPS #2 (own IP) -----------+--- Tailscale (private, encrypted) ---> Mac
   Dell on a phone hotspot ---/                                        |
   Mac itself (home IP) -----/                              Postgres, port 5433
                                                            (frontier + corpus)
```

The two VPSes were servers I already had running other things. The Dell sat on
my iPhone's hotspot so it had its own IP instead of sharing the home router's.
Everything joined one tailnet, Postgres listened only on the Mac's Tailscale
address, and every node read and wrote the same frontier and corpus.

## Postgres on the Mac

```sh
brew install postgresql@16
initdb -D ~/crawlerdata/pgdata

# listen on the tailnet, on 5433 so it can't clash with another local postgres
cat >> ~/crawlerdata/pgdata/postgresql.conf <<'EOF'
listen_addresses = '*'
port = 5433
EOF

# only tailscale addresses (100.64.0.0/10), password auth
echo "host  crawler  crawler  100.64.0.0/10  scram-sha-256" >> ~/crawlerdata/pgdata/pg_hba.conf

pg_ctl -D ~/crawlerdata/pgdata -l ~/crawlerdata/pg.log start
createuser -p 5433 -P crawler
createdb -p 5433 -O crawler crawler
```

`listen_addresses = '*'` is only safe because of the `pg_hba` line. Without it
Postgres would take connections from anywhere that can reach the port. I never
forwarded 5433 on the router either.

The connection string lives in `mesh.env` (gitignored, copy `mesh.env.example`),
and `run.sh` picks it up automatically. Then create the tables once:

```sh
set -a; . ./mesh.env; set +a
.venv/bin/python -m crawler init-db --db "$CRAWLER_DB"
```

A reboot stops Postgres, and then every node sits there waiting. So the Mac runs
`scripts/start-host.sh` at login through a LaunchAgent: it starts Postgres if it's
down, then starts the Mac's own crawler in tmux if it isn't running.

## Adding a node

On a fresh Ubuntu box, `scripts/bootstrap-node.sh` installs the system packages,
joins Tailscale (prints an auth URL to open), clones the repo and builds the venv:

```sh
export REPO_URL="https://github.com/calebolusola/WebHarvester.git"
export PG_DSN="postgresql+asyncpg://crawler:PASSWORD@<mac tailscale ip>:5433/crawler"
bash bootstrap-node.sh
```

Then put the same `mesh.env` on it and start the supervisor in tmux:

```sh
cd ~/webharvester && tmux new -s crawler -d './run.sh'
```

Every node runs with keep-alive on, so one that finds the queue momentarily empty
waits for the others to enqueue more instead of exiting.

To watch the whole thing from any machine on the tailnet:

```sh
.venv/bin/python -m crawler status --db "$CRAWLER_DB" --watch
```

## What I learned running it

**More nodes behind one router buys nothing.** Nairaland rate limits per IP, so
two machines on the same home connection just hit the limit twice as fast. Every
extra node only helped when it had its own IP, which is why the Dell went on the
hotspot.

**Datacenter IPs weren't the problem I expected.** I assumed Nairaland blocked
cloud ranges. `scripts/bench_ip.py` showed the VPSes got the same 200s and 429s as
my home IP. What actually gets a request past Cloudflare is looking like a
real browser, which is what `impit` handles.

**Remote nodes were a lot slower than the Mac.** The Mac did 1 to 3 pages a second,
the VPSes more like 0.2. Every page costs a few round trips to the frontier, and for
the remote nodes each one crossed the internet to my Mac. Leasing requests in
batches would fix it. I never built that because the data was coming in fast enough.

**The throttle quietly broke the mesh at first.** Crawlee's per-domain throttle
builds its own sub-queues, and it got its storage client from a global locator
rather than the one passed to the crawler. So nairaland's queue landed on each
node's local disk, and every node crawled its own copy of the site. The fix in
`crawler/crawl.py` is setting the storage client globally before the crawler
starts.

**A frozen heartbeat looks like a working node.** When the Dell's hotspot ran out
of data, its last reported rate stuck around in `status`. Nodes that haven't
reported in 20 seconds now show as stale with a rate of zero.

**Home upload barely moves.** Nodes fetch pages with their own bandwidth
and send home only extracted text, a few KB per page. The hotspot was the
exception: there the node does the fetching itself, roughly 100 to 200KB a page of
mobile data.
