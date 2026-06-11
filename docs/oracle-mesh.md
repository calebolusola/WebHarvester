# Running WebHarvester as a free mesh on Oracle Cloud

This sets up a distributed crawl using free Oracle Cloud VMs as worker nodes, all
sharing one PostgreSQL frontier hosted on your Mac. Each VM crawls with its own
datacenter IP, so they run in parallel without tripping a single IP's rate limit,
and the shared frontier guarantees no two nodes ever crawl the same page.

## Topology

```
   Oracle VM #1 (own IP) ---\
   Oracle VM #2 (own IP) -----\        Tailscale (private, encrypted)
   Oracle VM #3 (own IP) -------+----------------------------------> Mac
   (your Mac, home IP) --------/                                      |
                                                              PostgreSQL on the SSD
                                                              (frontier + corpus)
```

Every machine joins one Tailscale network. Postgres listens on the Mac's Tailscale
IP. Nodes fetch pages with their own public IPs and write results home over
Tailscale. The corpus stays on your SSD.

## What you need

- An Oracle Cloud account (free tier, no charge for Always Free resources).
- A Tailscale account (free, up to 100 devices).
- The WebHarvester repo reachable by the VMs (a private GitHub repo is easiest;
  alternatively `scp` the code to each VM).
- Your Mac stays powered on while crawling (it is the database host). Run it on AC
  and keep it awake with `caffeinate`.

## A. Tailscale: link every machine

On the Mac:

```sh
brew install tailscale          # or the Tailscale.app from tailscale.com
sudo tailscale up
tailscale ip -4                 # note this address, e.g. 100.92.1.5  (the "Mac Tailscale IP")
```

You will run `tailscale up` on each VM later (the bootstrap script does it). All
machines on the same account share a private network and can reach each other by
their `100.x.y.z` addresses.

## B. PostgreSQL on the Mac, data on the SSD

```sh
brew install postgresql@16

# initialize a cluster with its data directory on the SSD
initdb -D /Volumes/MainSSD/CRAWLER/pgdata

# let it listen on all interfaces (we restrict access in pg_hba below)
echo "listen_addresses = '*'" >> /Volumes/MainSSD/CRAWLER/pgdata/postgresql.conf

# allow only the Tailscale subnet (100.64.0.0/10) with password auth
cat >> /Volumes/MainSSD/CRAWLER/pgdata/pg_hba.conf <<'EOF'
host  crawler  crawler  100.64.0.0/10  scram-sha-256
EOF

# start it
pg_ctl -D /Volumes/MainSSD/CRAWLER/pgdata -l /Volumes/MainSSD/CRAWLER/pg.log start

# create the role and database (you will be prompted for a password)
createuser -P crawler           # pick a strong password, remember it
createdb -O crawler crawler
```

Your connection string (DSN), used everywhere below, is:

```
postgresql+asyncpg://crawler:YOURPASSWORD@<Mac Tailscale IP>:5432/crawler
```

macOS may prompt to allow incoming connections to Postgres the first time a VM
connects; allow it. Access is still restricted to your Tailscale network by
`pg_hba.conf`, so this is not exposed to the public internet.

### Create the tables

From the Mac (or anywhere on the tailnet):

```sh
cd ~/Developer/web-crawler
DSN="postgresql+asyncpg://crawler:YOURPASSWORD@<Mac Tailscale IP>:5432/crawler"
.venv/bin/python -m crawler init-db --db "$DSN"
```

## C. (Optional) run a node on the Mac too

The Mac can be both the database host and a worker using its home IP:

```sh
CRAWLER_DB="$DSN" CRAWLER_KEEP_ALIVE=true caffeinate -i ./run.sh
```

Environment variables override `crawler.toml`, so this reuses your tuned board
config but points the database at Postgres.

## D. Oracle VMs

### Provision

In the Oracle console, create instances (Always Free):

- **2x VM.Standard.E2.1.Micro** (x86, reliably available). Weak but fine, a crawler
  node is network bound. Each is a separate IP.
- **VM.Standard.A1.Flex** (ARM, up to 4 cores / 24GB) if you can get capacity. Often
  "out of capacity"; retry across availability domains, or skip it.

Use **Ubuntu 22.04 or 24.04**. You do not need to open any inbound ports: Tailscale
makes outbound connections only, and crawling is outbound. Add your SSH key at
creation and SSH in.

### Bootstrap each VM

Copy `scripts/bootstrap-node.sh` to the VM (or clone the repo and run it), set the
two variables, and run it. It installs dependencies, joins Tailscale, clones the
code, and builds the venv:

```sh
export REPO_URL="git@github.com:YOURNAME/WebHarvester.git"   # or https url
export PG_DSN="postgresql+asyncpg://crawler:YOURPASSWORD@<Mac Tailscale IP>:5432/crawler"
bash bootstrap-node.sh
```

During the run, Tailscale prints an auth URL; open it to add the VM to your tailnet.

### Start crawling on the VM

```sh
cd ~/webharvester
CRAWLER_DB="$PG_DSN" CRAWLER_BASE_DIR="$HOME/crawldata" CRAWLER_KEEP_ALIVE=true \
  tmux new -s crawler -d './run.sh'
```

`--keep-alive` (set here via env) makes the node wait for peers to enqueue work
rather than exiting when its local view of the queue is momentarily empty. Repeat on
every VM. They will all claim from the same frontier and never double-crawl.

## E. Watch the whole mesh

From any machine on the tailnet:

```sh
.venv/bin/python -m crawler status --db "$DSN" --watch
```

The `nodes` section shows every machine by hostname with its page count and rate, so
you can see each VM and the Mac pulling their weight. Top domains and recent errors
are aggregated across the mesh.

## Notes and gotchas

- **Throughput.** Each node still throttles per IP, so leave `throttle = true` and a
  low pinned concurrency per node. Three nodes at ~100 pages/min each is ~300/min
  total, which is roughly 3x a single machine. More VMs, more IPs, more throughput.
- **Bandwidth.** VMs fetch pages with Oracle's egress (10TB/month free, far more than
  you will use). They send only extracted text home, a few KB per page, so your home
  upload is barely touched.
- **Frontier latency.** Frontier reads and writes now cross the internet over
  Tailscale. At these rates it is fine; the big corpus writes are batched and off the
  hot path anyway.
- **Security.** Postgres is reachable only on the Tailscale network and only by the
  `crawler` role with a password. Do not set `listen_addresses` without the matching
  `pg_hba` restriction, and never port-forward 5432 on your router.
- **Keep the Mac awake.** It is the database. Run on AC with `caffeinate -i`, lid open
  or clamshell-on-power. If the Mac sleeps, nodes pause and resume when it wakes.
- **Cost.** Always Free resources do not incur charges. Watch out for accidentally
  creating non-free shapes or exceeding the free block storage (about 200GB total),
  though a worker node needs almost no disk.
```
