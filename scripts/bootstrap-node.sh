#!/usr/bin/env bash
# bootstrap a WebHarvester crawler node on a fresh ubuntu VM (e.g. oracle free tier).
#
# set REPO_URL and PG_DSN, then run:
#   export REPO_URL="git@github.com:YOURNAME/WebHarvester.git"
#   export PG_DSN="postgresql+asyncpg://crawler:PASS@100.x.y.z:5432/crawler"
#   bash bootstrap-node.sh
#
# it installs deps, joins tailscale, clones the code, and builds the venv.
# it does NOT start crawling; the last line prints how to.
set -euo pipefail

: "${REPO_URL:?set REPO_URL to your WebHarvester git url}"
: "${PG_DSN:?set PG_DSN to your postgres connection string}"

echo "[bootstrap] installing system packages..."
sudo apt-get update -y
sudo apt-get install -y python3 python3-venv python3-pip git tmux \
  libxml2-dev libxslt1-dev build-essential

echo "[bootstrap] installing + joining tailscale (an auth URL will be printed)..."
if ! command -v tailscale >/dev/null 2>&1; then
  curl -fsSL https://tailscale.com/install.sh | sh
fi
sudo tailscale up

echo "[bootstrap] cloning code..."
cd "$HOME"
if [ ! -d webharvester ]; then
  git clone "$REPO_URL" webharvester
fi
cd webharvester

echo "[bootstrap] building venv..."
python3 -m venv .venv
.venv/bin/pip install -U pip
.venv/bin/pip install -r requirements.txt

mkdir -p "$HOME/crawldata"

cat <<EOF

[bootstrap] done.

start the node (reuses the repo's crawler.toml, overrides db + dirs via env):

  cd ~/webharvester
  CRAWLER_DB="$PG_DSN" CRAWLER_BASE_DIR="\$HOME/crawldata" CRAWLER_KEEP_ALIVE=true \\
    tmux new -s crawler -d './run.sh'

watch it:    tmux attach -t crawler        (ctrl-b d to detach)
mesh status: .venv/bin/python -m crawler status --db "$PG_DSN" --watch
EOF
