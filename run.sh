#!/usr/bin/env bash
# set-and-forget supervisor. runs the crawl and auto-resumes if it crashes
# (e.g. an external-SSD SIGBUS). stops cleanly on normal finish or ctrl-c.
#
# usage: ./run.sh                 # uses crawler.toml
#        ./run.sh --some-flag     # extra flags passed through
set -u
cd "$(dirname "$0")"

# if a mesh.env exists, load it. that is how a node joins the shared postgres
# frontier: same mesh.env on every machine (mac, dell, oracle vm) -> one crawl.
# it sets CRAWLER_DB (the postgres dsn), CRAWLER_KEEP_ALIVE, CRAWLER_BASE_DIR.
# env vars override crawler.toml, so the boards/throttle/depth tuning is reused.
if [ -f mesh.env ]; then
  echo "[supervisor] loading mesh.env (joining the shared frontier)"
  set -a; . ./mesh.env; set +a
fi

PY=.venv/bin/python
ARGS=(-m crawler crawl --config crawler.toml "$@")

attempt=0
while true; do
  attempt=$((attempt + 1))
  echo "[supervisor] start attempt #$attempt at $(date '+%H:%M:%S')"
  "$PY" "${ARGS[@]}"
  code=$?

  if [ $code -eq 0 ]; then
    echo "[supervisor] crawl finished cleanly (frontier exhausted). done."
    break
  fi
  if [ $code -eq 130 ]; then
    echo "[supervisor] stopped by ctrl-c. done."
    break
  fi
  # any other exit (crash, SIGBUS=135, killed, etc) -> resume after a short pause
  echo "[supervisor] crawl exited with code $code. resuming in 10s... (ctrl-c to stop)"
  sleep 10
done
