#!/usr/bin/env bash
# bring up the mesh's postgres host. idempotent, safe to run at every login.
# the remote crawler nodes reconnect on their own once postgres is reachable.
export PATH="$(brew --prefix postgresql@16 2>/dev/null)/bin:$PATH"
PGDATA="$HOME/crawlerdata/pgdata"
pg_ctl -D "$PGDATA" status >/dev/null 2>&1 || pg_ctl -D "$PGDATA" -l "$HOME/crawlerdata/pg.log" start
