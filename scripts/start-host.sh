#!/usr/bin/env bash
# bring the mac's side of the mesh back after a reboot: postgres first, then this
# machine's crawler in tmux. safe to run any time, it skips whatever is already up.
# launchd runs this at login with a bare PATH, hence the export.
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd "$(dirname "$0")/.." || exit 1

echo "[$(date '+%F %T')] start-host"
bash scripts/start-db.sh

if tmux has-session -t crawler 2>/dev/null; then
  echo "crawler already running"
else
  tmux new -d -s crawler 'caffeinate -i ./run.sh'
  echo "crawler started in tmux session 'crawler'"
fi
