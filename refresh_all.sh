#!/usr/bin/env bash
# Purple Carrot dashboard refresh.
# Runs both v2 (purchase + programmatic) and v3 (city-level trade area) reports,
# then commits and pushes the updated JSONs so Render auto-deploys.
#
# Used by:
#   - Local Mac crontab (every 30 min)
#   - Render cron job (purplecarrot-data-refresh)
#
# Safe to run concurrently: lockfile prevents double-runs.

set -e  # exit on first error
cd "$(dirname "$0")"

LOCKFILE="/tmp/purplecarrot_report.lock"
LOG="refresh_log.txt"

# Lockfile guard
if [ -f "$LOCKFILE" ]; then
  echo "$(date): Skipping - already running" >> "$LOG"
  exit 0
fi
touch "$LOCKFILE"
trap 'rm -f "$LOCKFILE"' EXIT

PYTHON="${PYTHON_BIN:-python3}"

echo "=== PC Refresh started at $(date) ===" >> "$LOG"

# 1. v2 — main purchase + programmatic report
echo "[$(date)] Running purplecarrot_report_v2.py" >> "$LOG"
"$PYTHON" purplecarrot_report_v2.py >> "$LOG" 2>&1 || {
  echo "[$(date)] v2 report FAILED" >> "$LOG"
  exit 1
}

# 2. v3 — city-level trade area aggregator
echo "[$(date)] Running purplecarrot_v3_aggregator.py" >> "$LOG"
"$PYTHON" purplecarrot_v3_aggregator.py >> "$LOG" 2>&1 || {
  echo "[$(date)] v3 aggregator FAILED (non-fatal, dashboard still works on prior data)" >> "$LOG"
}

# 3. Commit + push any updated JSONs
git add dashboard_data_v2.json dashboard_v3_data.json >> "$LOG" 2>&1 || true
if git diff --staged --quiet; then
  echo "[$(date)] No data changes to commit" >> "$LOG"
else
  git commit -m "Auto-refresh dashboard data [scheduled]" >> "$LOG" 2>&1
  git push origin main >> "$LOG" 2>&1
fi

echo "=== PC Refresh completed at $(date) ===" >> "$LOG"
