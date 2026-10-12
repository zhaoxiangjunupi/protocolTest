#!/usr/bin/env bash
# One full run: fresh snapshots -> classify -> availability -> latency/cache -> a new report page
# (rpc_report_<date>_<time>.html) compared against the earlier runs in runs/.
#
#   ./run_all.sh
#
# Scheduled, e.g. every 6 hours (crontab -e):
#   0 */6 * * * /home/ubuntu/zxj/protocolTest/run_all.sh >> /home/ubuntu/zxj/protocolTest/runs/cron.log 2>&1
set -euo pipefail
cd "$(dirname "$0")"
BASE=https://rpc.cryptostack.ai

echo "=== run_all $(date -u '+%F %T UTC') ==="
fetch() {   # keep the previous snapshot if the API is unreachable
  if curl -fsS --max-time 60 "$BASE$1" -o "$2.tmp"; then mv "$2.tmp" "$2"
  else rm -f "$2.tmp"; echo "warning: $BASE$1 failed, keeping the old $2"; fi
}
fetch "/api/public/chains?limit=500" chains_all.json
fetch "/api/public/overview" overview.json

python3 classify_chains.py > /dev/null
python3 rpc_bench.py --chains all
python3 latency_cache.py --chains all
python3 build_report.py
