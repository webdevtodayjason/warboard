#!/bin/bash
# warboard-wake.service body: if the Tiiny gateway answers but fewer than 3
# models are running, run the full wake script. Quiet no-op otherwise.
set -u
source /etc/warboard.env
code=$(curl -s -m5 -o /tmp/wake-roster.json -w "%{http_code}" \
  -H "Authorization: Bearer $TIINY_KEY" \
  "http://$TIINY_HOST:8800/api/v1/models/npu/status" 2>/dev/null)
[ "$code" = "200" ] || exit 0
up=$(python3 - <<'PY'
import json
try:
    d = json.load(open("/tmp/wake-roster.json"))
    print(sum(1 for m in d.get("models", []) if m.get("status") == "running"))
except Exception:
    print(0)
PY
)
[ "$up" -ge 3 ] && exit 0
echo "$(date -u) gateway up with $up/3 models — waking" >> /var/log/warboard-wake.log
TIINY_KEY="$TIINY_KEY" bash /opt/warboard/deploy/wake-tiiny.sh "$TIINY_HOST" >> /var/log/warboard-wake.log 2>&1
