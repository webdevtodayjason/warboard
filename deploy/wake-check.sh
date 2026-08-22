#!/bin/bash
# warboard-wake.service body (runs every 3 min via warboard-wake.timer).
#
# The device boots with its encrypted data volume CLOSED. The TiinyOS desktop app
# normally opens it by calling POST /api/v1/auth/login on connect — headless there
# is nobody to do that, so the whole AI stack stays down after every power event.
# This script performs that login itself, then loads the models.
#
# Needs in /etc/warboard.env (0600): TIINY_HOST TIINY_KEY TIINY_USER TIINY_PASS
set -u
source /etc/warboard.env
H="${TIINY_HOST:-}"; [ -n "$H" ] || exit 0

gw() { curl -s -m5 -o /tmp/wake-roster.json -w "%{http_code}" \
        -H "Authorization: Bearer $TIINY_KEY" \
        "http://$H:8800/api/v1/models/npu/status" 2>/dev/null; }

code=$(gw)

# Gateway dark => the encrypted data volume is closed. The desktop app normally
# opens it on connect; headless, nobody does, so we call authd ourselves.
# Learned the hard way: authd listens on 127.0.0.1:6666 ONLY (nginx :80 502s for
# this path), so the call must run ON the device over ssh. Payload is
# {"password": "<master password>"}; success returns an attestation token and
# /dev/mapper/userdata_crypt appears, which starts the compose stack.
if [ "$code" != "200" ] && [ -n "${TIINY_PASS:-}" ]; then
  body=$(P="$TIINY_PASS" python3 -c "import json,os;print(json.dumps({'password':os.environ['P']}))")
  resp=$(printf '%s' "$body" | ssh -i /etc/warboard-tiiny.key -p 3588 -o BatchMode=yes \
      -o StrictHostKeyChecking=no -o ConnectTimeout=8 "tiiny@$H" \
      "curl -s -m15 -X POST -H 'Content-Type: application/json' -d @- http://127.0.0.1:6666/api/v1/account/auth" 2>/dev/null)
  case "$resp" in
    *attestation*)
      echo "$(date -u +%FT%TZ) authd unlock OK — waiting for compose stack" >> /var/log/warboard-wake.log
      for _ in $(seq 1 30); do
        sleep 10
        code=$(gw); [ "$code" = "200" ] && break
      done ;;
    *)
      echo "$(date -u +%FT%TZ) authd unlock failed: ${resp:0:120}" >> /var/log/warboard-wake.log ;;
  esac
fi

[ "$code" = "200" ] || exit 0   # still locked/offline — try again next tick

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

echo "$(date -u +%FT%TZ) gateway up with $up/3 models — waking" >> /var/log/warboard-wake.log
TIINY_KEY="$TIINY_KEY" bash /opt/warboard/deploy/wake-tiiny.sh "$H" >> /var/log/warboard-wake.log 2>&1
