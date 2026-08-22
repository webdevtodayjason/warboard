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

# Gateway dark => volume is locked. Try the documented login on every base the
# firmware might expose it on (port 80 vhost per the SDK docs, plus direct ports).
if [ "$code" != "200" ] && [ -n "${TIINY_USER:-}" ] && [ -n "${TIINY_PASS:-}" ]; then
  body=$(printf '{"username":"%s","password":"%s"}' "$TIINY_USER" "$TIINY_PASS")
  for attempt in \
      "http://$H/api/v1/auth/login|auth.api.tiiny.local" \
      "http://$H/api/v1/auth/login|" \
      "http://$H:8888/api/v1/auth/login|" \
      "http://$H:9080/api/v1/auth/login|" \
      "http://$H:8800/api/v1/auth/login|" ; do
    url="${attempt%%|*}"; host="${attempt##*|}"
    if [ -n "$host" ]; then
      r=$(curl -s -m10 -w "\n%{http_code}" -X POST -H "Host: $host" \
           -H "Content-Type: application/json" -d "$body" "$url" 2>/dev/null)
    else
      r=$(curl -s -m10 -w "\n%{http_code}" -X POST \
           -H "Content-Type: application/json" -d "$body" "$url" 2>/dev/null)
    fi
    rc="${r##*$'\n'}"
    case "$rc" in
      200|201|204)
        echo "$(date -u +%FT%TZ) auth/login OK via $url (host=${host:-none})" >> /var/log/warboard-wake.log
        sleep 20   # give authd time to open the volume and start compose
        break ;;
    esac
  done
  code=$(gw)
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
