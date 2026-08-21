#!/usr/bin/env python3
"""WARBOARD read-only HTTP API + static host + camera proxy.

stdlib ThreadingHTTPServer on PORT (default 8811). One sqlite connection per
request (WAL, read-only usage) so nothing is shared across handler threads.

Routes:
  GET /            static/index.html
  GET /healthz     {"ok":true,...}
  GET /api/items?region&category&since&limit
  GET /api/clusters
  GET /api/stats
  GET /cam.mjpg    streaming proxy -> CAM_URL/stream   (fast 503 when down)
  GET /cam.jpg     snapshot proxy  -> CAM_URL/snapshot (fast 503 when down)

The Tiiny API key never appears here; the browser only ever talks to /api/*.
`python3 server.py --selfcheck` boots on an ephemeral port, hits every route and
prints the status codes.
"""

import http.client
import json
import os
import platform
import shutil
import signal
import socket
import sys
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db  # noqa: E402

PORT = int(os.environ.get("PORT", "8811"))
# Loopback by default: the board is unauthenticated and the cloudflared tunnel is the
# only intended path in (deploy/TUNNEL.md). Set BIND=0.0.0.0 to expose it on the LAN.
BIND = os.environ.get("BIND", "127.0.0.1")
DB_PATH = os.environ.get("WARBOARD_DB") or os.path.join(BASE_DIR, "warboard.db")
CAM_URL = (os.environ.get("CAM_URL") or "http://127.0.0.1:8812").rstrip("/")
TIINY_BASE = "http://%s:8800" % (os.environ.get("TIINY_HOST") or "192.168.1.158")
TIINY_KEY = os.environ.get("TIINY_KEY", "")
IMAGE_MODEL = os.environ.get("IMAGE_MODEL", "Tongyi-MAI/Z-Image-Turbo")
IMAGES_DIR = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "images")
INDEX_PATH = os.path.join(BASE_DIR, "static", "index.html")

REGIONS = {"NORTHCOM", "SOUTHCOM", "EUCOM", "CENTCOM", "AFRICOM", "INDOPACOM", "GLOBAL"}
CATEGORIES = {"conflict", "terrorism", "cyber", "diplomacy", "economy", "disaster",
              "health", "crime", "politics", "tech", "energy"}

SERIES_KEYS = ("npu_util", "gen_tps", "queue_depth")
METRIC_KEYS = ("npu_util", "npu_mem_mb", "cpu_pct", "mem_pct",
               "gen_tps", "queue_depth", "enrich_ms", "tokens_out")

CAM_CONNECT_TIMEOUT = 2.0    # fast 503 when ustreamer is down
CAM_READ_TIMEOUT = 15.0      # stalled camera drops the stream instead of pinning a thread
CAM_FAIL_COOLDOWN = 3.0      # breaker: skip the connect entirely right after a failure
CAM_MAX_STREAMS = 4
CAM_MAX_SNAPSHOT = 8 * 1024 * 1024
DEVICE_STALE_S = 120.0

_LOG_LOCK = threading.Lock()


def log(msg):
    line = "%s %s\n" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), msg)
    with _LOG_LOCK:
        try:
            sys.stdout.write(line)
            sys.stdout.flush()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

_index_lock = threading.Lock()
_index_cache = {"key": None, "data": b""}


def read_index():
    try:
        st = os.stat(INDEX_PATH)
    except OSError:
        return None
    key = (st.st_mtime_ns, st.st_size)
    with _index_lock:
        if _index_cache["key"] != key:
            try:
                with open(INDEX_PATH, "rb") as fh:
                    _index_cache["data"] = fh.read()
            except OSError:
                return None
            _index_cache["key"] = key
        return _index_cache["data"]


def open_db():
    con = db.connect(DB_PATH)
    try:
        con.execute("PRAGMA busy_timeout=5000")
    except Exception:
        pass
    return con


def rget(row, key, default=None):
    try:
        v = row[key]
    except (IndexError, KeyError):
        return default
    return default if v is None else v


def as_float(v, default=None):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if f != f:
        return default
    return f


def as_int(v, default=None):
    f = as_float(v, None)
    return default if f is None else int(f)


def json_countries(v):
    if not v:
        return []
    try:
        out = json.loads(v)
    except (TypeError, ValueError):
        return []
    if isinstance(out, list):
        return [str(x) for x in out if x is not None][:12]
    return []


_cam_lock = threading.Lock()
_cam_fail_until = 0.0
_cam_slots = threading.BoundedSemaphore(CAM_MAX_STREAMS)


# On-click article imagery via the device's Z-Image-Turbo. One generation at a
# time (the NPU is shared with enrichment); results cached forever on disk so a
# popular story costs one generation. Lock holders release in finally, always.
_IMG_LOCK = threading.Lock()


def image_path(item_id):
    return os.path.join(IMAGES_DIR, "%d.png" % item_id)


def generate_image(item_id, title, summary):
    """Blocking call to the device's native image route. Returns (bytes|None, err|None).

    The native /v1/image/generate returns raw image bytes on success and a small
    JSON {"code","message"} on device errors. seed = item id, so a story's image
    is reproducible. The device fails with code 150004 when the NPU is busy with
    chat — callers must hold the img_hold lease first (see r_item_image)."""
    import urllib.request
    # "headline/news" wording invites the model to paint signage; describe the
    # SCENE instead and ban writing surfaces in the negative prompt
    prompt = (
        "Photorealistic documentary photograph of the scene: %s. %s "
        "Dramatic natural lighting, cinematic composition, shallow depth of "
        "field." % (title, (summary or "")[:300]))
    body = json.dumps({
        "model": IMAGE_MODEL, "prompt": prompt,
        "negative_prompt": "text, letters, words, signage, signs, billboards, posters, newspaper, captions, watermark, subtitles, logos, low quality",
        # ponytail: 512x512 is the ONLY resolution this device generates (bug #039);
        # the drawer crops it to 16:9 with object-fit
        "width": 512, "height": 512, "seed": int(item_id) % 2147483647,
        "steps": 8}).encode()
    req = urllib.request.Request(
        TIINY_BASE + "/v1/image/generate", data=body,
        headers={"Authorization": "Bearer " + TIINY_KEY,
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            ctype = resp.headers.get("Content-Type", "")
            raw = resp.read()
    except Exception as exc:
        return None, str(exc)[:200]
    if "image" in ctype and raw[:1] != b"{":
        return raw, None
    try:
        err = json.loads(raw)
        return None, "device %s: %s" % (err.get("code"), err.get("message"))
    except Exception:
        return None, "unexpected response (%s, %d bytes)" % (ctype, len(raw))


# Host (Orange Pi) stats, read from local /proc//sys — the server runs on the box
# it reports on. Identity is immutable per boot; cpu% needs a previous /proc/stat
# sample, kept in _HOST. Every field degrades to None off-Linux (dev on macOS).
_HOST = {"ident": None, "stat": None, "lock": threading.Lock()}


def _read(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return None


def _host_ident():
    ident = {"model": None, "soc": None, "os": None, "arch": platform.machine(),
             "cores": os.cpu_count(), "mem_total_mb": None, "hostname": socket.gethostname()}
    # HOST_MODEL env wins (marketing name), then device-tree, then ACPI DMI —
    # this board boots ACPI, where DMI reports the CIX reference design name
    ident["model"] = os.environ.get("HOST_MODEL") or None
    if not ident["model"]:
        raw = _read("/proc/device-tree/model")
        if raw:
            ident["model"] = raw.replace("\x00", "").strip()
    if not ident["model"]:
        raw = _read("/sys/class/dmi/id/product_name")
        if raw:
            ident["model"] = raw.strip()
    raw = _read("/proc/device-tree/compatible")
    if raw:
        # e.g. "orangepi,6-plus\0cix,sky1\0" — last entry names the SoC family
        parts = [p for p in raw.split("\x00") if p]
        if parts:
            ident["soc"] = parts[-1].replace(",", " ").upper()
    if not ident["soc"]:
        raw = _read("/sys/class/dmi/id/product_name")
        if raw and "cix" in raw.lower():
            ident["soc"] = raw.strip().upper()
    raw = _read("/etc/os-release")
    if raw:
        for line in raw.splitlines():
            if line.startswith("PRETTY_NAME="):
                ident["os"] = line.split("=", 1)[1].strip().strip('"')
    raw = _read("/proc/meminfo")
    if raw:
        for line in raw.splitlines():
            if line.startswith("MemTotal:"):
                ident["mem_total_mb"] = round(int(line.split()[1]) / 1024)
                break
    return ident


def _host_cpu_pct():
    raw = _read("/proc/stat")
    if not raw:
        return None
    parts = raw.splitlines()[0].split()[1:]
    vals = [int(x) for x in parts[:8]]
    total = sum(vals)
    idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
    with _HOST["lock"]:
        prev = _HOST["stat"]
        _HOST["stat"] = (total, idle)
    if not prev or total <= prev[0]:
        return None
    dt, di = total - prev[0], idle - prev[1]
    return round(100.0 * (dt - di) / dt, 1) if dt > 0 else None


def host_stats():
    with _HOST["lock"]:
        if _HOST["ident"] is None:
            _HOST["ident"] = _host_ident()
        ident = _HOST["ident"]
    out = {"ident": ident, "cpu_pct": _host_cpu_pct(), "mem_pct": None,
           "mem_used_mb": None, "temp_c": None, "load1": None,
           "disk_used_gb": None, "disk_total_gb": None, "uptime_s": None}
    raw = _read("/proc/meminfo")
    if raw:
        mi = {}
        for line in raw.splitlines():
            k = line.split(":")[0]
            if k in ("MemTotal", "MemAvailable"):
                mi[k] = int(line.split()[1])
        if "MemTotal" in mi and "MemAvailable" in mi and mi["MemTotal"]:
            used = mi["MemTotal"] - mi["MemAvailable"]
            out["mem_used_mb"] = round(used / 1024)
            out["mem_pct"] = round(100.0 * used / mi["MemTotal"], 1)
    temps = []
    zones = []
    try:
        for zone in sorted(os.listdir("/sys/class/thermal")):
            if zone.startswith("thermal_zone"):
                raw = _read("/sys/class/thermal/%s/temp" % zone)
                name = (_read("/sys/class/thermal/%s/type" % zone) or zone).strip()
                if raw and raw.strip().lstrip("-").isdigit():
                    t = int(raw.strip()) / 1000.0
                    temps.append(t)
                    zones.append({"name": name[:24], "c": round(t, 1)})
    except OSError:
        pass
    if temps:
        out["temp_c"] = round(max(temps), 1)
    out["temp_zones"] = zones[:12]
    # drive identity
    model = _read("/sys/block/nvme0n1/device/model")
    out["disk_model"] = model.strip() if model else None
    # network: default-route iface, addr, link speed, live rx/tx rates
    out["net"] = None
    try:
        iface = None
        raw = _read("/proc/net/route") or ""
        for line in raw.splitlines()[1:]:
            f = line.split()
            if len(f) > 1 and f[1] == "00000000":
                iface = f[0]
                break
        if iface:
            rx = int(_read("/sys/class/net/%s/statistics/rx_bytes" % iface) or 0)
            tx = int(_read("/sys/class/net/%s/statistics/tx_bytes" % iface) or 0)
            now = time.time()
            with _HOST["lock"]:
                prev = _HOST.get("net")
                _HOST["net"] = (now, rx, tx)
            rate_rx = rate_tx = None
            if prev and now > prev[0]:
                dt = now - prev[0]
                rate_rx = max(0, (rx - prev[1]) / dt)
                rate_tx = max(0, (tx - prev[2]) / dt)
            speed = _read("/sys/class/net/%s/speed" % iface)
            addr = None
            try:
                probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                probe.settimeout(1)
                probe.connect(("8.8.8.8", 53))
                addr = probe.getsockname()[0]
                probe.close()
            except OSError:
                pass
            out["net"] = {
                "iface": iface, "addr": addr,
                "wireless": os.path.isdir("/sys/class/net/%s/wireless" % iface),
                "speed_mb": int(speed.strip()) if speed and speed.strip().lstrip("-").isdigit() else None,
                "rx_bps": round(rate_rx) if rate_rx is not None else None,
                "tx_bps": round(rate_tx) if rate_tx is not None else None,
            }
    except Exception:
        pass
    # archive growth: how big is this thing getting
    grow = {}
    try:
        grow["db_mb"] = round(os.path.getsize(DB_PATH) / 1e6, 1)
        wal = DB_PATH + "-wal"
        if os.path.exists(wal):
            grow["db_mb"] = round(grow["db_mb"] + os.path.getsize(wal) / 1e6, 1)
    except OSError:
        pass
    try:
        n = tot = 0
        with os.scandir(IMAGES_DIR) as it:
            for e in it:
                if e.name.endswith(".png"):
                    n += 1
                    tot += e.stat().st_size
        grow["images_n"] = n
        grow["images_mb"] = round(tot / 1e6, 1)
    except OSError:
        grow["images_n"] = 0
        grow["images_mb"] = 0.0
    out["growth"] = grow
    try:
        out["load1"] = round(os.getloadavg()[0], 2)
    except OSError:
        pass
    try:
        du = shutil.disk_usage("/")
        out["disk_used_gb"] = round((du.total - du.free) / 1e9, 1)
        out["disk_total_gb"] = round(du.total / 1e9, 1)
    except OSError:
        pass
    raw = _read("/proc/uptime")
    if raw:
        out["uptime_s"] = float(raw.split()[0])
    return out


def cam_recently_failed():
    with _cam_lock:
        return time.time() < _cam_fail_until


def cam_note_fail():
    global _cam_fail_until
    with _cam_lock:
        _cam_fail_until = time.time() + CAM_FAIL_COOLDOWN


def cam_note_ok():
    global _cam_fail_until
    with _cam_lock:
        _cam_fail_until = 0.0


def cam_connect(timeout):
    u = urlparse(CAM_URL)
    host = u.hostname or "127.0.0.1"
    port = u.port or (443 if u.scheme == "https" else 80)
    prefix = (u.path or "").rstrip("/")
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    return conn, prefix


# --------------------------------------------------------------------------- #
# handler
# --------------------------------------------------------------------------- #

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "warboard"
    sys_version = ""
    # Idle keep-alive connections each pin a thread; recycle them promptly.
    timeout = 15.0

    # keep journald readable over a week: only failures get a line
    def log_message(self, fmt, *args):
        try:
            code = args[1] if len(args) > 1 else ""
        except Exception:
            code = ""
        if str(code).startswith(("4", "5")):
            log("[http] %s %s" % (self.address_string(), fmt % args))

    def log_error(self, fmt, *args):  # noqa: A003
        log("[http] %s %s" % (self.address_string(), fmt % args))

    # ---- plumbing -------------------------------------------------------- #

    def send_bytes(self, code, body, ctype, headers=None, close=False):
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            if close:
                self.send_header("Connection", "close")
                self.close_connection = True
            self.end_headers()
            if body and self.command != "HEAD":
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, socket.timeout, TimeoutError):
            self.close_connection = True

    def json(self, code, payload):
        body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        self.send_bytes(code, body, "application/json; charset=utf-8")

    # ---- routing --------------------------------------------------------- #

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query or "")
            if path in ("/", "/index.html"):
                return self.r_index()
            if path == "/healthz":
                return self.r_health()
            if path == "/api/items":
                return self.r_items(query)
            if path == "/api/clusters":
                return self.r_clusters(query)
            if path == "/api/stats":
                return self.r_stats(query)
            if path == "/api/item":
                return self.r_item(query)
            if path == "/api/log":
                return self.r_log(query)
            if path == "/api/ask":
                return self.r_ask(query)
            if path == "/api/item/image":
                return self.r_item_image(query)
            if path == "/cam.mjpg":
                return self.r_cam_stream()
            if path == "/cam.jpg":
                return self.r_cam_snapshot()
            if path == "/og.png":
                try:
                    with open(os.path.join(BASE_DIR, "static", "og.png"), "rb") as f:
                        return self.send_bytes(200, f.read(), "image/png")
                except OSError:
                    return self.json(404, {"error": "not found"})
            if path == "/favicon.ico":
                return self.send_bytes(204, b"", "image/x-icon")
            return self.json(404, {"error": "not found"})
        except (BrokenPipeError, ConnectionResetError, socket.timeout, TimeoutError):
            self.close_connection = True
        except Exception:
            log("[http] 500 %s\n%s" % (self.path, traceback.format_exc()))
            try:
                self.json(500, {"error": "internal error"})
            except Exception:
                self.close_connection = True

    def do_HEAD(self):
        # never open an upstream stream for a HEAD probe
        if urlparse(self.path).path.rstrip("/") == "/cam.mjpg":
            return self.send_bytes(200, b"", "multipart/x-mixed-replace")
        self.do_GET()

    def do_POST(self):
        self.json(405, {"error": "method not allowed"})

    do_PUT = do_DELETE = do_PATCH = do_POST

    # ---- routes ---------------------------------------------------------- #

    def r_index(self):
        data = read_index()
        if data is None:
            return self.json(503, {"error": "index.html not built yet"})
        self.send_bytes(200, data, "text/html; charset=utf-8")

    def r_health(self):
        ok = True
        try:
            con = open_db()
            try:
                con.execute("SELECT 1").fetchone()
            finally:
                con.close()
        except Exception:
            ok = False
        self.json(200 if ok else 503, {"ok": ok, "ts": time.time(), "db": DB_PATH})

    def r_items(self, q):
        region = _first(q, "region")
        if region:
            region = region.upper()
        if region not in REGIONS:
            region = None
        category = _first(q, "category")
        if category:
            category = category.lower()
        if category not in CATEGORIES:
            category = None
        since = as_float(_first(q, "since"), None)
        limit = as_int(_first(q, "limit"), 100) or 100
        limit = max(1, min(200, limit))

        con = open_db()
        try:
            rows = db.recent_items(con, region=region, category=category,
                                   since=since, limit=limit)
        finally:
            con.close()

        items = []
        for r in rows:
            items.append({
                "id": rget(r, "id"),
                "url": rget(r, "url", ""),
                "source": rget(r, "source", ""),
                "title": rget(r, "title", ""),
                "published": as_float(rget(r, "published"), None),
                "fetched_at": as_float(rget(r, "fetched_at"), None),
                "summary": rget(r, "summary"),
                "category": rget(r, "category"),
                "region": rget(r, "region"),
                "severity": as_int(rget(r, "severity"), 1),
                "countries": json_countries(rget(r, "countries")),
                "cluster_id": rget(r, "cluster_id"),
            })
        self.json(200, {"items": items})

    def r_clusters(self, q):
        limit = as_int(_first(q, "limit"), 12) or 12
        limit = max(1, min(50, limit))
        con = open_db()
        try:
            rows = db.top_clusters(con, limit=limit)
            out = []
            for r in rows:
                cid = rget(r, "id")
                titles = []
                members = []
                try:
                    trows = con.execute(
                        "SELECT id, title FROM items WHERE cluster_id = ? "
                        "ORDER BY COALESCE(published, fetched_at) DESC LIMIT 3",
                        (cid,)).fetchall()
                    titles = [t["title"] for t in trows if t["title"]]
                    members = [{"id": t["id"], "title": t["title"]}
                               for t in trows if t["title"]]
                except Exception:
                    titles = []
                out.append({
                    "items": members,
                    "id": cid,
                    "label": rget(r, "label"),
                    "item_count": as_int(rget(r, "item_count"), 0),
                    "top_severity": as_int(rget(r, "top_severity"), 1),
                    "updated_at": as_float(rget(r, "updated_at"), None),
                    "titles": titles,
                })
        finally:
            con.close()
        self.json(200, {"clusters": out})

    def r_ask(self, q):
        import urllib.request
        question = (_first(q, "q") or "").strip()
        if not (3 <= len(question) <= 200):
            return self.json(400, {"error": "q must be 3-200 chars"})
        kb_port = os.environ.get("TIINY_KB_PORT", "5003")
        host = os.environ.get("TIINY_HOST") or "192.168.1.158"
        req = urllib.request.Request(
            "http://%s:%s/kb/retrieve" % (host, kb_port),
            data=json.dumps({"question": question}).encode(),
            headers={"Authorization": "Bearer " + TIINY_KEY,
                     "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                data = json.loads(resp.read())
        except Exception as exc:
            log("[ask] kb retrieve failed: %s" % str(exc)[:120])
            return self.json(502, {"error": "vault unreachable"})
        out = []
        for r in (data.get("results") or []):
            if not isinstance(r, dict):
                continue
            text = str(r.get("lossless_restatement") or "")
            # PRIVACY BOUNDARY: the device vault also holds the owner's chat-history
            # summaries and other personal files. The public board may only surface
            # what warboard itself filed.
            if "warboard-" not in text[:80].lower():
                continue
            if len(out) >= 5:
                break
            out.append({"text": text[:700],
                        "topic": str(r.get("topic") or "")[:40],
                        "entities": [str(e)[:40] for e in (r.get("entities") or [])[:6]],
                        "keywords": [str(k)[:40] for k in (r.get("keywords") or [])[:6]]})
        try:
            ec = open_db()
            db.add_event(ec, "VAULT", "archive query: %s" % question[:90])
            ec.close()
        except Exception:
            pass
        self.json(200, {"question": question, "results": out})

    def r_log(self, q):
        limit = as_int(_first(q, "limit"), 50) or 50
        con = open_db()
        try:
            rows = db.recent_events(con, limit=limit)
            now = db.get_meta(con, "now_doing", "") or ""
            imgs = as_int(db.get_meta(con, "images_total"), 0) or 0
        finally:
            con.close()
        self.json(200, {"now": now,
                        "images_total": imgs,
                        "events": [{"ts": as_float(rget(r, "ts"), None),
                                    "kind": rget(r, "kind", ""),
                                    "msg": rget(r, "msg", "")} for r in rows]})

    def r_item(self, q):
        item_id = as_int(_first(q, "id"), None)
        if not item_id:
            return self.json(400, {"error": "id required"})
        con = open_db()
        try:
            row = con.execute("SELECT * FROM items WHERE id = ?",
                              (item_id,)).fetchone()
            if row is None:
                return self.json(404, {"error": "no such item"})
            siblings = []
            cid = rget(row, "cluster_id")
            label = None
            if cid:
                try:
                    crow = con.execute("SELECT label FROM clusters WHERE id = ?",
                                       (cid,)).fetchone()
                    label = rget(crow, "label") if crow else None
                    srows = con.execute(
                        "SELECT id, title, source, severity FROM items "
                        "WHERE cluster_id = ? AND id != ? "
                        "ORDER BY COALESCE(published, fetched_at) DESC LIMIT 6",
                        (cid, item_id)).fetchall()
                    siblings = [{"id": rget(s, "id"), "title": rget(s, "title"),
                                 "source": rget(s, "source"),
                                 "severity": as_int(rget(s, "severity"), 1)}
                                for s in srows]
                except Exception:
                    siblings = []
        finally:
            con.close()
        self.json(200, {
            "id": rget(row, "id"), "url": rget(row, "url", ""),
            "source": rget(row, "source", ""), "title": rget(row, "title", ""),
            "published": as_float(rget(row, "published"), None),
            "fetched_at": as_float(rget(row, "fetched_at"), None),
            "raw_summary": rget(row, "raw_summary"),
            "summary": rget(row, "summary"),
            "category": rget(row, "category"), "region": rget(row, "region"),
            "severity": as_int(rget(row, "severity"), None),
            "countries": json_countries(rget(row, "countries")),
            "cluster_id": cid, "cluster_label": label, "siblings": siblings,
            "image_ready": os.path.exists(image_path(item_id)),
        })

    def r_item_image(self, q):
        item_id = as_int(_first(q, "id"), None)
        if not item_id:
            return self.json(400, {"error": "id required"})
        p = image_path(item_id)
        if os.path.exists(p):
            try:
                with open(p, "rb") as f:
                    data = f.read()
                return self.send_bytes(200, data, "image/png")
            except OSError:
                pass
        con = open_db()
        try:
            row = con.execute(
                "SELECT title, summary, raw_summary FROM items WHERE id = ?",
                (item_id,)).fetchone()
        finally:
            con.close()
        if row is None:
            return self.json(404, {"error": "no such item"})
        if not _IMG_LOCK.acquire(blocking=False):
            # one generation at a time; the client polls back
            return self.json(202, {"status": "busy", "retry_s": 4})
        try:
            # NPU handshake with pipeline.py: take the lease, then wait out any
            # in-flight enrichment (chat + image concurrently => device 150004).
            hold_con = open_db()
            try:
                db.set_meta(hold_con, "img_hold_until", "%.3f" % (time.time() + 180))
                db.set_meta(hold_con, "now_doing",
                            "GENERATING IMAGE #%d — Z-IMAGE-TURBO" % item_id)
                db.add_event(hold_con, "IMAGE",
                             "tasking Z-Image-Turbo for #%d — %s"
                             % (item_id, str(rget(row, "title", ""))[:80]))
                deadline = time.time() + 60
                while time.time() < deadline:
                    busy = as_float(db.get_meta(hold_con, "enrich_busy_until"), 0) or 0
                    if time.time() >= busy:
                        break
                    time.sleep(1.5)
            finally:
                hold_con.close()
            # 150004 = NPU busy on the device; a stray chat call can still slip
            # into our window, so retry into the gaps rather than failing fast
            data = err = None
            for attempt in range(3):
                data, err = generate_image(
                    item_id, rget(row, "title", ""),
                    rget(row, "summary") or rget(row, "raw_summary") or "")
                if data is not None or "150004" not in str(err):
                    break
                log("[image] #%d attempt %d hit busy NPU; retrying" %
                    (item_id, attempt + 1))
                time.sleep(6)
            if data is None:
                log("[image] #%d failed: %s" % (item_id, err))
                try:
                    ec = open_db()
                    db.add_event(ec, "ERROR", "image #%d failed: %s"
                                 % (item_id, str(err)[:100]))
                    ec.close()
                except Exception:
                    pass
                return self.json(502, {"error": "generation failed",
                                       "detail": err})
            try:
                os.makedirs(IMAGES_DIR, exist_ok=True)
                tmp = p + ".tmp"
                with open(tmp, "wb") as f:
                    f.write(data)
                os.replace(tmp, p)
            except OSError as exc:
                log("[image] cache write failed: %s" % exc)
            log("[image] #%d generated (%d bytes)" % (item_id, len(data)))
            try:
                done_con = open_db()
                db.add_event(done_con, "IMAGE",
                             "#%d rendered (%d KB, 512x512, 8 steps)"
                             % (item_id, len(data) // 1024))
                cur = as_int(db.get_meta(done_con, "images_total"), 0) or 0
                db.set_meta(done_con, "images_total", str(cur + 1))
                done_con.close()
            except Exception:
                pass
            return self.send_bytes(200, data, "image/png")
        finally:
            try:
                rel = open_db()
                db.set_meta(rel, "img_hold_until", "0")
                db.set_meta(rel, "now_doing", "")
                rel.close()
            except Exception:
                pass
            _IMG_LOCK.release()

    def r_stats(self, q):
        payload = {"counts": {}, "meta": {}, "device": {}, "series": {}}
        try:
            payload["host"] = host_stats()
        except Exception as exc:
            log("[stats] host stats failed: %s" % exc)
            payload["host"] = {}
        con = open_db()
        try:
            try:
                payload["counts"] = dict(db.counts(con) or {})
            except Exception as exc:
                log("[stats] counts failed: %s" % exc)

            meta = {}
            try:
                meta["embeddings"] = db.get_meta(con, "embeddings", "off") or "off"
                meta["last_fetch_ts"] = as_float(db.get_meta(con, "last_fetch_ts"), None)
                meta["pipeline_started_ts"] = as_float(
                    db.get_meta(con, "pipeline_started_ts"), None)
                meta["tokens_total"] = as_int(db.get_meta(con, "tokens_total"), 0)
                meta["items_enriched_total"] = as_int(
                    db.get_meta(con, "items_enriched_total"), 0)
                meta["vault_digests_total"] = as_int(
                    db.get_meta(con, "vault_digests_total"), 0)
                meta["latest_sitrep"] = str(db.get_meta(con, "latest_sitrep", "") or "")[:2000]
                meta["latest_sitrep_ts"] = as_float(db.get_meta(con, "latest_sitrep_ts"), None)
            except Exception as exc:
                log("[stats] meta failed: %s" % exc)
            payload["meta"] = meta

            metrics = {}
            try:
                metrics = db.latest_metrics(con, list(METRIC_KEYS), window_s=3600) or {}
            except Exception as exc:
                log("[stats] metrics failed: %s" % exc)

            snap = {}
            snap_ts = None
            try:
                raw = db.get_meta(con, "device_last")
                if raw:
                    snap = json.loads(raw)
                    if not isinstance(snap, dict):
                        snap = {}
                snap_ts = as_float(snap.get("ts"), None)
                if snap_ts is None:
                    snap_ts = as_float(db.get_meta(con, "device_last_ts"), None)
            except Exception as exc:
                log("[stats] device snapshot failed: %s" % exc)
        finally:
            con.close()

        def latest(key, fallback=None):
            entry = metrics.get(key) or {}
            v = as_float(entry.get("latest"), None)
            return fallback if v is None else v

        models = snap.get("models")
        if not isinstance(models, list):
            models = []
        payload["device"] = {
            "ts": snap_ts,
            "online": bool(snap_ts is not None and (time.time() - snap_ts) < DEVICE_STALE_S),
            "npu_util": latest("npu_util", as_float(snap.get("npu_util"), None)),
            "npu_mem_mb": latest("npu_mem_mb", as_float(snap.get("npu_mem_used_mb"), None)),
            "npu_mem_used_mb": as_float(snap.get("npu_mem_used_mb"),
                                        latest("npu_mem_mb", None)),
            "npu_mem_total_mb": as_float(snap.get("npu_mem_total_mb"), None),
            "cpu_pct": latest("cpu_pct", as_float(snap.get("cpu_pct"), None)),
            "mem_pct": latest("mem_pct", as_float(snap.get("mem_pct"), None)),
            "npu_used": as_float(snap.get("npu_used"), None),
            "npu_available": as_float(snap.get("npu_available"), None),
            "models": models,
            "gen_tps": latest("gen_tps", None),
            "queue_depth": latest("queue_depth", None),
            "enrich_ms": latest("enrich_ms", None),
        }
        for key in SERIES_KEYS:
            entry = metrics.get(key) or {}
            series = entry.get("series") or []
            payload["series"][key] = series if isinstance(series, list) else []
        self.json(200, payload)

    # ---- camera ---------------------------------------------------------- #

    def r_cam_snapshot(self):
        if cam_recently_failed():
            return self.json(503, {"error": "camera offline"})
        conn = None
        try:
            conn, prefix = cam_connect(CAM_CONNECT_TIMEOUT)
            conn.request("GET", prefix + "/snapshot",
                         headers={"User-Agent": "warboard/1.0"})
            resp = conn.getresponse()
            if resp.status != 200:
                raise OSError("upstream %s" % resp.status)
            data = resp.read(CAM_MAX_SNAPSHOT)
            ctype = resp.getheader("Content-Type") or "image/jpeg"
        except Exception as exc:
            cam_note_fail()
            log("[cam] snapshot unavailable: %s" % exc)
            self._close_quiet(conn)
            return self.json(503, {"error": "camera offline"})
        cam_note_ok()
        self.send_bytes(200, data, ctype)
        self._close_quiet(conn)

    def r_cam_stream(self):
        if cam_recently_failed():
            return self.json(503, {"error": "camera offline"})
        if not _cam_slots.acquire(blocking=False):
            return self.json(503, {"error": "camera busy"})
        conn = None
        try:
            try:
                conn, prefix = cam_connect(CAM_CONNECT_TIMEOUT)
                conn.request("GET", prefix + "/stream",
                             headers={"User-Agent": "warboard/1.0"})
                resp = conn.getresponse()
                if resp.status != 200:
                    raise OSError("upstream %s" % resp.status)
            except Exception as exc:
                cam_note_fail()
                log("[cam] stream unavailable: %s" % exc)
                self._close_quiet(conn)
                return self.json(503, {"error": "camera offline"})

            cam_note_ok()
            ctype = (resp.getheader("Content-Type")
                     or "multipart/x-mixed-replace; boundary=boundarydonotcross")
            # a stalled camera must not pin this thread forever
            try:
                if conn.sock is not None:
                    conn.sock.settimeout(CAM_READ_TIMEOUT)
            except Exception:
                pass

            self.close_connection = True
            try:
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                while True:
                    chunk = resp.read(16384)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                pass  # viewer navigated away
            except (socket.timeout, TimeoutError, OSError, http.client.HTTPException) as exc:
                cam_note_fail()
                log("[cam] stream ended: %s" % exc)
            self._close_quiet(conn)
        finally:
            _cam_slots.release()

    @staticmethod
    def _close_quiet(conn):
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            pass


def _first(q, key, default=None):
    v = q.get(key)
    if not v:
        return default
    v = v[0].strip()
    return v or default


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def selfcheck():
    import urllib.request
    srv = Server(("127.0.0.1", 0), Handler)
    host, port = srv.server_address[0], srv.server_address[1]
    th = threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.2},
                          daemon=True)
    th.start()
    rc = 0
    for path in ("/healthz", "/api/items?limit=5", "/api/clusters", "/api/stats",
                 "/cam.jpg", "/nope"):
        url = "http://%s:%d%s" % (host, port, path)
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                body = r.read()
                print("  %-24s %s %d bytes" % (path, r.status, len(body)))
                if path == "/api/stats":
                    print("    %s" % json.dumps(json.loads(body))[:400])
        except Exception as exc:
            code = getattr(exc, "code", None)
            expected = (path in ("/cam.jpg", "/nope"))
            print("  %-24s %s%s" % (path, code or exc, "" if expected else "  <-- UNEXPECTED"))
            if not expected:
                rc = 1
    srv.shutdown()
    srv.server_close()
    return rc


def main():
    if "--selfcheck" in sys.argv[1:]:
        return selfcheck()

    srv = Server((BIND, PORT), Handler)

    def stop(signum, _frame):
        log("signal %s -> shutting down" % signum)
        threading.Thread(target=srv.shutdown, daemon=True).start()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, stop)
        except (ValueError, OSError):
            pass

    log("warboard server on %s:%d db=%s cam=%s" % (BIND, PORT, DB_PATH, CAM_URL))
    try:
        srv.serve_forever(poll_interval=0.5)
    finally:
        srv.server_close()
        log("warboard server stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
