#!/usr/bin/env python3
"""WARBOARD ingest + enrichment daemon.

Four daemon threads, each with its own sqlite connection (WAL lets the server
read concurrently):

  fetch    feeds.fetch_all every FETCH_INTERVAL (300s), stamps meta.last_fetch_ts
  enrich   serial queue: pending_items -> enrich_item -> mark_enriched ->
           assign_cluster -> metrics/meta; labels clusters once they reach 3 items
  device   Tiiny.device_stats every 30s -> npu/cpu/mem metrics + queue_depth,
           latest snapshot cached in meta.device_last for the server
  janitor  db.prune hourly

Every loop iteration is wrapped: an exception logs one line and backs off, never
kills the thread. SIGTERM/SIGINT = clean exit.

Env: TIINY_HOST, TIINY_KEY, WARBOARD_DB (default ./warboard.db).
Run `python3 pipeline.py --once` for a single pass of every loop (integrator check).
"""

import json
import os
import signal
import sqlite3
import sys
import threading
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db  # noqa: E402
import enrich  # noqa: E402
import feeds  # noqa: E402

DB_PATH = os.environ.get("WARBOARD_DB") or os.path.join(BASE_DIR, "warboard.db")

FETCH_INTERVAL = float(os.environ.get("WARBOARD_FETCH_INTERVAL", "300"))
DEVICE_INTERVAL = 30.0
JANITOR_INTERVAL = 3600.0

ENRICH_BATCH = 20
IDLE_SLEEP = 2.0           # queue empty
CLUSTER_REFRESH_S = 60.0   # how often to re-read cluster labels/counts
CLUSTER_WINDOW_S = 72 * 3600.0
LABEL_MIN_ITEMS = 3
LABEL_PER_PASS = 3

STOP = threading.Event()

_LOG_LOCK = threading.Lock()


def log(msg):
    line = "%s %s\n" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), msg)
    with _LOG_LOCK:
        try:
            sys.stdout.write(line)
            sys.stdout.flush()
        except Exception:
            pass


class DeviceDown(Exception):
    """Tiiny is unreachable; back off instead of burning the pending queue."""


# --------------------------------------------------------------------------- #
# Tiiny handle (lazy: a missing TIINY_KEY must not stop the fetcher from running)
# --------------------------------------------------------------------------- #

_TIINY = None
_TIINY_LOCK = threading.Lock()


def tiiny():
    global _TIINY
    with _TIINY_LOCK:
        if _TIINY is None:
            _TIINY = enrich.Tiiny()
        return _TIINY


# --------------------------------------------------------------------------- #
# meta helpers
# --------------------------------------------------------------------------- #

_META_LOCK = threading.Lock()
_META_CACHE = {}


def _meta_float(con, key, default=0.0):
    try:
        v = db.get_meta(con, key)
    except Exception:
        return default
    if v in (None, ""):
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _bump_meta(con, key, amount):
    """Accumulate a running total in meta. Only the enricher thread writes these."""
    with _META_LOCK:
        if key not in _META_CACHE:
            _META_CACHE[key] = _meta_float(con, key, 0.0)
        _META_CACHE[key] += float(amount)
        val = _META_CACHE[key]
    db.set_meta(con, key, "%d" % int(val))


def _set_embeddings(con, on):
    want = "on" if on else "off"
    with _META_LOCK:
        if _META_CACHE.get("embeddings") == want:
            return
        _META_CACHE["embeddings"] = want
    try:
        db.set_meta(con, "embeddings", want)
        log("[meta] embeddings=%s" % want)
    except Exception as exc:
        log("[meta] embeddings write failed: %s" % exc)


# --------------------------------------------------------------------------- #
# generic supervised loop
# --------------------------------------------------------------------------- #

def _spawn(name, body, interval):
    """Run body(con) forever. body may return a float to override the next sleep."""

    def run():
        con = None
        fails = 0
        dev_fails = 0
        while not STOP.is_set():
            delay = interval
            try:
                if con is None:
                    con = db.connect(DB_PATH)
                    # long busy timeout: the server holds read connections on the same file
                    try:
                        con.execute("PRAGMA busy_timeout=15000")
                    except Exception:
                        pass
                hint = body(con)
                fails = 0
                dev_fails = 0
                if hint is not None:
                    delay = float(hint)
            except DeviceDown as exc:
                dev_fails += 1
                delay = min(15.0 * (2 ** min(dev_fails, 4)), 300.0)
                log("[%s] tiiny unreachable (%s); retry in %.0fs" % (name, exc, delay))
            except sqlite3.Error as exc:
                fails += 1
                delay = min(max(interval, 5.0) * (2 ** min(fails, 5)), 600.0)
                log("[%s] db error %s: %s; reconnect, retry in %.0fs"
                    % (name, type(exc).__name__, exc, delay))
                try:
                    con.close()
                except Exception:
                    pass
                con = None
            except Exception as exc:
                fails += 1
                delay = min(max(interval, 5.0) * (2 ** min(fails, 5)), 600.0)
                log("[%s] error %s: %s; retry in %.0fs"
                    % (name, type(exc).__name__, exc, delay))
            if STOP.wait(max(0.5, delay)):
                break
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
        log("[%s] stopped" % name)

    return threading.Thread(target=run, name=name, daemon=True)


# --------------------------------------------------------------------------- #
# 1. fetcher
# --------------------------------------------------------------------------- #

def fetch_body(con):
    t0 = time.time()
    res = feeds.fetch_all(con) or {}
    new = res.get("new", 0)
    checked = res.get("checked", 0)
    errors = res.get("errors") or {}
    ts = time.time()
    db.set_meta(con, "last_fetch_ts", "%.3f" % ts)
    detail = ""
    if errors:
        detail = " failed=" + ",".join(sorted(errors)[:6])
    log("[fetch] new=%s checked=%s errors=%d%s in %.1fs"
        % (new, checked, len(errors), detail, ts - t0))
    return None


# --------------------------------------------------------------------------- #
# 2. enricher (serial — the device does one inference at a time)
# --------------------------------------------------------------------------- #

_cluster_labels = {}   # cluster_id -> label (enricher thread only)
_last_cluster_refresh = 0.0


def _refresh_cluster_state(con):
    """Cache existing labels so upsert_cluster never writes a NULL over one."""
    global _last_cluster_refresh
    cutoff = time.time() - CLUSTER_WINDOW_S
    rows = con.execute(
        "SELECT id, label FROM clusters WHERE COALESCE(updated_at,0) >= ? "
        "ORDER BY updated_at DESC LIMIT 500", (cutoff,)).fetchall()
    for r in rows:
        _cluster_labels[r["id"]] = r["label"]
    _last_cluster_refresh = time.time()


def _reconcile_counts(con):
    """Recompute item_count/top_severity for recently touched clusters.

    Self-healing: independent of whatever assign_cluster/upsert_cluster already
    did, so counts can never drift or double-count. Labels are untouched.
    """
    cutoff = time.time() - CLUSTER_WINDOW_S
    with con:
        con.execute(
            "UPDATE clusters SET"
            "  item_count = (SELECT COUNT(*) FROM items WHERE items.cluster_id = clusters.id),"
            "  top_severity = COALESCE((SELECT MAX(severity) FROM items"
            "                           WHERE items.cluster_id = clusters.id), 1)"
            " WHERE id IN (SELECT DISTINCT cluster_id FROM items"
            "              WHERE cluster_id IS NOT NULL AND COALESCE(enriched_at,0) >= ?)",
            (cutoff,))


def _cluster_titles(con, cluster_id, limit=6):
    rows = con.execute(
        "SELECT title FROM items WHERE cluster_id = ? "
        "ORDER BY COALESCE(published, fetched_at) DESC LIMIT ?",
        (cluster_id, limit)).fetchall()
    return [r["title"] for r in rows if r["title"]]


def _label_clusters(con):
    rows = con.execute(
        "SELECT id, top_severity FROM clusters "
        "WHERE (label IS NULL OR label = '') AND item_count >= ? "
        "ORDER BY updated_at DESC LIMIT ?",
        (LABEL_MIN_ITEMS, LABEL_PER_PASS)).fetchall()
    for r in rows:
        if STOP.is_set():
            return
        cid = r["id"]
        titles = _cluster_titles(con, cid)
        if len(titles) < 2:
            continue
        # same NPU handshake as enrichment: labeling is a chat call too, and a
        # label fired mid-image-generation kills the image with device 150004
        while not STOP.is_set():
            try:
                hold = float(db.get_meta(con, "img_hold_until", "0") or 0)
            except (TypeError, ValueError):
                hold = 0
            if time.time() >= hold:
                break
            time.sleep(1.5)
        try:
            db.set_meta(con, "enrich_busy_until", "%.3f" % (time.time() + 60))
        except Exception:
            pass
        try:
            label = enrich.label_cluster(tiiny(), titles)
        except Exception as exc:
            log("[cluster] label %s failed: %s" % (cid, exc))
            return
        finally:
            try:
                db.set_meta(con, "enrich_busy_until", "0")
            except Exception:
                pass
        if not label or not str(label).strip():
            continue
        label = " ".join(str(label).split())[:120]
        _cluster_labels[cid] = label
        try:
            db.upsert_cluster(con, cid, label, r["top_severity"] or 1, time.time())
            log("[cluster] %s labelled: %s" % (cid, label))
        except Exception as exc:
            log("[cluster] label write %s failed: %s" % (cid, exc))


# Failure strings that mean "the call never got a usable answer out of the box",
# not "this article confused the model". These must never park an item.
_TRANSPORT_MARKERS = (
    "HTTPError", "URLError", "timed out", "TimeoutError", "socket.timeout",
    "ConnectionError", "ConnectionResetError", "ConnectionRefused",
    "RemoteDisconnected", "IncompleteRead", "BadStatusLine", "OSError",
    "RuntimeError",  # TIINY_KEY unset
)

# A model that is stopped/erroring/still downloading cannot serve chat.
_BAD_MODEL_STATUS = ("stop", "error", "fail", "download", "unload", "pending", "crash")


def _transport_error(err):
    e = str(err or "")
    return any(m in e for m in _TRANSPORT_MARKERS)


def _device_ready():
    """True only if the device answers AND the chat model is loaded on it.

    `device_stats()` alone only proves the management API is alive: with the chat
    model evicted, /api/v1/npu/status still returns 200 while every enrichment
    fails, which would burn the whole pending queue into enrich_error.
    """
    try:
        st = tiiny().device_stats()
    except Exception:
        return False
    if not st:
        return False
    models = st.get("models")
    if not models:
        # models endpoint gave us nothing; we cannot prove the model is gone, and
        # the failure was not transport-level, so fall back to the old behaviour.
        # db.mark_error's retry budget is the backstop.
        return True
    for m in models:
        if str((m or {}).get("model_id") or "").strip() == enrich.CHAT_MODEL:
            status = str((m or {}).get("status") or "").strip().lower()
            return not any(bad in status for bad in _BAD_MODEL_STATUS)
    return False


def _enrich_one(con, row):
    item_id = row["id"]
    t0 = time.time()
    err = ""
    stats_out = {}
    res = None
    try:
        res = enrich.enrich_item(tiiny(), row, stats_out=stats_out)
    except Exception as exc:
        err = "%s: %s" % (type(exc).__name__, exc)
    if not err:
        # The real reason lives in the chat stats -- enrich_item returns None, it
        # does not raise, so without this every parked row reads "empty result".
        err = str(stats_out.get("error") or "").strip() or "empty result"

    if not res:
        # Tell an outage apart from one bad article: a bad article gets parked,
        # an outage must NOT burn the whole pending queue.
        if _transport_error(err) or not _device_ready():
            raise DeviceDown(err)
        db.mark_error(con, item_id, err[:300])
        log("[enrich] #%s FAILED %s" % (item_id, err[:160]))
        return False

    stats = res.get("stats") or {}
    emb = res.get("embedding")
    try:
        sev = int(res.get("severity") or 1)
    except (TypeError, ValueError):
        sev = 1
    sev = max(1, min(5, sev))
    countries = res.get("countries")
    if not isinstance(countries, list):
        countries = [] if countries in (None, "") else [str(countries)]

    db.mark_enriched(con, item_id, res.get("summary"), res.get("category"),
                     res.get("region"), sev, json.dumps(countries), emb)

    ts = time.time()
    ms = _num(stats.get("ms"), (ts - t0) * 1000.0)
    tps = _num(stats.get("gen_tps"), 0.0)
    tok = _num(stats.get("tokens_out"), 0.0)
    try:
        db.record_metric(con, "enrich_ms", ms, ts)
        if tps > 0:
            db.record_metric(con, "gen_tps", tps, ts)
        if tok > 0:
            db.record_metric(con, "tokens_out", tok, ts)
    except Exception as exc:
        log("[enrich] metric write failed: %s" % exc)

    _bump_meta(con, "tokens_total", tok)
    _bump_meta(con, "items_enriched_total", 1)
    _set_embeddings(con, emb is not None)

    cid = None
    try:
        cid = enrich.assign_cluster(con, item_id, emb)
    except Exception as exc:
        log("[cluster] assign failed for #%s: %s" % (item_id, exc))
    if cid:
        try:
            db.set_cluster(con, item_id, cid)
            db.upsert_cluster(con, cid, _cluster_labels.get(cid), sev, ts)
        except Exception as exc:
            log("[cluster] upsert %s failed: %s" % (cid, exc))

    log("[enrich] #%s S%d %s/%s cl=%s %.1ftok/s %.0fms %s"
        % (item_id, sev, res.get("region"), res.get("category"), cid, tps, ms,
           (row["title"] or "")[:70]))
    return True


def _num(v, default):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if f != f or f in (float("inf"), float("-inf")):
        return default
    return f


def enrich_body(con):
    if time.time() - _last_cluster_refresh > CLUSTER_REFRESH_S:
        _refresh_cluster_state(con)

    rows = db.pending_items(con, limit=ENRICH_BATCH)
    if not rows:
        return IDLE_SLEEP

    done = 0
    for row in rows:
        if STOP.is_set():
            break
        # NPU handshake: image generation (server.py) needs a quiet NPU — a
        # concurrent Z-Image job fails with device error 150004 while chat is
        # running. The server takes a short lease in meta.img_hold_until; we
        # yield between items and advertise our own busy window while chatting.
        while not STOP.is_set():
            try:
                hold = float(db.get_meta(con, "img_hold_until", "0") or 0)
            except (TypeError, ValueError):
                hold = 0
            if time.time() >= hold:
                break
            time.sleep(1.5)
        try:
            db.set_meta(con, "enrich_busy_until", "%.3f" % (time.time() + 120))
        except Exception:
            pass
        try:
            if _enrich_one(con, row):
                done += 1
        finally:
            try:
                db.set_meta(con, "enrich_busy_until", "0")
            except Exception:
                pass

    if done:
        _reconcile_counts(con)
        _refresh_cluster_state(con)
        _label_clusters(con)
    return 0.5 if done else 5.0


# --------------------------------------------------------------------------- #
# 3. device poller
# --------------------------------------------------------------------------- #

_embed_probed = False
_device_misses = 0

_DEVICE_METRICS = (
    ("npu_util", "npu_util"),
    ("npu_mem_used_mb", "npu_mem_mb"),
    ("cpu_pct", "cpu_pct"),
    ("mem_pct", "mem_pct"),
)


def device_body(con):
    global _embed_probed, _device_misses
    stats = None
    try:
        stats = tiiny().device_stats()
    except Exception as exc:
        log("[device] stats error %s: %s" % (type(exc).__name__, exc))

    ts = time.time()
    if not stats:
        # every 5th miss only — a week of downtime must not flood journald
        if _device_misses % 5 == 0:
            log("[device] offline (no stats; miss #%d)" % (_device_misses + 1))
        _device_misses += 1
    else:
        if _device_misses:
            log("[device] back online after %d misses" % _device_misses)
        _device_misses = 0

    if isinstance(stats, dict) and stats:
        for src, key in _DEVICE_METRICS:
            v = stats.get(src)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                try:
                    db.record_metric(con, key, float(v), ts)
                except Exception as exc:
                    log("[device] metric %s failed: %s" % (key, exc))
        snap = dict(stats)
        snap["ts"] = ts
        try:
            blob = json.dumps(snap, default=str)
            if len(blob) > 32000:          # keep meta rows sane
                snap.pop("models", None)
                blob = json.dumps(snap, default=str)
            db.set_meta(con, "device_last", blob)
            db.set_meta(con, "device_last_ts", "%.3f" % ts)
        except Exception as exc:
            log("[device] snapshot write failed: %s" % exc)

    try:
        pending = db.counts(con).get("pending") or 0
        db.record_metric(con, "queue_depth", float(pending), ts)
    except Exception as exc:
        log("[device] queue_depth failed: %s" % exc)

    if stats and not _embed_probed:
        _embed_probed = True
        vec = None
        try:
            vec = tiiny().embed("warboard embedding availability probe")
        except Exception as exc:
            log("[device] embed probe error: %s" % exc)
        _set_embeddings(con, bool(vec))
    return None


# --------------------------------------------------------------------------- #
# 4. janitor
# --------------------------------------------------------------------------- #

def janitor_body(con):
    t0 = time.time()
    db.prune(con)
    log("[janitor] prune ok in %.1fs" % (time.time() - t0))
    return None


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def _handle_signal(signum, _frame):
    log("signal %s -> shutting down" % signum)
    STOP.set()


def run_once():
    """One pass of every loop, sequential. For integrator verification."""
    con = db.connect(DB_PATH)
    try:
        for name, body in (("fetch", fetch_body), ("device", device_body),
                           ("enrich", enrich_body), ("janitor", janitor_body)):
            try:
                body(con)
            except Exception as exc:
                log("[%s] once failed %s: %s" % (name, type(exc).__name__, exc))
    finally:
        con.close()


def main():
    log("warboard pipeline start db=%s tiiny=%s"
        % (DB_PATH, os.environ.get("TIINY_HOST", "192.168.1.158")))
    if not os.environ.get("TIINY_KEY"):
        log("WARNING TIINY_KEY unset — enrichment will fail until it is set")

    try:
        con = db.connect(DB_PATH)
        db.set_meta(con, "pipeline_started_ts", "%.3f" % time.time())
        con.close()
    except Exception as exc:
        log("startup db init failed %s: %s (threads will retry)"
            % (type(exc).__name__, exc))

    if "--once" in sys.argv[1:]:
        run_once()
        return 0

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _handle_signal)
        except (ValueError, OSError):
            pass

    threads = [
        _spawn("fetch", fetch_body, FETCH_INTERVAL),
        _spawn("enrich", enrich_body, IDLE_SLEEP),
        _spawn("device", device_body, DEVICE_INTERVAL),
        _spawn("janitor", janitor_body, JANITOR_INTERVAL),
    ]
    for th in threads:
        th.start()
    log("threads up: %s" % ", ".join(t.name for t in threads))

    while not STOP.wait(1.0):
        pass

    for th in threads:
        th.join(timeout=10.0)
    # The enricher can be parked in a 20-60s device call when SIGTERM lands. It is a
    # daemon thread so the process still exits, but say so: otherwise journald just
    # shows three of four threads stopping and an operator cannot tell a clean drain
    # from a cut-off inference. (Unit gives us TimeoutStopSec=45 either way.)
    stragglers = [t.name for t in threads if t.is_alive()]
    if stragglers:
        log("abandoning mid-flight thread(s): %s (daemon; no write in progress)"
            % ", ".join(stragglers))
    log("warboard pipeline stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
