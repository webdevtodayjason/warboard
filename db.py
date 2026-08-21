"""WARBOARD storage layer. Stdlib only.

One SQLite file, WAL mode: the pipeline holds a writer connection and the server
holds reader connections to the same file at the same time. Writers wrap mutations
in `with con:` so a crash mid-batch never leaves a half-written row.

Prefer one connection per thread (connect() is cheap). Connections are created with
check_same_thread=False so a shared connection also works, but transactions on a
shared connection are global to that connection -- two threads writing through one
connection can commit each other's work.
"""

import json
import os
import sqlite3
import time

SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")

ITEM_RETENTION_S = 21 * 86400
# A parked item is retried a couple of times before it is considered unenrichable:
# a device-side hiccup that slips past the liveness probe must not silently retire a row.
ENRICH_MAX_ATTEMPTS = 3
ENRICH_RETRY_S = 600.0
METRIC_RETENTION_S = 8 * 86400
MAX_SERIES_POINTS = 120
# Cap rows pulled per metric key so a runaway poller can't make the stats endpoint slow.
_METRIC_SCAN_LIMIT = 5000


def _now():
    return time.time()


def connect(path="warboard.db"):
    """Open (creating if needed) the warboard DB with schema applied."""
    if not os.path.exists(SCHEMA_PATH):
        raise RuntimeError("schema.sql missing next to db.py: %s" % SCHEMA_PATH)
    con = sqlite3.connect(path, timeout=30.0, check_same_thread=False)
    con.row_factory = sqlite3.Row
    with open(SCHEMA_PATH, "r", encoding="utf-8") as fh:
        con.executescript(fh.read())
    # WAL is set by schema.sql; NORMAL sync is the right partner for it on flash storage.
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=30000")
    _migrate(con)
    con.commit()
    return con


def _migrate(con):
    """Add columns that CREATE TABLE IF NOT EXISTS cannot add to an existing DB."""
    have = {r["name"] for r in con.execute("PRAGMA table_info(items)").fetchall()}
    for name, decl in (("enrich_attempts", "INTEGER DEFAULT 0"),
                       ("enrich_error_at", "REAL")):
        if name not in have:
            try:
                con.execute("ALTER TABLE items ADD COLUMN %s %s" % (name, decl))
            except sqlite3.OperationalError:
                pass  # another process won the race


# --- items ---------------------------------------------------------------


def insert_item(con, url, source, title, published, raw_summary):
    """Insert a feed item. Returns new row id, or None if the URL is already known."""
    url = (url or "").strip()
    title = (title or "").strip()
    # The URL lands in an href on the board: only http(s) is ever stored, so a
    # javascript:/data: link from an untrusted feed cannot reach the frontend.
    if not url or not title or not url[:8].lower().startswith(("http://", "https://")):
        return None
    fetched_at = _now()
    if published is None:
        published = fetched_at
    with con:
        cur = con.execute(
            "INSERT OR IGNORE INTO items(url, source, title, published, fetched_at, raw_summary)"
            " VALUES(?,?,?,?,?,?)",
            (url, source, title, float(published), fetched_at, raw_summary),
        )
    return cur.lastrowid if cur.rowcount else None


_PENDING_WHERE = (
    "enriched_at IS NULL AND (enrich_error IS NULL OR"
    " (COALESCE(enrich_attempts,0) < ? AND COALESCE(enrich_error_at,0) <= ?))")


def pending_items(con, limit=20):
    """Items waiting on enrichment, oldest first.

    Never-tried rows plus rows parked fewer than ENRICH_MAX_ATTEMPTS times whose
    cooldown has expired -- so a transient device fault does not retire a row forever.
    """
    return con.execute(
        "SELECT * FROM items WHERE " + _PENDING_WHERE +
        " ORDER BY fetched_at ASC, id ASC LIMIT ?",
        (ENRICH_MAX_ATTEMPTS, _now() - ENRICH_RETRY_S, int(limit)),
    ).fetchall()


def mark_enriched(con, item_id, summary, category, region, severity, countries_json,
                  embedding_bytes):
    with con:
        con.execute(
            "UPDATE items SET summary=?, category=?, region=?, severity=?, countries=?,"
            " embedding=?, enriched_at=?, enrich_error=NULL, enrich_attempts=0,"
            " enrich_error_at=NULL WHERE id=?",
            (summary, category, region, severity, countries_json,
             sqlite3.Binary(embedding_bytes) if embedding_bytes else None,
             _now(), int(item_id)),
        )


def mark_error(con, item_id, err):
    """Park an item that could not be enriched.

    Counts the attempt and stamps the time: pending_items picks the row back up
    after ENRICH_RETRY_S until ENRICH_MAX_ATTEMPTS is reached, after which it
    stays parked for good.
    """
    with con:
        con.execute(
            "UPDATE items SET enrich_error=?, enrich_attempts=COALESCE(enrich_attempts,0)+1,"
            " enrich_error_at=? WHERE id=?",
            (str(err)[:500] or "unknown", _now(), int(item_id)),
        )


def recent_items(con, region=None, category=None, since=None, limit=100):
    """Enriched items, newest first (by publish time, falling back to fetch time)."""
    sql = ["SELECT id, url, source, title, published, fetched_at, raw_summary, summary,"
           " category, region, severity, countries, cluster_id, enriched_at"
           " FROM items WHERE enriched_at IS NOT NULL"]
    args = []
    if region:
        sql.append("AND region=?")
        args.append(region)
    if category:
        sql.append("AND category=?")
        args.append(category)
    if since is not None:
        sql.append("AND COALESCE(published, fetched_at) >= ?")
        args.append(float(since))
    sql.append("ORDER BY COALESCE(published, fetched_at) DESC, id DESC LIMIT ?")
    args.append(max(1, min(int(limit), 500)))
    return con.execute(" ".join(sql), args).fetchall()


# --- clusters ------------------------------------------------------------


def set_cluster(con, item_id, cluster_id):
    """Attach an item to a cluster and refresh that cluster's rollup."""
    with con:
        con.execute("UPDATE items SET cluster_id=? WHERE id=?", (cluster_id, int(item_id)))
        if cluster_id is not None:
            _refresh_cluster(con, cluster_id, _now())


def _refresh_cluster(con, cluster_id, ts):
    """Recompute item_count/top_severity from members. Caller holds the transaction."""
    con.execute(
        "UPDATE clusters SET"
        " item_count=(SELECT COUNT(*) FROM items WHERE items.cluster_id=clusters.id),"
        " top_severity=MAX(COALESCE(top_severity,1),"
        "   COALESCE((SELECT MAX(severity) FROM items WHERE items.cluster_id=clusters.id),1)),"
        " updated_at=? WHERE id=?",
        (ts, cluster_id),
    )


def clustered_embeddings(con, since_ts):
    """(item_id, cluster_id, embedding) for recently enriched clustered items, newest first."""
    rows = con.execute(
        "SELECT id, cluster_id, embedding FROM items"
        " WHERE cluster_id IS NOT NULL AND embedding IS NOT NULL AND enriched_at >= ?"
        " ORDER BY enriched_at DESC",
        (float(since_ts),),
    ).fetchall()
    return [(r["id"], r["cluster_id"], bytes(r["embedding"])) for r in rows]


def upsert_cluster(con, cluster_id, label, severity, ts):
    """Create or update a cluster; returns its id. label=None leaves an existing label alone."""
    sev = int(severity) if severity else 1
    ts = float(ts) if ts else _now()
    with con:
        if cluster_id is None:
            cur = con.execute(
                "INSERT INTO clusters(label, created_at, updated_at, top_severity, item_count)"
                " VALUES(?,?,?,?,0)", (label, ts, ts, sev))
            cluster_id = cur.lastrowid
        else:
            if label:
                con.execute("UPDATE clusters SET label=? WHERE id=?", (label, cluster_id))
            con.execute(
                "UPDATE clusters SET top_severity=MAX(COALESCE(top_severity,1),?) WHERE id=?",
                (sev, cluster_id))
        _refresh_cluster(con, cluster_id, ts)
    return cluster_id


def top_clusters(con, limit=12):
    return con.execute(
        "SELECT id, label, created_at, updated_at, top_severity, item_count FROM clusters"
        " WHERE item_count >= 2 ORDER BY updated_at DESC LIMIT ?",
        (max(1, int(limit)),),
    ).fetchall()


# --- metrics & meta ------------------------------------------------------


def record_metric(con, key, value, ts=None):
    with con:
        con.execute("INSERT INTO metrics(ts, key, value) VALUES(?,?,?)",
                    (float(ts if ts is not None else _now()), key, float(value)))


def latest_metrics(con, keys, window_s=3600):
    """{key: {"latest": float|None, "series": [[ts, value], ...]}} downsampled to <=120 points."""
    cutoff = _now() - float(window_s)
    out = {}
    for key in keys:
        rows = con.execute(
            "SELECT ts, value FROM metrics WHERE key=? AND ts>=? ORDER BY ts DESC LIMIT ?",
            (key, cutoff, _METRIC_SCAN_LIMIT),
        ).fetchall()
        if not rows:
            out[key] = {"latest": None, "series": []}
            continue
        latest = float(rows[0]["value"])
        points = [(float(r["ts"]), float(r["value"])) for r in reversed(rows)]
        out[key] = {"latest": latest, "series": _downsample(points, MAX_SERIES_POINTS)}
    return out


def _downsample(points, max_points):
    """Time-bucket average. Input ascending by ts."""
    if len(points) <= max_points:
        return [[ts, v] for ts, v in points]
    t0, t1 = points[0][0], points[-1][0]
    span = t1 - t0
    if span <= 0:
        step = max(1, len(points) // max_points)
        return [[ts, v] for ts, v in points[::step]][:max_points]
    width = span / max_points
    buckets = {}
    for ts, v in points:
        idx = min(max_points - 1, int((ts - t0) / width))
        acc = buckets.get(idx)
        if acc is None:
            buckets[idx] = [ts, v, 1]
        else:
            acc[1] += v
            acc[2] += 1
            acc[0] = ts
    return [[buckets[i][0], buckets[i][1] / buckets[i][2]] for i in sorted(buckets)]


def get_meta(con, key, default=None):
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row is not None else default


def set_meta(con, key, value):
    with con:
        con.execute(
            "INSERT INTO meta(key, value) VALUES(?,?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )


# --- housekeeping --------------------------------------------------------


def prune(con):
    """Drop aged-out items/metrics and clusters left with no members."""
    now = _now()
    with con:
        items = con.execute("DELETE FROM items WHERE fetched_at < ?",
                            (now - ITEM_RETENTION_S,)).rowcount
        metrics = con.execute("DELETE FROM metrics WHERE ts < ?",
                              (now - METRIC_RETENTION_S,)).rowcount
        clusters = con.execute(
            "DELETE FROM clusters WHERE id NOT IN"
            " (SELECT DISTINCT cluster_id FROM items WHERE cluster_id IS NOT NULL)").rowcount
        con.execute(
            "UPDATE clusters SET"
            " item_count=(SELECT COUNT(*) FROM items WHERE items.cluster_id=clusters.id),"
            " top_severity=COALESCE("
            "   (SELECT MAX(severity) FROM items WHERE items.cluster_id=clusters.id),"
            "   COALESCE(top_severity,1))")
    return {"items": items, "metrics": metrics, "clusters": clusters}


def counts(con):
    now = _now()
    day, hour = now - 86400, now - 3600
    q = lambda sql, args: con.execute(sql, args).fetchone()[0]  # noqa: E731
    return {
        "items_total": q("SELECT COUNT(*) FROM items", ()),
        "items_24h": q("SELECT COUNT(*) FROM items WHERE fetched_at > ?", (day,)),
        "enriched_24h": q("SELECT COUNT(*) FROM items WHERE enriched_at > ?", (day,)),
        "pending": q("SELECT COUNT(*) FROM items WHERE " + _PENDING_WHERE,
                     (ENRICH_MAX_ATTEMPTS, now - ENRICH_RETRY_S)),
        "errors_24h": q("SELECT COUNT(*) FROM items WHERE enrich_error IS NOT NULL"
                        " AND fetched_at > ?", (day,)),
        "sources_alive_1h": q("SELECT COUNT(DISTINCT source) FROM items WHERE fetched_at > ?",
                              (hour,)),
    }


if __name__ == "__main__":
    import tempfile

    path = os.path.join(tempfile.mkdtemp(prefix="warboard-"), "smoke.db")
    con = connect(path)
    now = time.time()

    iid = insert_item(con, "https://example.test/a", "SMOKE", "Test headline", now - 60, "desc")
    dup = insert_item(con, "https://example.test/a", "SMOKE", "Test headline", now - 60, "desc")
    assert iid and dup is None, (iid, dup)
    assert len(pending_items(con)) == 1

    mark_enriched(con, iid, "One line summary.", "conflict", "EUCOM", 4,
                  json.dumps(["Ukraine"]), b"\x00\x01\x02\x03")
    assert not pending_items(con)
    row = recent_items(con, region="EUCOM")[0]
    assert row["summary"] == "One line summary." and row["severity"] == 4

    cid = upsert_cluster(con, None, "Test cluster", 4, now)
    set_cluster(con, iid, cid)
    iid2 = insert_item(con, "https://example.test/b", "SMOKE", "Second headline", now, "d2")
    mark_enriched(con, iid2, "s2", "conflict", "EUCOM", 5, "[]", b"\x00\x01\x02\x04")
    set_cluster(con, iid2, cid)
    upsert_cluster(con, cid, None, 5, now)
    cl = top_clusters(con)[0]
    assert cl["item_count"] == 2 and cl["top_severity"] == 5, dict(cl)
    assert len(clustered_embeddings(con, now - 3600)) == 2

    bad = insert_item(con, "https://example.test/c", "SMOKE", "Bad", now, "")
    mark_error(con, bad, "device timeout")
    assert not pending_items(con)

    for i in range(400):
        record_metric(con, "npu_util", i % 100, now - 3600 + i * 9)
    m = latest_metrics(con, ["npu_util", "gen_tps"], 3600)
    assert m["gen_tps"] == {"latest": None, "series": []}
    assert len(m["npu_util"]["series"]) <= MAX_SERIES_POINTS and m["npu_util"]["latest"] is not None

    set_meta(con, "embeddings", "on")
    set_meta(con, "embeddings", "off")
    assert get_meta(con, "embeddings") == "off" and get_meta(con, "nope", "d") == "d"

    reader = connect(path)  # concurrent second connection, as server.py does
    assert reader.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 3

    print("counts:", counts(con))
    print("prune:", prune(con))
    print("db.py self-check OK ->", path)
