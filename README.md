# WARBOARD

A 24/7 OSINT world-news board, published at **https://warboard.semfreak.dev**.

It pulls world news from free, keyless feeds every five minutes and hands every single
article to a **Tiiny AI Pocket** — an NPU edge device sitting in the rack — which writes the
summary, picks the category, geotags it to a COCOM area of responsibility, scores its
severity 1–5, and produces the embedding used to cluster developing stories. Nothing is
sent to a cloud model. The board renders the result as a Forward-Observer-style wall
display, alongside the Tiiny's live NPU telemetry and a camera pointed at the rack.

The whole backend is **Python 3.11 standard library only** — no pip, no venv, no build
step, no Node. It runs unattended for a week on an Orange Pi 6 Plus.

---

## Architecture

```
                  14 free OSINT feeds   (RSS / Atom / RDF, keyless)
   BBC · Guardian · Al Jazeera · DW · France24 · Kyiv Independent · Times of Israel
   Channel NewsAsia · Ukrinform · UN News · CISA advisories · USGS M4.5+
   ReliefWeb · The Record
                                       │
                                       │ 15 s timeout · a dead feed never stops the loop
                                       ▼
 ┌───────────────────── ORANGE PI 6 PLUS · arm64 · in rack ──────────────────────┐
 │                                                                               │
 │  warboard-pipeline.service                     warboard.service               │
 │  ┌─────────────────────────────┐               ┌─────────────────────────┐    │
 │  │ fetcher        every 300 s  │               │ server.py        :8811  │    │
 │  │ enricher       SERIAL queue │               │  GET /        the board │    │
 │  │ device-poller  every  30 s  │               │  GET /api/items         │    │
 │  │ janitor        every  60 m  │               │  GET /api/clusters      │    │
 │  └──────────────┬──────────────┘               │  GET /api/stats         │    │
 │                 │ writes                       │  GET /cam.mjpg ───┐     │    │
 │                 ▼                              └────────┬──────────┼─────┘    │
 │      ┌──────────────────────────┐   reads (WAL)         │          │          │
 │      │ sqlite   warboard.db     │◀──────────────────────┘          │          │
 │      │ items · clusters         │                                  ▼          │
 │      │ metrics · meta           │              warboard-camera.service        │
 │      └──────────────────────────┘              ustreamer /dev/video0 :8812    │
 │                                                (bound to 127.0.0.1 only)      │
 └──────────────┬────────────────────────────────────────────┬───────────────────┘
                │ LAN · Bearer TIINY_KEY                     │ localhost:8811
                ▼                                            ▼
  ┌─────────────────────────────────┐              ┌────────────────────┐
  │  TIINY AI POCKET   (NPU 100 u)  │              │    cloudflared     │
  │  Ornith-1.0-35B          50 u   │              └─────────┬──────────┘
  │  Qwen3-Embedding-0.6B     1 u   │                        │
  │  ~24 tok/s · does NOT batch     │                        ▼
  │  chat  → summary · category ·   │          https://warboard.semfreak.dev
  │          region · severity      │
  │  embed → developing-story       │
  │          clustering             │
  └─────────────────────────────────┘
```

**The device API key never reaches the browser.** Every Tiiny call is server-side; the
frontend only ever talks to our own `/api/*` on port 8811.

---

## Layout

```
warboard/
  CONTRACT.md          build contract — the pinned interfaces
  schema.sql           sqlite schema (items, clusters, metrics, meta)
  db.py                every DB access in the system
  feeds.py             feed list + fetch/parse/insert
  enrich.py            Tiiny client, enrichment prompt, clustering math
  pipeline.py          daemon: fetcher · enricher · device-poller · janitor
  server.py            daemon: HTTP API + static + camera proxy  (:8811)
  static/index.html    the board — one self-contained file, no CDN, no build
  deploy/
    install.sh              idempotent Pi installer
    warboard.service        systemd: API + UI
    warboard-pipeline.service   systemd: ingest + enrichment
    warboard-camera.service     systemd: ustreamer rack cam
    load-models.sh          load the embedder onto the NPU, verify it
    cloudflared-config.yml  named-tunnel ingress
    TUNNEL.md               operator steps for warboard.semfreak.dev
  README.md            this file
```

---

## Run it locally

One-time, in the shell you'll use:

```bash
export TIINY_HOST=192.168.1.158
export TIINY_KEY='<uuid from the Tiiny: Settings → API key>'
```

Then two commands, from the `warboard/` directory:

```bash
python3 pipeline.py &      # ingest + NPU enrichment, logs to stdout
python3 server.py          # http://localhost:8811
```

`warboard.db` is created next to the scripts on first run. There is nothing to install.

Each module also self-checks on its own:

```bash
python3 feeds.py     # fetch every feed once, print per-source counts
python3 enrich.py    # one live enrichment + one embedding + device stats
```

---

## Deploy to the Pi

```bash
# 1 — copy the tree over
rsync -a --delete ~/code/tiiny/warboard/ root@orangepi:/opt/warboard-src/

# 2 — install: service user, /opt/warboard, /etc/warboard.env, ustreamer, 3 systemd units
ssh root@orangepi 'bash /opt/warboard-src/deploy/install.sh'

# 3 — put the key in, start, and load the embedding model onto the NPU
ssh -t root@orangepi 'nano /etc/warboard.env \
  && systemctl restart warboard warboard-pipeline \
  && bash /opt/warboard/load-models.sh'
```

`install.sh` is idempotent — re-run it after every code change; it re-copies the files,
reinstalls the units, restarts the services, and leaves `/etc/warboard.env` alone.

Publishing it on `warboard.semfreak.dev` is a separate, one-time job: **`deploy/TUNNEL.md`**.

### Where things live on the Pi

| Path | What |
|---|---|
| `/opt/warboard/` | code, owned root, group-writable by `warboard` |
| `/opt/warboard/warboard.db` | **all the data** (+ `-wal`, `-shm` sidecars) |
| `/etc/warboard.env` | `TIINY_KEY` and friends, `0640 root:warboard` |
| `/etc/systemd/system/warboard*.service` | the three units |
| `/etc/cloudflared/config.yml` | tunnel ingress |
| journald | every log line, per unit |

---

## Operating it

```bash
systemctl status warboard warboard-pipeline warboard-camera
journalctl -fu warboard-pipeline          # what the NPU is chewing on right now
journalctl -fu warboard -n 100
curl -s localhost:8811/healthz            # {"ok":true,...}
curl -s localhost:8811/api/stats | python3 -m json.tool | head -40

systemctl restart warboard warboard-pipeline    # after a code change
bash /opt/warboard/load-models.sh               # when the board reads EMBEDDINGS OFF
sqlite3 /opt/warboard/warboard.db 'select count(*) from items'   # if sqlite3 is installed
```

| Symptom | Look at |
|---|---|
| Wire is empty | `journalctl -u warboard-pipeline` — feed errors, or `TIINY_KEY` unset |
| Items arrive but never get summaries | Tiiny unreachable or model stopped: `curl -H "Authorization: Bearer $TIINY_KEY" http://$TIINY_HOST:8800/api/v1/models/running` |
| `EMBEDDINGS OFF` in the bottom bar | embedder not loaded → `bash /opt/warboard/load-models.sh` |
| `CAM OFFLINE` | `systemctl status warboard-camera`, `curl -I 127.0.0.1:8812/snapshot` |
| Board reachable locally, 1033 publicly | cloudflared — see `deploy/TUNNEL.md` |

---

## Environment

| Variable | Default | Notes |
|---|---|---|
| `TIINY_HOST` | `192.168.1.158` | host only; port is fixed at 8800 |
| `TIINY_KEY` | — | **required**, server-side only, never sent to the browser |
| `WARBOARD_DB` | `./warboard.db` | `/opt/warboard/warboard.db` on the Pi |
| `PORT` | `8811` | server.py listen port |
| `BIND` | `127.0.0.1` | loopback only — the tunnel is the intended path in. Set `0.0.0.0` to also expose the (unauthenticated) board on the LAN |
| `CAM_URL` | `http://127.0.0.1:8812` | ustreamer origin proxied by `/cam.mjpg` |
| `GNEWS_API_KEY` | unset | optional extra source; everything else is keyless |

---

## HTTP API

| Route | Returns |
|---|---|
| `GET /` | the board |
| `GET /healthz` | liveness json |
| `GET /api/items?region&category&since&limit` | enriched articles, newest first (limit ≤ 200) |
| `GET /api/clusters` | developing stories: label, count, top severity, 3 newest titles |
| `GET /api/stats` | counts, meta, live device telemetry, 1 h series for the sparklines |
| `GET /cam.mjpg` · `GET /cam.jpg` | rack camera, proxied from ustreamer |

---

## Constraints worth knowing before you change anything

- **Stdlib only.** No pip, ever. `sqlite3`, `urllib`, `xml.etree`, `json`, `threading`,
  `http.server`, `struct`, `email.utils` cover the whole system.
- **The Tiiny does not batch.** One inference at a time, ~24 tok/s. Enrichment is a
  strictly serial queue; parallelising it makes the device slower, not faster.
- **Ornith quirk.** Reasoning lands in `message.reasoning_content` and *counts against*
  `max_tokens`, so a small budget returns an empty `content`. We use `max_tokens: 800` and
  fall back to scanning `reasoning_content` for the trailing `{...}` block.
- **Embeddings are optional.** If `Qwen3-Embedding-0.6B` will not load, clustering falls
  back to normalized-title Jaccard overlap ≥ 0.55 and `meta.embeddings` is set to `off`,
  which the board surfaces in the bottom bar. The pipeline never stops.
- **Cluster join threshold is 0.80 cosine** (`enrich.CLUSTER_THRESHOLD`) and it is not an
  arbitrary number. Measured on 2026-08-21 against the live embedder over 89 real enriched
  articles (3,916 pairs): every pair at ≥ 0.808 was a genuine same-story match, the next
  one down at 0.774 was junk, and it sat between two genuine matches at 0.724 and 0.720.
  The 0.72–0.78 band is mixed, so lowering the threshold buys two real joins and one
  visibly wrong story on the DEVELOPING panel. Don't move it without re-running that
  measurement over a few hundred live items.
- **Every network call has a timeout and a try/except.** A dead feed, a Tiiny timeout or a
  malformed article gets logged and skipped. This thing runs a week with nobody watching.
