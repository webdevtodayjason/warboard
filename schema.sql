PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS items(
  id INTEGER PRIMARY KEY,
  url TEXT UNIQUE NOT NULL,
  source TEXT NOT NULL,
  title TEXT NOT NULL,
  published REAL,              -- epoch; feed pubDate parsed via email.utils, else fetched_at
  fetched_at REAL NOT NULL,
  raw_summary TEXT,            -- feed's own description, stripped of HTML
  summary TEXT,                -- Ornith 1-2 sentence summary (NULL until enriched)
  category TEXT,               -- conflict|terrorism|cyber|diplomacy|economy|disaster|health|crime|politics|tech|energy
  region TEXT,                 -- NORTHCOM|SOUTHCOM|EUCOM|CENTCOM|AFRICOM|INDOPACOM|GLOBAL
  severity INTEGER,            -- 1..5 (5 = flash/critical)
  countries TEXT,              -- JSON array of country names
  embedding BLOB,              -- float32 array bytes (struct.pack), NULL if unavailable
  cluster_id INTEGER,
  enriched_at REAL,
  enrich_error TEXT,
  enrich_attempts INTEGER DEFAULT 0,   -- failed enrichment attempts (retry budget)
  enrich_error_at REAL                 -- when the last failure parked the row
);
CREATE INDEX IF NOT EXISTS idx_items_fetched ON items(fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_items_cluster ON items(cluster_id);
CREATE TABLE IF NOT EXISTS clusters(
  id INTEGER PRIMARY KEY,
  label TEXT,                  -- short headline for the developing story
  created_at REAL, updated_at REAL,
  top_severity INTEGER DEFAULT 1,
  item_count INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS metrics(
  ts REAL NOT NULL, key TEXT NOT NULL, value REAL NOT NULL
);  -- keys: npu_util, npu_mem_mb, cpu_pct, mem_pct, gen_tps, enrich_ms, queue_depth, tokens_out
CREATE INDEX IF NOT EXISTS idx_metrics ON metrics(key, ts DESC);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
-- meta keys: embeddings(on|off), last_fetch_ts, pipeline_started_ts, tokens_total, items_enriched_total
