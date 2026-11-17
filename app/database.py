"""SQLite schema and connection management."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import DB_PATH

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS fragments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    station      TEXT    NOT NULL,
    minute_utc   TEXT    NOT NULL,
    fragment_no  INTEGER NOT NULL,
    revision     INTEGER NOT NULL CHECK (revision >= 1),
    start_sample INTEGER NOT NULL CHECK (start_sample >= 0 AND start_sample < 60),
    samples      TEXT    NOT NULL,
    content_hash TEXT    NOT NULL,
    UNIQUE (station, minute_utc, fragment_no)
);

CREATE INDEX IF NOT EXISTS idx_fragments_minute
    ON fragments (station, minute_utc);

CREATE TABLE IF NOT EXISTS publications (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    station          TEXT    NOT NULL,
    minute_utc       TEXT    NOT NULL,
    wave             TEXT    NOT NULL,
    fragment_set_hash TEXT   NOT NULL,
    stale            INTEGER NOT NULL DEFAULT 0 CHECK (stale IN (0, 1)),
    created_at       TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_publications_minute
    ON publications (station, minute_utc);

CREATE TABLE IF NOT EXISTS publication_items (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    publication_id INTEGER NOT NULL REFERENCES publications(id) ON DELETE CASCADE,
    fragment_no    INTEGER NOT NULL,
    revision       INTEGER NOT NULL,
    start_sample   INTEGER NOT NULL,
    samples        TEXT    NOT NULL,
    content_hash   TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pub_items_pub
    ON publication_items (publication_id);

-- ----------------------------------------------------------------------- --
-- Single-station data feed (CDC over publications)
--
-- ``feed_events`` is the append-only change log.  Every business transition
-- that matters to a subscriber is committed here *inside the same
-- transaction* as the fragment/publication change that caused it:
--   * upsert  - a publication becomes the current value of a station-minute
--               (initial publish or replacement after a correction)
--   * withdraw - a previously current publication is no longer current
--                (the fragment set changed and the record went stale)
-- ``seq`` is a global monotonic log offset, so per-station ordering follows
-- the global commit order.  Rows are never updated or deleted.
-- ----------------------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS feed_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    station    TEXT    NOT NULL,
    minute_utc TEXT    NOT NULL,
    event_type TEXT    NOT NULL CHECK (event_type IN ('upsert', 'withdraw')),
    pub_id     INTEGER REFERENCES publications(id),
    created_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_feed_station_seq
    ON feed_events (station, id);

-- A subscription freezes one station's set of non-stale publications at
-- creation time.  ``max_event_id`` marks the high-water mark of the change
-- log at that instant; everything with seq <= max_event_id is represented
-- in the snapshot, everything after it arrives as live events.
CREATE TABLE IF NOT EXISTS subscriptions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    station      TEXT    NOT NULL,
    max_event_id INTEGER NOT NULL,
    created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Immutable starting point of a subscription: one row per fresh publication
-- at the moment the subscription was created.
CREATE TABLE IF NOT EXISTS subscription_snapshot (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    station         TEXT    NOT NULL,
    minute_utc      TEXT    NOT NULL,
    pub_id          INTEGER NOT NULL REFERENCES publications(id),
    UNIQUE (subscription_id, pub_id)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_sub
    ON subscription_snapshot (subscription_id, id);
"""


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    # Writers take the write lock up front so concurrent uploads/publishes
    # serialize instead of failing with SQLITE_BUSY on commit.
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(db_path: Path | None = None) -> None:
    """Create the schema if it does not exist yet (safe on every startup)."""
    path = db_path or DB_PATH
    conn = _connect(path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    """Yield a request-scoped connection; the service layer owns commits."""
    conn = _connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()
